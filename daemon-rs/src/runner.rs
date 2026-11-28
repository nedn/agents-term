use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::attachments::{attachments_label, prompt_with_attachments, remove_attachment_files};
use crate::config::{
    agents_debugging_tips_path, enriched_path, home_dir, resolve_binary, tab_attachments_dir,
    WORK_DONE_SYSTEM_INSTRUCTION,
};
use crate::harnesses::turn::{
    value_as_u64, GenerationSample, SessionUsageBaseline, TurnOutcome, TurnParser, TurnRequest,
    TurnSink,
};
use crate::harnesses::{self, Harness};
use crate::log::{log_error, log_info, log_warn};
use crate::shell::{signal_process_group, PersistentShell, StdinState};
use crate::store::{validate_working_dir, StateStore, RESTART_INTERRUPTED_REASON};
use crate::util::{
    is_task_finished_response, js_str, json_pretty, now_ms, pad_first_prompt_for_task_summary,
    rand_hex, summarize_task_prompt, summarize_tool_input, truncate, TaskSummaryStreamFilter,
    WorkDoneStreamFilter,
};

pub type BroadcastFn = Arc<dyn Fn(Value) + Send + Sync>;

fn model_cmd_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)^/model(?:\s+(.*))?$").unwrap())
}

fn model_reset_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)^(?:default|reset|auto)$").unwrap())
}

/// The tab's image attachment folder, once the app has uploaded something into it.
fn existing_attachments_dir(tab_id: &str) -> Option<String> {
    tab_attachments_dir(tab_id)
        .filter(|dir| dir.is_dir())
        .map(|dir| dir.to_string_lossy().into_owned())
}

fn tab_system_prompt(tab: &Value) -> Option<String> {
    tab.get("systemPrompt")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn effective_system_prompt(tab: &Value) -> String {
    let prompt = match tab_system_prompt(tab) {
        Some(sp) if sp.contains(WORK_DONE_SYSTEM_INSTRUCTION) => sp,
        Some(sp) => format!("{sp}\n\n{WORK_DONE_SYSTEM_INSTRUCTION}"),
        None => WORK_DONE_SYSTEM_INSTRUCTION.to_string(),
    };
    format!(
        "{prompt}\n\nFor help debugging a tab from its on-screen ID, read `{}`.",
        agents_debugging_tips_path().display()
    )
}

struct ActiveTurn {
    assistant_msg_id: String,
    agent: String,
    pid: Option<u32>,
    interrupted: bool,
    interrupt_reason: Option<String>,
    drain_queue_on_exit: bool,
    kill_scheduled: bool,
    signal_on_spawn: Option<libc::c_int>,
    escalation_handle: Option<tokio::task::JoinHandle<()>>,
    stdin_rx: Option<tokio::sync::watch::Receiver<StdinState>>,
}

impl ActiveTurn {
    fn new(assistant_msg_id: String, agent: String) -> Self {
        Self {
            assistant_msg_id,
            agent,
            pid: None,
            interrupted: false,
            interrupt_reason: None,
            drain_queue_on_exit: false,
            kill_scheduled: false,
            signal_on_spawn: None,
            escalation_handle: None,
            stdin_rx: None,
        }
    }

    fn send_signal(&mut self, sig: libc::c_int) {
        if let Some(pid) = self.pid {
            signal_process_group(pid, sig);
        } else {
            self.signal_on_spawn = Some(sig);
        }
    }

    fn cancel_escalation(&mut self) {
        if let Some(h) = self.escalation_handle.take() {
            h.abort();
        }
    }
}

#[derive(Clone)]
struct ShellSessionEntry {
    pid: u32,
    session_dir: PathBuf,
    shell: Arc<tokio::sync::Mutex<PersistentShell>>,
}

pub type OnIdleFn = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone)]
pub struct AgentRunner {
    store: StateStore,
    broadcast: BroadcastFn,
    active: Arc<Mutex<HashMap<String, Arc<Mutex<ActiveTurn>>>>>,
    shell_sessions: Arc<Mutex<HashMap<String, ShellSessionEntry>>>,
    system_prompt_dirty: Arc<Mutex<HashSet<String>>>,
    summary_filters: Arc<Mutex<HashMap<String, TaskSummaryStreamFilter>>>,
    done_filters: Arc<Mutex<HashMap<String, WorkDoneStreamFilter>>>,
    shutting_down: Arc<AtomicBool>,
    on_idle: Arc<Mutex<Option<OnIdleFn>>>,
}

impl AgentRunner {
    pub fn new(store: StateStore, broadcast: BroadcastFn) -> Self {
        Self {
            store,
            broadcast,
            active: Arc::new(Mutex::new(HashMap::new())),
            shell_sessions: Arc::new(Mutex::new(HashMap::new())),
            system_prompt_dirty: Arc::new(Mutex::new(HashSet::new())),
            summary_filters: Arc::new(Mutex::new(HashMap::new())),
            done_filters: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
            on_idle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_on_idle(&self, cb: Arc<dyn Fn() + Send + Sync>) {
        *self.on_idle.lock().unwrap() = Some(cb);
    }

    pub fn active_tab_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.active.lock().unwrap().keys().cloned().collect();
        ids.sort();
        ids
    }

    fn notify_if_idle(&self) {
        if self.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        if !self.active.lock().unwrap().is_empty() {
            return;
        }
        let cb = self.on_idle.lock().unwrap().clone();
        if let Some(cb) = cb {
            cb();
        }
    }

    pub fn is_running(&self, tab_id: &str) -> bool {
        self.active.lock().unwrap().contains_key(tab_id)
    }

    pub fn mark_system_prompt_updated(&self, tab_id: &str) {
        self.system_prompt_dirty
            .lock()
            .unwrap()
            .insert(tab_id.to_string());
    }

    pub async fn send_stdin(
        &self,
        tab_id: &str,
        data: Option<&str>,
        eof: bool,
    ) -> Result<(), &'static str> {
        let mut rx = {
            let active = self.active.lock().unwrap();
            let Some(turn_arc) = active.get(tab_id) else {
                return Err("No running shell command accepts stdin");
            };
            let turn = turn_arc.lock().unwrap();
            let Some(ref rx) = turn.stdin_rx else {
                return Err("No running shell command accepts stdin");
            };
            rx.clone()
        };

        let handle = loop {
            let state = rx.borrow_and_update().clone();
            match state {
                StdinState::Ready(h) => break h,
                StdinState::Closed => return Err("No running shell command accepts stdin"),
                StdinState::Pending => {
                    if tokio::time::timeout(Duration::from_secs(5), rx.changed())
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .is_none()
                    {
                        return Err("No running shell command accepts stdin");
                    }
                }
            }
        };

        handle.write_and_maybe_eof(data, eof).await
    }

    pub fn reset_shell_session(&self, tab_id: &str) {
        let entry = self.shell_sessions.lock().unwrap().remove(tab_id);
        if let Some(entry) = entry {
            if let Ok(mut shell) = entry.shell.try_lock() {
                shell.kill_and_cleanup();
            } else {
                signal_process_group(entry.pid, libc::SIGKILL);
                let _ = fs::remove_dir_all(&entry.session_dir);
            }
        }
    }

    pub fn shutdown_active_turns(&self, reason: &str) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let entries: Vec<(String, Arc<Mutex<ActiveTurn>>)> = {
            let mut map = self.active.lock().unwrap();
            map.drain().collect()
        };
        if !entries.is_empty() {
            log_warn(&format!(
                "Stopping {} active turn(s) due to shutdown ({reason})",
                entries.len()
            ));
            for (tab_id, turn_arc) in entries {
                let mut turn = turn_arc.lock().unwrap();
                turn.cancel_escalation();
                let msg_id = turn.assistant_msg_id.clone();
                let pid = turn.pid;
                log_warn(&format!(
                    "Stopping active turn tab={tab_id} msg={msg_id} agent={} pid={:?}",
                    turn.agent, pid
                ));
                turn.interrupted = true;
                turn.interrupt_reason = Some(RESTART_INTERRUPTED_REASON.to_string());
                turn.send_signal(libc::SIGTERM);
                if let Some(p) = pid {
                    std::thread::sleep(Duration::from_millis(100));
                    signal_process_group(p, libc::SIGKILL);
                }
                // Persist explicit restart interruption reason before daemon exits (#61).
                let mut msgs = self.store.get_messages(&tab_id);
                if let Some(m) = msgs
                    .iter_mut()
                    .find(|m| m.get("id").and_then(Value::as_str) == Some(msg_id.as_str()))
                {
                    if let Some(obj) = m.as_object_mut() {
                        let st = obj.get("status").and_then(Value::as_str).unwrap_or("");
                        if st == "streaming"
                            || (st == "interrupted"
                                && obj.get("error").map(Value::is_null).unwrap_or(true))
                        {
                            obj.insert("status".into(), json!("interrupted"));
                            obj.insert("error".into(), json!(RESTART_INTERRUPTED_REASON));
                            obj.insert("updatedAt".into(), json!(now_ms()));
                            self.store.replace_message(&tab_id, m.clone());
                        }
                    }
                }
                let mut patch = Map::new();
                patch.insert("status".into(), json!("idle"));
                patch.insert("statusDetail".into(), Value::Null);
                let _ = self.store.update_tab(&tab_id, patch);
            }
        }

        // Clean up all persistent shell sessions (both active and idle).
        let shells: Vec<ShellSessionEntry> = {
            let mut map = self.shell_sessions.lock().unwrap();
            map.drain().map(|(_, entry)| entry).collect()
        };
        for entry in shells {
            if let Ok(mut shell) = entry.shell.try_lock() {
                shell.kill_and_cleanup();
            } else {
                signal_process_group(entry.pid, libc::SIGKILL);
                let _ = fs::remove_dir_all(&entry.session_dir);
            }
        }
    }

    pub fn submit_message(
        &self,
        tab: &Value,
        prompt_text: &str,
        override_permission_mode: Option<&str>,
        attachments: &[Value],
    ) -> Value {
        let tab_id = tab.get("id").and_then(Value::as_str).unwrap_or("");
        if self.is_running(tab_id) {
            return self
                .enqueue_message(tab_id, prompt_text, override_permission_mode, attachments)
                .unwrap_or_else(|| json!({ "queued": false }));
        }
        let has_queued = tab
            .get("queuedMessages")
            .and_then(Value::as_array)
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        if has_queued {
            self.enqueue_message(tab_id, prompt_text, override_permission_mode, attachments);
            if let Some(res) = self.drain_and_start_queued(tab_id) {
                return res;
            }
        }
        self.start_turn(tab, prompt_text, override_permission_mode, attachments)
    }

    pub fn enqueue_message(
        &self,
        tab_id: &str,
        prompt_text: &str,
        override_permission_mode: Option<&str>,
        attachments: &[Value],
    ) -> Option<Value> {
        let (updated_tab, queued_item) = self.store.enqueue_message(
            tab_id,
            prompt_text,
            override_permission_mode,
            attachments,
        )?;
        (self.broadcast)(json!({ "type": "tab_updated", "tab": updated_tab }));
        Some(json!({
            "queued": true,
            "queuedMessage": queued_item,
            "tab": updated_tab,
        }))
    }

    pub fn remove_queued_message(&self, tab_id: &str, queued_id: &str) -> Option<Value> {
        // The images of a message that will never be sent are not kept.
        if let Some(tab) = self.store.get_tab(tab_id) {
            let removed = tab
                .get("queuedMessages")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|m| m.get("id").and_then(Value::as_str) == Some(queued_id));
            if let Some(item) = removed {
                remove_attachment_files(tab_id, item.get("attachments"));
            }
        }
        let updated = self.store.remove_queued_message(tab_id, queued_id)?;
        (self.broadcast)(json!({ "type": "tab_updated", "tab": updated }));
        Some(updated)
    }

    fn combine_queued_messages(items: &[Value]) -> (String, Option<String>, Vec<Value>) {
        let attachments: Vec<Value> = items
            .iter()
            .filter_map(|m| m.get("attachments").and_then(Value::as_array))
            .flatten()
            .cloned()
            .collect();
        let texts: Vec<&str> = items
            .iter()
            .filter_map(|m| m.get("text").and_then(Value::as_str).map(str::trim))
            .filter(|s| !s.is_empty())
            .collect();
        let overrides: Vec<&str> = items
            .iter()
            .filter_map(|m| {
                m.get("overridePermissionMode")
                    .and_then(Value::as_str)
                    .map(str::trim)
            })
            .filter(|s| !s.is_empty())
            .collect();
        let combined_override = if overrides.contains(&"full-access") {
            Some("full-access".to_string())
        } else {
            overrides.last().map(|s| (*s).to_string())
        };
        (texts.join("\n\n"), combined_override, attachments)
    }

    pub fn drain_and_start_queued(&self, tab_id: &str) -> Option<Value> {
        let is_shell = self
            .store
            .get_tab(tab_id)?
            .get("agent")
            .and_then(Value::as_str)
            == Some("shell");
        if is_shell {
            loop {
                let (updated_tab, item) = self.store.pop_next_queued_message(tab_id)?;
                let text = item
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("")
                    .to_string();
                let override_mode = item
                    .get("overridePermissionMode")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let attachments = item
                    .get("attachments")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                (self.broadcast)(json!({ "type": "tab_updated", "tab": updated_tab }));
                if text.is_empty() && attachments.is_empty() {
                    continue;
                }
                return Some(self.start_turn(
                    &updated_tab,
                    &text,
                    override_mode.as_deref(),
                    &attachments,
                ));
            }
        }
        let (updated_tab, popped) = self.store.pop_queued_messages(tab_id)?;
        if popped.is_empty() {
            return None;
        }
        let (combined_text, combined_override, attachments) =
            Self::combine_queued_messages(&popped);
        if combined_text.is_empty() && attachments.is_empty() {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": updated_tab }));
            return None;
        }
        Some(self.start_turn(
            &updated_tab,
            &combined_text,
            combined_override.as_deref(),
            &attachments,
        ))
    }

    pub fn interrupt(
        &self,
        tab_id: &str,
        process_queue: bool,
        text: Option<&str>,
        override_permission_mode: Option<&str>,
        attachments: &[Value],
    ) -> bool {
        let text = text.map(str::trim).unwrap_or("");
        if !text.is_empty() || !attachments.is_empty() {
            self.enqueue_message(tab_id, text, override_permission_mode, attachments);
        }

        let turn_arc = self.active.lock().unwrap().get(tab_id).cloned();
        let Some(turn_arc) = turn_arc else {
            if process_queue && self.drain_and_start_queued(tab_id).is_some() {
                return true;
            }
            let mut patch = Map::new();
            patch.insert("status".into(), json!("idle"));
            patch.insert("statusDetail".into(), Value::Null);
            if let Some(idle) = self.store.update_tab(tab_id, patch) {
                (self.broadcast)(json!({ "type": "tab_updated", "tab": idle }));
            }
            return false;
        };

        let has_queue = self
            .store
            .get_tab(tab_id)
            .and_then(|t| {
                t.get("queuedMessages")
                    .and_then(Value::as_array)
                    .map(|a| !a.is_empty())
            })
            .unwrap_or(false);

        let drain_queue;
        {
            let mut turn = turn_arc.lock().unwrap();
            turn.interrupted = true;
            if process_queue && has_queue {
                turn.drain_queue_on_exit = true;
            }
            drain_queue = turn.drain_queue_on_exit;

            log_info(&format!(
                "User interrupt requested for tab={tab_id} msg={} pid={:?} process_queue={drain_queue}",
                turn.assistant_msg_id, turn.pid
            ));

            if !turn.kill_scheduled {
                turn.kill_scheduled = true;
                let is_shell = turn.agent == "shell";
                turn.send_signal(if is_shell {
                    libc::SIGTERM
                } else {
                    libc::SIGINT
                });
                let weak_turn = Arc::downgrade(&turn_arc);
                let tid_log = tab_id.to_string();
                let handle = tokio::spawn(async move {
                    if !is_shell {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                        if let Some(t_arc) = weak_turn.upgrade() {
                            let mut t = t_arc.lock().unwrap();
                            if t.pid.is_some() {
                                log_warn(&format!(
                                    "Escalating interrupt to SIGTERM for tab={tid_log} pid={:?}",
                                    t.pid
                                ));
                                t.send_signal(libc::SIGTERM);
                            }
                        } else {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                    }
                    if let Some(t_arc) = weak_turn.upgrade() {
                        let mut t = t_arc.lock().unwrap();
                        if t.pid.is_some() {
                            log_warn(&format!(
                                "Escalating interrupt to SIGKILL for tab={tid_log} pid={:?}",
                                t.pid
                            ));
                            t.send_signal(libc::SIGKILL);
                        }
                    }
                });
                turn.escalation_handle = Some(handle);
            }
        }

        // Keep tab in "working" (stopping) state until the child process group has actually exited (#52).
        let detail = if drain_queue {
            "Interrupting to run queued messages…"
        } else {
            "Stopping…"
        };
        let mut patch = Map::new();
        patch.insert("status".into(), json!("working"));
        patch.insert("statusDetail".into(), json!(detail));
        if let Some(stopping_tab) = self.store.update_tab(tab_id, patch) {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": stopping_tab }));
        }
        true
    }

    pub fn kill_if_running(&self, tab_id: &str) {
        self.system_prompt_dirty.lock().unwrap().remove(tab_id);
        let turn_arc = self.active.lock().unwrap().remove(tab_id);
        if let Some(turn_arc) = turn_arc {
            let mut turn = turn_arc.lock().unwrap();
            self.summary_filters
                .lock()
                .unwrap()
                .remove(&turn.assistant_msg_id);
            self.done_filters
                .lock()
                .unwrap()
                .remove(&turn.assistant_msg_id);
            turn.interrupted = true;
            turn.cancel_escalation();
            turn.send_signal(libc::SIGKILL);
        }
        self.reset_shell_session(tab_id);
        self.notify_if_idle();
    }

    fn new_message(tab_id: &str, role: &str, text: &str, status: &str, ts: i64) -> Value {
        json!({
            "id": format!("msg_{}", rand_hex(6)),
            "tabId": tab_id,
            "role": role,
            "text": text,
            "thinking": Value::Null,
            "toolCalls": [],
            "segments": [],
            "deniedActions": [],
            "status": status,
            "error": Value::Null,
            "usage": Value::Null,
            "createdAt": ts,
            "updatedAt": ts,
        })
    }

    pub fn start_turn(
        &self,
        tab: &Value,
        prompt_text: &str,
        override_permission_mode: Option<&str>,
        attachments: &[Value],
    ) -> Value {
        let tab_id = tab
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let agent = tab
            .get("agent")
            .and_then(Value::as_str)
            .unwrap_or("shell")
            .to_string();

        let has_prior_task = self.store.get_messages(&tab_id).iter().any(|m| {
            let has_text = m
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .map(|t| !t.is_empty() && !model_cmd_re().is_match(t))
                .unwrap_or(false);
            let has_images = m
                .get("attachments")
                .and_then(Value::as_array)
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            m.get("role").and_then(Value::as_str) == Some("user") && (has_text || has_images)
        });
        let is_first_task_prompt = !has_prior_task;

        let effective_mode = override_permission_mode
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or_else(|| tab.get("permissionMode").and_then(Value::as_str))
            .unwrap_or("full-access")
            .to_string();

        let now = now_ms();
        let mut user_message = Self::new_message(&tab_id, "user", prompt_text, "complete", now);
        if !attachments.is_empty() {
            user_message["attachments"] = Value::Array(attachments.to_vec());
        }
        let mut assistant_message =
            Self::new_message(&tab_id, "assistant", "", "streaming", now + 1);
        if agent != "shell" {
            assistant_message["model"] = tab
                .get("model")
                .filter(|v| v.is_string())
                .cloned()
                .unwrap_or_else(|| {
                    crate::models::get_cached_agent_models(&agent)["defaultModel"].clone()
                });
            assistant_message["permissionMode"] = json!(effective_mode);
        }
        self.store.add_message(&tab_id, user_message.clone());
        self.store.add_message(&tab_id, assistant_message.clone());

        if agent != "shell" {
            if let Some(caps) = model_cmd_re().captures(prompt_text.trim()) {
                let arg = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
                return self.handle_model_command(tab, user_message, assistant_message, arg);
            }
        }

        let mut working_patch = Map::new();
        working_patch.insert("status".into(), json!("working"));
        working_patch.insert("statusDetail".into(), json!(format!("Starting {agent}…")));
        let preview = if prompt_text.trim().is_empty() && !attachments.is_empty() {
            attachments_label(attachments.len())
        } else {
            truncate(prompt_text, 120)
        };
        working_patch.insert("lastMessagePreview".into(), json!(preview));
        working_patch.insert("unread".into(), json!(false));
        let has_summary = tab
            .get("taskSummary")
            .and_then(Value::as_str)
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if !has_summary {
            if let Some(fallback) = summarize_task_prompt(prompt_text, 160) {
                working_patch.insert("taskSummary".into(), json!(fallback));
            }
        }
        let working_tab = self.store.update_tab(&tab_id, working_patch);
        (self.broadcast)(
            json!({ "type": "message_created", "tabId": tab_id, "message": user_message }),
        );
        (self.broadcast)(
            json!({ "type": "message_created", "tabId": tab_id, "message": assistant_message }),
        );
        if let Some(wt) = working_tab {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": wt }));
        }

        let raw_cwd = tab
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| home_dir().to_string_lossy().to_string());

        let msg_id = assistant_message
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let msg_arc = Arc::new(Mutex::new(assistant_message));

        let run_cwd = match validate_working_dir(&raw_cwd) {
            Ok(valid) => valid,
            Err(err_msg) => {
                self.finish_turn_with_error(&tab_id, &msg_arc, &err_msg);
                let final_assistant = msg_arc.lock().unwrap().clone();
                return json!({
                    "userMessage": user_message,
                    "assistantMessage": final_assistant,
                });
            }
        };

        let response = json!({
            "userMessage": user_message,
            "assistantMessage": msg_arc.lock().unwrap().clone(),
        });

        let is_coding_agent = harnesses::get(&agent).is_some();
        if is_coding_agent {
            self.done_filters
                .lock()
                .unwrap()
                .insert(msg_id.clone(), WorkDoneStreamFilter::new());
        }
        // Agents are handed the image file paths in the prompt and open the files themselves.
        let agent_prompt = prompt_with_attachments(prompt_text, attachments);
        let cli_prompt = if is_coding_agent && is_first_task_prompt {
            self.summary_filters
                .lock()
                .unwrap()
                .insert(msg_id.clone(), TaskSummaryStreamFilter::new());
            pad_first_prompt_for_task_summary(&agent_prompt)
        } else {
            if is_coding_agent {
                // The first prompt's summary request stays in the agent's context, so later
                // replies can repeat the tag; strip it there too.
                self.summary_filters
                    .lock()
                    .unwrap()
                    .insert(msg_id.clone(), TaskSummaryStreamFilter::strip_only());
            }
            agent_prompt
        };

        log_info(&format!(
            "Turn started tab={tab_id} msg={msg_id} agent={agent} model={:?} cwd={run_cwd}",
            tab.get("model").and_then(Value::as_str)
        ));

        match agent.as_str() {
            "shell" => self.run_shell_turn(tab.clone(), msg_arc, prompt_text.to_string(), run_cwd),
            other => match harnesses::get(other) {
                Some(harness) => self.run_harness_turn(
                    harness,
                    tab.clone(),
                    msg_arc,
                    cli_prompt,
                    run_cwd,
                    effective_mode,
                ),
                None => {
                    self.finish_turn_with_error(
                        &tab_id,
                        &msg_arc,
                        &format!("Unsupported agent type: {other}"),
                    );
                }
            },
        }
        response
    }

    fn handle_model_command(
        &self,
        tab: &Value,
        user_message: Value,
        mut assistant_message: Value,
        arg: &str,
    ) -> Value {
        let tab_id = tab.get("id").and_then(Value::as_str).unwrap_or("");
        let (updated_tab, reply) = if arg.is_empty() {
            let current = match tab.get("model").and_then(Value::as_str) {
                Some(m) if !m.is_empty() => format!("`{m}`"),
                _ => "CLI default".to_string(),
            };
            (
                Some(tab.clone()),
                format!(
                    "Current model for this tab: {current}. Use the model picker or send \
`/model <model-id>` (or `/model default`) to change it."
                ),
            )
        } else if model_reset_re().is_match(arg) {
            let mut patch = Map::new();
            patch.insert("model".into(), Value::Null);
            patch.insert("status".into(), json!("idle"));
            patch.insert("statusDetail".into(), Value::Null);
            patch.insert("lastMessagePreview".into(), json!("Model reset to default"));
            patch.insert("unread".into(), json!(false));
            (
                self.store.update_tab(tab_id, patch),
                "Model reset to CLI default for this tab.".to_string(),
            )
        } else {
            let mut patch = Map::new();
            patch.insert("model".into(), json!(arg));
            patch.insert("status".into(), json!("idle"));
            patch.insert("statusDetail".into(), Value::Null);
            patch.insert("lastMessagePreview".into(), json!(format!("Model: {arg}")));
            patch.insert("unread".into(), json!(false));
            (
                self.store.update_tab(tab_id, patch),
                format!("Model switched to `{arg}` for this tab. Subsequent messages will use this model."),
            )
        };

        if let Some(obj) = assistant_message.as_object_mut() {
            obj.insert("text".into(), json!(reply));
            obj.insert(
                "segments".into(),
                json!([{ "type": "text", "text": reply }]),
            );
            obj.insert("status".into(), json!("complete"));
            obj.insert("updatedAt".into(), json!(now_ms()));
        }
        self.store
            .replace_message(tab_id, assistant_message.clone());
        if let Err(err) = self.store.save_now() {
            log_error(&format!("Immediate store flush failed: {err}"));
        }
        (self.broadcast)(
            json!({ "type": "message_created", "tabId": tab_id, "message": user_message }),
        );
        (self.broadcast)(
            json!({ "type": "message_created", "tabId": tab_id, "message": assistant_message }),
        );
        if let Some(ut) = updated_tab {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": ut }));
        }
        json!({ "userMessage": user_message, "assistantMessage": assistant_message })
    }

    pub(crate) fn persist_session_ref(
        &self,
        tab_id: &str,
        current: &mut Option<Value>,
        kind: &str,
        value: &str,
    ) {
        if value.is_empty() {
            return;
        }
        let already_same = current
            .as_ref()
            .and_then(Value::as_object)
            .map(|o| {
                o.get("kind").and_then(Value::as_str) == Some(kind)
                    && o.get("value").and_then(Value::as_str) == Some(value)
            })
            .unwrap_or(false);
        if already_same {
            return;
        }
        let new_ref = json!({ "kind": kind, "value": value });
        *current = Some(new_ref.clone());
        // Persist sessionRef immediately on discovery so restarts mid-turn 1 never lose context (#62).
        let mut patch = Map::new();
        patch.insert("sessionRef".into(), new_ref);
        if let Some(updated_tab) = self.store.update_tab(tab_id, patch) {
            if let Err(err) = self.store.save_now() {
                log_error(&format!("Immediate store flush failed: {err}"));
            }
            log_info(&format!(
                "Persisted sessionRef immediately for tab={tab_id} kind={kind} value={value}"
            ));
            (self.broadcast)(json!({ "type": "tab_updated", "tab": updated_tab }));
        }
    }

    pub(crate) fn emit_delta(
        &self,
        tab_id: &str,
        msg_arc: &Arc<Mutex<Value>>,
        text_delta: Option<String>,
        thinking_delta: Option<String>,
        bypass_filters: bool,
    ) {
        let msg_id = {
            let m = msg_arc.lock().unwrap();
            m.get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };

        let mut final_text = text_delta;
        if !bypass_filters {
            if let Some(raw_td) = final_text.as_deref() {
                if !raw_td.is_empty() {
                    let filtered = {
                        let mut filters = self.summary_filters.lock().unwrap();
                        filters.get_mut(&msg_id).map(|f| f.feed(raw_td))
                    };
                    if let Some((vis, extracted)) = filtered {
                        if let Some(summary) = extracted {
                            let mut patch = Map::new();
                            patch.insert("taskSummary".into(), json!(summary));
                            if let Some(updated_tab) = self.store.update_tab(tab_id, patch) {
                                (self.broadcast)(
                                    json!({ "type": "tab_updated", "tab": updated_tab }),
                                );
                            }
                        }
                        final_text = if vis.is_empty() { None } else { Some(vis) };
                    }
                }
            }
            if let Some(after_summary) = final_text.as_deref() {
                if !after_summary.is_empty() {
                    let done_filtered = {
                        let mut filters = self.done_filters.lock().unwrap();
                        filters.get_mut(&msg_id).map(|f| f.feed(after_summary))
                    };
                    if let Some(vis) = done_filtered {
                        final_text = if vis.is_empty() { None } else { Some(vis) };
                    }
                }
            }
        }

        let has_text = final_text
            .as_deref()
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let has_think = thinking_delta
            .as_deref()
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_text && !has_think {
            return;
        }

        let snapshot = {
            let mut m = msg_arc.lock().unwrap();
            if let Some(obj) = m.as_object_mut() {
                if let Some(ref td) = final_text {
                    if !td.is_empty() {
                        let prev = obj.get("text").and_then(Value::as_str).unwrap_or("");
                        let new_text = format!("{prev}{td}");
                        obj.insert("text".into(), json!(new_text));
                        let segments = obj
                            .entry("segments")
                            .or_insert_with(|| Value::Array(Vec::new()))
                            .as_array_mut()
                            .unwrap();
                        let appended = if let Some(last) = segments.last_mut() {
                            if last.get("type").and_then(Value::as_str) == Some("text") {
                                if let Some(lo) = last.as_object_mut() {
                                    let prev_s =
                                        lo.get("text").and_then(Value::as_str).unwrap_or("");
                                    lo.insert("text".into(), json!(format!("{prev_s}{td}")));
                                }
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        if !appended {
                            let chunk = if !segments.is_empty() {
                                td.trim_start_matches(['\r', '\n'])
                            } else {
                                td.as_str()
                            };
                            if !chunk.is_empty() {
                                segments.push(json!({ "type": "text", "text": chunk }));
                            }
                        }
                    }
                }
                if let Some(ref th) = thinking_delta {
                    if !th.is_empty() {
                        let prev = obj.get("thinking").and_then(Value::as_str).unwrap_or("");
                        obj.insert("thinking".into(), json!(format!("{prev}{th}")));
                    }
                }
                obj.insert("updatedAt".into(), json!(now_ms()));
            }
            m.clone()
        };

        self.store.replace_message(tab_id, snapshot);
        let mut env = Map::new();
        env.insert("type".into(), json!("message_delta"));
        env.insert("tabId".into(), json!(tab_id));
        env.insert("messageId".into(), json!(msg_id));
        if let Some(td) = final_text.filter(|s| !s.is_empty()) {
            env.insert("textDelta".into(), json!(td));
        }
        if let Some(th) = thinking_delta.filter(|s| !s.is_empty()) {
            env.insert("thinkingDelta".into(), json!(th));
        }
        (self.broadcast)(Value::Object(env));
    }

    pub(crate) fn upsert_tool_call(
        &self,
        tab_id: &str,
        msg_arc: &Arc<Mutex<Value>>,
        tool_call: Value,
    ) {
        let tool_id = tool_call
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let (msg_id, snapshot) = {
            let mut m = msg_arc.lock().unwrap();
            let obj = m.as_object_mut().unwrap();
            let msg_id = obj
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            {
                let calls = obj
                    .entry("toolCalls")
                    .or_insert_with(|| Value::Array(Vec::new()))
                    .as_array_mut()
                    .unwrap();
                if let Some(existing) = calls
                    .iter_mut()
                    .find(|t| t.get("id").and_then(Value::as_str) == Some(tool_id.as_str()))
                {
                    *existing = tool_call.clone();
                } else {
                    calls.push(tool_call.clone());
                }
            }
            {
                let segments = obj
                    .entry("segments")
                    .or_insert_with(|| Value::Array(Vec::new()))
                    .as_array_mut()
                    .unwrap();
                let exists = segments.iter().any(|s| {
                    s.get("type").and_then(Value::as_str) == Some("tool")
                        && s.get("toolCallId").and_then(Value::as_str) == Some(tool_id.as_str())
                });
                if !exists {
                    segments.push(json!({ "type": "tool", "toolCallId": tool_id }));
                }
            }
            obj.insert("updatedAt".into(), json!(now_ms()));
            (msg_id, m.clone())
        };
        self.store.replace_message(tab_id, snapshot);
        (self.broadcast)(json!({
            "type": "tool_update",
            "tabId": tab_id,
            "messageId": msg_id,
            "toolCall": tool_call,
        }));
    }

    pub(crate) fn find_tool(msg_arc: &Arc<Mutex<Value>>, tool_id: &str) -> Option<Value> {
        let m = msg_arc.lock().unwrap();
        m.get("toolCalls")
            .and_then(Value::as_array)
            .and_then(|arr| {
                arr.iter()
                    .find(|t| t.get("id").and_then(Value::as_str) == Some(tool_id))
                    .cloned()
            })
    }

    pub(crate) fn set_status_detail(&self, tab_id: &str, detail: &str) {
        let mut patch = Map::new();
        patch.insert("statusDetail".into(), json!(detail));
        if let Some(updated) = self.store.update_tab(tab_id, patch) {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": updated }));
        }
    }

    pub(crate) fn running_status(&self, tab_id: &str, name: &str, summary: &str) {
        let detail = if summary.is_empty() {
            format!("Running {name}")
        } else {
            format!("Running {name}: {}", truncate(summary, 40))
        };
        self.set_status_detail(tab_id, &detail);
    }

    fn finish_turn_with_error(&self, tab_id: &str, msg_arc: &Arc<Mutex<Value>>, error_text: &str) {
        let (msg_id, final_msg) = {
            let mut m = msg_arc.lock().unwrap();
            let obj = m.as_object_mut().unwrap();
            let msg_id = obj
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            obj.insert("status".into(), json!("error"));
            obj.insert("error".into(), json!(error_text));
            obj.insert("updatedAt".into(), json!(now_ms()));
            (msg_id, m.clone())
        };
        self.summary_filters.lock().unwrap().remove(&msg_id);
        self.done_filters.lock().unwrap().remove(&msg_id);
        log_error(&format!(
            "Turn error tab={tab_id} msg={msg_id}: {error_text}"
        ));
        self.store.replace_message(tab_id, final_msg.clone());
        let mut patch = Map::new();
        patch.insert("status".into(), json!("error"));
        patch.insert("statusDetail".into(), Value::Null);
        patch.insert("unread".into(), json!(true));
        let updated_tab = self.store.update_tab(tab_id, patch);
        if let Err(err) = self.store.save_now() {
            log_error(&format!("Immediate store flush failed: {err}"));
        }
        (self.broadcast)(
            json!({ "type": "message_updated", "tabId": tab_id, "message": final_msg }),
        );
        if let Some(ut) = updated_tab {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": ut }));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_turn(
        &self,
        tab_id: &str,
        msg_arc: &Arc<Mutex<Value>>,
        interrupted: bool,
        exit_code: Option<i32>,
        stderr_text: &str,
        duration_ms: i64,
        session_ref: Option<Value>,
        new_cwd: Option<String>,
        usage: Option<Value>,
        denied_actions: Vec<Value>,
        throughput: Option<GenerationSample>,
        session_usage: Option<SessionUsageBaseline>,
    ) {
        let active_arc = self.active.lock().unwrap().remove(tab_id);
        let mut drain_queue_on_exit = false;
        let mut shutdown_reason: Option<String> = None;
        if let Some(ref a_arc) = active_arc {
            let mut a = a_arc.lock().unwrap();
            a.cancel_escalation();
            drain_queue_on_exit = a.drain_queue_on_exit;
            shutdown_reason = a.interrupt_reason.clone();
            if let Some(pid) = a.pid {
                if interrupted {
                    // Final sweep to ensure zero orphan grandchildren remain in the process group (#52).
                    signal_process_group(pid, libc::SIGKILL);
                }
            }
        }

        let msg_id = {
            let m = msg_arc.lock().unwrap();
            m.get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let summary_leftover = self
            .summary_filters
            .lock()
            .unwrap()
            .remove(&msg_id)
            .map(|mut f| f.flush())
            .unwrap_or_default();
        let (leftover, work_done_detected) = {
            let done_filter = self.done_filters.lock().unwrap().remove(&msg_id);
            if let Some(mut f) = done_filter {
                let mut out = String::new();
                if !summary_leftover.is_empty() {
                    out.push_str(&f.feed(&summary_leftover));
                }
                out.push_str(&f.flush());
                (out, f.detected())
            } else {
                (summary_leftover, false)
            }
        };

        let Some(tab) = self.store.get_tab(tab_id) else {
            return;
        };
        if !leftover.is_empty() {
            self.emit_delta(tab_id, msg_arc, Some(leftover), None, true);
        }
        if work_done_detected {
            let mut m = msg_arc.lock().unwrap();
            if let Some(obj) = m.as_object_mut() {
                if let Some(trimmed) = obj
                    .get("text")
                    .and_then(Value::as_str)
                    .map(|s| s.trim_end().to_string())
                {
                    obj.insert("text".into(), json!(trimmed));
                }
                if let Some(segments) = obj.get_mut("segments").and_then(Value::as_array_mut) {
                    if let Some(last) = segments.last_mut() {
                        if last.get("type").and_then(Value::as_str) == Some("text") {
                            let trimmed = last
                                .get("text")
                                .and_then(Value::as_str)
                                .map(|s| s.trim_end().to_string())
                                .unwrap_or_default();
                            if trimmed.is_empty() {
                                segments.pop();
                            } else if let Some(lo) = last.as_object_mut() {
                                lo.insert("text".into(), json!(trimmed));
                            }
                        }
                    }
                }
            }
        }

        let final_usage = if let Some(mut u) = usage {
            if let Some(uo) = u.as_object_mut() {
                if uo.get("durationMs").map(Value::is_null).unwrap_or(true) {
                    uo.insert("durationMs".into(), json!(duration_ms));
                }
            }
            u
        } else {
            json!({
                "inputTokens": Value::Null,
                "outputTokens": Value::Null,
                "thinkingTokens": Value::Null,
                "costUsd": Value::Null,
                "durationMs": duration_ms,
            })
        };

        let (msg_text, existing_err, existing_denied, msg_permission_mode) = {
            let m = msg_arc.lock().unwrap();
            (
                m.get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                m.get("error").and_then(Value::as_str).map(str::to_string),
                m.get("deniedActions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                m.get("permissionMode")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            )
        };
        let denied = if !denied_actions.is_empty() {
            denied_actions
        } else {
            existing_denied
        };
        let turn_permission_mode = msg_permission_mode
            .or_else(|| {
                tab.get("permissionMode")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "full-access".to_string());
        let denied_under_full_access = turn_permission_mode == "full-access";

        let is_shell = tab.get("agent").and_then(Value::as_str) == Some("shell");
        let failed = exit_code.map(|c| c != 0).unwrap_or(false);
        let mut final_status = "complete";
        let mut final_error = existing_err;
        let mut tab_status = "idle";
        let mut status_detail: Option<String> = None;

        if interrupted {
            final_status = "interrupted";
            tab_status = "idle";
            if final_error.is_none() {
                if let Some(reason) = shutdown_reason {
                    final_error = Some(reason);
                } else if self.shutting_down.load(Ordering::SeqCst) {
                    final_error = Some(RESTART_INTERRUPTED_REASON.to_string());
                }
            }
        } else if is_shell {
            match exit_code {
                Some(0) => {
                    final_status = "complete";
                    final_error = None;
                    tab_status = "idle";
                }
                Some(code) => {
                    final_status = "error";
                    final_error = Some(format!("Exited with code {code}"));
                    tab_status = "idle";
                }
                None => {
                    final_status = "error";
                    final_error = Some(if !stderr_text.trim().is_empty() {
                        truncate(stderr_text.trim(), 240)
                    } else {
                        "Shell process terminated unexpectedly".to_string()
                    });
                    tab_status = "idle";
                }
            }
        } else if !denied.is_empty() && !denied_under_full_access {
            final_status = "complete";
            tab_status = "blocked";
            status_detail = Some("Waiting for approval".to_string());
        } else if failed && msg_text.trim().is_empty() {
            final_status = "error";
            final_error = Some(if !stderr_text.trim().is_empty() {
                stderr_text.trim().to_string()
            } else {
                format!("Process exited with code {}", exit_code.unwrap_or(-1))
            });
            tab_status = "error";
        } else if failed && !stderr_text.trim().is_empty() {
            final_error = Some(format!(
                "Exited with code {}: {}",
                exit_code.unwrap_or(-1),
                truncate(stderr_text.trim(), 240)
            ));
        } else if !failed && (work_done_detected || is_task_finished_response(Some(&msg_text))) {
            tab_status = "done";
            status_detail = Some("Task finished".to_string());
        }

        let final_msg = {
            let mut m = msg_arc.lock().unwrap();
            let obj = m.as_object_mut().unwrap();
            obj.insert("deniedActions".into(), Value::Array(denied));
            obj.insert("status".into(), json!(final_status));
            obj.insert(
                "error".into(),
                final_error
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            if is_shell && !interrupted {
                if let Some(code) = exit_code {
                    obj.insert("exitCode".into(), json!(code));
                }
            }
            obj.insert("usage".into(), final_usage);
            obj.insert("updatedAt".into(), json!(now_ms()));
            m.clone()
        };

        log_info(&format!(
            "Turn finished tab={tab_id} msg={msg_id} status={final_status} tab_status={tab_status} exit_code={exit_code:?} interrupted={interrupted} duration_ms={duration_ms}"
        ));

        if !interrupted && !failed && final_status == "complete" {
            if let (Some(agent), Some(model), Some(sample)) = (
                tab["agent"].as_str(),
                final_msg["model"].as_str(),
                throughput,
            ) {
                self.store.record_throughput(
                    agent,
                    model,
                    sample.output_tokens,
                    sample.duration_ms,
                    |rates| {
                        (self.broadcast)(json!({"type":"model_throughput", "throughput":rates}));
                    },
                );
            }
        }
        self.store.replace_message(tab_id, final_msg.clone());
        let mut patch = Map::new();
        patch.insert("status".into(), json!(tab_status));
        patch.insert(
            "statusDetail".into(),
            status_detail.map(Value::String).unwrap_or(Value::Null),
        );
        patch.insert("unread".into(), json!(true));
        if let Some(sref) = session_ref {
            patch.insert("sessionRef".into(), sref);
        }
        if let Some(mut su) = session_usage {
            if interrupted || failed || final_status != "complete" {
                su.valid = false;
            }
            patch.insert("sessionUsage".into(), su.to_value());
        } else if (interrupted || failed) && !is_shell {
            if let Some(mut existing_su) = tab
                .get("sessionUsage")
                .and_then(SessionUsageBaseline::from_value)
            {
                if existing_su.valid {
                    existing_su.valid = false;
                    patch.insert("sessionUsage".into(), existing_su.to_value());
                }
            }
        }
        if let Some(cwd) = new_cwd {
            patch.insert("cwd".into(), json!(cwd));
        }
        if !msg_text.trim().is_empty() {
            patch.insert("lastMessagePreview".into(), json!(truncate(&msg_text, 120)));
        }
        let updated_tab = self.store.update_tab(tab_id, patch);
        if let Err(err) = self.store.save_now() {
            log_error(&format!("Immediate store flush failed: {err}"));
        }

        (self.broadcast)(
            json!({ "type": "message_updated", "tabId": tab_id, "message": final_msg }),
        );
        if let Some(ref ut) = updated_tab {
            (self.broadcast)(json!({ "type": "tab_updated", "tab": ut }));
        }

        let has_queue = updated_tab
            .as_ref()
            .and_then(|t| t.get("queuedMessages").and_then(Value::as_array))
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        let should_drain = (!interrupted && matches!(tab_status, "idle" | "done"))
            || (interrupted && drain_queue_on_exit);
        if should_drain && has_queue {
            self.drain_and_start_queued(tab_id);
        }
        self.notify_if_idle();
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_process<FOut, FErr, FClose>(
        &self,
        tab_id: String,
        msg_arc: Arc<Mutex<Value>>,
        argv: Vec<String>,
        cwd: String,
        label: String,
        on_stdout: FOut,
        on_stderr: FErr,
        on_close: FClose,
    ) where
        FOut: FnMut(String) + Send + 'static,
        FErr: FnMut(String) + Send + 'static,
        FClose: FnOnce(Option<i32>, bool) + Send + 'static,
    {
        let msg_id = msg_arc
            .lock()
            .unwrap()
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let active_arc = Arc::new(Mutex::new(ActiveTurn::new(msg_id, label.clone())));
        self.active
            .lock()
            .unwrap()
            .insert(tab_id.clone(), active_arc.clone());

        let runner = self.clone();
        tokio::spawn(async move {
            let mut cmd = Command::new(&argv[0]);
            if argv.len() > 1 {
                cmd.args(&argv[1..]);
            }
            cmd.current_dir(&cwd)
                .env("PATH", enriched_path())
                .env("FORCE_COLOR", "0")
                .env("NO_COLOR", "1")
                .env("TERM", "dumb")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            // Put child and all its descendants into a new session / process group so
            // interrupt can cleanly signal and kill the entire subtree (#52).
            unsafe {
                cmd.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(err) => {
                    {
                        let mut map = runner.active.lock().unwrap();
                        if map
                            .get(&tab_id)
                            .map(|a| Arc::ptr_eq(a, &active_arc))
                            .unwrap_or(false)
                        {
                            map.remove(&tab_id);
                        }
                    }
                    runner.finish_turn_with_error(
                        &tab_id,
                        &msg_arc,
                        &format!("Failed to start {label}: {err}"),
                    );
                    return;
                }
            };

            let child_pid = child.id();
            {
                let mut turn = active_arc.lock().unwrap();
                turn.pid = child_pid;
                if let Some(sig) = turn.signal_on_spawn.take() {
                    if let Some(pid) = child_pid {
                        signal_process_group(pid, sig);
                    }
                }
            }

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            async fn pump_utf8<R: AsyncRead + Unpin, F: FnMut(String)>(mut reader: R, mut cb: F) {
                let mut buf = [0u8; 65_536];
                let mut pending = Vec::new();
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            pending.extend_from_slice(&buf[..n]);
                            loop {
                                match std::str::from_utf8(&pending) {
                                    Ok(s) => {
                                        if !s.is_empty() {
                                            cb(s.to_string());
                                        }
                                        pending.clear();
                                        break;
                                    }
                                    Err(err) => {
                                        let valid_len = err.valid_up_to();
                                        if valid_len > 0 {
                                            let valid =
                                                String::from_utf8_lossy(&pending[..valid_len])
                                                    .into_owned();
                                            cb(valid);
                                            pending.drain(..valid_len);
                                        }
                                        if let Some(err_len) = err.error_len() {
                                            cb("\u{FFFD}".to_string());
                                            pending.drain(..err_len);
                                        } else {
                                            // Incomplete trailing multi-byte UTF-8 sequence; wait for more bytes.
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                if !pending.is_empty() {
                    cb(String::from_utf8_lossy(&pending).into_owned());
                }
            }

            let (_, _, status) = tokio::join!(
                pump_utf8(stdout, on_stdout),
                pump_utf8(stderr, on_stderr),
                child.wait()
            );

            let code = status.ok().and_then(|s| s.code());
            let interrupted = active_arc.lock().unwrap().interrupted;
            on_close(code, interrupted);
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_json_lines<FLine, FFinish>(
        &self,
        tab_id: String,
        msg_arc: Arc<Mutex<Value>>,
        argv: Vec<String>,
        cwd: String,
        label: String,
        handle_line: FLine,
        finish: FFinish,
    ) where
        FLine: FnMut(&str) + Send + 'static,
        FFinish: FnOnce(Option<i32>, bool, String, i64) + Send + 'static,
    {
        let start = now_ms();
        let line_buf = Arc::new(Mutex::new(String::new()));
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let handler = Arc::new(Mutex::new(handle_line));

        let lb_out = line_buf.clone();
        let h_out = handler.clone();
        let on_stdout = move |chunk: String| {
            let mut lb = lb_out.lock().unwrap();
            lb.push_str(&chunk);
            let mut complete_lines = Vec::new();
            while let Some(idx) = lb.find('\n') {
                let mut line = lb[..idx].to_string();
                if line.ends_with('\r') {
                    line.pop();
                }
                complete_lines.push(line);
                lb.drain(..=idx);
            }
            drop(lb);
            let mut h = h_out.lock().unwrap();
            for line in complete_lines {
                h(&line);
            }
        };

        let sb_err = stderr_buf.clone();
        let on_stderr = move |chunk: String| {
            sb_err.lock().unwrap().push_str(&chunk);
        };

        let on_close = move |code: Option<i32>, interrupted: bool| {
            let tail = std::mem::take(&mut *line_buf.lock().unwrap());
            if !tail.trim().is_empty() {
                let mut h = handler.lock().unwrap();
                h(&tail);
            }
            let stderr = std::mem::take(&mut *stderr_buf.lock().unwrap());
            finish(code, interrupted, stderr, now_ms() - start);
        };

        self.spawn_process(
            tab_id, msg_arc, argv, cwd, label, on_stdout, on_stderr, on_close,
        );
    }

    // ------------------------------------------------------------------ shell

    fn run_shell_turn(&self, tab: Value, msg_arc: Arc<Mutex<Value>>, command: String, cwd: String) {
        let tab_id = tab
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let start = now_ms();
        self.set_status_detail(&tab_id, &format!("$ {}", truncate(&command, 60)));

        let msg_id = msg_arc
            .lock()
            .unwrap()
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let (stdin_tx, stdin_rx) = tokio::sync::watch::channel(StdinState::Pending);
        let mut active_turn = ActiveTurn::new(msg_id, "shell".to_string());
        active_turn.stdin_rx = Some(stdin_rx);
        let active_arc = Arc::new(Mutex::new(active_turn));
        self.active
            .lock()
            .unwrap()
            .insert(tab_id.clone(), active_arc.clone());

        let runner = self.clone();
        tokio::spawn(async move {
            let existing = runner.shell_sessions.lock().unwrap().get(&tab_id).cloned();
            let mut reused_entry: Option<ShellSessionEntry> = None;
            if let Some(entry) = existing {
                let alive = {
                    let mut guard = entry.shell.lock().await;
                    guard.is_alive()
                };
                if alive {
                    reused_entry = Some(entry);
                } else {
                    let mut map = runner.shell_sessions.lock().unwrap();
                    if map
                        .get(&tab_id)
                        .map(|e| Arc::ptr_eq(&e.shell, &entry.shell))
                        .unwrap_or(false)
                    {
                        map.remove(&tab_id);
                    }
                }
            }

            let entry = match reused_entry {
                Some(e) => e,
                None => match PersistentShell::spawn(&tab_id, &cwd) {
                    Ok(shell) => {
                        let entry = ShellSessionEntry {
                            pid: shell.pid(),
                            session_dir: shell.session_dir().to_path_buf(),
                            shell: Arc::new(tokio::sync::Mutex::new(shell)),
                        };
                        runner
                            .shell_sessions
                            .lock()
                            .unwrap()
                            .insert(tab_id.clone(), entry.clone());
                        entry
                    }
                    Err(err) => {
                        let _ = stdin_tx.send(StdinState::Closed);
                        {
                            let mut map = runner.active.lock().unwrap();
                            if map
                                .get(&tab_id)
                                .map(|a| Arc::ptr_eq(a, &active_arc))
                                .unwrap_or(false)
                            {
                                map.remove(&tab_id);
                            }
                        }
                        runner.finish_turn_with_error(&tab_id, &msg_arc, &err);
                        return;
                    }
                },
            };

            {
                let mut turn = active_arc.lock().unwrap();
                turn.pid = Some(entry.pid);
                if let Some(sig) = turn.signal_on_spawn.take() {
                    signal_process_group(entry.pid, sig);
                }
            }

            struct ShellState {
                stderr: String,
                fence: bool,
            }

            let st = Arc::new(Mutex::new(ShellState {
                stderr: String::new(),
                fence: false,
            }));

            let append_text = {
                let runner = runner.clone();
                let tid = tab_id.clone();
                let ma = msg_arc.clone();
                move |state: &mut ShellState, chunk: &str| {
                    if chunk.is_empty() {
                        return;
                    }
                    if !state.fence {
                        state.fence = true;
                        runner.emit_delta(
                            &tid,
                            &ma,
                            Some(format!("```text\n{chunk}")),
                            None,
                            false,
                        );
                    } else {
                        runner.emit_delta(&tid, &ma, Some(chunk.to_string()), None, false);
                    }
                }
            };

            let st_out = st.clone();
            let append_out = append_text.clone();
            let on_stdout = move |chunk: String| {
                let mut state = st_out.lock().unwrap();
                append_out(&mut state, &chunk);
            };

            let st_err = st.clone();
            let append_err = append_text;
            let on_stderr = move |chunk: String| {
                let mut state = st_err.lock().unwrap();
                state.stderr.push_str(&chunk);
                append_err(&mut state, &chunk);
            };

            let stdin_tx_ready = stdin_tx.clone();
            let outcome = {
                let mut shell = entry.shell.lock().await;
                let res = shell
                    .run_command(
                        &command,
                        move |stdin_handle| {
                            let _ = stdin_tx_ready.send(StdinState::Ready(stdin_handle));
                        },
                        on_stdout,
                        on_stderr,
                    )
                    .await;
                let _ = stdin_tx.send(StdinState::Closed);

                let interrupted = active_arc.lock().unwrap().interrupted;
                let drop_session = match &res {
                    Ok(o) => !o.shell_alive || interrupted,
                    Err(_) => true,
                };
                if drop_session {
                    shell.kill_and_cleanup();
                    let mut map = runner.shell_sessions.lock().unwrap();
                    if map
                        .get(&tab_id)
                        .map(|e| Arc::ptr_eq(&e.shell, &entry.shell))
                        .unwrap_or(false)
                    {
                        map.remove(&tab_id);
                    }
                }
                res
            };

            let interrupted = active_arc.lock().unwrap().interrupted;
            let (stderr, has_fence) = {
                let state = st.lock().unwrap();
                (state.stderr.clone(), state.fence)
            };

            if has_fence {
                let ends_nl = msg_arc
                    .lock()
                    .unwrap()
                    .get("text")
                    .and_then(Value::as_str)
                    .map(|s| s.ends_with('\n'))
                    .unwrap_or(false);
                runner.emit_delta(
                    &tab_id,
                    &msg_arc,
                    Some(if ends_nl { "```" } else { "\n```" }.to_string()),
                    None,
                    false,
                );
            }

            match outcome {
                Ok(outcome) => {
                    runner.finalize_turn(
                        &tab_id,
                        &msg_arc,
                        interrupted,
                        outcome.exit_code,
                        &stderr,
                        now_ms() - start,
                        None,
                        outcome.new_cwd,
                        None,
                        Vec::new(),
                        None,
                        None,
                    );
                }
                Err(err) => {
                    {
                        let mut map = runner.active.lock().unwrap();
                        if map
                            .get(&tab_id)
                            .map(|a| Arc::ptr_eq(a, &active_arc))
                            .unwrap_or(false)
                        {
                            map.remove(&tab_id);
                        }
                    }
                    runner.finish_turn_with_error(&tab_id, &msg_arc, &err);
                }
            }
        });
    }

    // ------------------------------------------------------------------ agy

    pub(crate) fn tool_use_started(
        &self,
        tab_id: &str,
        msg_arc: &Arc<Mutex<Value>>,
        tool_starts: &mut HashMap<String, i64>,
        raw_id: Option<&Value>,
        raw_name: Option<&Value>,
        input_obj: Option<&Value>,
    ) {
        let tool_id = raw_id
            .filter(|v| !v.is_null())
            .map(js_str)
            .unwrap_or_else(|| format!("tool_{}", rand_hex(4)));
        let name = raw_name
            .filter(|v| !v.is_null())
            .map(js_str)
            .unwrap_or_else(|| "Tool".to_string());
        tool_starts.insert(tool_id.clone(), now_ms());
        let summary = summarize_tool_input(input_obj);
        self.running_status(tab_id, &name, &summary);
        self.upsert_tool_call(
            tab_id,
            msg_arc,
            json!({
                "id": tool_id,
                "name": name,
                "inputSummary": summary,
                "inputJson": input_obj.filter(|v| !v.is_null()).map(json_pretty),
                "output": Value::Null,
                "status": "running",
                "durationMs": Value::Null,
            }),
        );
    }

    fn prior_session_usage_for_tab(
        &self,
        tab: &Value,
        harness_kind: &str,
        current_msg_id: &str,
    ) -> Option<SessionUsageBaseline> {
        if let Some(baseline) = tab
            .get("sessionUsage")
            .and_then(SessionUsageBaseline::from_value)
        {
            return Some(baseline);
        }
        let session_id = tab
            .get("sessionRef")
            .and_then(Value::as_object)
            .filter(|o| o.get("kind").and_then(Value::as_str) == Some(harness_kind))
            .and_then(|o| o.get("value").and_then(Value::as_str))
            .filter(|s| !s.is_empty())?
            .to_string();
        let tab_id = tab.get("id").and_then(Value::as_str)?;
        // Migrate pre-existing resumed tabs created before `sessionUsage` was persisted:
        // inspect previous assistant messages to recover the last cumulative totals.
        let messages = self.store.get_messages(tab_id);
        let mut last_baseline: Option<SessionUsageBaseline> = None;
        for msg in messages {
            if msg.get("role").and_then(Value::as_str) != Some("assistant") {
                continue;
            }
            if msg.get("id").and_then(Value::as_str) == Some(current_msg_id) {
                continue;
            }
            let status = msg.get("status").and_then(Value::as_str).unwrap_or("");
            let usage = msg.get("usage").and_then(Value::as_object);
            let out = usage.and_then(|u| value_as_u64(u.get("outputTokens")));
            if status == "complete" && out.is_some() {
                last_baseline = Some(SessionUsageBaseline {
                    session_id: session_id.clone(),
                    input_tokens: usage.and_then(|u| value_as_u64(u.get("inputTokens"))),
                    output_tokens: out,
                    thinking_tokens: usage.and_then(|u| value_as_u64(u.get("thinkingTokens"))),
                    valid: true,
                });
            } else if status != "complete"
                || msg
                    .get("toolCalls")
                    .and_then(Value::as_array)
                    .map(|a| !a.is_empty())
                    .unwrap_or(false)
            {
                // An intervening real turn ended without reporting usage; invalidate any baseline.
                last_baseline = Some(SessionUsageBaseline {
                    session_id: session_id.clone(),
                    input_tokens: last_baseline.as_ref().and_then(|b| b.input_tokens),
                    output_tokens: last_baseline.as_ref().and_then(|b| b.output_tokens),
                    thinking_tokens: last_baseline.as_ref().and_then(|b| b.thinking_tokens),
                    valid: false,
                });
            }
        }
        last_baseline
    }

    /// Runs one turn of a coding-agent harness: builds its command, spawns it, and feeds each
    /// output line to the harness's parser.
    fn run_harness_turn(
        &self,
        harness: &'static dyn Harness,
        tab: Value,
        msg_arc: Arc<Mutex<Value>>,
        prompt: String,
        cwd: String,
        permission_mode: String,
    ) {
        let tab_id = tab
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let Some(binary) = resolve_binary(harness.binary()) else {
            self.finish_turn_with_error(
                &tab_id,
                &msg_arc,
                &format!(
                    "'{}' binary not found in PATH on this VM.",
                    harness.binary()
                ),
            );
            return;
        };
        let current_msg_id = msg_arc
            .lock()
            .unwrap()
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let prior_session_usage =
            self.prior_session_usage_for_tab(&tab, harness.kind(), &current_msg_id);
        let system_prompt_dirty = self.system_prompt_dirty.lock().unwrap().remove(&tab_id);
        let prepared = harness.prepare_turn(TurnRequest {
            system_prompt: effective_system_prompt(&tab),
            attachments_dir: existing_attachments_dir(&tab_id),
            tab,
            prompt,
            cwd: cwd.clone(),
            permission_mode,
            system_prompt_dirty,
            prior_session_usage,
        });
        self.set_status_detail(&tab_id, prepared.status);

        let parser = Arc::new(Mutex::new(Some(prepared.parser)));
        let sink = TurnSink::new(self.clone(), tab_id.clone(), msg_arc.clone());
        let parser_line = parser.clone();
        let handle_line = move |line: &str| {
            if let Some(p) = parser_line.lock().unwrap().as_mut() {
                p.handle_line(line, &sink);
            }
        };

        let runner_fin = self.clone();
        let tid_fin = tab_id.clone();
        let msg_fin = msg_arc.clone();
        let finish = move |code: Option<i32>, interrupted: bool, stderr: String, duration: i64| {
            let outcome = parser
                .lock()
                .unwrap()
                .take()
                .map(TurnParser::finish)
                .unwrap_or(TurnOutcome {
                    session_ref: None,
                    usage: None,
                    denied: Vec::new(),
                    throughput: None,
                    session_usage: None,
                });
            runner_fin.finalize_turn(
                &tid_fin,
                &msg_fin,
                interrupted,
                code,
                &stderr,
                duration,
                outcome.session_ref,
                None,
                outcome.usage,
                outcome.denied,
                outcome.throughput,
                outcome.session_usage,
            );
        };

        let mut argv = vec![binary];
        argv.extend(prepared.args);
        self.spawn_json_lines(
            tab_id,
            msg_arc,
            argv,
            cwd,
            harness.kind().to_string(),
            handle_line,
            finish,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScratchRunner {
        root: PathBuf,
        store: StateStore,
        runner: AgentRunner,
    }

    impl ScratchRunner {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("agentsterm-runner-test-{}", rand_hex(12)));
            let store = StateStore::open_in(root.clone());
            let runner = AgentRunner::new(store.clone(), Arc::new(|_| {}));
            Self {
                root,
                store,
                runner,
            }
        }
    }

    impl Drop for ScratchRunner {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn infers_prior_session_usage_from_legacy_messages_and_invalidates_after_interruption() {
        let env = ScratchRunner::new();
        let tab = env
            .store
            .create_tab(
                "codex",
                None,
                None,
                None,
                Some(&json!("gpt-test")),
                None,
                None,
            )
            .unwrap();
        let tab_id = tab["id"].as_str().unwrap().to_string();
        let mut patch = Map::new();
        patch.insert(
            "sessionRef".into(),
            json!({ "kind": "codex", "value": "thread-legacy" }),
        );
        let tab = env.store.update_tab(&tab_id, patch).unwrap();

        env.store.add_message(
            &tab_id,
            json!({
                "id": "m1",
                "role": "assistant",
                "status": "complete",
                "usage": { "inputTokens": 5000, "outputTokens": 700, "thinkingTokens": 100 },
            }),
        );
        let inferred = env
            .runner
            .prior_session_usage_for_tab(&tab, "codex", "m_current")
            .expect("inferred baseline");
        assert_eq!(inferred.session_id, "thread-legacy");
        assert_eq!(inferred.output_tokens, Some(700));
        assert!(inferred.valid);

        // A subsequent interrupted turn invalidates the inferred baseline.
        env.store.add_message(
            &tab_id,
            json!({
                "id": "m2",
                "role": "assistant",
                "status": "interrupted",
                "usage": { "durationMs": 12000 },
            }),
        );
        let invalidated = env
            .runner
            .prior_session_usage_for_tab(&tab, "codex", "m_current")
            .expect("invalidated baseline");
        assert_eq!(invalidated.output_tokens, Some(700));
        assert!(!invalidated.valid);
    }

    #[test]
    fn finalize_turn_records_pure_generation_throughput_not_wall_time() {
        let env = ScratchRunner::new();
        let tab = env
            .store
            .create_tab(
                "claude",
                None,
                None,
                None,
                Some(&json!("claude-test")),
                None,
                None,
            )
            .unwrap();
        let tab_id = tab["id"].as_str().unwrap().to_string();
        let msg = json!({
            "id": "m1",
            "tabId": tab_id,
            "role": "assistant",
            "text": "done",
            "thinking": "",
            "segments": [{ "type": "text", "text": "done" }],
            "toolCalls": [],
            "deniedActions": [],
            "status": "streaming",
            "model": "claude-test",
            "error": Value::Null,
            "usage": Value::Null,
            "createdAt": 1,
            "updatedAt": 1,
        });
        env.store.add_message(&tab_id, msg.clone());
        let msg_arc = Arc::new(Mutex::new(msg));

        // 20 minutes wall time, but only 10 seconds pure LLM generation for 800 tokens = 80 tok/s.
        env.runner.finalize_turn(
            &tab_id,
            &msg_arc,
            false,
            Some(0),
            "",
            1_200_000,
            None,
            None,
            Some(json!({
                "inputTokens": 10,
                "outputTokens": 800,
                "thinkingTokens": Value::Null,
                "costUsd": 0.1,
                "durationMs": 1_200_000,
            })),
            Vec::new(),
            Some(GenerationSample {
                output_tokens: 800,
                duration_ms: 10_000,
            }),
            None,
        );

        env.store.with_throughput(|rates| {
            assert_eq!(rates.len(), 1);
            assert_eq!(rates[0]["tokensPerSecond"], 80.0);
        });
    }
}
