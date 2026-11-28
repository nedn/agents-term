use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, io};

mod persistence;
mod throughput;
use persistence::{SaveRequest, StoreFiles};

use crate::attachments::normalize_attachments;
use crate::config::{agents_term_home, home_dir, write_atomic_file};
use crate::log::{log_error, log_warn};
use crate::util::{derive_task_summary_from_messages, now_ms, rand_hex};

pub const RESTART_INTERRUPTED_REASON: &str = "Interrupted because the VM daemon restarted";
const CLOSED_INTERRUPTED_REASON: &str = "Interrupted when tab was closed";
pub const WORKSPACE_SCHEMA_VERSION: u32 = 1;
pub const MAX_OPEN_TABS: usize = 10;
pub const MAX_CLOSED_TABS: usize = 50;
pub const CLOSED_TAB_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
const SAVE_DEBOUNCE: Duration = Duration::from_millis(250);
const PRUNE_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTabError {
    InvalidCwd(String),
    InvalidShellMode(String),
    OpenTabLimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviveTabError {
    NotFound,
    OpenTabLimitReached,
}

pub fn default_agent_label(agent: &str) -> &str {
    if let Some(harness) = crate::harnesses::get(agent) {
        return harness.label();
    }
    match agent {
        "shell" => "Shell",
        other => other,
    }
}

fn strip_or_none(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn normalize_queued_messages(raw: Option<&Value>) -> Vec<Value> {
    let Some(arr) = raw.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in arr {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(qid) = strip_or_none(obj.get("id")) else {
            continue;
        };
        let attachments = normalize_attachments(obj.get("attachments"));
        let text = strip_or_none(obj.get("text")).unwrap_or_default();
        if text.is_empty() && attachments.is_empty() {
            continue;
        }
        let created = obj
            .get("createdAt")
            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
            .unwrap_or_else(now_ms);
        let mut queued = json!({
            "id": qid,
            "text": text,
            "overridePermissionMode": strip_or_none(obj.get("overridePermissionMode")),
            "createdAt": created,
        });
        if !attachments.is_empty() {
            queued["attachments"] = Value::Array(attachments);
        }
        out.push(queued);
    }
    out
}

pub fn resolve_abs_path(raw: &str) -> String {
    let trimmed = raw.trim();
    let expanded = if trimmed == "~" {
        home_dir()
    } else if let Some(rest) = trimmed.strip_prefix("~/") {
        home_dir().join(rest)
    } else {
        std::path::PathBuf::from(trimmed)
    };
    let joined = if expanded.is_absolute() {
        expanded
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| home_dir())
            .join(expanded)
    };
    let mut normalized = std::path::PathBuf::new();
    for comp in joined.components() {
        match comp {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    if normalized.as_os_str().is_empty() {
        "/".to_string()
    } else {
        normalized.to_string_lossy().to_string()
    }
}

pub fn validate_working_dir(raw: &str) -> Result<String, String> {
    let resolved = resolve_abs_path(raw);
    match fs::metadata(&resolved) {
        Ok(meta) if meta.is_dir() => Ok(resolved),
        Ok(_) => Err(format!("Working directory is not a directory: {resolved}")),
        Err(_) => Err(format!("Working directory does not exist: {resolved}")),
    }
}

#[derive(Default)]
struct StoreInner {
    throughput: throughput::Throughput,
    tabs: Vec<Value>,
    closed_tabs: Vec<Value>,
    active_tab_id: Option<String>,
    messages_by_tab: HashMap<String, Vec<Value>>,
    revision: u64,
    index_revision: Option<u64>,
    throughput_revision: Option<u64>,
    dirty_message_tabs: HashMap<String, u64>,
    deleted_tab_dirs: HashSet<String>,
}

impl StoreInner {
    fn mark_throughput_dirty(&mut self) {
        self.revision += 1;
        self.throughput_revision = Some(self.revision);
    }

    fn mark_index_dirty(&mut self) {
        self.revision += 1;
        self.index_revision = Some(self.revision);
    }

    fn mark_messages_dirty(&mut self, tab_id: &str) {
        self.revision += 1;
        self.dirty_message_tabs
            .insert(tab_id.to_owned(), self.revision);
    }
}

fn value_as_i64(v: Option<&Value>) -> Option<i64> {
    v.and_then(|val| val.as_i64().or_else(|| val.as_f64().map(|f| f as i64)))
}

fn prune_closed_tabs_locked(inner: &mut StoreInner, now: i64) -> bool {
    let open_ids: HashSet<String> = inner
        .tabs
        .iter()
        .filter_map(|t| t.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();

    inner.closed_tabs.sort_by(|a, b| {
        let ca = value_as_i64(a.get("closedAt")).unwrap_or(0);
        let cb = value_as_i64(b.get("closedAt")).unwrap_or(0);
        cb.cmp(&ca)
    });

    let before_len = inner.closed_tabs.len();
    let mut seen: HashSet<String> = HashSet::new();
    let mut kept: Vec<Value> = Vec::with_capacity(inner.closed_tabs.len().min(MAX_CLOSED_TABS));
    let mut discarded_ids: Vec<String> = Vec::new();

    for tab in inner.closed_tabs.drain(..) {
        let tab_id = tab
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("")
            .to_string();
        let closed_at = value_as_i64(tab.get("closedAt")).unwrap_or(0);
        let expired = closed_at <= 0 || now.saturating_sub(closed_at) >= CLOSED_TAB_RETENTION_MS;
        let keep = !tab_id.is_empty()
            && !open_ids.contains(&tab_id)
            && !expired
            && kept.len() < MAX_CLOSED_TABS
            && seen.insert(tab_id.clone());
        if keep {
            kept.push(tab);
        } else if !tab_id.is_empty() {
            discarded_ids.push(tab_id);
        }
    }

    let mut changed = kept.len() != before_len || !discarded_ids.is_empty();
    inner.closed_tabs = kept;

    for did in discarded_ids {
        if !open_ids.contains(&did) && !seen.contains(&did) {
            if inner.messages_by_tab.remove(&did).is_some() {
                changed = true;
            }
            inner.dirty_message_tabs.remove(&did);
            inner.deleted_tab_dirs.insert(did);
        }
    }
    if changed {
        inner.mark_index_dirty();
    }
    changed
}

fn tab_with_disk_state(tab: &Value, state: &str) -> Value {
    let mut cloned = tab.clone();
    if let Some(obj) = cloned.as_object_mut() {
        obj.insert("state".into(), json!(state));
        if state == "open" {
            obj.remove("closedAt");
        }
    }
    cloned
}

#[derive(Clone)]
pub struct StateStore {
    inner: Arc<Mutex<StoreInner>>,
    writer: mpsc::SyncSender<SaveRequest>,
}

impl StateStore {
    pub fn new() -> Self {
        Self::open(StoreFiles::new(agents_term_home()), write_atomic_file)
    }

    #[cfg(test)]
    pub(crate) fn open_in(root: std::path::PathBuf) -> Self {
        Self::open(StoreFiles::new(root), write_atomic_file)
    }

    fn open(
        files: StoreFiles,
        write: impl Fn(&Path, &[u8], u32) -> io::Result<()> + Send + 'static,
    ) -> Self {
        let (writer, requests) = mpsc::sync_channel(1);
        let store = Self {
            inner: Arc::default(),
            writer,
        };
        let recovered = store.load(&files);
        let inner = store.inner.clone();
        std::thread::spawn(move || {
            persistence::run_writer(requests, || files.save(&inner, &write))
        });
        if recovered {
            if let Err(err) = store.save_now() {
                log_error(&format!("Startup recovery save failed: {err}"));
            }
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let prune_clone = store.clone();
            handle.spawn(async move {
                prune_clone.prune_loop().await;
            });
        }
        store
    }

    async fn prune_loop(&self) {
        let mut interval = tokio::time::interval(PRUNE_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            self.prune_expired_closed_tabs();
            let mut inner = self.inner.lock().unwrap();
            if inner.throughput.prune(now_ms()) {
                inner.mark_throughput_dirty();
                self.schedule_save();
            }
        }
    }

    pub fn prune_expired_closed_tabs(&self) -> bool {
        let changed = {
            let mut inner = self.inner.lock().unwrap();
            prune_closed_tabs_locked(&mut inner, now_ms())
        };
        if changed {
            self.schedule_save();
        }
        changed
    }

    fn load(&self, files: &StoreFiles) -> bool {
        let index_path = files.index_path();
        let ws_path = files.workspace_path();
        let now = now_ms();
        let (mut throughput, mut recovered_any) = match fs::read(files.throughput_path()) {
            Ok(raw) => match serde_json::from_slice::<throughput::Throughput>(&raw) {
                Ok(parsed) => (parsed, false),
                // Discard legacy v1 samples recorded with wall-clock / cumulative formulas
                // and rewrite the file cleanly in v2 format.
                Err(_) => (throughput::Throughput::default(), true),
            },
            Err(_) => (throughput::Throughput::default(), false),
        };
        recovered_any |= throughput.prune(now);
        {
            let mut inner = self.inner.lock().unwrap();
            inner.throughput = throughput;
            if recovered_any {
                inner.mark_throughput_dirty();
            }
        }
        if !index_path.exists() && !ws_path.exists() {
            return recovered_any;
        }
        let mut index_dirty = false;
        let mut dirty_message_tabs: HashSet<String> = HashSet::new();

        let workspace = fs::read_to_string(&ws_path)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .unwrap_or_default();
        let saved_active_tab_id = strip_or_none(workspace.get("activeTabId"));
        let mut tabs: Vec<Value> = Vec::new();
        let mut closed_tabs: Vec<Value> = Vec::new();

        if let Ok(raw_lines) = fs::read_to_string(&index_path) {
            for (line_no, line) in raw_lines.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(trimmed) else {
                    log_warn(&format!(
                        "Ignoring malformed line {} in {}",
                        line_no + 1,
                        index_path.display()
                    ));
                    continue;
                };
                let state_field = obj
                    .remove("state")
                    .and_then(|v| v.as_str().map(str::to_string));
                let is_closed = state_field.as_deref() == Some("closed")
                    || (state_field.is_none() && obj.contains_key("closedAt"));

                if is_closed {
                    obj.insert("queuedMessages".into(), Value::Array(Vec::new()));
                    obj.insert("unread".into(), json!(false));
                    if obj.get("status").and_then(Value::as_str) == Some("working") {
                        obj.insert("status".into(), json!("idle"));
                        obj.insert("statusDetail".into(), Value::Null);
                        recovered_any = true;
                        index_dirty = true;
                    }
                    if value_as_i64(obj.get("closedAt")).is_none() {
                        let fallback = value_as_i64(obj.get("updatedAt")).unwrap_or(now);
                        obj.insert("closedAt".into(), json!(fallback));
                        recovered_any = true;
                        index_dirty = true;
                    }
                    closed_tabs.push(Value::Object(obj));
                } else {
                    obj.remove("closedAt");
                    let queued = normalize_queued_messages(obj.get("queuedMessages"));
                    obj.insert("queuedMessages".into(), Value::Array(queued));
                    if obj.get("status").and_then(Value::as_str) == Some("working") {
                        let tab_id = obj
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_string();
                        log_warn(&format!(
                            "Startup recovery: resetting working tab {tab_id} to idle after daemon restart"
                        ));
                        obj.insert("status".into(), json!("idle"));
                        obj.insert("statusDetail".into(), Value::Null);
                        obj.insert("updatedAt".into(), json!(now));
                        recovered_any = true;
                        index_dirty = true;
                    }
                    tabs.push(Value::Object(obj));
                }
            }
        }

        let active_tab_id = saved_active_tab_id
            .filter(|id| {
                tabs.iter()
                    .any(|t| t.get("id").and_then(Value::as_str) == Some(id.as_str()))
            })
            .or_else(|| {
                tabs.first()
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });

        let mut known_ids: HashSet<String> = HashSet::new();
        for tab in tabs.iter().chain(closed_tabs.iter()) {
            if let Some(tid) = tab.get("id").and_then(Value::as_str) {
                if !tid.is_empty() {
                    known_ids.insert(tid.to_string());
                }
            }
        }

        let mut messages_by_tab: HashMap<String, Vec<Value>> = HashMap::new();
        for tab_id in &known_ids {
            let Some(mpath) = files.messages_path(tab_id) else {
                continue;
            };
            if !mpath.exists() {
                messages_by_tab.insert(tab_id.clone(), Vec::new());
                continue;
            }
            let Ok(raw_msgs) = fs::read_to_string(&mpath) else {
                messages_by_tab.insert(tab_id.clone(), Vec::new());
                continue;
            };
            let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(&raw_msgs) else {
                log_warn(&format!(
                    "Ignoring malformed messages file at {}",
                    mpath.display()
                ));
                messages_by_tab.insert(tab_id.clone(), Vec::new());
                continue;
            };
            let mut msgs = Vec::with_capacity(arr.len());
            for item in arr {
                let Some(mut obj) = item.as_object().cloned() else {
                    continue;
                };
                if obj.get("status").and_then(Value::as_str) == Some("streaming") {
                    let msg_id = obj
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    let has_err = obj
                        .get("error")
                        .and_then(Value::as_str)
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false);
                    obj.insert("status".into(), json!("interrupted"));
                    if !has_err {
                        obj.insert("error".into(), json!(RESTART_INTERRUPTED_REASON));
                    }
                    obj.insert("updatedAt".into(), json!(now));
                    log_warn(&format!(
                        "Startup recovery: marked streaming message {msg_id} in tab {tab_id} as interrupted ({RESTART_INTERRUPTED_REASON})"
                    ));
                    dirty_message_tabs.insert(tab_id.clone());
                    recovered_any = true;
                }
                msgs.push(Value::Object(obj));
            }
            messages_by_tab.insert(tab_id.clone(), msgs);
        }

        for tab in tabs.iter_mut().chain(closed_tabs.iter_mut()) {
            if let Some(obj) = tab.as_object_mut() {
                let existing = strip_or_none(obj.get("taskSummary"));
                let summary = existing.clone().or_else(|| {
                    let tid = obj.get("id").and_then(Value::as_str).unwrap_or("");
                    messages_by_tab
                        .get(tid)
                        .and_then(|msgs| derive_task_summary_from_messages(msgs, 160))
                });
                if existing.is_none() && summary.is_some() {
                    index_dirty = true;
                    recovered_any = true;
                }
                obj.insert(
                    "taskSummary".into(),
                    summary.map(Value::String).unwrap_or(Value::Null),
                );
            }
        }

        let mut deleted_tab_dirs: HashSet<String> = HashSet::new();
        if let Ok(entries) = fs::read_dir(&files.root) {
            for entry in entries.flatten() {
                let Ok(ft) = entry.file_type() else { continue };
                if !ft.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if !known_ids.contains(&name) {
                    deleted_tab_dirs.insert(name);
                    recovered_any = true;
                }
            }
        }

        let mut inner = self.inner.lock().unwrap();
        inner.tabs = tabs;
        inner.closed_tabs = closed_tabs;
        inner.active_tab_id = active_tab_id;
        inner.messages_by_tab = messages_by_tab;
        if index_dirty {
            inner.mark_index_dirty();
        }
        for id in dirty_message_tabs {
            inner.mark_messages_dirty(&id);
        }
        inner.deleted_tab_dirs = deleted_tab_dirs;
        if prune_closed_tabs_locked(&mut inner, now) {
            recovered_any = true;
        }
        recovered_any
    }

    fn schedule_save(&self) {
        // A queued request already covers all mutations made before this call.
        if let Err(mpsc::TrySendError::Disconnected(_)) =
            self.writer.try_send(SaveRequest::Scheduled)
        {
            log_error("Store writer stopped");
        }
    }

    pub fn save_now(&self) -> io::Result<()> {
        let (reply, result) = mpsc::channel();
        self.writer
            .send(SaveRequest::Flush(reply))
            .map_err(io::Error::other)?;
        result.recv().map_err(io::Error::other)?
    }

    pub fn with_throughput(&self, send: impl FnOnce(Vec<Value>)) {
        let inner = self.inner.lock().unwrap();
        send(inner.throughput.rates(now_ms()));
    }

    pub fn record_throughput(
        &self,
        agent: &str,
        model: &str,
        tokens: u64,
        duration_ms: u64,
        send: impl FnOnce(Vec<Value>),
    ) {
        if tokens == 0 || duration_ms == 0 {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        let now = now_ms();
        inner.throughput.prune(now);
        inner
            .throughput
            .record(agent, model, now, tokens, duration_ms);
        inner.mark_throughput_dirty();
        send(inner.throughput.rates(now));
        self.schedule_save();
    }

    pub fn list_tabs(&self) -> Value {
        let (result, pruned) = {
            let mut inner = self.inner.lock().unwrap();
            let pruned = prune_closed_tabs_locked(&mut inner, now_ms());
            let active = inner.active_tab_id.clone().or_else(|| {
                inner
                    .tabs
                    .first()
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
            (
                json!({
                    "tabs": inner.tabs,
                    "activeTabId": active,
                    "closedTabs": inner.closed_tabs,
                }),
                pruned,
            )
        };
        if pruned {
            self.schedule_save();
        }
        result
    }

    pub fn list_closed_tabs(&self) -> Vec<Value> {
        let (closed, pruned) = {
            let mut inner = self.inner.lock().unwrap();
            let pruned = prune_closed_tabs_locked(&mut inner, now_ms());
            (inner.closed_tabs.clone(), pruned)
        };
        if pruned {
            self.schedule_save();
        }
        closed
    }

    pub fn get_tab(&self, tab_id: &str) -> Option<Value> {
        let inner = self.inner.lock().unwrap();
        inner
            .tabs
            .iter()
            .find(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))
            .cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_tab(
        &self,
        agent: &str,
        title: Option<&Value>,
        cwd: Option<&Value>,
        permission_mode: Option<&Value>,
        model: Option<&Value>,
        system_prompt: Option<&Value>,
        shell_mode: Option<&Value>,
    ) -> Result<Value, CreateTabError> {
        let now = now_ms();
        let tab_id = format!("tab_{}", rand_hex(6));
        let resolved_shell_mode = if agent == "shell" {
            match strip_or_none(shell_mode).as_deref() {
                None | Some("command") => Some("command".to_string()),
                Some("pty") => Some("pty".to_string()),
                Some(other) => {
                    return Err(CreateTabError::InvalidShellMode(format!(
                        "Unsupported shellMode: {other} (expected \"command\" or \"pty\")"
                    )));
                }
            }
        } else {
            None
        };
        let resolved_cwd = match strip_or_none(cwd) {
            Some(raw_cwd) => validate_working_dir(&raw_cwd).map_err(CreateTabError::InvalidCwd)?,
            None => resolve_abs_path(&home_dir().to_string_lossy()),
        };
        let folder = Path::new(&resolved_cwd)
            .file_name()
            .and_then(|s| s.to_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(&resolved_cwd)
            .to_string();
        let default_label = if resolved_shell_mode.as_deref() == Some("pty") {
            "Terminal"
        } else {
            default_agent_label(agent)
        };
        let resolved_title =
            strip_or_none(title).unwrap_or_else(|| format!("{default_label} · {folder}"));
        let resolved_perm =
            strip_or_none(permission_mode).unwrap_or_else(|| "full-access".to_string());
        let resolved_model = strip_or_none(model);
        let resolved_sp = if agent == "shell" {
            None
        } else {
            strip_or_none(system_prompt)
        };

        let mut tab = json!({
            "id": tab_id,
            "title": resolved_title,
            "agent": agent,
            "cwd": resolved_cwd,
            "permissionMode": resolved_perm,
            "model": resolved_model,
            "systemPrompt": resolved_sp,
            "status": "idle",
            "statusDetail": Value::Null,
            "sessionRef": Value::Null,
            "unread": false,
            "lastMessagePreview": Value::Null,
            "taskSummary": Value::Null,
            "queuedMessages": [],
            "createdAt": now,
            "updatedAt": now,
        });
        if let Some(mode) = resolved_shell_mode {
            if let Some(obj) = tab.as_object_mut() {
                obj.insert("shellMode".into(), Value::String(mode));
            }
        }

        {
            let mut inner = self.inner.lock().unwrap();
            if inner.tabs.len() >= MAX_OPEN_TABS {
                return Err(CreateTabError::OpenTabLimitReached);
            }
            inner.tabs.push(tab.clone());
            inner.messages_by_tab.insert(tab_id.clone(), Vec::new());
            inner.active_tab_id = Some(tab_id.clone());
            inner.mark_index_dirty();
            inner.mark_messages_dirty(&tab_id);
            inner.deleted_tab_dirs.remove(&tab_id);
        }
        self.schedule_save();
        Ok(tab)
    }

    pub fn update_tab(&self, tab_id: &str, patch: Map<String, Value>) -> Option<Value> {
        let updated = {
            let mut inner = self.inner.lock().unwrap();
            let tab = inner
                .tabs
                .iter_mut()
                .find(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))?;
            let obj = tab.as_object_mut()?;
            for (k, v) in patch {
                obj.insert(k, v);
            }
            obj.insert("id".into(), json!(tab_id));
            obj.insert("updatedAt".into(), json!(now_ms()));
            let cloned = tab.clone();
            inner.mark_index_dirty();
            cloned
        };
        self.schedule_save();
        Some(updated)
    }

    pub fn enqueue_message(
        &self,
        tab_id: &str,
        text: &str,
        override_permission_mode: Option<&str>,
        attachments: &[Value],
    ) -> Option<(Value, Value)> {
        let tab = self.get_tab(tab_id)?;
        let mut item = json!({
            "id": format!("q_{}", rand_hex(6)),
            "text": text,
            "overridePermissionMode": override_permission_mode.map(str::trim).filter(|s| !s.is_empty()),
            "createdAt": now_ms(),
        });
        if !attachments.is_empty() {
            item["attachments"] = Value::Array(attachments.to_vec());
        }
        let mut queued = tab
            .get("queuedMessages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        queued.push(item.clone());
        let mut patch = Map::new();
        patch.insert("queuedMessages".into(), Value::Array(queued));
        let updated = self.update_tab(tab_id, patch)?;
        Some((updated, item))
    }

    pub fn remove_queued_message(&self, tab_id: &str, queued_id: &str) -> Option<Value> {
        let tab = self.get_tab(tab_id)?;
        let queued: Vec<Value> = tab
            .get("queuedMessages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|m| m.get("id").and_then(Value::as_str) != Some(queued_id))
            .collect();
        let mut patch = Map::new();
        patch.insert("queuedMessages".into(), Value::Array(queued));
        self.update_tab(tab_id, patch)
    }

    pub fn pop_next_queued_message(&self, tab_id: &str) -> Option<(Value, Value)> {
        let result = {
            let mut inner = self.inner.lock().unwrap();
            let tab = inner
                .tabs
                .iter_mut()
                .find(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))?;
            let obj = tab.as_object_mut()?;
            let queued = obj.get_mut("queuedMessages")?.as_array_mut()?;
            if queued.is_empty() {
                return None;
            }
            let item = queued.remove(0);
            obj.insert("updatedAt".into(), json!(now_ms()));
            let updated_tab = tab.clone();
            inner.mark_index_dirty();
            (updated_tab, item)
        };
        self.schedule_save();
        Some(result)
    }

    pub fn pop_queued_messages(&self, tab_id: &str) -> Option<(Value, Vec<Value>)> {
        let tab = self.get_tab(tab_id)?;
        let popped = tab
            .get("queuedMessages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if popped.is_empty() {
            return Some((tab, Vec::new()));
        }
        let mut patch = Map::new();
        patch.insert("queuedMessages".into(), Value::Array(Vec::new()));
        let updated = self.update_tab(tab_id, patch)?;
        Some((updated, popped))
    }

    pub fn delete_tab(&self, tab_id: &str) -> Option<Value> {
        let archived = {
            let mut inner = self.inner.lock().unwrap();
            let pos = inner
                .tabs
                .iter()
                .position(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))?;
            let mut tab = inner.tabs.remove(pos);
            if inner.active_tab_id.as_deref() == Some(tab_id) {
                inner.active_tab_id = inner
                    .tabs
                    .last()
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            let now = now_ms();
            let mut interrupted_any = false;
            if let Some(msgs) = inner.messages_by_tab.get_mut(tab_id) {
                for msg in msgs.iter_mut() {
                    if let Some(obj) = msg.as_object_mut() {
                        if obj.get("status").and_then(Value::as_str) == Some("streaming") {
                            let has_err = obj
                                .get("error")
                                .and_then(Value::as_str)
                                .map(|s| !s.trim().is_empty())
                                .unwrap_or(false);
                            obj.insert("status".into(), json!("interrupted"));
                            if !has_err {
                                obj.insert("error".into(), json!(CLOSED_INTERRUPTED_REASON));
                            }
                            obj.insert("updatedAt".into(), json!(now));
                            interrupted_any = true;
                        }
                    }
                }
            }
            if interrupted_any {
                inner.mark_messages_dirty(tab_id);
            }
            if let Some(obj) = tab.as_object_mut() {
                if obj.get("status").and_then(Value::as_str) == Some("working") {
                    obj.insert("status".into(), json!("idle"));
                    obj.insert("statusDetail".into(), Value::Null);
                }
                obj.insert("unread".into(), json!(false));
                obj.insert("queuedMessages".into(), Value::Array(Vec::new()));
                obj.insert("closedAt".into(), json!(now));
            }
            inner
                .closed_tabs
                .retain(|t| t.get("id").and_then(Value::as_str) != Some(tab_id));
            inner.closed_tabs.insert(0, tab.clone());
            inner.mark_index_dirty();
            prune_closed_tabs_locked(&mut inner, now);
            tab
        };
        self.schedule_save();
        Some(archived)
    }

    pub fn revive_tab(&self, tab_id: &str) -> Result<Value, ReviveTabError> {
        let now = now_ms();
        let outcome = {
            let mut inner = self.inner.lock().unwrap();
            let pruned = prune_closed_tabs_locked(&mut inner, now);
            let Some(pos) = inner
                .closed_tabs
                .iter()
                .position(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))
            else {
                return if pruned {
                    drop(inner);
                    self.schedule_save();
                    Err(ReviveTabError::NotFound)
                } else {
                    Err(ReviveTabError::NotFound)
                };
            };
            if inner.tabs.len() >= MAX_OPEN_TABS {
                return if pruned {
                    drop(inner);
                    self.schedule_save();
                    Err(ReviveTabError::OpenTabLimitReached)
                } else {
                    Err(ReviveTabError::OpenTabLimitReached)
                };
            }
            let mut tab = inner.closed_tabs.remove(pos);
            if let Some(obj) = tab.as_object_mut() {
                obj.remove("closedAt");
                if obj.get("status").and_then(Value::as_str) == Some("working") {
                    obj.insert("status".into(), json!("idle"));
                    obj.insert("statusDetail".into(), Value::Null);
                }
                obj.insert("unread".into(), json!(false));
                if !obj.contains_key("queuedMessages") {
                    obj.insert("queuedMessages".into(), Value::Array(Vec::new()));
                }
                obj.insert("updatedAt".into(), json!(now));
            }
            inner.tabs.push(tab.clone());
            inner.active_tab_id = Some(tab_id.to_string());
            inner.messages_by_tab.entry(tab_id.to_string()).or_default();
            inner.mark_index_dirty();
            inner.deleted_tab_dirs.remove(tab_id);
            Ok(tab)
        };
        if outcome.is_ok() {
            self.schedule_save();
        }
        outcome
    }

    pub fn get_messages(&self, tab_id: &str) -> Vec<Value> {
        let inner = self.inner.lock().unwrap();
        inner
            .messages_by_tab
            .get(tab_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn add_message(&self, tab_id: &str, msg: Value) {
        let saved = {
            let mut inner = self.inner.lock().unwrap();
            if !inner
                .tabs
                .iter()
                .any(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))
            {
                false
            } else {
                inner
                    .messages_by_tab
                    .entry(tab_id.to_string())
                    .or_default()
                    .push(msg);
                inner.mark_messages_dirty(tab_id);
                true
            }
        };
        if saved {
            self.schedule_save();
        }
    }

    pub fn replace_message(&self, tab_id: &str, updated: Value) {
        let target_id = updated
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let saved = {
            let mut inner = self.inner.lock().unwrap();
            if !inner
                .tabs
                .iter()
                .any(|t| t.get("id").and_then(Value::as_str) == Some(tab_id))
            {
                false
            } else {
                let msgs = inner.messages_by_tab.entry(tab_id.to_string()).or_default();
                if let Some(ref tid) = target_id {
                    if let Some(existing) = msgs
                        .iter_mut()
                        .find(|m| m.get("id").and_then(Value::as_str) == Some(tid.as_str()))
                    {
                        *existing = updated;
                    } else {
                        msgs.push(updated);
                    }
                } else {
                    msgs.push(updated);
                }
                inner.mark_messages_dirty(tab_id);
                true
            }
        };
        if saved {
            self.schedule_save();
        }
    }
}

#[cfg(test)]
mod persistence_tests;
