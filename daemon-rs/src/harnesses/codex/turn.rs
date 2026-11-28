//! Turn execution for the Codex harness: CLI arguments and stream-json event parsing.

use serde_json::{json, Value};
use std::collections::HashMap;

use crate::harnesses::turn::{
    cumulative_delta, session_ref_value, toml_basic_string, truncate_chars, union_interval_ms,
    value_as_u64, GenerationSample, PreparedTurn, SessionUsageBaseline, TurnOutcome, TurnParser,
    TurnRequest, TurnSink, MAX_TOOL_OUTPUT,
};
use crate::util::{js_str, json_pretty, now_ms, rand_hex, summarize_tool_input, truncate};

/// Builds the CLI arguments for one turn and the parser for its output.
pub(crate) fn prepare_turn(req: TurnRequest) -> PreparedTurn {
    let TurnRequest {
        tab,
        prompt,
        cwd,
        permission_mode,
        system_prompt,
        prior_session_usage,
        ..
    } = req;

    let session = tab.get("sessionRef").cloned().filter(|v| !v.is_null());
    let resume_id = session
        .as_ref()
        .and_then(Value::as_object)
        .filter(|o| o.get("kind").and_then(Value::as_str) == Some("codex"))
        .and_then(|o| o.get("value").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let is_resume = resume_id.is_some();

    let mut args = if is_resume {
        vec![
            "exec".to_string(),
            "resume".to_string(),
            "--json".to_string(),
            "--skip-git-repo-check".to_string(),
        ]
    } else {
        vec![
            "exec".to_string(),
            "--json".to_string(),
            "--skip-git-repo-check".to_string(),
            "-C".to_string(),
            cwd.clone(),
        ]
    };

    if permission_mode == "full-access" {
        args.push("--dangerously-bypass-approvals-and-sandbox".to_string());
    } else if !is_resume {
        args.push("-s".to_string());
        args.push(if permission_mode == "plan" {
            "read-only".to_string()
        } else {
            "workspace-write".to_string()
        });
    }
    if let Some(model) = tab
        .get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        args.push("-m".to_string());
        args.push(model.to_string());
    }
    args.push("-c".to_string());
    args.push(format!(
        "developer_instructions={}",
        toml_basic_string(&system_prompt)
    ));
    if let Some(ref tid) = resume_id {
        args.push(tid.clone());
        args.push(prompt);
    } else {
        args.push(prompt);
    }

    let parser = CodexTurn {
        session_ref: session,
        is_resume,
        prior_session_usage,
        raw_usage: None,
        started_at: now_ms(),
        turn_started_at: None,
        turn_completed_at: None,
        tool_starts: HashMap::new(),
        active_tool_starts: HashMap::new(),
        tool_intervals: Vec::new(),
    };
    PreparedTurn {
        args,
        status: "Codex is working…",
        parser: Box::new(parser),
    }
}

struct CodexTurn {
    session_ref: Option<Value>,
    is_resume: bool,
    prior_session_usage: Option<SessionUsageBaseline>,
    raw_usage: Option<(Option<u64>, Option<u64>, Option<u64>)>,
    started_at: i64,
    turn_started_at: Option<i64>,
    turn_completed_at: Option<i64>,
    tool_starts: HashMap<String, i64>,
    active_tool_starts: HashMap<String, i64>,
    tool_intervals: Vec<(i64, i64)>,
}

impl TurnParser for CodexTurn {
    fn handle_line(&mut self, line: &str, sink: &TurnSink) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }
        let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(trimmed) else {
            return;
        };
        let ev_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        if ev_type == "thread.started" {
            if let Some(thread_id) = obj
                .get("thread_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                sink.persist_session_ref(&mut self.session_ref, "codex", thread_id);
            }
        } else if ev_type == "turn.started" {
            self.turn_started_at.get_or_insert_with(now_ms);
        } else if matches!(ev_type, "item.started" | "item.updated" | "item.completed") {
            if let Some(item) = obj.get("item").and_then(Value::as_object) {
                let item_id = item
                    .get("id")
                    .filter(|v| !v.is_null())
                    .map(js_str)
                    .unwrap_or_else(|| format!("item_{}", rand_hex(4)));
                let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
                let completed = ev_type == "item.completed";
                if !item_type.is_empty() && !matches!(item_type, "reasoning" | "agent_message") {
                    if ev_type == "item.started" {
                        self.active_tool_starts
                            .entry(item_id.clone())
                            .or_insert_with(now_ms);
                    } else if completed {
                        if let Some(s) = self.active_tool_starts.remove(&item_id) {
                            self.tool_intervals.push((s, now_ms()));
                        }
                    }
                }
                if item_type == "reasoning" && completed {
                    if let Some(t) = item.get("text").and_then(Value::as_str) {
                        let has_prev = sink
                            .message()
                            .get("thinking")
                            .and_then(Value::as_str)
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        let prefix = if has_prev { "\n" } else { "" };
                        sink.emit_delta(None, Some(format!("{prefix}{t}")), false);
                    }
                } else if item_type == "agent_message" && completed {
                    if let Some(t) = item.get("text").and_then(Value::as_str) {
                        let has_prev = sink
                            .message()
                            .get("text")
                            .and_then(Value::as_str)
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        let prefix = if has_prev { "\n\n" } else { "" };
                        sink.emit_delta(Some(format!("{prefix}{t}")), None, false);
                    }
                } else if item_type == "command_execution" {
                    let cmd = item
                        .get("command")
                        .filter(|v| !v.is_null())
                        .map(js_str)
                        .unwrap_or_else(|| "command".to_string());
                    let cmd_summary = summarize_tool_input(Some(&json!({ "command": cmd })));
                    if ev_type == "item.started" {
                        self.tool_starts.insert(item_id.clone(), now_ms());
                        sink.set_status_detail(&format!("$ {}", truncate(&cmd_summary, 50)));
                    }
                    let exit_code = item.get("exit_code").and_then(Value::as_i64);
                    let agg = item
                        .get("aggregated_output")
                        .and_then(Value::as_str)
                        .map(|s| truncate_chars(s, MAX_TOOL_OUTPUT));
                    let dur = if completed {
                        self.tool_starts.get(&item_id).map(|s| json!(now_ms() - s))
                    } else {
                        None
                    };
                    let status_str = if completed {
                        if exit_code.map(|c| c != 0).unwrap_or(false) {
                            "error"
                        } else {
                            "done"
                        }
                    } else {
                        "running"
                    };
                    sink.upsert_tool_call(json!({
                        "id": item_id,
                        "name": "Bash",
                        "inputSummary": cmd_summary,
                        "inputJson": json_pretty(&json!({ "command": cmd })),
                        "output": agg,
                        "status": status_str,
                        "durationMs": dur,
                    }));
                }
            }
        } else if ev_type == "turn.completed" {
            self.turn_completed_at = Some(now_ms());
            if let Some(u) = obj.get("usage").and_then(Value::as_object) {
                self.raw_usage = Some((
                    value_as_u64(u.get("input_tokens")),
                    value_as_u64(u.get("output_tokens")),
                    value_as_u64(u.get("reasoning_output_tokens")),
                ));
            }
        } else if ev_type == "error" {
            if let Some(err_msg) = obj.get("message").and_then(Value::as_str) {
                sink.message()
                    .as_object_mut()
                    .unwrap()
                    .insert("error".into(), json!(err_msg));
            }
        }
    }

    fn finish(mut self: Box<Self>) -> TurnOutcome {
        let finished_at = self.turn_completed_at.unwrap_or_else(now_ms);
        let window_start = self.turn_started_at.unwrap_or(self.started_at);
        for (_, start) in self.active_tool_starts.drain() {
            self.tool_intervals.push((start, finished_at));
        }

        let session_id = session_ref_value(self.session_ref.as_ref());
        let matching_prior = self
            .prior_session_usage
            .as_ref()
            .filter(|b| Some(b.session_id.as_str()) == session_id.as_deref());

        let (usage, throughput, session_usage) = if let Some((raw_in, raw_out, raw_think)) =
            self.raw_usage
        {
            let turn_in = cumulative_delta(raw_in, matching_prior.and_then(|p| p.input_tokens));
            let turn_out = cumulative_delta(raw_out, matching_prior.and_then(|p| p.output_tokens));
            let turn_think =
                cumulative_delta(raw_think, matching_prior.and_then(|p| p.thinking_tokens));
            let baseline_clean = match matching_prior {
                Some(p) => p.valid,
                None => !self.is_resume,
            };
            let usage = Some(json!({
                "inputTokens": turn_in,
                "outputTokens": turn_out,
                "thinkingTokens": turn_think,
                "costUsd": Value::Null,
                "durationMs": Value::Null,
            }));
            let throughput = if baseline_clean {
                turn_out
                    .filter(|&tokens| tokens > 0)
                    .and_then(|output_tokens| {
                        let elapsed_ms = (finished_at - window_start).max(0) as u64;
                        let tool_ms =
                            union_interval_ms(&self.tool_intervals, window_start, finished_at);
                        let active_ms = elapsed_ms.saturating_sub(tool_ms);
                        (active_ms > 0).then_some(GenerationSample {
                            output_tokens,
                            duration_ms: active_ms,
                        })
                    })
            } else {
                None
            };
            let updated_baseline = session_id.map(|sid| SessionUsageBaseline {
                session_id: sid,
                input_tokens: raw_in,
                output_tokens: raw_out,
                thinking_tokens: raw_think,
                valid: true,
            });
            (usage, throughput, updated_baseline)
        } else {
            let invalidated = session_id.map(|sid| SessionUsageBaseline {
                session_id: sid,
                input_tokens: matching_prior.and_then(|p| p.input_tokens),
                output_tokens: matching_prior.and_then(|p| p.output_tokens),
                thinking_tokens: matching_prior.and_then(|p| p.thinking_tokens),
                valid: false,
            });
            (None, None, invalidated)
        };

        TurnOutcome {
            session_ref: self.session_ref,
            usage,
            denied: Vec::new(),
            throughput,
            session_usage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harnesses::turn::{test_request, TestTurnEnv};

    #[test]
    fn new_session_arguments() {
        let turn = prepare_turn(test_request(json!({ "id": "tab_test" }), "plan", false));
        assert_eq!(
            turn.args,
            [
                "exec",
                "--json",
                "--skip-git-repo-check",
                "-C",
                "/work",
                "-s",
                "read-only",
                "-c",
                "developer_instructions=\"SYS\"",
                "hello",
            ]
        );
    }

    #[test]
    fn resume_arguments() {
        let tab = json!({
            "id": "tab_test",
            "model": "gpt-x",
            "sessionRef": { "kind": "codex", "value": "thread-1" },
        });
        let turn = prepare_turn(test_request(tab, "default", false));
        assert_eq!(
            turn.args,
            [
                "exec",
                "resume",
                "--json",
                "--skip-git-repo-check",
                "-m",
                "gpt-x",
                "-c",
                "developer_instructions=\"SYS\"",
                "thread-1",
                "hello",
            ]
        );
    }

    #[test]
    fn subtracts_cumulative_baseline_and_parallel_tool_intervals() {
        let env = TestTurnEnv::new("codex");
        let mut parser = CodexTurn {
            session_ref: Some(json!({ "kind": "codex", "value": "thread-1" })),
            is_resume: true,
            prior_session_usage: Some(SessionUsageBaseline {
                session_id: "thread-1".to_string(),
                input_tokens: Some(10_000),
                output_tokens: Some(1_000),
                thinking_tokens: Some(200),
                valid: true,
            }),
            raw_usage: None,
            started_at: 1000,
            turn_started_at: Some(2000),
            turn_completed_at: None,
            tool_starts: HashMap::new(),
            active_tool_starts: HashMap::new(),
            // Overlapping parallel tools covering [3000, 11000] = 8000ms union.
            tool_intervals: vec![(3000, 9000), (6000, 11000)],
        };
        parser.handle_line(
            r#"{"type":"turn.completed","usage":{"input_tokens":15000,"output_tokens":1400,"reasoning_output_tokens":350}}"#,
            &env.sink,
        );
        parser.turn_completed_at = Some(15_000);
        let outcome = Box::new(parser).finish();
        let usage = outcome.usage.expect("usage");
        assert_eq!(usage["inputTokens"], 5_000);
        assert_eq!(usage["outputTokens"], 400);
        assert_eq!(usage["thinkingTokens"], 150);
        // Window [2000, 15000] = 13000ms minus 8000ms tool union = 5000ms active LLM time.
        assert_eq!(
            outcome.throughput,
            Some(GenerationSample {
                output_tokens: 400,
                duration_ms: 5000,
            })
        );
        let baseline = outcome.session_usage.expect("baseline");
        assert_eq!(baseline.output_tokens, Some(1400));
        assert!(baseline.valid);
    }

    #[test]
    fn skips_throughput_when_resumed_baseline_was_invalidated_or_missing() {
        let env = TestTurnEnv::new("codex");
        // Resumed turn after an interrupted turn (valid=false).
        let mut parser = CodexTurn {
            session_ref: Some(json!({ "kind": "codex", "value": "thread-1" })),
            is_resume: true,
            prior_session_usage: Some(SessionUsageBaseline {
                session_id: "thread-1".to_string(),
                input_tokens: Some(1_000),
                output_tokens: Some(500),
                thinking_tokens: None,
                valid: false,
            }),
            raw_usage: None,
            started_at: 1000,
            turn_started_at: Some(2000),
            turn_completed_at: None,
            tool_starts: HashMap::new(),
            active_tool_starts: HashMap::new(),
            tool_intervals: Vec::new(),
        };
        parser.handle_line(
            r#"{"type":"turn.completed","usage":{"input_tokens":9000,"output_tokens":2000}}"#,
            &env.sink,
        );
        parser.turn_completed_at = Some(10_000);
        let outcome = Box::new(parser).finish();
        assert_eq!(outcome.throughput, None);
        // Baseline recovers for subsequent turns.
        let baseline = outcome.session_usage.expect("baseline");
        assert_eq!(baseline.output_tokens, Some(2000));
        assert!(baseline.valid);

        // Interrupted turn without turn.completed invalidates baseline.
        let aborted = CodexTurn {
            session_ref: Some(json!({ "kind": "codex", "value": "thread-1" })),
            is_resume: true,
            prior_session_usage: Some(baseline),
            raw_usage: None,
            started_at: 10_000,
            turn_started_at: Some(11_000),
            turn_completed_at: None,
            tool_starts: HashMap::new(),
            active_tool_starts: HashMap::new(),
            tool_intervals: Vec::new(),
        };
        let aborted_outcome = Box::new(aborted).finish();
        assert_eq!(aborted_outcome.throughput, None);
        let invalidated = aborted_outcome.session_usage.expect("invalidated baseline");
        assert_eq!(invalidated.output_tokens, Some(2000));
        assert!(!invalidated.valid);
    }
}
