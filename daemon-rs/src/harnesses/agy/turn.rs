//! Turn execution for the Antigravity harness: CLI arguments and stream-json event parsing.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

use crate::harnesses::turn::{
    cumulative_delta, denied_from, session_ref_value, tool_output_text, truncate_chars,
    union_interval_ms, value_as_u64, GenerationSample, PreparedTurn, SessionUsageBaseline,
    TurnOutcome, TurnParser, TurnRequest, TurnSink, MAX_TOOL_OUTPUT,
};
use crate::util::{
    block_separator, is_finite_number, js_round, js_str, js_truthy, json_pretty, now_ms, rand_hex,
    summarize_tool_input, BlockBreaks,
};

/// Builds the CLI arguments for one turn and the parser for its output.
pub(crate) fn prepare_turn(req: TurnRequest) -> PreparedTurn {
    let TurnRequest {
        tab,
        prompt,
        permission_mode,
        system_prompt,
        system_prompt_dirty,
        attachments_dir,
        prior_session_usage,
        ..
    } = req;

    let mut args = vec!["--output-format".to_string(), "stream-json".to_string()];
    match permission_mode.as_str() {
        "full-access" => args.push("--dangerously-skip-permissions".to_string()),
        "accept-edits" => {
            args.push("--mode".to_string());
            args.push("accept-edits".to_string());
        }
        "plan" => {
            args.push("--mode".to_string());
            args.push("plan".to_string());
        }
        _ => {
            args.push("--mode".to_string());
            args.push("auto".to_string());
        }
    }
    let session = tab.get("sessionRef").cloned().filter(|v| !v.is_null());
    let resume_val = session
        .as_ref()
        .and_then(Value::as_object)
        .filter(|o| o.get("kind").and_then(Value::as_str) == Some("agy"))
        .and_then(|o| o.get("value").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let is_resume = resume_val.is_some();
    if let Some(ref conv) = resume_val {
        args.push("--conversation".to_string());
        args.push(conv.clone());
    }
    if let Some(model) = tab
        .get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        args.push("--model".to_string());
        args.push(model.to_string());
    }
    let effective_prompt = if !is_resume || system_prompt_dirty {
        format!("<system_instructions>\n{system_prompt}\n</system_instructions>\n\n{prompt}")
    } else {
        prompt
    };
    if let Some(dir) = attachments_dir {
        args.push(format!("--add-dir={dir}"));
    }
    args.push(format!("--print={effective_prompt}"));

    let parser = AgyTurn {
        session_ref: session,
        is_resume,
        prior_session_usage,
        started_at: now_ms(),
        step_usages: BTreeMap::new(),
        synthetic_step_seq: 0,
        result_usage: None,
        saw_text: false,
        text_step: None,
        think_step: None,
        denied: Vec::new(),
        tool_starts: HashMap::new(),
        tool_intervals: Vec::new(),
        breaks: BlockBreaks::default(),
    };
    PreparedTurn {
        args,
        status: "Antigravity is thinking…",
        parser: Box::new(parser),
    }
}

#[derive(Clone, Debug, Default)]
struct AgyStepUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    thinking_tokens: Option<u64>,
    cost_usd: Option<f64>,
    duration_ms: Option<u64>,
}

struct AgyTurn {
    session_ref: Option<Value>,
    is_resume: bool,
    prior_session_usage: Option<SessionUsageBaseline>,
    started_at: i64,
    step_usages: BTreeMap<String, AgyStepUsage>,
    synthetic_step_seq: u64,
    result_usage: Option<AgyStepUsage>,
    saw_text: bool,
    text_step: Option<Value>,
    think_step: Option<Value>,
    denied: Vec<Value>,
    tool_starts: HashMap<String, i64>,
    tool_intervals: Vec<(i64, i64)>,
    breaks: BlockBreaks,
}

fn parse_agy_usage(
    raw_usage: Option<&Value>,
    duration_seconds: Option<&Value>,
    duration_ms_override: Option<&Value>,
    cost_override: Option<&Value>,
) -> Option<AgyStepUsage> {
    let u = raw_usage
        .filter(|v| js_truthy(v))
        .and_then(Value::as_object)?;
    let duration_ms = duration_seconds
        .and_then(is_finite_number)
        .and_then(|d| (d >= 0.0).then(|| js_round(d * 1000.0) as u64))
        .or_else(|| value_as_u64(duration_ms_override))
        .or_else(|| value_as_u64(u.get("duration_ms")));
    let cost_usd = cost_override
        .and_then(is_finite_number)
        .or_else(|| u.get("total_cost_usd").and_then(is_finite_number))
        .or_else(|| u.get("cost_usd").and_then(is_finite_number));
    Some(AgyStepUsage {
        input_tokens: value_as_u64(u.get("input_tokens")),
        output_tokens: value_as_u64(u.get("output_tokens")),
        thinking_tokens: value_as_u64(u.get("thinking_tokens")),
        cost_usd,
        duration_ms,
    })
}

fn sum_opt_u64(iter: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    let mut sum: Option<u64> = None;
    for val in iter.flatten() {
        sum = Some(sum.unwrap_or(0).saturating_add(val));
    }
    sum
}

fn sum_opt_f64(iter: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut sum: Option<f64> = None;
    for val in iter.flatten() {
        sum = Some(sum.unwrap_or(0.0) + val);
    }
    sum
}

impl TurnParser for AgyTurn {
    fn handle_line(&mut self, line: &str, sink: &TurnSink) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }
        let ev: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                let has_prev = !sink
                    .message()
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .is_empty();
                let delta = if has_prev {
                    format!("\n{trimmed}")
                } else {
                    trimmed.to_string()
                };
                sink.emit_delta(Some(delta), None, false);
                return;
            }
        };
        let Some(obj) = ev.as_object() else {
            return;
        };

        for key in [
            "conversation_id",
            "conversationId",
            "session_id",
            "sessionId",
        ] {
            if let Some(val) = obj
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                sink.persist_session_ref(&mut self.session_ref, "agy", val);
                break;
            }
        }

        let agy_event = obj.get("event").and_then(Value::as_str).unwrap_or("");
        if agy_event == "init" {
            if let Some(cid) = obj
                .get("init")
                .and_then(Value::as_object)
                .and_then(|i| i.get("conversation_id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                sink.persist_session_ref(&mut self.session_ref, "agy", cid);
            }
            return;
        }
        if agy_event == "step_update" {
            if let Some(step) = obj.get("step_update").and_then(Value::as_object) {
                if let Some(cid) = step
                    .get("conversation_id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    sink.persist_session_ref(&mut self.session_ref, "agy", cid);
                }
                let step_type = step.get("step_type").and_then(Value::as_str).unwrap_or("");
                let step_state = step
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_uppercase();

                let step_idx = step.get("step_index").filter(|v| !v.is_null()).cloned();

                if step_type == "agent_response" {
                    if let Some(td) = step
                        .get("thinking_delta")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    {
                        let sep = if let Some(ref idx) = step_idx {
                            let prev = self.think_step.replace(idx.clone());
                            if prev.as_ref().map(|p| p != idx).unwrap_or(false) {
                                let existing = sink
                                    .message()
                                    .get("thinking")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                block_separator(existing.as_deref())
                            } else {
                                ""
                            }
                        } else {
                            ""
                        };
                        sink.emit_delta(None, Some(format!("{sep}{td}")), false);
                    }
                    let maybe_text = step
                        .get("text_delta")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .or_else(|| {
                            if !self.saw_text {
                                step.get("text")
                                    .and_then(Value::as_str)
                                    .filter(|s| !s.is_empty())
                            } else {
                                None
                            }
                        });
                    if let Some(td) = maybe_text {
                        self.saw_text = true;
                        let sep = if let Some(ref idx) = step_idx {
                            let prev = self.text_step.replace(idx.clone());
                            if prev.as_ref().map(|p| p != idx).unwrap_or(false) {
                                let existing = sink
                                    .message()
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                block_separator(existing.as_deref())
                            } else {
                                ""
                            }
                        } else {
                            ""
                        };
                        sink.emit_delta(Some(format!("{sep}{td}")), None, false);
                    }
                    if let Some(u) = parse_agy_usage(
                        step.get("usage"),
                        step.get("duration_seconds"),
                        step.get("duration_ms"),
                        None,
                    ) {
                        let key = step_idx.as_ref().map(js_str).unwrap_or_else(|| {
                            self.synthetic_step_seq += 1;
                            format!("__anon_{}", self.synthetic_step_seq)
                        });
                        self.step_usages.insert(key, u);
                    }
                    return;
                }

                if matches!(step_type, "thinking" | "thought" | "reasoning") {
                    let think_text =
                        ["thinking_delta", "text_delta", "text"]
                            .iter()
                            .find_map(|k| {
                                step.get(*k)
                                    .and_then(Value::as_str)
                                    .filter(|s| !s.is_empty())
                            });
                    if let Some(th) = think_text {
                        let sep = if let Some(ref idx) = step_idx {
                            let prev = self.think_step.replace(idx.clone());
                            if prev.as_ref().map(|p| p != idx).unwrap_or(false) {
                                let existing = sink
                                    .message()
                                    .get("thinking")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                block_separator(existing.as_deref())
                            } else {
                                ""
                            }
                        } else {
                            ""
                        };
                        sink.emit_delta(None, Some(format!("{sep}{th}")), false);
                    }
                    return;
                }

                if step_type == "tool" {
                    let empty_map = Map::new();
                    let tool_info = step
                        .get("tool_info")
                        .and_then(Value::as_object)
                        .unwrap_or(&empty_map);
                    let tool_id = step
                        .get("tool_call_id")
                        .or_else(|| step.get("tool_use_id"))
                        .or_else(|| tool_info.get("id"))
                        .filter(|v| !v.is_null())
                        .map(js_str)
                        .unwrap_or_else(|| match &step_idx {
                            Some(idx) => format!("agy_step_{}", js_str(idx)),
                            None => format!("tool_{}", rand_hex(4)),
                        });
                    let name = step
                        .get("tool_name")
                        .or_else(|| tool_info.get("name"))
                        .filter(|v| !v.is_null())
                        .map(js_str)
                        .unwrap_or_else(|| "Tool".to_string());
                    let params = tool_info
                        .get("parameters")
                        .or_else(|| tool_info.get("input"))
                        .or_else(|| tool_info.get("args"));
                    let summary = summarize_tool_input(params);
                    let existing = sink.find_tool(&tool_id);

                    if matches!(step_state.as_str(), "ACTIVE" | "RUNNING" | "PENDING") {
                        self.tool_starts
                            .entry(tool_id.clone())
                            .or_insert_with(now_ms);
                        sink.running_status(&name, &summary);
                        let input_summary = if !summary.is_empty() {
                            summary
                        } else {
                            existing
                                .as_ref()
                                .and_then(|e| e.get("inputSummary").and_then(Value::as_str))
                                .unwrap_or("")
                                .to_string()
                        };
                        let input_json = params
                            .filter(|v| !v.is_null())
                            .map(|v| Value::String(json_pretty(v)))
                            .or_else(|| existing.as_ref().and_then(|e| e.get("inputJson").cloned()))
                            .unwrap_or(Value::Null);
                        let output = existing
                            .as_ref()
                            .and_then(|e| e.get("output").cloned())
                            .unwrap_or(Value::Null);
                        sink.upsert_tool_call(json!({
                            "id": tool_id,
                            "name": name,
                            "inputSummary": input_summary,
                            "inputJson": input_json,
                            "output": output,
                            "status": "running",
                            "durationMs": Value::Null,
                        }));
                    } else {
                        let raw_out = tool_info
                            .get("output")
                            .or_else(|| tool_info.get("result"))
                            .or_else(|| tool_info.get("error"))
                            .or_else(|| step.get("output"))
                            .or_else(|| step.get("error"));
                        let out_text = raw_out.and_then(|v| {
                            if v.is_null() {
                                None
                            } else if let Some(s) = v.as_str() {
                                Some(truncate_chars(s, MAX_TOOL_OUTPUT))
                            } else {
                                Some(truncate_chars(&json_pretty(v), MAX_TOOL_OUTPUT))
                            }
                        });
                        let is_denied = step_state == "DENIED"
                            || tool_info.get("denied").map(js_truthy).unwrap_or(false);
                        let is_err = !is_denied
                            && (matches!(step_state.as_str(), "ERROR" | "FAILED")
                                || tool_info.get("is_error").map(js_truthy).unwrap_or(false)
                                || tool_info.get("error").map(js_truthy).unwrap_or(false));
                        let end = now_ms();
                        let reported_ms = step
                            .get("duration_seconds")
                            .and_then(is_finite_number)
                            .map(|d| js_round(d * 1000.0));
                        let tracked_start = self.tool_starts.remove(&tool_id);
                        if let Some(s) = tracked_start {
                            self.tool_intervals.push((s, end));
                        } else if let Some(ms) = reported_ms.filter(|&m| m > 0) {
                            self.tool_intervals.push((end.saturating_sub(ms), end));
                        }
                        let dur = reported_ms
                            .map(|ms| json!(ms))
                            .or_else(|| tracked_start.map(|s| json!(end - s)))
                            .unwrap_or(Value::Null);
                        if is_denied {
                            self.denied.push(json!({
                                    "action": name,
                                    "displayName": name,
                                    "detail": if summary.is_empty() { Value::Null } else { json!(summary) },
                                }));
                        }
                        let final_name = existing
                            .as_ref()
                            .and_then(|e| e.get("name").and_then(Value::as_str))
                            .unwrap_or(&name)
                            .to_string();
                        let final_summary = existing
                            .as_ref()
                            .and_then(|e| e.get("inputSummary").and_then(Value::as_str))
                            .filter(|s| !s.is_empty())
                            .unwrap_or(&summary)
                            .to_string();
                        let final_input_json = existing
                            .as_ref()
                            .and_then(|e| e.get("inputJson").cloned())
                            .filter(|v| !v.is_null())
                            .or_else(|| {
                                params
                                    .filter(|v| !v.is_null())
                                    .map(|v| json!(json_pretty(v)))
                            })
                            .unwrap_or(Value::Null);
                        let status_str = if is_denied {
                            "denied"
                        } else if is_err {
                            "error"
                        } else {
                            "done"
                        };
                        sink.upsert_tool_call(json!({
                            "id": tool_id,
                            "name": final_name,
                            "inputSummary": final_summary,
                            "inputJson": final_input_json,
                            "output": out_text,
                            "status": status_str,
                            "durationMs": dur,
                        }));
                        sink.set_status_detail("Antigravity is thinking…");
                    }
                }
            }
            return;
        }
        if agy_event == "result" {
            if let Some(res) = obj.get("result").and_then(Value::as_object) {
                if let Some(cid) = res
                    .get("conversation_id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    sink.persist_session_ref(&mut self.session_ref, "agy", cid);
                }
                let response_text = res.get("response").and_then(Value::as_str).unwrap_or("");
                let cur_empty = sink
                    .message()
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .is_empty();
                if !response_text.trim().is_empty() && cur_empty {
                    self.saw_text = true;
                    sink.emit_delta(Some(response_text.to_string()), None, false);
                }
                let res_status = res
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_uppercase();
                if let Some(err_s) = res
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    sink.message()
                        .as_object_mut()
                        .unwrap()
                        .insert("error".into(), json!(err_s));
                } else if matches!(res_status.as_str(), "ERROR" | "FAILED") {
                    let mut m = sink.message();
                    if m.get("error").map(Value::is_null).unwrap_or(true) {
                        let msg_err = if !response_text.trim().is_empty() {
                            response_text.trim().to_string()
                        } else {
                            format!(
                                "Antigravity finished with status {}",
                                res.get("status").map(js_str).unwrap_or_default()
                            )
                        };
                        m.as_object_mut()
                            .unwrap()
                            .insert("error".into(), json!(msg_err));
                    }
                }
                if let Some(u) = parse_agy_usage(
                    res.get("usage"),
                    res.get("duration_seconds"),
                    res.get("duration_ms"),
                    res.get("total_cost_usd"),
                ) {
                    self.result_usage = Some(u);
                }
                if let Some(denials) = res.get("permission_denials").and_then(Value::as_array) {
                    for d in denials {
                        if let Some(dobj) = d.as_object() {
                            self.denied.push(denied_from(
                                dobj,
                                "permission",
                                "Permission Required",
                                true,
                            ));
                        }
                    }
                }
            }
            return;
        }

        let ev_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
        if let Some(delta) = obj
            .get("delta")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            if ev_type.contains("think") || ev_type.contains("reason") {
                sink.emit_delta(None, Some(delta.to_string()), false);
            } else {
                self.saw_text = true;
                sink.emit_delta(Some(delta.to_string()), None, false);
            }
            return;
        }

        if ev_type == "stream_event" {
            if let Some(inner) = obj.get("event").and_then(Value::as_object) {
                let itype = inner.get("type").and_then(Value::as_str).unwrap_or("");
                if itype == "content_block_start" {
                    let bt = inner
                        .get("content_block")
                        .and_then(Value::as_object)
                        .and_then(|b| b.get("type"))
                        .and_then(Value::as_str);
                    self.breaks.start(bt);
                } else if itype == "content_block_delta" {
                    if let Some(d) = inner.get("delta").and_then(Value::as_object) {
                        let dtype = d.get("type").and_then(Value::as_str).unwrap_or("");
                        if dtype == "text_delta" {
                            if let Some(t) = d.get("text").and_then(Value::as_str) {
                                self.saw_text = true;
                                if !t.is_empty() {
                                    let existing = sink
                                        .message()
                                        .get("text")
                                        .and_then(Value::as_str)
                                        .map(str::to_string);
                                    let sep = self.breaks.take_text(existing.as_deref());
                                    sink.emit_delta(Some(format!("{sep}{t}")), None, false);
                                }
                            }
                        } else if dtype == "thinking_delta" {
                            if let Some(t) = d
                                .get("thinking")
                                .and_then(Value::as_str)
                                .filter(|s| !s.is_empty())
                            {
                                let existing = sink
                                    .message()
                                    .get("thinking")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let sep = self.breaks.take_thinking(existing.as_deref());
                                sink.emit_delta(None, Some(format!("{sep}{t}")), false);
                            }
                        }
                    }
                }
            }
            return;
        }

        if ev_type == "assistant" {
            if let Some(content) = obj
                .get("message")
                .and_then(Value::as_object)
                .and_then(|m| m.get("content"))
                .and_then(Value::as_array)
            {
                for block in content {
                    let Some(b) = block.as_object() else { continue };
                    match b.get("type").and_then(Value::as_str) {
                        Some("thinking") => {
                            if let Some(th) = b
                                .get("thinking")
                                .and_then(Value::as_str)
                                .filter(|s| !s.is_empty())
                            {
                                let existing = sink
                                    .message()
                                    .get("thinking")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                if !existing.as_deref().map(|e| e.contains(th)).unwrap_or(false) {
                                    let sep = block_separator(existing.as_deref());
                                    sink.emit_delta(None, Some(format!("{sep}{th}")), false);
                                }
                            }
                        }
                        Some("text") if !self.saw_text => {
                            if let Some(t) = b
                                .get("text")
                                .and_then(Value::as_str)
                                .filter(|s| !s.is_empty())
                            {
                                let existing = sink
                                    .message()
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let sep = block_separator(existing.as_deref());
                                sink.emit_delta(Some(format!("{sep}{t}")), None, false);
                            }
                        }
                        Some("tool_use") => {
                            sink.tool_use_started(
                                &mut self.tool_starts,
                                b.get("id"),
                                b.get("name"),
                                b.get("input"),
                            );
                        }
                        _ => {}
                    }
                }
            }
            return;
        }

        if matches!(ev_type, "tool_use" | "tool_call" | "tool_start") {
            let input_obj = obj
                .get("input")
                .or_else(|| obj.get("args"))
                .or_else(|| obj.get("arguments"));
            sink.tool_use_started(
                &mut self.tool_starts,
                obj.get("id").or_else(|| obj.get("tool_use_id")),
                obj.get("name").or_else(|| obj.get("tool")),
                input_obj,
            );
            return;
        }

        if matches!(ev_type, "tool_result" | "tool_end" | "user") {
            let blocks: Vec<Value> = if ev_type == "user" {
                obj.get("message")
                    .and_then(Value::as_object)
                    .and_then(|m| m.get("content"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            } else {
                vec![ev.clone()]
            };
            for raw_block in blocks {
                let Some(b) = raw_block.as_object() else {
                    continue;
                };
                let tool_id = b
                    .get("tool_use_id")
                    .or_else(|| b.get("id"))
                    .filter(|v| !v.is_null())
                    .map(js_str)
                    .unwrap_or_default();
                if tool_id.is_empty() {
                    continue;
                }
                let existing = sink.find_tool(&tool_id);
                let out_text = tool_output_text(
                    b.get("content")
                        .or_else(|| b.get("output"))
                        .or_else(|| b.get("result")),
                )
                .map(|s| truncate_chars(&s, MAX_TOOL_OUTPUT));
                let is_err = b.get("is_error").map(js_truthy).unwrap_or(false)
                    || b.get("error").map(js_truthy).unwrap_or(false);
                let end = now_ms();
                let dur = self.tool_starts.remove(&tool_id).map(|s| {
                    self.tool_intervals.push((s, end));
                    json!(end - s)
                });
                let name = existing
                    .as_ref()
                    .and_then(|e| e.get("name").and_then(Value::as_str))
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        b.get("name")
                            .map(js_str)
                            .unwrap_or_else(|| "Tool".to_string())
                    });
                let input_summary = existing
                    .as_ref()
                    .and_then(|e| e.get("inputSummary").and_then(Value::as_str))
                    .unwrap_or("")
                    .to_string();
                let input_json = existing
                    .as_ref()
                    .and_then(|e| e.get("inputJson").cloned())
                    .unwrap_or(Value::Null);
                sink.upsert_tool_call(json!({
                    "id": tool_id,
                    "name": name,
                    "inputSummary": input_summary,
                    "inputJson": input_json,
                    "output": out_text,
                    "status": if is_err { "error" } else { "done" },
                    "durationMs": dur,
                }));
            }
            return;
        }

        if ev_type == "result" {
            if let Some(res_val) = obj.get("result").and_then(Value::as_str) {
                let cur_empty = sink
                    .message()
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .is_empty();
                if !res_val.is_empty() && cur_empty {
                    sink.emit_delta(Some(res_val.to_string()), None, false);
                }
                if obj.get("is_error").map(js_truthy).unwrap_or(false) {
                    sink.message()
                        .as_object_mut()
                        .unwrap()
                        .insert("error".into(), json!(res_val));
                }
            }
            if let Some(u) = parse_agy_usage(
                obj.get("usage"),
                obj.get("duration_seconds"),
                obj.get("duration_ms"),
                obj.get("total_cost_usd"),
            ) {
                self.result_usage = Some(u);
            }
            if let Some(denials) = obj.get("permission_denials").and_then(Value::as_array) {
                for d in denials {
                    if let Some(dobj) = d.as_object() {
                        self.denied.push(denied_from(
                            dobj,
                            "permission",
                            "Permission Required",
                            true,
                        ));
                    }
                }
            }
            return;
        }

        if let Some(t) = obj
            .get("text")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            self.saw_text = true;
            sink.emit_delta(Some(t.to_string()), None, false);
        }
    }

    fn finish(mut self: Box<Self>) -> TurnOutcome {
        let finished_at = now_ms();
        for (_, start) in self.tool_starts.drain() {
            self.tool_intervals.push((start, finished_at));
        }

        let session_id = session_ref_value(self.session_ref.as_ref());
        let matching_prior = self
            .prior_session_usage
            .as_ref()
            .filter(|b| Some(b.session_id.as_str()) == session_id.as_deref());

        let (usage, throughput, session_usage) = if !self.step_usages.is_empty() {
            // Real `agy` CLI emits exact per-LLM-call usage and duration_seconds on each
            // `agent_response` step, excluding tool calls and prior turns in the conversation.
            let turn_in = sum_opt_u64(self.step_usages.values().map(|s| s.input_tokens));
            let turn_out = sum_opt_u64(self.step_usages.values().map(|s| s.output_tokens));
            let turn_think = sum_opt_u64(self.step_usages.values().map(|s| s.thinking_tokens));
            let turn_cost = sum_opt_f64(self.step_usages.values().map(|s| s.cost_usd));

            let mut gen_tokens: u64 = 0;
            let mut gen_duration_ms: u64 = 0;
            for step in self.step_usages.values() {
                if let (Some(tok), Some(dur)) = (step.output_tokens, step.duration_ms) {
                    if tok > 0 && dur > 0 {
                        gen_tokens = gen_tokens.saturating_add(tok);
                        gen_duration_ms = gen_duration_ms.saturating_add(dur);
                    }
                }
            }
            let throughput = (gen_tokens > 0 && gen_duration_ms > 0).then_some(GenerationSample {
                output_tokens: gen_tokens,
                duration_ms: gen_duration_ms,
            });
            let usage = Some(json!({
                "inputTokens": turn_in,
                "outputTokens": turn_out,
                "thinkingTokens": turn_think,
                "costUsd": turn_cost,
                "durationMs": Value::Null,
            }));
            let updated_baseline = session_id.and_then(|sid| {
                self.result_usage.as_ref().map(|raw| SessionUsageBaseline {
                    session_id: sid,
                    input_tokens: raw.input_tokens,
                    output_tokens: raw.output_tokens,
                    thinking_tokens: raw.thinking_tokens,
                    valid: true,
                })
            });
            (usage, throughput, updated_baseline)
        } else if let Some(raw) = self.result_usage {
            // Fallback for older/fake `agy` output that only emits a final `result` event.
            let turn_in = cumulative_delta(
                raw.input_tokens,
                matching_prior.and_then(|p| p.input_tokens),
            );
            let turn_out = cumulative_delta(
                raw.output_tokens,
                matching_prior.and_then(|p| p.output_tokens),
            );
            let turn_think = cumulative_delta(
                raw.thinking_tokens,
                matching_prior.and_then(|p| p.thinking_tokens),
            );
            let baseline_clean = match matching_prior {
                Some(p) => p.valid,
                None => !self.is_resume,
            };
            let turn_dur_ms = if self.is_resume {
                None
            } else {
                raw.duration_ms
            };
            let usage = Some(json!({
                "inputTokens": turn_in,
                "outputTokens": turn_out,
                "thinkingTokens": turn_think,
                "costUsd": raw.cost_usd,
                "durationMs": turn_dur_ms,
            }));
            let throughput = if baseline_clean {
                turn_out
                    .filter(|&tokens| tokens > 0)
                    .and_then(|output_tokens| {
                        let wall_ms = (finished_at - self.started_at).max(0) as u64;
                        let base_ms = turn_dur_ms.unwrap_or(wall_ms);
                        let tool_ms =
                            union_interval_ms(&self.tool_intervals, self.started_at, finished_at);
                        let active_ms = base_ms.saturating_sub(tool_ms);
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
                input_tokens: raw.input_tokens,
                output_tokens: raw.output_tokens,
                thinking_tokens: raw.thinking_tokens,
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
            denied: self.denied,
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
    fn new_session_sends_system_prompt_in_the_print_argument() {
        let turn = prepare_turn(test_request(json!({ "id": "tab_test" }), "default", false));
        assert_eq!(
            turn.args,
            [
                "--output-format",
                "stream-json",
                "--mode",
                "auto",
                "--print=<system_instructions>\nSYS\n</system_instructions>\n\nhello",
            ]
        );
    }

    #[test]
    fn resume_passes_the_conversation_and_skips_an_unchanged_system_prompt() {
        let tab = json!({
            "id": "tab_test",
            "model": "gemini-x",
            "sessionRef": { "kind": "agy", "value": "conv-1" },
        });
        let turn = prepare_turn(test_request(tab.clone(), "plan", false));
        assert_eq!(
            turn.args,
            [
                "--output-format",
                "stream-json",
                "--mode",
                "plan",
                "--conversation",
                "conv-1",
                "--model",
                "gemini-x",
                "--print=hello",
            ]
        );
        let dirty = prepare_turn(test_request(tab, "full-access", true));
        assert_eq!(dirty.args[2], "--dangerously-skip-permissions");
        assert!(dirty
            .args
            .last()
            .unwrap()
            .starts_with("--print=<system_instructions>"));
    }

    #[test]
    fn sums_per_step_agent_response_usage_and_ignores_cumulative_result() {
        let env = TestTurnEnv::new("agy");
        let tab = json!({
            "id": "tab_test",
            "sessionRef": { "kind": "agy", "value": "conv-1" },
        });
        let mut turn = prepare_turn(test_request(tab, "full-access", false));
        // Step 6 (first LLM call in resumed turn 2) updates twice with same step_index; deduplicated.
        turn.parser.handle_line(
            r#"{"event":"step_update","conversation_id":"conv-1","step_update":{"step_index":6,"step_type":"agent_response","state":"DONE","duration_seconds":4.0,"usage":{"input_tokens":5000,"output_tokens":300,"thinking_tokens":200}}}"#,
            &env.sink,
        );
        turn.parser.handle_line(
            r#"{"event":"step_update","conversation_id":"conv-1","step_update":{"step_index":6,"step_type":"agent_response","state":"DONE","duration_seconds":4.5,"usage":{"input_tokens":5000,"output_tokens":350,"thinking_tokens":220}}}"#,
            &env.sink,
        );
        // Long tool call step (300s) — must NOT count toward model generation time.
        turn.parser.handle_line(
            r#"{"event":"step_update","conversation_id":"conv-1","step_update":{"step_index":7,"step_type":"tool","state":"DONE","tool_call_id":"t1","tool_name":"run_command","duration_seconds":300.0,"tool_info":{"output":"ok"}}}"#,
            &env.sink,
        );
        // Step 8 (second LLM call in this turn).
        turn.parser.handle_line(
            r#"{"event":"step_update","conversation_id":"conv-1","step_update":{"step_index":8,"step_type":"agent_response","state":"DONE","duration_seconds":1.5,"usage":{"input_tokens":2000,"output_tokens":150,"thinking_tokens":80}}}"#,
            &env.sink,
        );
        // Cumulative result from conversation start (includes 15,000 tokens from turn 1 & 1 hour duration).
        turn.parser.handle_line(
            r#"{"event":"result","result":{"conversation_id":"conv-1","status":"SUCCESS","response":"done","duration_seconds":3600.0,"usage":{"input_tokens":50000,"output_tokens":15500,"thinking_tokens":9300}}}"#,
            &env.sink,
        );
        let outcome = turn.parser.finish();
        let usage = outcome.usage.expect("usage");
        assert_eq!(usage["inputTokens"], 7000);
        assert_eq!(usage["outputTokens"], 500);
        assert_eq!(usage["thinkingTokens"], 300);
        // durationMs left null so runner fills per-turn wall time instead of 3600s conversation age.
        assert!(usage["durationMs"].is_null());
        assert_eq!(
            outcome.throughput,
            Some(GenerationSample {
                output_tokens: 500,
                duration_ms: 6000,
            })
        );
        let baseline = outcome.session_usage.expect("baseline");
        assert_eq!(baseline.output_tokens, Some(15_500));
        assert!(baseline.valid);
    }

    #[test]
    fn fallback_result_only_subtracts_prior_session_baseline() {
        let env = TestTurnEnv::new("agy");
        let tab = json!({
            "id": "tab_test",
            "sessionRef": { "kind": "agy", "value": "conv-1" },
        });
        let mut req = test_request(tab, "full-access", false);
        req.prior_session_usage = Some(SessionUsageBaseline {
            session_id: "conv-1".to_string(),
            input_tokens: Some(1000),
            output_tokens: Some(200),
            thinking_tokens: Some(50),
            valid: true,
        });
        let mut turn = prepare_turn(req);
        turn.parser.handle_line(
            r#"{"event":"result","result":{"conversation_id":"conv-1","status":"SUCCESS","response":"ok","duration_seconds":500.0,"usage":{"input_tokens":1400,"output_tokens":260,"thinking_tokens":80}}}"#,
            &env.sink,
        );
        let outcome = turn.parser.finish();
        let usage = outcome.usage.expect("usage");
        assert_eq!(usage["inputTokens"], 400);
        assert_eq!(usage["outputTokens"], 60);
        assert_eq!(usage["thinkingTokens"], 30);
        assert!(usage["durationMs"].is_null());
    }
}
