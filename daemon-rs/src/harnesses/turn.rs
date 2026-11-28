//! What a harness needs to run one turn: the request it builds CLI arguments from, the parser
//! that turns the CLI's output into chat updates, and the [`TurnSink`] it reports them to.

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::runner::AgentRunner as Runner;
use crate::util::{
    is_finite_number, js_round, js_str, js_truthy, json_compact, json_pretty, summarize_tool_input,
};

/// Tool output longer than this many characters is truncated in the chat.
pub(crate) const MAX_TOOL_OUTPUT: usize = 12_000;

/// Pure model-generation measurement for throughput tracking: output tokens generated and
/// active LLM generation duration in milliseconds (excluding tool execution and waits).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GenerationSample {
    pub output_tokens: u64,
    pub duration_ms: u64,
}

/// Persisted cumulative token counters for harnesses whose CLI reports session-cumulative
/// totals across resumed turns (e.g. Codex, or legacy Agy results without per-step usage).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionUsageBaseline {
    pub session_id: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub thinking_tokens: Option<u64>,
    /// False when an intervening turn on this session ended without reporting usage, so the
    /// next cumulative report includes unattributed tokens and must not emit a throughput sample.
    pub valid: bool,
}

impl SessionUsageBaseline {
    pub(crate) fn from_value(v: &Value) -> Option<Self> {
        let obj = v.as_object()?;
        let session_id = obj
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?
            .to_string();
        Some(Self {
            session_id,
            input_tokens: value_as_u64(obj.get("inputTokens")),
            output_tokens: value_as_u64(obj.get("outputTokens")),
            thinking_tokens: value_as_u64(obj.get("thinkingTokens")),
            valid: obj.get("valid").and_then(Value::as_bool).unwrap_or(true),
        })
    }

    pub(crate) fn to_value(&self) -> Value {
        json!({
            "sessionId": self.session_id,
            "inputTokens": self.input_tokens,
            "outputTokens": self.output_tokens,
            "thinkingTokens": self.thinking_tokens,
            "valid": self.valid,
        })
    }
}

/// Everything a harness needs to build the CLI arguments for one turn.
#[derive(Clone)]
pub(crate) struct TurnRequest {
    /// The tab as stored (model, sessionRef, ...).
    pub tab: Value,
    /// The prompt to send, with attachments and any summary request already added.
    pub prompt: String,
    pub cwd: String,
    /// `"full-access"`, `"accept-edits"`, `"plan"` or the default mode.
    pub permission_mode: String,
    /// The tab's system prompt, including the daemon's standing instructions.
    pub system_prompt: String,
    /// True once after the tab's system prompt changed, for harnesses that only send it on new sessions.
    pub system_prompt_dirty: bool,
    /// Directory holding the tab's attached images, if any.
    pub attachments_dir: Option<String>,
    /// Last recorded cumulative usage baseline for this tab's session, if any.
    pub prior_session_usage: Option<SessionUsageBaseline>,
}

/// A turn ready to spawn: CLI arguments (without the binary), the status shown while it runs,
/// and the parser for its output.
pub(crate) struct PreparedTurn {
    pub args: Vec<String>,
    pub status: &'static str,
    pub parser: Box<dyn TurnParser>,
}

/// What a finished turn hands back to the runner.
pub(crate) struct TurnOutcome {
    pub session_ref: Option<Value>,
    pub usage: Option<Value>,
    pub denied: Vec<Value>,
    /// Pure LLM generation measurement for throughput tracking.
    pub throughput: Option<GenerationSample>,
    /// Updated cumulative session usage baseline for harnesses that report cumulative totals.
    pub session_usage: Option<SessionUsageBaseline>,
}

/// Parses a harness CLI's output, one line at a time.
pub(crate) trait TurnParser: Send {
    fn handle_line(&mut self, line: &str, sink: &TurnSink);
    fn finish(self: Box<Self>) -> TurnOutcome;
}

/// The chat message and tab a turn reports to.
pub(crate) struct TurnSink {
    runner: Runner,
    tab_id: String,
    message: Arc<Mutex<Value>>,
}

impl TurnSink {
    pub(crate) fn new(runner: Runner, tab_id: String, message: Arc<Mutex<Value>>) -> Self {
        Self {
            runner,
            tab_id,
            message,
        }
    }

    /// The assistant message being built.
    pub(crate) fn message(&self) -> MutexGuard<'_, Value> {
        self.message.lock().unwrap()
    }

    pub(crate) fn emit_delta(
        &self,
        text: Option<String>,
        thinking: Option<String>,
        bypass_filters: bool,
    ) {
        self.runner
            .emit_delta(&self.tab_id, &self.message, text, thinking, bypass_filters);
    }

    pub(crate) fn upsert_tool_call(&self, tool_call: Value) {
        self.runner
            .upsert_tool_call(&self.tab_id, &self.message, tool_call);
    }

    pub(crate) fn tool_use_started(
        &self,
        tool_starts: &mut HashMap<String, i64>,
        raw_id: Option<&Value>,
        raw_name: Option<&Value>,
        input: Option<&Value>,
    ) {
        self.runner.tool_use_started(
            &self.tab_id,
            &self.message,
            tool_starts,
            raw_id,
            raw_name,
            input,
        );
    }

    pub(crate) fn find_tool(&self, tool_id: &str) -> Option<Value> {
        Runner::find_tool(&self.message, tool_id)
    }

    /// Records the harness's session ID on the tab so the next turn resumes it.
    pub(crate) fn persist_session_ref(&self, current: &mut Option<Value>, kind: &str, value: &str) {
        self.runner
            .persist_session_ref(&self.tab_id, current, kind, value);
    }

    pub(crate) fn set_status_detail(&self, detail: &str) {
        self.runner.set_status_detail(&self.tab_id, detail);
    }

    pub(crate) fn running_status(&self, name: &str, summary: &str) {
        self.runner.running_status(&self.tab_id, name, summary);
    }
}

pub(crate) fn toml_basic_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

pub(crate) fn value_as_u64(v: Option<&Value>) -> Option<u64> {
    let val = v?;
    if let Some(u) = val.as_u64() {
        return Some(u);
    }
    if let Some(i) = val.as_i64() {
        return u64::try_from(i).ok();
    }
    is_finite_number(val).and_then(|f| {
        if f >= 0.0 {
            u64::try_from(js_round(f)).ok()
        } else {
            None
        }
    })
}

pub(crate) fn cumulative_delta(raw: Option<u64>, prev: Option<u64>) -> Option<u64> {
    let r = raw?;
    Some(match prev {
        Some(p) if r >= p => r - p,
        _ => r,
    })
}

pub(crate) fn session_ref_value(session_ref: Option<&Value>) -> Option<String> {
    session_ref
        .and_then(Value::as_object)
        .and_then(|o| o.get("value"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Total duration covered by `[start, end]` intervals clamped to `[window_start, window_end]`,
/// merging overlapping intervals so parallel tool calls are never subtracted twice.
pub(crate) fn union_interval_ms(
    intervals: &[(i64, i64)],
    window_start: i64,
    window_end: i64,
) -> u64 {
    if window_end <= window_start {
        return 0;
    }
    let mut clamped: Vec<(i64, i64)> = intervals
        .iter()
        .filter_map(|&(s, e)| {
            let cs = s.max(window_start);
            let ce = e.min(window_end);
            (ce > cs).then_some((cs, ce))
        })
        .collect();
    if clamped.is_empty() {
        return 0;
    }
    clamped.sort_unstable_by_key(|&(s, e)| (s, e));
    let mut total: u64 = 0;
    let (mut cur_s, mut cur_e) = clamped[0];
    for (s, e) in clamped.into_iter().skip(1) {
        if s <= cur_e {
            cur_e = cur_e.max(e);
        } else {
            total = total.saturating_add((cur_e - cur_s) as u64);
            cur_s = s;
            cur_e = e;
        }
    }
    total.saturating_add((cur_e - cur_s) as u64)
}

pub(crate) fn tool_output_text(raw: Option<&Value>) -> Option<String> {
    let val = raw?;
    if val.is_null() {
        return None;
    }
    if let Some(s) = val.as_str() {
        return Some(s.to_string());
    }
    if let Some(arr) = val.as_array() {
        let parts: Vec<String> = arr
            .iter()
            .map(|x| {
                if let Some(t) = x.as_object().and_then(|o| o.get("text")) {
                    js_str(t)
                } else {
                    json_compact(x)
                }
            })
            .collect();
        return Some(parts.join("\n"));
    }
    Some(json_pretty(val))
}

pub(crate) fn denied_from(
    rec: &Map<String, Value>,
    default_action: &str,
    default_name: &str,
    use_alt_keys: bool,
) -> Value {
    let action = if use_alt_keys {
        rec.get("tool_name")
            .or_else(|| rec.get("action"))
            .filter(|v| !v.is_null())
            .map(js_str)
            .unwrap_or_else(|| default_action.to_string())
    } else {
        rec.get("tool_name")
            .filter(|v| !v.is_null())
            .map(js_str)
            .unwrap_or_else(|| default_action.to_string())
    };
    let display = if use_alt_keys {
        rec.get("tool_name")
            .or_else(|| rec.get("displayName"))
            .filter(|v| !v.is_null())
            .map(js_str)
            .unwrap_or_else(|| default_name.to_string())
    } else {
        rec.get("tool_name")
            .filter(|v| !v.is_null())
            .map(js_str)
            .unwrap_or_else(|| default_name.to_string())
    };
    let detail = rec
        .get("tool_input")
        .filter(|v| js_truthy(v))
        .map(|v| summarize_tool_input(Some(v)));
    json!({
        "action": action,
        "displayName": display,
        "detail": detail,
    })
}

pub(crate) fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect()
    }
}

/// A turn request for argument tests: tab `tab_test`, cwd `/work`, system prompt `SYS`.
#[cfg(test)]
pub(crate) fn test_request(tab: Value, permission_mode: &str, dirty: bool) -> TurnRequest {
    TurnRequest {
        tab,
        prompt: "hello".to_string(),
        cwd: "/work".to_string(),
        permission_mode: permission_mode.to_string(),
        system_prompt: "SYS".to_string(),
        system_prompt_dirty: dirty,
        attachments_dir: None,
        prior_session_usage: None,
    }
}

#[cfg(test)]
pub(crate) struct TestTurnEnv {
    root: std::path::PathBuf,
    pub sink: TurnSink,
}

#[cfg(test)]
impl TestTurnEnv {
    pub(crate) fn new(agent: &str) -> Self {
        use crate::store::StateStore;
        use crate::util::rand_hex;

        let root = std::env::temp_dir().join(format!("agentsterm-turn-test-{}", rand_hex(12)));
        let store = StateStore::open_in(root.clone());
        let tab = store
            .create_tab(agent, None, None, None, None, None, None)
            .unwrap();
        let tab_id = tab["id"].as_str().unwrap().to_string();
        let msg = json!({
            "id": "msg_test",
            "tabId": tab_id,
            "role": "assistant",
            "text": "",
            "thinking": "",
            "segments": [],
            "toolCalls": [],
            "deniedActions": [],
            "status": "streaming",
            "model": Value::Null,
            "error": Value::Null,
            "usage": Value::Null,
            "createdAt": 1,
            "updatedAt": 1,
        });
        store.add_message(&tab_id, msg.clone());
        let runner = Runner::new(store, Arc::new(|_| {}));
        let sink = TurnSink::new(runner, tab_id, Arc::new(Mutex::new(msg)));
        Self { root, sink }
    }
}

#[cfg(test)]
impl Drop for TestTurnEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_interval_merges_overlapping_parallel_tools_and_clamps_to_window() {
        let intervals = vec![(50, 200), (150, 350), (500, 600), (900, 1200)];
        // Clamped to [100, 1000]: [100, 350] (250) + [500, 600] (100) + [900, 1000] (100) = 450
        assert_eq!(union_interval_ms(&intervals, 100, 1000), 450);
        assert_eq!(union_interval_ms(&[], 100, 1000), 0);
        assert_eq!(union_interval_ms(&[(200, 100)], 100, 1000), 0);
        assert_eq!(union_interval_ms(&intervals, 1000, 1000), 0);
    }

    #[test]
    fn cumulative_delta_handles_growth_and_resets() {
        assert_eq!(cumulative_delta(Some(100), None), Some(100));
        assert_eq!(cumulative_delta(Some(150), Some(100)), Some(50));
        assert_eq!(cumulative_delta(Some(100), Some(100)), Some(0));
        assert_eq!(cumulative_delta(Some(40), Some(100)), Some(40));
        assert_eq!(cumulative_delta(None, Some(100)), None);
    }
}
