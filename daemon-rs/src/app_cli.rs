//! Native Rust CLI for inspecting and controlling the live AgentsTerm Android app
//! (`agents_term_app_cli` symlink, `agentsterm-daemon app ...`, or legacy `app_state_cli` symlink).
//!
//! All coordinates are in screenshot pixels (shared across `screenshot`, `ui`, `tap`, `long-press`,
//! and `swipe`). Output is JSON on stdout (unless `--raw` or `--brief`); errors are JSON on stderr
//! with exit codes:
//!   0  success
//!   1  usage / local config error (`bad_args`, `bad_config`)
//!   2  daemon unreachable, unauthorized, or too old (`daemon_unreachable`, `unauthorized`, `daemon_unsupported`)
//!   3  app not ready (`no_app_connected`, `unsupported_app`, `inspection_disabled`, `control_disabled`, `app_disconnected`)
//!   4  app error, command timeout, or `wait` timeout (`app_error`, `timeout`, `wait_timeout`)

use base64::Engine;
use chrono::Local;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use crate::config::{agents_term_home, config_path, home_dir, tmp_dir};
use crate::http_client::http_request;

const EXIT_USAGE: i32 = 1;
const EXIT_DAEMON: i32 = 2;
const EXIT_NOT_READY: i32 = 3;
const EXIT_APP: i32 = 4;

const NOT_READY_CODES: &[&str] = &[
    "no_app_connected",
    "unsupported_app",
    "inspection_disabled",
    "control_disabled",
    "recording_disabled",
    "app_disconnected",
];

/// How long `record start` waits: the user has to approve the recording on the phone, and the
/// app gives up on them after 60 seconds.
const RECORD_START_TIMEOUT_S: f64 = 75.0;
/// Bytes of video asked for per `record_read` round trip (the app's own maximum).
const RECORD_CHUNK_BYTES: u64 = 512 * 1024;

#[derive(Debug)]
struct CliErr {
    message: String,
    exit_code: i32,
    code: Option<String>,
}

impl CliErr {
    fn new(message: impl Into<String>, exit_code: i32, code: Option<&str>) -> Self {
        Self {
            message: message.into(),
            exit_code,
            code: code.map(str::to_string),
        }
    }

    fn bad_args(message: impl Into<String>) -> Self {
        Self::new(message, EXIT_USAGE, Some("bad_args"))
    }
}

trait AppClient {
    fn status(&self) -> Result<Value, CliErr>;
    fn command(&self, cmd: &str, args: Value, timeout_s: f64) -> Result<Value, CliErr>;
}

struct Client {
    host: String,
    port: u16,
    token: String,
    http_timeout: Duration,
}

impl Client {
    fn base_url(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("http://[{}]:{}", self.host, self.port)
        } else {
            format!("http://{}:{}", self.host, self.port)
        }
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        timeout: Option<Duration>,
    ) -> Result<Value, CliErr> {
        let body_bytes = body.map(|v| v.to_string().into_bytes());
        let resp = http_request(
            method,
            &self.host,
            self.port,
            path,
            Some(&self.token),
            body_bytes.as_deref(),
            timeout.unwrap_or(self.http_timeout),
        )
        .map_err(|e| {
            CliErr::new(
                format!("Cannot reach daemon at {}: {e}", self.base_url()),
                EXIT_DAEMON,
                Some("daemon_unreachable"),
            )
        })?;

        if resp.status >= 200 && resp.status < 300 {
            if resp.body.is_empty() {
                return Ok(json!({}));
            }
            return resp.json().map_err(|_| {
                CliErr::new(
                    "Daemon returned non-JSON response",
                    EXIT_DAEMON,
                    Some("daemon_error"),
                )
            });
        }

        let payload = resp.json().unwrap_or_else(|_| json!({}));
        let message = payload
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("HTTP {} from daemon", resp.status));
        let code = payload.get("code").and_then(Value::as_str);

        if resp.status == 401 {
            return Err(CliErr::new(
                format!("{message} (check the token in {})", config_path().display()),
                EXIT_DAEMON,
                Some("unauthorized"),
            ));
        }
        if let Some(c) = code {
            if NOT_READY_CODES.contains(&c) {
                return Err(CliErr::new(message, EXIT_NOT_READY, Some(c)));
            }
            return Err(CliErr::new(message, EXIT_APP, Some(c)));
        }
        if resp.status == 404 {
            return Err(CliErr::new(
                format!(
                    "{message}. The daemon is too old for app state reporting; update and restart it."
                ),
                EXIT_DAEMON,
                Some("daemon_unsupported"),
            ));
        }
        Err(CliErr::new(message, EXIT_DAEMON, Some("daemon_error")))
    }
}

impl AppClient for Client {
    fn status(&self) -> Result<Value, CliErr> {
        self.request("GET", "/api/app/status", None, None)
    }

    fn command(&self, cmd: &str, args: Value, timeout_s: f64) -> Result<Value, CliErr> {
        let body = json!({
            "command": cmd,
            "args": args,
            "timeoutMs": (timeout_s * 1000.0) as u64,
        });
        // Give HTTP slightly longer than the daemon-side timeout so the daemon's error wins.
        let http_timeout = Duration::from_secs_f64((timeout_s + 10.0).max(5.0));
        self.request("POST", "/api/app/command", Some(&body), Some(http_timeout))
    }
}

fn expand_tilde(raw: &str) -> PathBuf {
    if raw == "~" {
        return home_dir();
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return home_dir().join(rest);
    }
    PathBuf::from(raw)
}

fn emit(val: &Value, pretty: bool) {
    let mut stdout = io::stdout().lock();
    if pretty {
        let _ = writeln!(
            stdout,
            "{}",
            serde_json::to_string_pretty(val).unwrap_or_default()
        );
    } else {
        let _ = writeln!(stdout, "{}", serde_json::to_string(val).unwrap_or_default());
    }
    let _ = stdout.flush();
}

/// Where a screenshot or recording goes: `output` if given, else a new file under
/// `~/.agents-term/tmp/`. Creates the parent directory.
fn capture_output_path(output: Option<&str>, kind: &str, ext: &str) -> Result<PathBuf, CliErr> {
    let out_path = match output {
        Some(p) => expand_tilde(p),
        None => {
            let root = agents_term_home();
            let root_existed = root.exists();
            let dir = tmp_dir();
            if let Err(e) = fs::create_dir_all(&dir) {
                return Err(CliErr::new(
                    format!("Failed to create {}: {e}", dir.display()),
                    EXIT_USAGE,
                    Some("io_error"),
                ));
            }
            if !root_existed {
                let _ = fs::set_permissions(&root, fs::Permissions::from_mode(0o700));
            }
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
            dir.join(format!(
                "agentsterm-{kind}-{}-{}.{ext}",
                Local::now().format("%Y%m%d-%H%M%S"),
                std::process::id()
            ))
        }
    };
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = fs::create_dir_all(parent);
        }
    }
    Ok(out_path)
}

/// Downloads the app's finished recording chunk by chunk into `output` and returns its path
/// and size.
fn fetch_recording(
    client: &dyn AppClient,
    timeout_s: f64,
    recording_id: &str,
    output: Option<&str>,
) -> Result<(PathBuf, u64), CliErr> {
    let bad_chunk = |what: &str| {
        CliErr::new(
            format!("App returned a bad recording chunk: {what}"),
            EXIT_APP,
            Some("app_error"),
        )
    };
    let out_path = capture_output_path(output, "recording", "mp4")?;
    let io_err = |e: std::io::Error| {
        CliErr::new(
            format!("Failed to write {}: {e}", out_path.display()),
            EXIT_USAGE,
            Some("io_error"),
        )
    };
    let mut file = fs::File::create(&out_path).map_err(io_err)?;
    let mut offset: u64 = 0;
    loop {
        let resp = client.command(
            "record_read",
            json!({ "recordingId": recording_id, "offset": offset, "length": RECORD_CHUNK_BYTES }),
            timeout_s,
        )?;
        let result = resp.get("result").ok_or_else(|| bad_chunk("no result"))?;
        let chunk = base64::engine::general_purpose::STANDARD
            .decode(
                result
                    .get("dataBase64")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim(),
            )
            .map_err(|e| bad_chunk(&e.to_string()))?;
        file.write_all(&chunk).map_err(io_err)?;
        offset += chunk.len() as u64;
        if result.get("eof").and_then(Value::as_bool) == Some(true) {
            break;
        }
        if chunk.is_empty() {
            return Err(bad_chunk("empty before the end of the file"));
        }
    }
    file.flush().map_err(io_err)?;
    Ok((fs::canonicalize(&out_path).unwrap_or(out_path), offset))
}

fn save_screenshot(
    client: &dyn AppClient,
    timeout_s: f64,
    output: Option<&str>,
) -> Result<Value, CliErr> {
    let mut resp = client.command("screenshot", json!({}), timeout_s)?;
    let b64 = resp
        .get_mut("result")
        .and_then(Value::as_object_mut)
        .and_then(|r| r.remove("pngBase64"))
        .and_then(|v| v.as_str().map(str::to_string))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            CliErr::new(
                "App returned no screenshot data",
                EXIT_APP,
                Some("app_error"),
            )
        })?;

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| {
            CliErr::new(
                format!("Invalid screenshot base64: {e}"),
                EXIT_APP,
                Some("app_error"),
            )
        })?;

    let out_path = capture_output_path(output, "screenshot", "png")?;
    fs::write(&out_path, &bytes).map_err(|e| {
        CliErr::new(
            format!("Failed to write {}: {e}", out_path.display()),
            EXIT_USAGE,
            Some("io_error"),
        )
    })?;
    let abs = fs::canonicalize(&out_path).unwrap_or(out_path);
    if let Some(r) = resp.get_mut("result").and_then(Value::as_object_mut) {
        r.insert("path".into(), json!(abs.to_string_lossy()));
        r.insert("bytes".into(), json!(bytes.len()));
    }
    Ok(resp)
}

fn run_action(
    client: &dyn AppClient,
    command: &str,
    cmd_args: Value,
    timeout_s: f64,
    screenshot_out: Option<&str>,
    settle_s: f64,
    pretty: bool,
) -> Result<i32, CliErr> {
    let mut resp = client.command(command, cmd_args, timeout_s)?;
    if let Some(out) = screenshot_out {
        if settle_s > 0.0 {
            thread::sleep(Duration::from_secs_f64(settle_s));
        }
        let shot = save_screenshot(client, timeout_s, Some(out))?;
        let shot_path = shot
            .get("result")
            .and_then(|r| r.get("path"))
            .cloned()
            .unwrap_or(Value::Null);
        if let Some(obj) = resp.as_object_mut() {
            obj.insert("screenshot".into(), shot_path);
        }
    }
    emit(&resp, pretty);
    Ok(0)
}

fn brief_node(node: &Value) -> String {
    let (cx, cy) = if let Some(arr) = node.get("center").and_then(Value::as_array) {
        (
            arr.first().and_then(Value::as_i64).unwrap_or(0),
            arr.get(1).and_then(Value::as_i64).unwrap_or(0),
        )
    } else if let Some(arr) = node.get("bounds").and_then(Value::as_array) {
        let l = arr.first().and_then(Value::as_i64).unwrap_or(0);
        let t = arr.get(1).and_then(Value::as_i64).unwrap_or(0);
        let r = arr.get(2).and_then(Value::as_i64).unwrap_or(l);
        let b = arr.get(3).and_then(Value::as_i64).unwrap_or(t);
        ((l + r) / 2, (t + b) / 2)
    } else if let Some(obj) = node.get("bounds").and_then(Value::as_object) {
        let l = obj.get("left").and_then(Value::as_i64).unwrap_or(0);
        let t = obj.get("top").and_then(Value::as_i64).unwrap_or(0);
        let r = obj.get("right").and_then(Value::as_i64).unwrap_or(l);
        let b = obj.get("bottom").and_then(Value::as_i64).unwrap_or(t);
        ((l + r) / 2, (t + b) / 2)
    } else {
        (0, 0)
    };

    let mut parts = vec![format!("[{cx},{cy}]")];
    if let Some(role) = node.get("role").and_then(Value::as_str) {
        if !role.is_empty() {
            parts.push(role.to_string());
        }
    }
    for key in ["text", "value", "description"] {
        let val = node.get(key).filter(|v| !v.is_null()).or_else(|| {
            if key == "description" {
                node.get("contentDescription").filter(|v| !v.is_null())
            } else {
                None
            }
        });
        if let Some(v) = val {
            let encoded = serde_json::to_string(v).unwrap_or_default();
            if key == "text" {
                parts.push(encoded);
            } else {
                parts.push(format!("{key}={encoded}"));
            }
        }
    }
    if let Some(tag) = node.get("tag").and_then(Value::as_str) {
        if !tag.is_empty() {
            parts.push(format!("tag={tag}"));
        }
    }
    if let Some(toggle) = node.get("toggle").and_then(Value::as_str) {
        if !toggle.is_empty() {
            parts.push(format!("toggle={toggle}"));
        }
    }
    if let Some(selected) = node.get("selected").and_then(Value::as_bool) {
        parts.push(format!("selected={selected}"));
    }
    for flag in [
        "clickable",
        "longClickable",
        "scrollable",
        "editable",
        "focused",
        "password",
    ] {
        if node.get(flag).and_then(Value::as_bool) == Some(true) {
            parts.push(flag.to_string());
        }
    }
    if node.get("enabled").and_then(Value::as_bool) == Some(false) {
        parts.push("disabled".to_string());
    }
    if let Some(window) = node.get("window").and_then(Value::as_i64) {
        if window != 0 {
            parts.push(format!("window={window}"));
        }
    }
    parts.join(" ")
}

fn format_ui_brief(result: &Value) -> String {
    let w = result
        .get("width")
        .or_else(|| result.get("windowWidth"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let h = result
        .get("height")
        .or_else(|| result.get("windowHeight"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let windows = result
        .get("windows")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|win| {
                    let idx = win.get("index").and_then(Value::as_i64).unwrap_or(0);
                    let typ = win.get("type").and_then(Value::as_str).unwrap_or("unknown");
                    format!("{idx}:{typ}")
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    let mut lines = vec![format!(
        "# {w}x{h} screenshot pixels; windows {windows}; [x,y] is each element's center"
    )];
    if let Some(nodes) = result.get("nodes").and_then(Value::as_array) {
        for n in nodes {
            lines.push(brief_node(n));
        }
    }
    lines.join("\n")
}

fn node_matches_selector(node: &Value, tag: Option<&str>, text: Option<&str>, exact: bool) -> bool {
    if let Some(t) = tag {
        if node.get("tag").and_then(Value::as_str) != Some(t) {
            return false;
        }
    }
    if let Some(q) = text {
        let candidates = ["text", "value", "description", "contentDescription"]
            .iter()
            .filter_map(|k| node.get(*k).and_then(Value::as_str));
        let hit = if exact {
            candidates.into_iter().any(|c| c == q)
        } else {
            let ql = q.to_lowercase();
            candidates
                .into_iter()
                .any(|c| c.to_lowercase().contains(&ql))
        };
        if !hit {
            return false;
        }
    }
    true
}

fn main_help(prog: &str) -> String {
    format!(
        "\
{prog} — Inspect and drive the live AgentsTerm Android app from the VM.

HOW IT WORKS
  agent -> {prog} -> daemon (/api/app/*) -> WebSocket -> AgentsTerm Android app

PREREQUISITES (ON THE PHONE)
  1. The phone is connected to this VM's AgentsTerm daemon.
  2. Read-only inspection commands require:
       Settings > Debug sharing > Share debug logs to VM
       Settings > Debug sharing > Allow VM agents to inspect the app
     (Logcat and screenshots also follow Include Android logcat / Include screenshot.)
  3. UI control commands (tap, long-press, swipe, scroll, type, key) also require:
       Settings > Debug sharing > Allow VM agents to control the app
  4. Screen recording (record) also requires:
       Settings > Debug sharing > Allow VM agents to record the screen
     and the user approves every recording in an Android dialog.
  5. Screenshots, `ui`, and control commands need the app in the foreground (the user sees
     a brief toast on the phone). Only drive the app when the task calls for it.

RECOMMENDED AGENT WORKFLOW
  1. Always run `{prog} status` first.
     - Exit code 3 means disconnected or permission is off; ask the user to enable it
       rather than retrying in a loop. Check `controlReady` before sending UI actions.
  2. Look before acting:
       {prog} ui --brief
       {prog} screenshot -o /tmp/app.png
     All coordinates are in screenshot pixels (shared by `screenshot`, `ui`, `tap`, `swipe`).
     `ui --brief` prints each element's `[x,y]` center, role, text/value/description, `tag=...`,
     and interactive flags.
  3. Drive the app cleanly:
     - Prefer `--tag` (most stable), then `--text`, then screenshot pixel `X Y` coordinates.
     - Pass `-s /tmp/after.png` on any action to capture the screen right after it settles.
     - Use `wait --tag ...` or `wait --text ...` (and `--gone`) instead of sleeping.
     - `ui` only lists what is currently on screen; use `scroll` then `ui` to reach off-screen items.
  4. Verify & collect evidence:
     - Check `state`, `logs --category error`, `logcat --raw`, `crash --raw`.
     - Capture a full sanitized bundle with `{prog} dump --note \"context\"`
       (saved to ~/.agents-term/debug-dumps/latest/).

USAGE
  {prog} [GLOBAL FLAGS] <COMMAND> [COMMAND OPTIONS]
  {prog} <COMMAND> --help
  {prog} help [COMMAND]

GLOBAL FLAGS (accepted before or after COMMAND)
  --host HOST        Daemon host (default: config.json / AGENTS_TERM_HOST / 127.0.0.1)
  --port PORT        Daemon port (default: config.json / AGENTS_TERM_PORT / 7501)
  --token TOKEN      Bearer token (default: config.json / AGENTS_TERM_TOKEN)
  --config PATH      Daemon config JSON path (default: ~/.agents-term/config.json)
  --timeout SEC      Seconds to wait for the app to answer (default: 15)
  --pretty           Pretty-print JSON output

INSPECTION COMMANDS (read-only; need remote inspection opt-in)
  status             Check connection, inspection (`ready`), and control (`controlReady`)
  ping               Round-trip ping to the connected app
  state              Dump current screen, connection, tabs, recent messages, and settings
  logs               Query in-memory debug events (--limit, --category, --since, --grep, -f)
  logcat             Fetch app process Android logcat (--lines N, --raw)
  crash              Fetch last saved uncaught-exception stack trace (--raw)
  screenshot         Capture app window PNG (-o PATH)
  ui                 List on-screen UI elements (--brief, --interactive, --tag, --text, --exact)
  wait               Poll `ui` until a matching element appears or disappears (--gone)
  dump               Upload full debug bundle to ~/.agents-term/debug-dumps/latest/
  record             Record a video of the screen: `record start`, then `record stop -o PATH`
  raw                Send an arbitrary command name with a JSON object of args

UI CONTROL COMMANDS (need remote control opt-in; all accept -s/--screenshot and --settle)
  tap                Tap `X Y` (or `--xy X Y`) or `--tag` / `--text`
  long-press         Press and hold `X Y` (or `--xy X Y`) or `--tag` / `--text` (--duration-ms)
  swipe              Drag `X1 Y1 X2 Y2` (--duration-ms, --hold-ms)
  scroll             Scroll `up|down|left|right` without fling (--amount, --tag/--text)
  type               Type `TEXT` into focused field or `--tag` / `--field` (--replace, --submit)
  key                Press `back` or `enter`

EXAMPLES
  {prog} status
  {prog} ui --brief
  {prog} tap --tag send_button -s /tmp/after-send.png
  {prog} tap 980 2200
  {prog} long-press --text \"Summarize\" --duration-ms 1000
  {prog} type \"hello\" --tag composer_input --replace --submit
  {prog} scroll down --amount 0.5
  {prog} swipe 100 1200 900 1200 --hold-ms 150
  {prog} key back
  {prog} wait --tag menu_app_settings --gone --wait-timeout 5
  {prog} logs --category error --category ws --limit 50
  {prog} dump --note \"repro context\"
  {prog} record start --max-duration 120
  {prog} record stop -o /tmp/flow.mp4

OUTPUT & EXIT CODES
  Stdout is JSON (unless `--raw` or `--brief`).
  Errors are printed to stderr as `{{\"ok\":false,\"code\":\"...\",\"error\":\"...\"}}`.
    0  Success
    1  Usage or local config error (`bad_args`, `bad_config`)
    2  Daemon unreachable, unauthorized, or too old (`daemon_unreachable`, `unauthorized`, `daemon_unsupported`)
    3  App not ready (`no_app_connected`, `unsupported_app`, `inspection_disabled`, `control_disabled`,
       `recording_disabled`, `app_disconnected`)
    4  App failed, timed out, or `wait` timed out (`app_error`, `timeout`, `wait_timeout`)
"
    )
}

fn subcommand_help(prog: &str, cmd: &str) -> Option<String> {
    let text = match cmd {
        "status" => format!(
            "Usage: {prog} status\n\n\
             Show whether an AgentsTerm app is connected and ready for inspection (`ready`) and UI\n\
             control (`controlReady`). Exits 0 if `ready` is true, or 3 otherwise.\n"
        ),
        "ping" => format!(
            "Usage: {prog} ping\n\n\
             Round-trip a ping to the connected Android app.\n"
        ),
        "state" => format!(
            "Usage: {prog} state\n\n\
             Dump sanitized app state: connection status, current screen, tabs, recent messages, and settings.\n"
        ),
        "logs" => format!(
            "Usage: {prog} logs [--limit N] [--category CAT]... [--since MS] [--grep TEXT] [-f|--follow] [--interval SEC]\n\n\
             Fetch the app's in-memory debug event log (categories: http, ws, ssh, ui, lifecycle, error, app_state).\n\n\
             Options:\n\
               --limit N          Max events to return (default: 100, max: 600)\n\
               --category CAT     Filter to category (repeatable)\n\
               --since MS         Only events with ts >= epoch milliseconds\n\
               --grep TEXT        Case-insensitive substring filter on event/detail/payload\n\
               -f, --follow       Poll continuously and print one JSON event per line\n\
               --interval SEC     Poll interval for --follow in seconds (default: 2.0)\n"
        ),
        "logcat" => format!(
            "Usage: {prog} logcat [--lines N] [--raw]\n\n\
             Fetch Android logcat lines for the AgentsTerm app process.\n\n\
             Options:\n\
               --lines N          Lines to fetch (default: 400, max: 2000)\n\
               --raw              Print plain text instead of JSON\n"
        ),
        "crash" => format!(
            "Usage: {prog} crash [--raw]\n\n\
             Fetch the last uncaught-exception crash report saved by the app, if any.\n\n\
             Options:\n\
               --raw              Print plain text instead of JSON\n"
        ),
        "screenshot" => format!(
            "Usage: {prog} screenshot [-o|--output PATH]\n\n\
             Capture the foreground app window (including open dialogs and menus) to a PNG file.\n\
             Replaces `pngBase64` in the JSON output with `path` and `bytes`.\n\n\
             Options:\n\
               -o, --output PATH  Output PNG path (default: ~/.agents-term/tmp/agentsterm-screenshot-<timestamp>-<pid>.png)\n"
        ),
        "record" => format!(
            "Usage: {prog} record start [--max-duration SEC]\n\
             \x20      {prog} record stop [-o|--output PATH] [--no-fetch]\n\
             \x20      {prog} record status\n\n\
             Record a video of the phone screen (MP4, no audio), for example as evidence of a test.\n\
             Needs Settings > Debug sharing > Allow VM agents to record the screen, and the app in the\n\
             foreground when it starts.\n\n\
             `start` makes Android ask the user to approve the recording, and returns once it is\n\
             running (it waits up to 75 seconds for the user). The video shows the whole screen,\n\
             keyboard and system bars included, in the same pixel space as `screenshot`.\n\
             `stop` ends it and downloads the video to this machine. A recording that ended by itself\n\
             (time limit reached, or stopped by the user) is downloaded the same way.\n\
             `status` reports `idle`, `recording` or `finished`.\n\n\
             Options:\n\
               --max-duration SEC  Stop by itself after SEC seconds (default: 60, at most 600)\n\
               -o, --output PATH   Output MP4 path (default: ~/.agents-term/tmp/agentsterm-recording-<timestamp>-<pid>.mp4)\n\
               --no-fetch          Stop without downloading the video\n"
        ),
        "dump" => format!(
            "Usage: {prog} dump [--note TEXT] [--no-screenshot]\n\n\
             Ask the app to upload a full sanitized debug bundle to ~/.agents-term/debug-dumps/latest/.\n\
             Automatically waits at least 60 seconds.\n\n\
             Options:\n\
               --note TEXT        Note included in summary.md\n\
               --no-screenshot    Skip capturing a screenshot in the bundle\n"
        ),
        "ui" => format!(
            "Usage: {prog} ui [--brief] [--interactive] [--tag TAG] [--text TEXT] [--exact]\n\n\
             List elements currently visible on screen with screenshot-pixel bounds, center `[x,y]`,\n\
             test tags, roles, text/value/description, and interactive flags.\n\n\
             Options:\n\
               --brief            Print one readable line per element instead of JSON\n\
               --interactive      Only clickable, longClickable, scrollable, or editable elements\n\
               --tag TAG          Filter by exact Compose testTag\n\
               --text TEXT        Filter by text, value, or description (case-insensitive substring)\n\
               --exact            Require --text to match exactly\n"
        ),
        "wait" => format!(
            "Usage: {prog} wait (--tag TAG | --text TEXT) [--exact] [--gone] [--wait-timeout SEC] [--interval SEC]\n\n\
             Poll `ui` until a matching element appears (or disappears with --gone). Exits 4 (`wait_timeout`) on timeout.\n\n\
             Options:\n\
               --tag TAG          Element with exact testTag\n\
               --text TEXT        Element whose text/value/description matches\n\
               --exact            Require --text to match exactly\n\
               --gone             Wait until no element matches\n\
               --wait-timeout SEC Seconds to keep polling (default: 10)\n\
               --interval SEC     Seconds between polls (default: 0.4, min: 0.05)\n"
        ),
        "tap" => format!(
            "Usage: {prog} tap (X Y | --xy X Y | --tag TAG | --text TEXT) [--exact] [--index N] [-s|--screenshot PNG] [--settle SEC]\n\n\
             Tap a screenshot-pixel coordinate or UI element. Requires 'Allow VM agents to control the app'.\n\n\
             Options:\n\
               X Y / --xy X Y     Tap position in screenshot pixels\n\
               --tag TAG          Element with exact testTag\n\
               --text TEXT        Element whose text/value/description matches\n\
               --exact            Require --text to match exactly\n\
               --index N          Pick N-th match when several match (default: 0)\n\
               -s, --screenshot   Capture a PNG after the action and include its path in JSON\n\
               --settle SEC       Seconds to wait before --screenshot (default: 0.6)\n"
        ),
        "long-press" => format!(
            "Usage: {prog} long-press (X Y | --xy X Y | --tag TAG | --text TEXT) [--exact] [--index N] [--duration-ms MS] [-s|--screenshot PNG] [--settle SEC]\n\n\
             Press and hold a screenshot-pixel coordinate or UI element. Requires 'Allow VM agents to control the app'.\n\n\
             Options:\n\
               X Y / --xy X Y     Position in screenshot pixels\n\
               --tag TAG          Element with exact testTag\n\
               --text TEXT        Element whose text/value/description matches\n\
               --exact            Require --text to match exactly\n\
               --index N          Pick N-th match when several match (default: 0)\n\
               --duration-ms MS   Hold duration in ms (default: 800; alias: --duration)\n\
               -s, --screenshot   Capture a PNG after the action\n\
               --settle SEC       Seconds to wait before --screenshot (default: 0.6)\n"
        ),
        "swipe" => format!(
            "Usage: {prog} swipe X1 Y1 X2 Y2 [--duration-ms MS] [--hold-ms MS] [-s|--screenshot PNG] [--settle SEC]\n\n\
             Drag a finger from (X1, Y1) to (X2, Y2) in screenshot pixels. Requires 'Allow VM agents to control the app'.\n\n\
             Options:\n\
               --duration-ms MS   Travel time in ms; shorter flings further (default: 300; alias: --duration)\n\
               --hold-ms MS       Rest at end before lifting to prevent fling (default: 0)\n\
               -s, --screenshot   Capture a PNG after the action\n\
               --settle SEC       Seconds to wait before --screenshot (default: 0.6)\n"
        ),
        "scroll" => format!(
            "Usage: {prog} scroll [up|down|left|right] [--amount FRAC] [--tag TAG] [--text TEXT] [--exact] [--index N] [-s|--screenshot PNG] [--settle SEC]\n\n\
             Scroll a list by dragging it without a fling. Direction names the content to reveal (default: down).\n\
             Requires 'Allow VM agents to control the app'.\n\n\
             Options:\n\
               up|down|left|right Content direction to reveal (default: down; also --direction DIR)\n\
               --amount FRAC      Fraction of container size to scroll (0.05..0.9, default: 0.6)\n\
               --tag / --text     Select a specific scrollable container (default: largest in top window)\n\
               --exact / --index  Refine selector matching\n\
               -s, --screenshot   Capture a PNG after the action\n\
               --settle SEC       Seconds to wait before --screenshot (default: 0.6)\n"
        ),
        "type" => format!(
            "Usage: {prog} type TEXT [--tag TAG] [--field LABEL] [--exact] [--index N] [--replace] [--submit] [-s|--screenshot PNG] [--settle SEC]\n\n\
             Enter TEXT in the focused text field or the field named by --tag / --field.\n\
             Requires 'Allow VM agents to control the app'.\n\n\
             Options:\n\
               TEXT               Text to enter (use `--` before text starting with `-`)\n\
               --tag TAG          Text field with exact testTag\n\
               --field LABEL      Text field whose label, text, or description contains LABEL\n\
               --exact            Require --field to match exactly\n\
               --index N          Pick N-th matching field (default: 0)\n\
               --replace          Replace existing content instead of inserting at cursor (alias: --clear)\n\
               --submit           Run the field's keyboard action (Send, Done, Search...)\n\
               -s, --screenshot   Capture a PNG after the action\n\
               --settle SEC       Seconds to wait before --screenshot (default: 0.6)\n"
        ),
        "key" => format!(
            "Usage: {prog} key (back|enter) [-s|--screenshot PNG] [--settle SEC]\n\n\
             Press Back (closes keyboard/menu/dialog or navigates back) or Enter (runs focused field's IME action).\n\
             Requires 'Allow VM agents to control the app'.\n\n\
             Options:\n\
               -s, --screenshot   Capture a PNG after the action\n\
               --settle SEC       Seconds to wait before --screenshot (default: 0.6)\n"
        ),
        "raw" => format!(
            "Usage: {prog} raw COMMAND [--args JSON_OBJECT | JSON_OBJECT]\n\n\
             Send an arbitrary command name and optional JSON object of arguments to the app.\n\n\
             Examples:\n\
               {prog} raw ping --args '{{}}'\n\
               {prog} raw state\n"
        ),
        _ => return None,
    };
    Some(text)
}

pub fn run_app_cli(prog: &str, raw_args: &[String]) -> i32 {
    match run_app_cli_inner(prog, raw_args) {
        Ok(code) => code,
        Err(err) => {
            let mut obj = Map::new();
            obj.insert("ok".into(), json!(false));
            if let Some(c) = err.code {
                obj.insert("code".into(), json!(c));
            }
            obj.insert("error".into(), json!(err.message));
            eprintln!("{}", Value::Object(obj));
            err.exit_code
        }
    }
}

fn next_val(args: &[String], i: &mut usize, flag: &str) -> Result<String, CliErr> {
    if *i + 1 >= args.len() {
        return Err(CliErr::bad_args(format!("Missing value for {flag}")));
    }
    *i += 1;
    Ok(args[*i].clone())
}

fn split_flag(arg: &str) -> (&str, Option<&str>) {
    if arg.starts_with("--") {
        if let Some((k, v)) = arg.split_once('=') {
            return (k, Some(v));
        }
    }
    (arg, None)
}

fn take_opt_val(
    args: &[String],
    i: &mut usize,
    flag: &str,
    inline: Option<&str>,
) -> Result<String, CliErr> {
    match inline {
        Some(v) => Ok(v.to_string()),
        None => next_val(args, i, flag),
    }
}

fn load_connection(
    host_arg: Option<String>,
    port_arg: Option<u16>,
    token_arg: Option<String>,
    config_arg: Option<String>,
) -> Result<Client, CliErr> {
    let mut cfg_host: Option<String> = None;
    let mut cfg_port: Option<u16> = None;
    let mut cfg_token: Option<String> = None;

    let (path, explicit) = match config_arg {
        Some(p) => (expand_tilde(&p), true),
        None => (config_path(), false),
    };

    if explicit || path.exists() {
        let raw = fs::read_to_string(&path).map_err(|e| {
            CliErr::new(
                format!("Cannot read daemon config {}: {e}", path.display()),
                EXIT_USAGE,
                Some("bad_config"),
            )
        })?;
        let parsed: Value = serde_json::from_str(&raw).map_err(|e| {
            CliErr::new(
                format!("Cannot read daemon config {}: {e}", path.display()),
                EXIT_USAGE,
                Some("bad_config"),
            )
        })?;
        let obj = parsed.as_object().ok_or_else(|| {
            CliErr::new(
                format!(
                    "Cannot read daemon config {}: expected JSON object",
                    path.display()
                ),
                EXIT_USAGE,
                Some("bad_config"),
            )
        })?;
        cfg_host = obj
            .get("host")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        cfg_port = obj
            .get("port")
            .and_then(Value::as_u64)
            .and_then(|p| u16::try_from(p).ok());
        cfg_token = obj.get("token").and_then(Value::as_str).map(str::to_string);
    }

    let mut host = host_arg
        .filter(|s| !s.is_empty())
        .or_else(|| env::var("AGENTS_TERM_HOST").ok().filter(|s| !s.is_empty()))
        .or(cfg_host)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    if host == "0.0.0.0" || host == "::" {
        host = "127.0.0.1".to_string();
    }

    let port = if let Some(p) = port_arg {
        p
    } else if let Ok(env_port) = env::var("AGENTS_TERM_PORT") {
        if env_port.is_empty() {
            cfg_port.unwrap_or(7501)
        } else {
            env_port
                .parse::<u16>()
                .map_err(|_| CliErr::bad_args(format!("Invalid AGENTS_TERM_PORT: {env_port}")))?
        }
    } else {
        cfg_port.unwrap_or(7501)
    };

    let token = match token_arg {
        Some(t) => t,
        None => match env::var("AGENTS_TERM_TOKEN") {
            Ok(t) => t,
            Err(_) => cfg_token.unwrap_or_default(),
        },
    };

    Ok(Client {
        host,
        port,
        token,
        http_timeout: Duration::from_secs(10),
    })
}

fn follow_logs(
    client: &dyn AppClient,
    build_args: &dyn Fn(Option<i64>) -> Value,
    since: Option<i64>,
    timeout_s: f64,
    interval: f64,
    max_polls: Option<usize>,
    out: &mut dyn Write,
) -> Result<i32, CliErr> {
    let mut seen_ts = since;
    let mut seen_at_ts: HashSet<String> = HashSet::new();
    let mut first = true;
    let mut polls = 0usize;
    loop {
        if let Some(max) = max_polls {
            if polls >= max {
                return Ok(0);
            }
        }
        polls += 1;
        match client.command("logs", build_args(seen_ts), timeout_s) {
            Ok(resp) => {
                first = false;
                if let Some(events) = resp
                    .get("result")
                    .and_then(|r| r.get("events"))
                    .and_then(Value::as_array)
                {
                    for ev in events {
                        let ts = ev
                            .get("ts")
                            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
                        if let Some(ts) = ts {
                            let id = serde_json::to_string(ev).unwrap_or_default();
                            if let Some(st) = seen_ts {
                                if ts < st || (ts == st && seen_at_ts.contains(&id)) {
                                    continue;
                                }
                                if ts > st {
                                    seen_ts = Some(ts);
                                    seen_at_ts.clear();
                                }
                            } else {
                                seen_ts = Some(ts);
                            }
                            seen_at_ts.insert(id);
                        }
                        let _ = writeln!(out, "{}", serde_json::to_string(ev).unwrap_or_default());
                        let _ = out.flush();
                    }
                }
            }
            Err(err)
                if !first && matches!(err.exit_code, EXIT_NOT_READY | EXIT_DAEMON | EXIT_APP) =>
            {
                eprintln!("{}", json!({ "warning": err.message, "code": err.code }));
            }
            Err(err) => return Err(err),
        }
        if let Some(max) = max_polls {
            if polls >= max {
                return Ok(0);
            }
        }
        if interval > 0.0 {
            thread::sleep(Duration::from_secs_f64(interval));
        }
    }
}

fn run_app_cli_inner(prog: &str, raw_args: &[String]) -> Result<i32, CliErr> {
    if raw_args.is_empty() {
        print!("{}", main_help(prog));
        return Ok(0);
    }

    let mut host: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut token: Option<String> = None;
    let mut config_arg: Option<String> = None;
    let mut timeout_s: f64 = 15.0;
    let mut pretty = false;
    let mut rest: Vec<String> = Vec::new();

    // Extract global flags from anywhere in argv (before `--`) so they work before or after subcommand.
    let mut i = 0;
    let mut after_double_dash = false;
    while i < raw_args.len() {
        let a = &raw_args[i];
        if after_double_dash {
            rest.push(a.clone());
            i += 1;
            continue;
        }
        if a == "--" {
            after_double_dash = true;
            rest.push(a.clone());
            i += 1;
            continue;
        }
        let (flag, inline) = split_flag(a);
        match flag {
            "--host" => host = Some(take_opt_val(raw_args, &mut i, "--host", inline)?),
            "--port" => {
                let v = take_opt_val(raw_args, &mut i, "--port", inline)?;
                port = Some(
                    v.parse::<u16>()
                        .map_err(|_| CliErr::bad_args(format!("Invalid --port: {v}")))?,
                );
            }
            "--token" => token = Some(take_opt_val(raw_args, &mut i, "--token", inline)?),
            "--config" => config_arg = Some(take_opt_val(raw_args, &mut i, "--config", inline)?),
            "--pretty" => {
                if inline.is_some() {
                    return Err(CliErr::bad_args(format!("--pretty takes no value: {a}")));
                }
                pretty = true;
            }
            "--timeout" => {
                let v = take_opt_val(raw_args, &mut i, "--timeout", inline)?;
                let parsed = v
                    .parse::<f64>()
                    .map_err(|_| CliErr::bad_args(format!("Invalid --timeout: {v}")))?;
                if !parsed.is_finite() || parsed <= 0.0 {
                    return Err(CliErr::bad_args(format!("--timeout must be positive: {v}")));
                }
                timeout_s = parsed;
            }
            _ => rest.push(a.clone()),
        }
        i += 1;
    }

    if rest.is_empty() {
        print!("{}", main_help(prog));
        return Ok(EXIT_USAGE);
    }

    // Handle top-level and subcommand help before loading config or connecting.
    if rest[0] == "-h" || rest[0] == "--help" {
        print!("{}", main_help(prog));
        return Ok(0);
    }
    if rest[0] == "help" {
        if rest.len() == 1 {
            print!("{}", main_help(prog));
            return Ok(0);
        }
        if let Some(help) = subcommand_help(prog, &rest[1]) {
            print!("{help}");
            return Ok(0);
        }
        return Err(CliErr::bad_args(format!(
            "Unknown command for help: {}",
            rest[1]
        )));
    }

    let sub = rest[0].as_str();
    let sub_args = &rest[1..];

    // Subcommand-specific `--help` / `-h` (before any `--`).
    let mut wants_help = false;
    for a in sub_args {
        if a == "--" {
            break;
        }
        if a == "-h" || a == "--help" {
            wants_help = true;
            break;
        }
    }
    if wants_help {
        if let Some(help) = subcommand_help(prog, sub) {
            print!("{help}");
            return Ok(0);
        }
        print!("{}", main_help(prog));
        return Ok(0);
    }

    let client = load_connection(host, port, token, config_arg)?;

    match sub {
        "status" => {
            if let Some(extra) = sub_args.first() {
                return Err(CliErr::bad_args(format!("Unknown status option: {extra}")));
            }
            let data = client.status()?;
            emit(&data, pretty);
            if data.get("ready").and_then(Value::as_bool) == Some(true) {
                Ok(0)
            } else {
                Ok(EXIT_NOT_READY)
            }
        }
        "ping" => {
            if let Some(extra) = sub_args.first() {
                return Err(CliErr::bad_args(format!("Unknown ping option: {extra}")));
            }
            let resp = client.command("ping", json!({}), timeout_s)?;
            emit(&resp, pretty);
            Ok(0)
        }
        "state" => {
            if let Some(extra) = sub_args.first() {
                return Err(CliErr::bad_args(format!("Unknown state option: {extra}")));
            }
            let resp = client.command("state", json!({}), timeout_s)?;
            emit(&resp, pretty);
            Ok(0)
        }
        "logs" => {
            let mut limit: i64 = 100;
            let mut categories: Vec<String> = Vec::new();
            let mut since: Option<i64> = None;
            let mut grep: Option<String> = None;
            let mut follow = false;
            let mut interval = 2.0f64;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--limit" => {
                        limit = take_opt_val(sub_args, &mut j, "--limit", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --limit"))?;
                    }
                    "--category" => {
                        categories.push(take_opt_val(sub_args, &mut j, "--category", inline)?);
                    }
                    "--since" => {
                        since = Some(
                            take_opt_val(sub_args, &mut j, "--since", inline)?
                                .parse()
                                .map_err(|_| CliErr::bad_args("Invalid --since"))?,
                        );
                    }
                    "--grep" => grep = Some(take_opt_val(sub_args, &mut j, "--grep", inline)?),
                    "-f" | "--follow" => follow = true,
                    "--interval" => {
                        let v: f64 = take_opt_val(sub_args, &mut j, "--interval", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --interval"))?;
                        if !v.is_finite() || v < 0.0 {
                            return Err(CliErr::bad_args("Invalid --interval"));
                        }
                        interval = v;
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown logs option: {other}")));
                    }
                }
                j += 1;
            }
            let build_args = |since_ts: Option<i64>| -> Value {
                let mut m = Map::new();
                m.insert("limit".into(), json!(limit));
                if !categories.is_empty() {
                    m.insert("categories".into(), json!(categories));
                }
                if let Some(s) = since_ts {
                    m.insert("sinceTs".into(), json!(s));
                }
                if let Some(ref g) = grep {
                    m.insert("contains".into(), json!(g));
                }
                Value::Object(m)
            };
            if !follow {
                let resp = client.command("logs", build_args(since), timeout_s)?;
                emit(&resp, pretty);
                return Ok(0);
            }
            let mut stdout = io::stdout().lock();
            follow_logs(
                &client,
                &build_args,
                since,
                timeout_s,
                interval,
                None,
                &mut stdout,
            )
        }
        "logcat" => {
            let mut lines: i64 = 400;
            let mut raw = false;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--lines" => {
                        lines = take_opt_val(sub_args, &mut j, "--lines", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --lines"))?;
                    }
                    "--raw" => raw = true,
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown logcat option: {other}")));
                    }
                }
                j += 1;
            }
            let resp = client.command("logcat", json!({ "lines": lines }), timeout_s)?;
            if raw {
                println!(
                    "{}",
                    resp.get("result")
                        .and_then(|r| r.get("text"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                );
            } else {
                emit(&resp, pretty);
            }
            Ok(0)
        }
        "crash" => {
            let mut raw = false;
            for a in sub_args {
                match a.as_str() {
                    "--raw" => raw = true,
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown crash option: {other}")));
                    }
                }
            }
            let resp = client.command("crash", json!({}), timeout_s)?;
            if raw {
                println!(
                    "{}",
                    resp.get("result")
                        .and_then(|r| r.get("text"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                );
            } else {
                emit(&resp, pretty);
            }
            Ok(0)
        }
        "screenshot" => {
            let mut output: Option<String> = None;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "-o" | "--output" => {
                        output = Some(take_opt_val(sub_args, &mut j, "--output", inline)?);
                    }
                    other => {
                        return Err(CliErr::bad_args(format!(
                            "Unknown screenshot option: {other}"
                        )));
                    }
                }
                j += 1;
            }
            let resp = save_screenshot(&client, timeout_s, output.as_deref())?;
            emit(&resp, pretty);
            Ok(0)
        }
        "record" => {
            let Some(action) = sub_args.first().map(String::as_str) else {
                return Err(CliErr::bad_args("record requires start, stop or status"));
            };
            let opts = &sub_args[1..];
            let mut max_duration: Option<u64> = None;
            let mut output: Option<String> = None;
            let mut no_fetch = false;
            let mut j = 0;
            while j < opts.len() {
                let (flag, inline) = split_flag(&opts[j]);
                match (action, flag) {
                    ("start", "--max-duration") => {
                        let v = take_opt_val(opts, &mut j, "--max-duration", inline)?;
                        max_duration =
                            Some(v.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
                                CliErr::bad_args(format!("Invalid --max-duration: {v}"))
                            })?);
                    }
                    ("stop", "-o" | "--output") => {
                        output = Some(take_opt_val(opts, &mut j, "--output", inline)?);
                    }
                    ("stop", "--no-fetch") => no_fetch = true,
                    (_, other) => {
                        return Err(CliErr::bad_args(format!(
                            "Unknown record {action} option: {other}"
                        )));
                    }
                }
                j += 1;
            }
            match action {
                "start" => {
                    let mut args = Map::new();
                    if let Some(secs) = max_duration {
                        args.insert("maxDurationSec".into(), json!(secs));
                    }
                    let resp = client.command(
                        "record_start",
                        Value::Object(args),
                        timeout_s.max(RECORD_START_TIMEOUT_S),
                    )?;
                    emit(&resp, pretty);
                    Ok(0)
                }
                "stop" => {
                    let mut resp = client.command("record_stop", json!({}), timeout_s)?;
                    if !no_fetch {
                        let id = resp
                            .get("result")
                            .and_then(|r| r.get("recordingId"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let (path, bytes) =
                            fetch_recording(&client, timeout_s, &id, output.as_deref())?;
                        if let Some(r) = resp.get_mut("result").and_then(Value::as_object_mut) {
                            r.insert("path".into(), json!(path.to_string_lossy()));
                            r.insert("bytes".into(), json!(bytes));
                        }
                    }
                    emit(&resp, pretty);
                    Ok(0)
                }
                "status" => {
                    let resp = client.command("record_status", json!({}), timeout_s)?;
                    emit(&resp, pretty);
                    Ok(0)
                }
                other => Err(CliErr::bad_args(format!(
                    "Unknown record action: {other} (expected start, stop or status)"
                ))),
            }
        }
        "dump" => {
            let mut note: Option<String> = None;
            let mut no_screenshot = false;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--note" => note = Some(take_opt_val(sub_args, &mut j, "--note", inline)?),
                    "--no-screenshot" => no_screenshot = true,
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown dump option: {other}")));
                    }
                }
                j += 1;
            }
            let mut args = Map::new();
            args.insert("includeScreenshot".into(), json!(!no_screenshot));
            if let Some(n) = note {
                args.insert("note".into(), json!(n));
            }
            let resp = client.command("dump", Value::Object(args), timeout_s.max(60.0))?;
            emit(&resp, pretty);
            Ok(0)
        }
        "ui" => {
            let mut args = Map::new();
            let mut brief = false;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--brief" => brief = true,
                    "--interactive" => {
                        args.insert("interactiveOnly".into(), json!(true));
                    }
                    "--tag" => {
                        args.insert(
                            "tag".into(),
                            json!(take_opt_val(sub_args, &mut j, "--tag", inline)?),
                        );
                    }
                    "--text" => {
                        args.insert(
                            "text".into(),
                            json!(take_opt_val(sub_args, &mut j, "--text", inline)?),
                        );
                    }
                    "--exact" => {
                        args.insert("exact".into(), json!(true));
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown ui option: {other}")));
                    }
                }
                j += 1;
            }
            let resp = client.command("ui", Value::Object(args), timeout_s)?;
            if brief {
                if let Some(res) = resp.get("result") {
                    println!("{}", format_ui_brief(res));
                }
            } else {
                emit(&resp, pretty);
            }
            Ok(0)
        }
        "tap" | "long-press" => {
            let mut args = Map::new();
            let mut pos_coords: Vec<i64> = Vec::new();
            let mut xy_flag: Option<(i64, i64)> = None;
            let mut has_sel = false;
            let mut screenshot: Option<String> = None;
            let mut settle_s: f64 = 0.6;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--tag" => {
                        args.insert(
                            "tag".into(),
                            json!(take_opt_val(sub_args, &mut j, "--tag", inline)?),
                        );
                        has_sel = true;
                    }
                    "--text" => {
                        args.insert(
                            "text".into(),
                            json!(take_opt_val(sub_args, &mut j, "--text", inline)?),
                        );
                        has_sel = true;
                    }
                    "--exact" => {
                        args.insert("exact".into(), json!(true));
                    }
                    "--index" => {
                        let idx: i64 = take_opt_val(sub_args, &mut j, "--index", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --index"))?;
                        args.insert("index".into(), json!(idx));
                    }
                    "--xy" => {
                        let x: i64 = next_val(sub_args, &mut j, "--xy X")?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --xy X"))?;
                        let y: i64 = next_val(sub_args, &mut j, "--xy Y")?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --xy Y"))?;
                        xy_flag = Some((x, y));
                    }
                    "--duration-ms" | "--duration" if sub == "long-press" => {
                        let d: i64 = take_opt_val(sub_args, &mut j, flag, inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args(format!("Invalid {flag}")))?;
                        args.insert("durationMs".into(), json!(d));
                    }
                    "-s" | "--screenshot" => {
                        screenshot = Some(take_opt_val(sub_args, &mut j, "--screenshot", inline)?);
                    }
                    "--settle" => {
                        let s: f64 = take_opt_val(sub_args, &mut j, "--settle", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --settle"))?;
                        if !s.is_finite() || s < 0.0 {
                            return Err(CliErr::bad_args("Invalid --settle"));
                        }
                        settle_s = s;
                    }
                    other if !other.starts_with('-') => {
                        let n: i64 = other.parse().map_err(|_| {
                            CliErr::bad_args(format!("Invalid coordinate: {other}"))
                        })?;
                        pos_coords.push(n);
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown {sub} option: {other}")));
                    }
                }
                j += 1;
            }
            if !pos_coords.is_empty() && xy_flag.is_some() {
                return Err(CliErr::bad_args(format!(
                    "{sub} takes either X Y or --xy X Y, not both"
                )));
            }
            let coords = if let Some(pair) = xy_flag {
                Some(pair)
            } else if pos_coords.is_empty() {
                None
            } else if pos_coords.len() == 2 {
                Some((pos_coords[0], pos_coords[1]))
            } else {
                return Err(CliErr::bad_args(format!("{sub} needs both X and Y")));
            };

            if coords.is_some()
                && (has_sel || args.contains_key("exact") || args.contains_key("index"))
            {
                return Err(CliErr::bad_args(format!(
                    "{sub} takes either X Y or --tag/--text, not both"
                )));
            }
            if coords.is_none() && !has_sel {
                return Err(CliErr::bad_args(format!(
                    "{sub} needs a target: X Y (screenshot pixels), --tag or --text"
                )));
            }
            if let Some((x, y)) = coords {
                args.insert("x".into(), json!(x));
                args.insert("y".into(), json!(y));
            }
            let cmd_name = if sub == "long-press" {
                "long_press"
            } else {
                "tap"
            };
            run_action(
                &client,
                cmd_name,
                Value::Object(args),
                timeout_s,
                screenshot.as_deref(),
                settle_s,
                pretty,
            )
        }
        "swipe" => {
            let mut coords: Vec<i64> = Vec::new();
            let mut duration_ms: i64 = 300;
            let mut hold_ms: i64 = 0;
            let mut screenshot: Option<String> = None;
            let mut settle_s: f64 = 0.6;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--duration-ms" | "--duration" => {
                        duration_ms = take_opt_val(sub_args, &mut j, flag, inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args(format!("Invalid {flag}")))?;
                    }
                    "--hold-ms" => {
                        hold_ms = take_opt_val(sub_args, &mut j, "--hold-ms", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --hold-ms"))?;
                    }
                    "-s" | "--screenshot" => {
                        screenshot = Some(take_opt_val(sub_args, &mut j, "--screenshot", inline)?);
                    }
                    "--settle" => {
                        let s: f64 = take_opt_val(sub_args, &mut j, "--settle", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --settle"))?;
                        if !s.is_finite() || s < 0.0 {
                            return Err(CliErr::bad_args("Invalid --settle"));
                        }
                        settle_s = s;
                    }
                    other if !other.starts_with('-') => {
                        coords.push(other.parse::<i64>().map_err(|_| {
                            CliErr::bad_args(format!("Invalid swipe coord: {other}"))
                        })?);
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown swipe option: {other}")));
                    }
                }
                j += 1;
            }
            if coords.len() != 4 {
                return Err(CliErr::bad_args(
                    "swipe requires X1 Y1 X2 Y2 (screenshot pixels)",
                ));
            }
            let args = json!({
                "x1": coords[0],
                "y1": coords[1],
                "x2": coords[2],
                "y2": coords[3],
                "durationMs": duration_ms,
                "holdMs": hold_ms,
            });
            let stroke_s = (duration_ms.max(0) + hold_ms.max(0)) as f64 / 1000.0;
            run_action(
                &client,
                "swipe",
                args,
                timeout_s.max(stroke_s + 10.0),
                screenshot.as_deref(),
                settle_s,
                pretty,
            )
        }
        "scroll" => {
            let mut args = Map::new();
            let mut direction = "down".to_string();
            let mut has_pos_dir = false;
            let mut screenshot: Option<String> = None;
            let mut settle_s: f64 = 0.6;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--direction" => {
                        direction = take_opt_val(sub_args, &mut j, "--direction", inline)?;
                    }
                    "--amount" => {
                        let amt: f64 = take_opt_val(sub_args, &mut j, "--amount", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --amount"))?;
                        if !amt.is_finite() {
                            return Err(CliErr::bad_args("Invalid --amount"));
                        }
                        args.insert("amount".into(), json!(amt));
                    }
                    "--tag" => {
                        args.insert(
                            "tag".into(),
                            json!(take_opt_val(sub_args, &mut j, "--tag", inline)?),
                        );
                    }
                    "--text" => {
                        args.insert(
                            "text".into(),
                            json!(take_opt_val(sub_args, &mut j, "--text", inline)?),
                        );
                    }
                    "--exact" => {
                        args.insert("exact".into(), json!(true));
                    }
                    "--index" => {
                        let idx: i64 = take_opt_val(sub_args, &mut j, "--index", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --index"))?;
                        args.insert("index".into(), json!(idx));
                    }
                    "-s" | "--screenshot" => {
                        screenshot = Some(take_opt_val(sub_args, &mut j, "--screenshot", inline)?);
                    }
                    "--settle" => {
                        let s: f64 = take_opt_val(sub_args, &mut j, "--settle", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --settle"))?;
                        if !s.is_finite() || s < 0.0 {
                            return Err(CliErr::bad_args("Invalid --settle"));
                        }
                        settle_s = s;
                    }
                    val if !val.starts_with('-') && !has_pos_dir => {
                        direction = val.to_string();
                        has_pos_dir = true;
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown scroll option: {other}")));
                    }
                }
                j += 1;
            }
            let dir_lower = direction.to_lowercase();
            if !matches!(dir_lower.as_str(), "up" | "down" | "left" | "right") {
                return Err(CliErr::bad_args(format!(
                    "Invalid scroll direction '{direction}' (expected: up, down, left, right)"
                )));
            }
            args.insert("direction".into(), json!(dir_lower));
            run_action(
                &client,
                "scroll",
                Value::Object(args),
                timeout_s,
                screenshot.as_deref(),
                settle_s,
                pretty,
            )
        }
        "type" => {
            let mut args = Map::new();
            let mut text_val: Option<String> = None;
            let mut screenshot: Option<String> = None;
            let mut settle_s: f64 = 0.6;
            let mut j = 0;
            let mut after_sep = false;
            while j < sub_args.len() {
                if after_sep {
                    if text_val.is_none() {
                        text_val = Some(sub_args[j].clone());
                        j += 1;
                        continue;
                    }
                    return Err(CliErr::bad_args(format!(
                        "Unexpected extra argument to type: {}",
                        sub_args[j]
                    )));
                }
                if sub_args[j] == "--" {
                    after_sep = true;
                    j += 1;
                    continue;
                }
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--tag" => {
                        args.insert(
                            "tag".into(),
                            json!(take_opt_val(sub_args, &mut j, "--tag", inline)?),
                        );
                    }
                    "--field" => {
                        args.insert(
                            "field".into(),
                            json!(take_opt_val(sub_args, &mut j, "--field", inline)?),
                        );
                    }
                    "--exact" => {
                        args.insert("exact".into(), json!(true));
                    }
                    "--index" => {
                        let idx: i64 = take_opt_val(sub_args, &mut j, "--index", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --index"))?;
                        args.insert("index".into(), json!(idx));
                    }
                    "--replace" | "--clear" => {
                        args.insert("replace".into(), json!(true));
                    }
                    "--submit" => {
                        args.insert("submit".into(), json!(true));
                    }
                    "-s" | "--screenshot" => {
                        screenshot = Some(take_opt_val(sub_args, &mut j, "--screenshot", inline)?);
                    }
                    "--settle" => {
                        let s: f64 = take_opt_val(sub_args, &mut j, "--settle", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --settle"))?;
                        if !s.is_finite() || s < 0.0 {
                            return Err(CliErr::bad_args("Invalid --settle"));
                        }
                        settle_s = s;
                    }
                    val if !val.starts_with('-') && text_val.is_none() => {
                        text_val = Some(val.to_string());
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown type option: {other}")));
                    }
                }
                j += 1;
            }
            let input_text =
                text_val.ok_or_else(|| CliErr::bad_args("type requires TEXT argument"))?;
            args.insert("text".into(), json!(input_text));
            run_action(
                &client,
                "type",
                Value::Object(args),
                timeout_s,
                screenshot.as_deref(),
                settle_s,
                pretty,
            )
        }
        "key" => {
            let mut key_name: Option<String> = None;
            let mut screenshot: Option<String> = None;
            let mut settle_s: f64 = 0.6;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "-s" | "--screenshot" => {
                        screenshot = Some(take_opt_val(sub_args, &mut j, "--screenshot", inline)?);
                    }
                    "--settle" => {
                        let s: f64 = take_opt_val(sub_args, &mut j, "--settle", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --settle"))?;
                        if !s.is_finite() || s < 0.0 {
                            return Err(CliErr::bad_args("Invalid --settle"));
                        }
                        settle_s = s;
                    }
                    val if !val.starts_with('-') && key_name.is_none() => {
                        key_name = Some(val.to_string());
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown key option: {other}")));
                    }
                }
                j += 1;
            }
            let key = key_name
                .ok_or_else(|| CliErr::bad_args("key requires KEY (back or enter)"))?
                .to_lowercase();
            if !matches!(key.as_str(), "back" | "enter") {
                return Err(CliErr::bad_args(format!(
                    "Invalid key '{key}' (supported: back, enter)"
                )));
            }
            run_action(
                &client,
                "key",
                json!({ "key": key }),
                timeout_s,
                screenshot.as_deref(),
                settle_s,
                pretty,
            )
        }
        "wait" => {
            let mut tag: Option<String> = None;
            let mut text: Option<String> = None;
            let mut exact = false;
            let mut gone = false;
            let mut wait_timeout = 10.0f64;
            let mut interval = 0.4f64;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--tag" => tag = Some(take_opt_val(sub_args, &mut j, "--tag", inline)?),
                    "--text" => text = Some(take_opt_val(sub_args, &mut j, "--text", inline)?),
                    "--exact" => exact = true,
                    "--gone" => gone = true,
                    "--wait-timeout" => {
                        let v: f64 = take_opt_val(sub_args, &mut j, "--wait-timeout", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --wait-timeout"))?;
                        if !v.is_finite() || v <= 0.0 {
                            return Err(CliErr::bad_args("Invalid --wait-timeout"));
                        }
                        wait_timeout = v;
                    }
                    "--interval" => {
                        let v: f64 = take_opt_val(sub_args, &mut j, "--interval", inline)?
                            .parse()
                            .map_err(|_| CliErr::bad_args("Invalid --interval"))?;
                        if !v.is_finite() || v < 0.0 {
                            return Err(CliErr::bad_args("Invalid --interval"));
                        }
                        interval = v;
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown wait option: {other}")));
                    }
                }
                j += 1;
            }
            if tag.is_none() && text.is_none() {
                return Err(CliErr::bad_args("wait needs --tag or --text"));
            }
            let mut ui_args = Map::new();
            if let Some(ref t) = tag {
                ui_args.insert("tag".into(), json!(t));
            }
            if let Some(ref t) = text {
                ui_args.insert("text".into(), json!(t));
            }
            if exact {
                ui_args.insert("exact".into(), json!(true));
            }

            let started = Instant::now();
            loop {
                let resp = client.command("ui", Value::Object(ui_args.clone()), timeout_s)?;
                let nodes: Vec<Value> = resp
                    .get("result")
                    .and_then(|r| r.get("nodes"))
                    .and_then(Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .filter(|n| {
                                node_matches_selector(n, tag.as_deref(), text.as_deref(), exact)
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                let waited_ms = started.elapsed().as_millis() as u64;
                if (!nodes.is_empty()) != gone {
                    emit(
                        &json!({
                            "ok": true,
                            "waitedMs": waited_ms,
                            "gone": gone,
                            "nodes": nodes,
                        }),
                        pretty,
                    );
                    return Ok(0);
                }
                if started.elapsed().as_secs_f64() >= wait_timeout {
                    let what = if gone {
                        "still on screen"
                    } else {
                        "not on screen"
                    };
                    return Err(CliErr::new(
                        format!("Element is {what} after {wait_timeout}s"),
                        EXIT_APP,
                        Some("wait_timeout"),
                    ));
                }
                thread::sleep(Duration::from_secs_f64(interval.max(0.05)));
            }
        }
        "raw" => {
            let mut cmd_name: Option<String> = None;
            let mut args_json: Option<String> = None;
            let mut j = 0;
            while j < sub_args.len() {
                let (flag, inline) = split_flag(&sub_args[j]);
                match flag {
                    "--args" => {
                        args_json = Some(take_opt_val(sub_args, &mut j, "--args", inline)?);
                    }
                    other if !other.starts_with('-') && cmd_name.is_none() => {
                        cmd_name = Some(other.to_string());
                    }
                    other if args_json.is_none() => {
                        args_json = Some(other.to_string());
                    }
                    other => {
                        return Err(CliErr::bad_args(format!("Unknown raw option: {other}")));
                    }
                }
                j += 1;
            }
            let cmd =
                cmd_name.ok_or_else(|| CliErr::bad_args("raw requires COMMAND [--args JSON]"))?;
            let args: Value = match args_json {
                Some(raw_str) => {
                    let parsed: Value = serde_json::from_str(&raw_str)
                        .map_err(|e| CliErr::bad_args(format!("--args is not valid JSON: {e}")))?;
                    if !parsed.is_object() {
                        return Err(CliErr::bad_args("--args must be a JSON object"));
                    }
                    parsed
                }
                None => json!({}),
            };
            let resp = client.command(&cmd, args, timeout_s)?;
            emit(&resp, pretty);
            Ok(0)
        }
        other => Err(CliErr::bad_args(format!("Unknown app command: {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct FakeLogsClient {
        batches: RefCell<Vec<Result<Value, CliErr>>>,
        sent_since: RefCell<Vec<Option<i64>>>,
    }

    impl AppClient for FakeLogsClient {
        fn status(&self) -> Result<Value, CliErr> {
            unreachable!()
        }

        fn command(&self, _cmd: &str, args: Value, _timeout_s: f64) -> Result<Value, CliErr> {
            self.sent_since
                .borrow_mut()
                .push(args.get("sinceTs").and_then(Value::as_i64));
            self.batches.borrow_mut().remove(0)
        }
    }

    #[test]
    fn logs_follow_dedupes_inclusive_since_and_keeps_same_ms_events() {
        let client = FakeLogsClient {
            batches: RefCell::new(vec![
                Ok(json!({ "result": { "events": [
                    { "ts": 10, "event": "a" },
                    { "ts": 20, "event": "b" }
                ]}})),
                Ok(json!({ "result": { "events": [
                    { "ts": 20, "event": "b" },
                    { "ts": 20, "event": "c" },
                    { "ts": 30, "event": "d" }
                ]}})),
                Ok(json!({ "result": { "events": [
                    { "ts": 30, "event": "d" }
                ]}})),
                Err(CliErr::new(
                    "transient",
                    EXIT_NOT_READY,
                    Some("app_disconnected"),
                )),
                Ok(json!({ "result": { "events": [
                    { "ts": 30, "event": "d" },
                    { "ts": 40, "event": "e" }
                ]}})),
            ]),
            sent_since: RefCell::new(Vec::new()),
        };

        let build_args = |since_ts: Option<i64>| -> Value {
            let mut m = Map::new();
            m.insert("limit".into(), json!(100));
            if let Some(s) = since_ts {
                m.insert("sinceTs".into(), json!(s));
            }
            Value::Object(m)
        };

        let mut out = Vec::new();
        let rc = follow_logs(&client, &build_args, None, 15.0, 0.0, Some(5), &mut out).unwrap();
        assert_eq!(rc, 0);

        let lines: Vec<String> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                v["event"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(lines, vec!["a", "b", "c", "d", "e"]);
        assert_eq!(
            *client.sent_since.borrow(),
            vec![None, Some(20), Some(30), Some(30), Some(30)]
        );
    }

    #[test]
    fn ui_brief_formats_android_snapshot_exactly() {
        let result = json!({
            "width": 1080,
            "height": 2400,
            "nodeCount": 2,
            "windows": [
                { "index": 0, "type": "activity" },
                { "index": 1, "type": "popup" }
            ],
            "nodes": [
                {
                    "id": 4,
                    "window": 0,
                    "bounds": [900, 2100, 1060, 2300],
                    "center": [980, 2200],
                    "role": "Button",
                    "description": "Send",
                    "tag": "send_button",
                    "clickable": true
                },
                {
                    "id": 3,
                    "window": 1,
                    "bounds": [0, 2100, 880, 2300],
                    "center": [440, 2200],
                    "text": "Message",
                    "value": "dra\"ft",
                    "tag": "composer_input",
                    "editable": true,
                    "focused": true,
                    "enabled": false,
                    "toggle": "on",
                    "selected": false
                }
            ]
        });
        let formatted = format_ui_brief(&result);
        let lines: Vec<&str> = formatted.lines().collect();
        assert_eq!(
            lines,
            vec![
                "# 1080x2400 screenshot pixels; windows 0:activity, 1:popup; [x,y] is each element's center",
                "[980,2200] Button description=\"Send\" tag=send_button clickable",
                "[440,2200] \"Message\" value=\"dra\\\"ft\" tag=composer_input toggle=on selected=false editable focused disabled window=1",
            ]
        );
    }
}
