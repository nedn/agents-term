# Live Debugging AgentsTerm (Agent Guide)

AgentsTerm includes a live inspection and UI-control CLI (`agents_term_app_cli`, also available as `agentsterm-daemon app`) and debug dump uploader so coding agents running on the VM can inspect or drive the Android app while a human uses it on their phone.

```text
agent ─▶ agents_term_app_cli ─HTTP─▶ daemon (/api/app/*) ─WS─▶ Android app
```

The daemon passes each command to the app over the WebSocket the app already holds, so nothing listens on the phone. Run `agents_term_app_cli --help` (or `agents_term_app_cli help <command>`) anytime for full usage, options, and workflow guidance.

## Prerequisites

1. The Android phone is connected to the AgentsTerm VM daemon.
2. The user enabled **Settings > Debug sharing > Share debug logs to VM** and **Allow VM agents to inspect the app**.
   - Logcat and screenshots also respect the **Include Android logcat** and **Include screenshot** toggles.
   - UI control commands (`tap`, `long-press`, `swipe`, `scroll`, `type`, `key`) also require **Allow VM agents to control the app**. Without it they exit `3` with `control_disabled`.
   - Screen recording (`record`) also requires **Allow VM agents to record the screen**. Without it, it exits `3` with `recording_disabled`. The user also approves every recording in an Android dialog.
3. Always run `agents_term_app_cli status` first. Exit code `3` means the app is disconnected or inspection/control is turned off — ask the user to connect/enable it rather than retrying in a loop.

## Live Inspection Commands

The static `agentsterm-daemon` binary provides the `app` subcommand and installs the `~/.agents-term/bin/agents_term_app_cli` symlink. Connection details are loaded read-only from `~/.agents-term/config.json` (respecting `AGENTSTERM_HOME`; override with `--host/--port/--token/--config` or `AGENTS_TERM_HOST/PORT/TOKEN`).

```bash
agents_term_app_cli status                       # Check connection & opt-in (exit 3 if not ready)
agents_term_app_cli ping                         # Round-trip ping to the app
agents_term_app_cli state --pretty               # Current screen, connection, tabs, messages, settings
agents_term_app_cli logs --limit 50              # In-app debug events (http/ws/ssh/ui/lifecycle/error/app_state)
agents_term_app_cli logs --category error --category ws --since 1795536750000
agents_term_app_cli logs --grep timeout          # Substring match across event/detail/payload
agents_term_app_cli logs -f                      # Follow live events (one JSON object per line)
agents_term_app_cli logcat --lines 800 --raw     # Android logcat for the app process
agents_term_app_cli crash --raw                  # Last uncaught exception stack trace, if any
agents_term_app_cli screenshot -o /tmp/app.png   # Capture PNG of the current app window
agents_term_app_cli ui --brief                   # List visible Compose UI nodes and centers
agents_term_app_cli dump --note "repro context"  # Upload full bundle to ~/.agents-term/debug-dumps/latest/
agents_term_app_cli record start                 # Ask the user to approve, then record the screen
agents_term_app_cli record stop -o /tmp/flow.mp4 # Stop and download the video
agents_term_app_cli raw ping --args '{}'         # Send any command by name with a JSON args object
```

*(You can also invoke `agentsterm-daemon app <subcommand>` directly.)*

### Exit Codes

Output is JSON on stdout (unless `--raw` or `--brief`). Failures print `{"ok":false,"code":"...","error":"..."}` on stderr:

| Exit | Meaning | `code` values |
| :--- | :--- | :--- |
| `0` | Success | |
| `1` | Local usage or config error | `bad_config`, `bad_args`, `io_error` |
| `2` | Daemon unreachable, bad token, or daemon too old | `daemon_unreachable`, `unauthorized`, `daemon_unsupported`, `daemon_error` |
| `3` | App not connected or remote inspection/control/recording disabled | `no_app_connected`, `unsupported_app`, `inspection_disabled`, `control_disabled`, `recording_disabled`, `app_disconnected` |
| `4` | App returned an error, timed out, or `wait` gave up | `app_error`, `timeout`, `wait_timeout` |

Right after the app connects or comes back from the background, the daemon waits up to 2 seconds for it to announce itself before answering, so a command sent in that moment is slower rather than failing with `unsupported_app`.

## Driving the App

Look, act, look again. All coordinates are **screenshot pixels**: the `screenshot` PNG, the bounds/centers that `ui` reports, and the coordinates passed to `tap`, `long-press`, and `swipe` share one space.

```bash
agents_term_app_cli ui --brief                          # one line per element; [x,y] is its center
# 1080x2400 screenshot pixels; windows 0:activity; [x,y] is each element's center
[1010,150] Button description="More options" tag=overflow_menu_button clickable
[440,2200] "Message" tag=composer_input editable
[980,2200] Button description="Send" tag=send_button clickable disabled

agents_term_app_cli tap --tag overflow_menu_button      # by test tag (most stable)
agents_term_app_cli tap --text "Settings"               # by visible text, value, or description (substring)
agents_term_app_cli tap 980 2200                        # by screenshot-pixel position (or --xy 980 2200)
agents_term_app_cli long-press --text "Summarize" --duration-ms 1000
agents_term_app_cli type "Summarize the repo" --tag composer_input --replace --submit
agents_term_app_cli type "22" --field "Port"            # pick text field by label/content
agents_term_app_cli scroll down                         # reveal content further down (no fling)
agents_term_app_cli scroll up --amount 0.3 --tag messages
agents_term_app_cli swipe 100 1200 900 1200 --hold-ms 150
agents_term_app_cli key back                            # closes keyboard/menu/dialog, else goes back
agents_term_app_cli key enter                           # focused field's keyboard action (Send, Done...)
agents_term_app_cli wait --text "Repository is clean"   # poll until on screen (exit 4 after --wait-timeout)
agents_term_app_cli wait --tag menu_app_settings --gone
```

- Every control action accepts `-s /tmp/after.png` (`--screenshot`) and `--settle SEC` (default `0.6`) to capture the resulting screen in the same call.
- `ui` lists only what is currently on screen. To reach items further down a list: `scroll`, then `ui` again.
- When several elements match, the first one in the front-most window is used; pick another with `--index N`.
- Dialogs, bottom sheets, and popup menus are separate windows (`window` >= 1 in `ui`).
- `ui` never returns password field contents, and `type` text is never written to the debug log.

### QA Testing Workflow

1. Run `agents_term_app_cli status` (expect `controlReady: true`), then `ui --brief` and `screenshot`.
2. Walk the flow with `tap` / `type` / `scroll` / `key back`, using `wait` instead of sleeping and `-s step-N.png` for evidence.
3. Verify with `ui`, `state`, `logs --category error`, and `crash --raw`.
4. On a bug, capture it while visible: `screenshot`, then `dump --note "<what happened>"`.

## Recording the Screen

`record` captures a video of the phone screen (MP4, H.264, no audio), for example as evidence of a QA run or to show an animation glitch that a screenshot misses.

```bash
agents_term_app_cli record start --max-duration 120     # waits up to 75 s for the user to approve
agents_term_app_cli tap --tag send_button               # ...drive the app...
agents_term_app_cli record stop -o /tmp/flow.mp4        # stop and download; prints path, bytes, durationMs
agents_term_app_cli record status                       # idle | recording | finished
```

- The app must be in the foreground when the recording starts: Android shows its own "start recording?" dialog and the user has to accept it. Tell the user before you run `record start`. If they decline or do not answer within 60 seconds, the command exits `4`.
- The video is recorded by Android's screen capture, so it shows the whole screen, including the keyboard and system bars. If the user picks "a single app" in the dialog, it shows only that app. Other apps and notifications that appear on screen are recorded too.
- The video uses the same pixel space as `screenshot` (at most 1080 pixels wide).
- A recording ends by itself after `--max-duration` seconds (default 60, at most 600), when the user stops it from the notification or the status bar, or when they turn the setting off. `record stop` then downloads what was recorded; `stopReason` says why it ended.
- The phone keeps only the last recording, and `record start` discards it. Download it with `record stop` first.
- The video is downloaded in chunks over the app's WebSocket, at up to about 20 MB per minute of video. Keep recordings short.
- To look at a recording yourself, extract frames from it (for example `ffmpeg -i flow.mp4 -vf fps=2 frame-%03d.png`) and read those.

## Uploaded Debug Dumps

When the user taps **Upload Debug Dump to VM** (or the top-bar bug icon, or `agents_term_app_cli dump`), the daemon writes a sanitized bundle to `~/.agents-term/debug-dumps/` and updates the symlink:

```text
~/.agents-term/debug-dumps/latest/
├── summary.md      # Overview + user note
├── meta.json       # App version, device, timestamps
├── state.json      # Sanitized workspace/tab snapshot
├── events.ndjson   # In-app event buffer
├── logcat.txt      # Optional Android logcat
├── crash.txt       # Optional last uncaught crash trace
└── screenshot.png  # Optional window screenshot
```

## Protocol

| Direction | Frame |
| :--- | :--- |
| app → daemon (on connect, on toggle) | `{"type":"app_hello","reporter":{"enabled":true,"appVersion":"…","device":"…","commands":[…],"controlEnabled":false,"controlCommands":["tap",…],"recordingEnabled":false}}` |
| daemon → app | `{"type":"app_state_request","requestId":"…","command":"logs","args":{…}}` |
| app → daemon | `{"type":"app_state_response","requestId":"…","ok":true,"result":{…}}` or `"ok":false,"error":"…"` (plus `"code":"recording_disabled"` when that is the reason) |

Recording commands: `record_start` (`maxDurationSec`), `record_stop`, `record_status`, and `record_read` (`recordingId`, `offset`, `length`), which returns one chunk of the finished video as `dataBase64` with `totalBytes` and `eof`.

REST: `GET /api/app/status` and `POST /api/app/command` with body `{"command":"logs","args":{…},"timeoutMs":15000}` (bearer auth).

## Important Rules

- **Screenshots**, `ui`, and control commands require the app to be in the foreground and show a brief toast on the phone. Other commands may time out if Android freezes a backgrounded app.
- Only drive the app when the task calls for it: the user is holding that phone.
- **Bearer tokens** are redacted before leaving the phone.
- **Treat `~/.agents-term/` as read-only:** Only `agentsterm-daemon` writes to `~/.agents-term/`. Agents may read state/logs/dumps and execute binaries in `~/.agents-term/bin/`, but must never modify files in `~/.agents-term/` directly.
- **Never restart the running daemon from an AgentsTerm tab:** Agent sessions are child processes of the VM daemon (`agentsterm-daemon.service`). Running `agentsterm-daemon install` or restarting `agentsterm-daemon.service` from inside an agent tab kills your own session. Test daemon changes on a scratch port with a temporary `AGENTSTERM_HOME` instead (`AGENTSTERM_HOME=$(mktemp -d) agentsterm-daemon serve --host 127.0.0.1 --port 7599`).
