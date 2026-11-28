# Finding a tab on the VM

In a chat screenshot, look **below the message box** for the tab ID. On the
phone, tap or long-press the ID to copy it, including the `tab_` prefix.

## Inspect tab state

If the ID is cropped out or you want to see what other tabs are doing, list
the open tabs and match the title, agent, and task summary:

```sh
root="${AGENTSTERM_HOME:-$HOME/.agents-term}"
jq -c 'select(.state == "open") | {id, title, agent, status, taskSummary}' \
  "$root/agents_tabs/tabs.jsonl"
```

Use the full ID to see the tab's last reported activity and saved conversation:

```sh
tab_id=tab_2b9959750a80  # Replace with the ID you found.
jq --arg id "$tab_id" 'select(.id == $id) | {cwd, state, status, statusDetail, updatedAt}' \
  "$root/agents_tabs/tabs.jsonl"
less "$root/agents_tabs/$tab_id/messages.json"
```

## What lives under `~/.agents-term/`

- `agents_tabs/workspace.json` records the selected tab in `activeTabId`.
- `agents_tabs/tabs.jsonl` holds one current record per open or recently closed
  tab. `state` tells you whether the tab is open. `status` tells you whether
  its agent is working, idle, done, or in another reported state. Remove the open-tab filter above to
  include recently closed tabs.
- `agents_tabs/<tab_id>/` holds the saved chat in `messages.json` and any
  uploaded files in `attachments/`.
- `debug-dumps/latest/` points to the most recent uploaded phone debug bundle,
  when available. For live phone inspection, see `live-debugging.md`.
- `bin/` holds the daemon and app CLI, `config.json` holds connection settings
  and the bearer token, and `tmp/` holds temporary files. Detached daemons
  also write `daemon.log` and `daemon.pid`.

Treat this directory as read-only when debugging. The daemon manages its contents.
