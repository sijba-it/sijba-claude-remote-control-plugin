# sijba-claude-remote-control-plugin

sijba channel for **Claude Code Remote Control**: the plugin keeps a
`claude remote-control` server alive (in tmux `sijba-<project>`), mirrors
prompts and answers from paired phone/desktop sessions into the sijba thread
via `post`, and relays sijba chat messages into the active RC session.

## Security

Relayed sijba text is delivered into an **`auto`** permission session with
full shell access on `WORK_DIR`. Only use this channel with the same trust
model as other permission-relay plugins (`SIJBA_PERMISSION_RELAY` on the
channel plugin): anyone who can message the channel can steer the RC session.

The haiku relay helper is limited to **SendMessage** and **ToolSearch**
(`dontAsk`) so injected content in the relayed text cannot run local tools.

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install websockets

export SIJBA_URL=https://<account>.sijba.eu
export SIJBA_TOKEN=...
export PROJECT_NAME=my-project
export WORK_DIR=/path/to/project
export REGISTRY_FILE=/path/to/registry.json
export CLAUDE_CMD=/path/to/claude   # optional

python main.py
```

## Controller catalog

```json
"remote-control": {
  "kind": "git",
  "url": "https://github.com/sijba-it/sijba-claude-remote-control-plugin.git",
  "env": { "CLAUDE_CMD": "/home/you/.local/bin/claude" }
}
```

In-channel commands: `/archive`, `/pause`, `/log [n]` (same exit codes as other
git plugins).
