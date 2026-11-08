#!/usr/bin/env python3
"""Claude Remote Control plugin for sijba — keeps a `claude remote-control`
server alive, mirrors phone/chat prompts and answers into the sijba channel via
`post`, and relays sijba messages into the active RC session with a small
haiku helper (SendMessage only).

Plugin contract: sijba-agent-controller/README.md (env vars, /archive, /pause).
"""
from __future__ import annotations

import asyncio
import collections
import fcntl
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import websockets

EXIT_ARCHIVED = 90
EXIT_PAUSED = 91

PLUGIN_NAME = "remote-control"

SIJBA_URL = os.environ.get("SIJBA_URL", "")
SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
WORK_DIR = Path(os.environ.get("WORK_DIR", ""))
REGISTRY_FILE = os.environ.get("REGISTRY_FILE", "")
CLAUDE_CMD = os.environ.get("CLAUDE_CMD", "claude")

BOT_FLUSH_IDLE_SEC = 15
SERVER_WATCH_SEC = 30
SESSION_SCAN_SEC = 2
SESSION_WAIT_SEC = 45
MAX_CHARS_TO_UI = 8000
REMOTE_CONTROL_CAPACITY = max(1, int(os.environ.get("REMOTE_CONTROL_CAPACITY", "1")))
CONFIG_PATH = WORK_DIR / ".sijba-remote-control.json"

FALLBACK_SUBTYPES = ("model_fallback", "model_refusal_fallback", "model_consent_fallback")

if not SIJBA_URL or not SIJBA_TOKEN or not PROJECT_NAME or not WORK_DIR.is_dir() or not REGISTRY_FILE:
    print("ERROR: need SIJBA_URL, SIJBA_TOKEN, PROJECT_NAME, WORK_DIR (dir), REGISTRY_FILE", file=sys.stderr)
    sys.exit(1)

PLUGIN_ID = f"{PLUGIN_NAME}-{PROJECT_NAME}"
CHANNEL_ID = f"{PLUGIN_NAME}:{PROJECT_NAME}"
TMUX_SESSION = f"sijba-{PROJECT_NAME}"
SESSIONS_DIR = Path.home() / ".claude" / "sessions"
PROJECTS_DIR = Path.home() / ".claude" / "projects"

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)
_AGENT_ENV_STRIP = ("SIJBA_TOKEN", "SIJBA_URL", "REGISTRY_FILE")


def _agent_env() -> dict:
    return {k: v for k, v in os.environ.items() if k not in _AGENT_ENV_STRIP}


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [{PROJECT_NAME}] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _truncate(text: str, limit: int = MAX_CHARS_TO_UI) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…[truncated, {len(text) - limit} more chars]"


def _set_registry_status(name: str, status: str) -> None:
    path = Path(REGISTRY_FILE)
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else {"roots": [], "projects": {}}
            data.setdefault("projects", {}).setdefault(name, {})["status"] = status
            data["projects"][name]["status_changed_at"] = time.time()
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, path)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _cwd_slug(cwd: Path) -> str:
    return re.sub(r"[^A-Za-z0-9]", "-", str(cwd.resolve()))


def _jsonl_path(session_id: str) -> Path:
    return PROJECTS_DIR / _cwd_slug(WORK_DIR) / f"{session_id}.jsonl"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _is_descendant(child: int, ancestor: int) -> bool:
    pid = child
    while pid > 1:
        if pid == ancestor:
            return True
        try:
            with open(f"/proc/{pid}/stat") as f:
                pid = int(f.read().split()[3])
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
            break
    return False


def _base_model(model_id: str) -> str:
    return re.sub(r"(\[.*\]|-\d{8})$", "", model_id.strip().lower())


def _first(event: dict, *keys: str) -> str:
    return next((str(event[k]) for k in keys if event.get(k)), "")


def _tmux(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["tmux", *args], capture_output=True, text=True)


def _tmux_has_session() -> bool:
    return _tmux("has-session", "-t", TMUX_SESSION).returncode == 0


def _pane_pid() -> int | None:
    r = _tmux("list-panes", "-t", TMUX_SESSION, "-F", "#{pane_pid}")
    if r.returncode != 0:
        return None
    line = (r.stdout or "").strip().splitlines()
    if not line:
        return None
    try:
        return int(line[0])
    except ValueError:
        return None


def _socket_live(path: str | None) -> bool:
    if not path:
        return False
    return Path(path).exists()


def _session_live(meta: dict, root_pid: int | None) -> bool:
    pid = meta.get("pid")
    if not isinstance(pid, int) or not _pid_alive(pid):
        return False
    if root_pid and not _is_descendant(pid, root_pid):
        return False
    return _socket_live(meta.get("messagingSocketPath"))


def _load_rc_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_rc_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def _session_display(meta: dict) -> str:
    return str(meta.get("name") or meta["sessionId"][:8])


def _pick_session(
    sessions: list[dict],
    *,
    explicit: str | None = None,
    preferred: str | None = None,
) -> dict | None:
    """Choose a live RC session. explicit = user @name / name: prefix."""
    if not sessions:
        return None
    if explicit:
        want = explicit.lower()
        for s in sessions:
            if _session_display(s).lower() == want:
                return s
        return None
    if preferred:
        want = preferred.lower()
        for s in sessions:
            if _session_display(s).lower() == want:
                return s
    if len(sessions) == 1:
        return sessions[0]

    def _rank(s: dict) -> tuple[int, float]:
        busy = 1 if s.get("status") == "busy" else 0
        return (busy, float(s.get("updatedAt") or 0))

    return max(sessions, key=_rank)


def _parse_session_prefix(text: str, live_names: list[str]) -> tuple[str | None, str]:
    """If the message starts with @session or 'session: …', peel it off."""
    text = text.strip()
    if text.startswith("@"):
        head, _, rest = text[1:].partition(" ")
        if head and (not live_names or head.lower() in {n.lower() for n in live_names}):
            return head, rest.strip()
    for name in sorted(live_names, key=len, reverse=True):
        for sep in (": ", ":"):
            prefix = f"{name}{sep}"
            if text.lower().startswith(prefix.lower()):
                return name, text[len(prefix):].strip()
    return None, text


# ---------------------------------------------------------------------------
# Remote-control server (tmux + claude remote-control)
# ---------------------------------------------------------------------------

class RemoteControlServer:
    def __init__(self) -> None:
        self.root_pid: int | None = None
        self._wrap_proc: asyncio.subprocess.Process | None = None

    async def ensure(self) -> None:
        pid = _pane_pid() if _tmux_has_session() else None
        if pid and _pid_alive(pid):
            self.root_pid = pid
            return
        await self._spawn()

    async def _spawn(self) -> None:
        argv = [
            CLAUDE_CMD, "remote-control", "--name", PROJECT_NAME,
            "--permission-mode", "auto",
            "--capacity", str(REMOTE_CONTROL_CAPACITY),
            "--spawn=same-dir",
        ]
        inner = " ".join(shlex.quote(a) for a in argv)
        tmux_cmd = (
            f"tmux new-session -A -s {shlex.quote(TMUX_SESSION)} "
            f"-c {shlex.quote(str(WORK_DIR))} {inner}"
        )
        env = dict(os.environ)
        env.setdefault("TERM", "xterm-256color")
        self._wrap_proc = await asyncio.create_subprocess_exec(
            "script", "-qc", tmux_cmd, "/dev/null",
            cwd=str(WORK_DIR), env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        for _ in range(40):
            await asyncio.sleep(0.25)
            pid = _pane_pid()
            if pid and _pid_alive(pid):
                self.root_pid = pid
                _log(f"remote-control server ready (tmux pane pid={pid})")
                return
        raise RuntimeError("remote-control server did not start in tmux")

    async def watch(self) -> None:
        while True:
            await asyncio.sleep(SERVER_WATCH_SEC)
            try:
                pid = _pane_pid() if _tmux_has_session() else None
                if pid and _pid_alive(pid):
                    self.root_pid = pid
                    continue
                _log("remote-control server missing — respawning")
                await self.ensure()
            except Exception as exc:
                _log(f"server watch error: {exc}")


def _owned_sessions(root_pid: int | None) -> list[dict]:
    if not root_pid:
        return []
    out: list[dict] = []
    work = WORK_DIR.resolve()
    try:
        paths = list(SESSIONS_DIR.glob("*.json"))
    except OSError:
        return []
    for path in paths:
        try:
            meta = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        pid = meta.get("pid")
        if not isinstance(pid, int) or not _pid_alive(pid):
            continue
        if not _is_descendant(pid, root_pid):
            continue
        try:
            if Path(meta.get("cwd", "")).resolve() != work:
                continue
        except OSError:
            continue
        if not _session_live(meta, root_pid):
            continue
        out.append(meta)
    return out


async def _wait_for_live_session(server: RemoteControlServer, timeout: float = SESSION_WAIT_SEC) -> list[dict]:
    """After the RC server starts it pre-creates a session (--create-session-in-dir)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        owned = _owned_sessions(server.root_pid)
        if owned:
            return owned
        await asyncio.sleep(0.5)
    return []


# ---------------------------------------------------------------------------
# JSONL mirror → sijba post
# ---------------------------------------------------------------------------

def _user_text_from_event(d: dict) -> str | None:
    origin = d.get("origin") or {}
    if origin.get("kind") == "peer":
        return None
    if d.get("type") == "user":
        if origin.get("kind") != "human":
            return None
        content = (d.get("message") or {}).get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()
        if isinstance(content, list):
            parts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
            text = "".join(parts).strip()
            return text or None
        return None
    if d.get("type") == "attachment":
        att = d.get("attachment") or {}
        if att.get("type") != "queued_command":
            return None
        if (att.get("origin") or {}).get("kind") != "human":
            return None
        prompt = att.get("prompt")
        return str(prompt).strip() if prompt else None
    return None


def _assistant_text_blocks(d: dict) -> list[str]:
    if d.get("type") != "assistant":
        return []
    content = (d.get("message") or {}).get("content")
    if not isinstance(content, list):
        return []
    return [b.get("text", "") for b in content if b.get("type") == "text" and b.get("text")]


class SessionMirror:
    """Tails one session's jsonl from the current EOF — no history replay."""

    def __init__(self, client: "PluginClient", meta: dict, multi: bool) -> None:
        self.client = client
        self.meta = meta
        self.session_id = meta["sessionId"]
        self.display_name = str(meta.get("name") or self.session_id[:8])
        self.multi = multi
        self.path = _jsonl_path(self.session_id)
        self.offset = self.path.stat().st_size if self.path.exists() else 0
        self.pending: list[str] = []
        self.last_text_at = 0.0
        self.turn_model: str | None = None
        self.last_served: str | None = None
        self.warnings: list[str] = []

    def _fmt(self, text: str) -> str:
        if self.multi:
            return f"[{self.display_name}] {text}"
        return text

    async def _post_user(self, text: str) -> None:
        await self._flush_bot()
        await self.client.post("user", self._fmt(f"📱 {text}"), source="phone")

    async def _flush_bot(self, extra: str = "") -> None:
        if not self.pending and not extra and not self.warnings:
            return
        body = "".join(self.pending).strip()
        self.pending.clear()
        if self.warnings:
            body = ("\n\n".join([*self.warnings, body]) if body else "\n\n".join(self.warnings))
            self.warnings.clear()
        if extra:
            body = f"{body}\n\n{extra}".strip() if body else extra
        if not body:
            return
        await self.client.post("bot", self._fmt(_truncate(body)), source="claude")

    async def _note_model(self, model: str) -> None:
        if self.turn_model and model and _base_model(model) != _base_model(self.turn_model):
            self.warnings.append(f"⚠️ model changed: {self.turn_model} → {model}")
        self.turn_model = model

    async def _handle_line(self, d: dict) -> None:
        user = _user_text_from_event(d)
        if user:
            await self._post_user(user)
            self.turn_model = None
            return

        if d.get("type") == "system":
            subtype = d.get("subtype")
            if subtype in FALLBACK_SUBTYPES:
                to = _first(d, "fallback_model", "fallbackModel", "to_model", "toModel")
                frm = _first(d, "original_model", "originalModel", "from_model", "fromModel")
                self.warnings.append(f"⚠️ model fallback: {frm or '?'} → {to or '?'}")
            return

        texts = _assistant_text_blocks(d)
        if texts:
            model = (d.get("message") or {}).get("model") or ""
            if model:
                await self._note_model(model)
                self.last_served = model
            self.pending.extend(texts)
            self.last_text_at = time.monotonic()

        msg = d.get("message") or {}
        if d.get("type") == "assistant" and msg.get("stop_reason") == "end_turn":
            note = ""
            if self.turn_model and self.last_served and _base_model(self.turn_model) != _base_model(self.last_served):
                note = f"⚠️ answered by {self.last_served} (started as {self.turn_model})"
            await self._flush_bot(note)

    async def poll(self) -> None:
        if not self.path.exists():
            return
        size = self.path.stat().st_size
        if size < self.offset:
            self.offset = 0
        if size == self.offset:
            if self.pending and time.monotonic() - self.last_text_at >= BOT_FLUSH_IDLE_SEC:
                await self._flush_bot()
            return
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            chunk = f.read(size - self.offset)
        end = chunk.rfind(b"\n")
        if end < 0:
            return
        self.offset += end + 1
        for raw in chunk[:end].splitlines():
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await self._handle_line(d)
        if self.pending and time.monotonic() - self.last_text_at >= BOT_FLUSH_IDLE_SEC:
            await self._flush_bot()


# ---------------------------------------------------------------------------
# Relay sijba → active RC session (haiku SendMessage helper)
# ---------------------------------------------------------------------------

def _relay_prompt(session_name: str, text: str, *, candidates: list[str]) -> str:
    listed = ", ".join(repr(n) for n in candidates) if candidates else "(none)"
    return (
        "Relay a message with SendMessage only (use ToolSearch to find ListAgents/SendMessage if needed). "
        "STEP 1: List live Remote Control agents (ListAgents or equivalent). "
        f"STEP 2: Send ONLY if {session_name!r} appears in that list. "
        f"If it is missing, reply with one line: RELAY_FAIL: target not in ListAgents ({listed}). "
        "Do not message stale session names. "
        f"The message body for SendMessage must be exactly:\n\nRobert via sijba: {text}\n\n"
        "No shell, no other tools."
    )


async def relay_to_session(session_name: str, text: str, *, candidates: list[str]) -> tuple[bool, str]:
    prompt = _relay_prompt(session_name, text, candidates=candidates)
    cmd = [
        CLAUDE_CMD, "-p", "--model", "haiku", "--permission-mode", "dontAsk",
        "--allowedTools", "SendMessage", "ToolSearch", "ListAgents",
        "--output-format", "json", prompt,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        cwd=str(WORK_DIR), env=_agent_env(),
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    except asyncio.TimeoutError:
        proc.kill()
        return False, "relay helper timed out"
    err = stderr.decode(errors="replace").strip()
    if proc.returncode != 0:
        return False, err or f"exit {proc.returncode}"
    if err:
        _log(f"relay stderr: {err[:200]}")
    out = stdout.decode(errors="replace")
    if "RELAY_FAIL" in out or "not in ListAgents" in out:
        return False, "target not listed by ListAgents"
    if proc.returncode == 0 and "SendMessage" not in out and "RELAY_FAIL" not in out:
        # No tool trace — treat as success only if nothing obviously failed
        pass
    return True, "ok"


# ---------------------------------------------------------------------------
# sijba plugin WebSocket client
# ---------------------------------------------------------------------------

def _strip_trigger(text: str) -> str:
    for trigger in (f"@{CHANNEL_ID}", f"@{PROJECT_NAME}", CHANNEL_ID, PROJECT_NAME):
        if text.lower().startswith(trigger.lower()):
            return text[len(trigger):].strip()
    return text.strip()


def _http_base() -> str:
    return SIJBA_URL.replace("wss://", "https://").replace("ws://", "http://")


def _download_attachment(url: str, token: str, dest: Path) -> None:
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "User-Agent": "sijba-claude-remote-control/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        dest.write_bytes(resp.read())


async def _fetch_attachments(attachments: list, work_dir: Path) -> list[str]:
    if not attachments:
        return []
    dest_dir = work_dir / "sijba-uploads"
    dest_dir.mkdir(exist_ok=True)
    fetched = []
    for att in attachments:
        filename = Path(att.get("filename") or "file").name
        dest = dest_dir / filename
        url = f"{_http_base()}/api/upload/{att['id']}"
        try:
            await asyncio.to_thread(_download_attachment, url, SIJBA_TOKEN, dest)
            fetched.append(f"sijba-uploads/{filename}")
        except (urllib.error.URLError, OSError, KeyError) as exc:
            _log(f"attachment download failed ({filename}): {exc}")
    return fetched


def _project_description() -> str:
    """The channel hint: what this project is about. sijba's router tells
    channels apart by it, so it has to be specific to the project — generic
    plugin text would read the same on every channel. The controller's
    per-project description (PROJECT_DESCRIPTION, `describe` command) wins,
    else the first paragraph of the project's README.md, else just the
    project name. Not CLAUDE.md / AGENTS.md: those are instructions to the
    agent (personas, internal URLs), not a description of the project."""
    desc = os.environ.get("PROJECT_DESCRIPTION", "").strip()
    if desc:
        return desc
    for name in ("README.md",):
        try:
            text = (WORK_DIR / name).read_text(errors="replace")
        except OSError:
            continue
        para: list[str] = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "![", "[![", "<", "---", "```", "|", ">")):
                if para:
                    break
                continue
            para.append(line)
        if para:
            text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", " ".join(para))   # [text](url) -> text
            text = re.sub(r"[*_`]", "", text).strip()
            if text:
                return text[:300] + ("…" if len(text) > 300 else "")
    return f"Project {PROJECT_NAME}"


class PluginClient:
    def __init__(self) -> None:
        self.ws = None
        self.shutdown_event = asyncio.Event()
        self.exit_code = 0
        self._reconnect_backoff = 5
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._inflight: dict[str, asyncio.Event] = {}
        self._completed_replies: dict[str, str] = {}
        self.server = RemoteControlServer()
        self.mirrors: dict[str, SessionMirror] = {}

    async def post(self, role: str, text: str, source: str = "plugin") -> None:
        if not self.ws:
            return
        try:
            await self.ws.send(json.dumps({
                "type": "post", "channel": CHANNEL_ID, "role": role,
                "text": text, "source": source,
            }))
        except Exception as exc:
            _log(f"post failed: {exc}")

    async def connect(self) -> None:
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"
        _log(f"Connecting as {PLUGIN_ID}")
        self.ws = await websockets.connect(ws_url, ping_interval=30, ping_timeout=60, close_timeout=5)
        await self.ws.send(json.dumps({
            "type": "register",
            "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "relay"],
            "channels": {
                CHANNEL_ID: {
                    "label": f"Remote Control: {PROJECT_NAME}",
                    "hint": _project_description(),
                    "triggers": [PROJECT_NAME, CHANNEL_ID],
                    "capabilities": ["relay", "phone"],
                    "metadata": {"plugin": PLUGIN_NAME, "project": PROJECT_NAME, "working_dir": str(WORK_DIR),
                                 "note": "relays into an auto-approve session — trusted senders only"},
                },
            },
        }))
        self._reconnect_backoff = 5
        self._worker_task = asyncio.create_task(self._message_worker())
        try:
            await self.message_loop()
        finally:
            if self._worker_task is not None:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
                self._worker_task = None

    async def _message_worker(self) -> None:
        while True:
            msg = await self._msg_queue.get()
            try:
                await self.handle_message(msg)
            except Exception as exc:
                _log(f"handle_message error: {exc}")
            finally:
                self._msg_queue.task_done()

    async def message_loop(self) -> None:
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "registered":
                    _log(f"registered: {msg.get('plugin_id')}")
                elif mtype == "channel_registered":
                    _log(f"channel {msg.get('channel_id')} ok={msg.get('ok')}")
                elif mtype == "message":
                    await self._msg_queue.put(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "error":
                    _log(f"server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            _log("connection closed")

    async def _reply(self, channel_id: str, envelope_id: str, text: str) -> None:
        await self.ws.send(json.dumps({
            "type": "reply", "channel": channel_id, "envelope_id": envelope_id,
            "text": text, "continue": True,
        }))

    async def _handle_local_command(self, channel_id: str, envelope_id: str, text: str) -> bool:
        cmd, _, rest = text[1:].partition(" ")
        cmd = cmd.lower().strip()
        if cmd == "log":
            n = min(int(rest.strip()), len(_LOG_BUFFER)) if rest.strip().isdigit() else 10
            reply = "\n".join(list(_LOG_BUFFER)[-n:]) or "(no logs yet)"
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return True
        if cmd == "sessions":
            await self.server.ensure()
            owned = _owned_sessions(self.server.root_pid)
            if not owned:
                reply = (
                    f"No live Remote Control session for '{PROJECT_NAME}' yet — "
                    f"open it on your phone, or wait ~{SESSION_WAIT_SEC}s after server start."
                )
            else:
                lines = [
                    f"- {_session_display(s)} ({s.get('status', '?')})"
                    for s in owned
                ]
                reply = "Live sessions:\n" + "\n".join(lines)
                if len(owned) > 1:
                    reply += "\n\nSend with @<name> … or <name>: …"
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd == "session":
            name = rest.strip()
            if not name:
                reply = f"Default session is {_load_rc_config().get('default_session') or '(auto)'}. Usage: /session <name>"
            else:
                cfg = _load_rc_config()
                cfg["default_session"] = name
                _save_rc_config(cfg)
                reply = f"Default RC session set to {name!r} (if it is live)."
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd in ("archive", "pause"):
            status = "archived" if cmd == "archive" else "paused"
            code = EXIT_ARCHIVED if cmd == "archive" else EXIT_PAUSED
            _set_registry_status(PROJECT_NAME, status)
            reply = f"{PROJECT_NAME} {status}. Stopping."
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            self.exit_code = code
            self.shutdown_event.set()
            await self.ws.close()
            return True
        if cmd:
            reply = (
                "Commands: /archive, /pause, /log [n], /sessions, /session <name>. "
                "With capacity>1: @<session> … or <session>: …"
            )
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return True
        return False

    async def handle_message(self, msg: dict) -> None:
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        if envelope_id in self._completed_replies:
            await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return
        if envelope_id in self._inflight:
            await self._inflight[envelope_id].wait()
            if envelope_id in self._completed_replies:
                await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return
        done = asyncio.Event()
        self._inflight[envelope_id] = done
        try:
            await self._handle_inner(channel_id, envelope_id, payload)
        finally:
            done.set()
            self._inflight.pop(envelope_id, None)

    async def _handle_inner(self, channel_id: str, envelope_id: str, payload: dict) -> None:
        text = _strip_trigger(payload.get("text", ""))
        attachments = payload.get("attachments") or []
        if text.lstrip().startswith("/"):
            if await self._handle_local_command(channel_id, envelope_id, text.lstrip()):
                return
        if not text.strip() and not attachments:
            self._completed_replies[envelope_id] = ""
            await self._reply(channel_id, envelope_id, "")
            return

        fetched = await _fetch_attachments(attachments, WORK_DIR)
        parts = []
        if fetched:
            parts.append(f"[Anhänge: {', '.join(fetched)}]")
        if text.strip():
            parts.append(text.strip())
        relay_text = "\n\n".join(parts)
        if not relay_text:
            self._completed_replies[envelope_id] = ""
            await self._reply(channel_id, envelope_id, "")
            return

        await self.server.ensure()
        owned = await _wait_for_live_session(self.server) if not _owned_sessions(self.server.root_pid) else _owned_sessions(self.server.root_pid)
        if not owned:
            reply = (
                f"No live Remote Control session for '{PROJECT_NAME}' yet. "
                f"The server is running in tmux {TMUX_SESSION!r} and should pre-create one; "
                f"otherwise open Remote Control → {PROJECT_NAME} on your phone."
            )
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return

        live_names = [_session_display(s) for s in owned]
        explicit, relay_text = _parse_session_prefix(relay_text, live_names)
        cfg = _load_rc_config()
        target = _pick_session(
            owned,
            explicit=explicit,
            preferred=cfg.get("default_session"),
        )
        if explicit and not target:
            reply = f"Unknown session {explicit!r}. Live: {', '.join(live_names)} (/sessions)"
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return
        if len(owned) > 1 and not explicit and not cfg.get("default_session"):
            reply = f"Which session? Live: {', '.join(live_names)} — use @name … or /session <name>"
            self._completed_replies[envelope_id] = reply
            await self._reply(channel_id, envelope_id, reply)
            return
        if not target:
            target = owned[0]

        name = _session_display(target)
        ok, detail = await relay_to_session(name, relay_text, candidates=live_names)
        if not ok and len(owned) > 1:
            for alt in owned:
                alt_name = _session_display(alt)
                if alt_name == name:
                    continue
                ok, detail = await relay_to_session(alt_name, relay_text, candidates=live_names)
                if ok:
                    name = alt_name
                    break
        reply = f"→ sent to {name}" if ok else f"Relay failed: {detail} (live: {', '.join(live_names)})"
        self._completed_replies[envelope_id] = reply
        await self._reply(channel_id, envelope_id, reply)

    async def _sync_mirrors(self) -> None:
        owned = _owned_sessions(self.server.root_pid)
        multi = len(owned) > 1
        live = {s["sessionId"] for s in owned}
        for sid in list(self.mirrors):
            if sid not in live:
                del self.mirrors[sid]
        for meta in owned:
            sid = meta["sessionId"]
            if sid not in self.mirrors:
                self.mirrors[sid] = SessionMirror(self, meta, multi)
            else:
                self.mirrors[sid].multi = multi
                self.mirrors[sid].meta = meta
                self.mirrors[sid].display_name = str(meta.get("name") or sid[:8])
        for m in self.mirrors.values():
            await m.poll()

    async def _mirror_loop(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                await self._sync_mirrors()
            except Exception as exc:
                _log(f"mirror loop: {exc}")
            await asyncio.sleep(SESSION_SCAN_SEC)

    async def run_forever(self) -> None:
        loop = asyncio.get_event_loop()

        def _shutdown() -> None:
            self.shutdown_event.set()
            if self.ws:
                asyncio.ensure_future(self.ws.close())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _shutdown)
            except NotImplementedError:
                pass

        await self.server.ensure()
        asyncio.create_task(self.server.watch())
        asyncio.create_task(self._mirror_loop())

        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                _log(f"connect failed: {exc}")
            if self.shutdown_event.is_set():
                break
            await asyncio.sleep(self._reconnect_backoff)
            self._reconnect_backoff = min(self._reconnect_backoff * 2, 300)
        _log("shutdown complete")


async def main() -> int:
    client = PluginClient()
    await client.run_forever()
    return client.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
