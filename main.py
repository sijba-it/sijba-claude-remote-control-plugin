#!/usr/bin/env python3
"""Claude Code plugin for sijba — standalone, no dependency on
sijba-agent-controller's own code. Runnable on its own (env vars below) or
spawned by the controller as a `kind: git` catalog plugin.

Plugin contract (documented in sijba-agent-controller/README.md):
  - read SIJBA_URL / SIJBA_TOKEN / PROJECT_NAME / WORK_DIR / REGISTRY_FILE
    from the environment
  - self-register exactly one sijba channel over its own WebSocket connection
  - handle /archive and /pause locally: write REGISTRY_FILE's own status
    entry directly (no shared import — see _set_registry_status below) and
    exit with the reserved code (90 archived / 91 paused) *before* the
    process actually stops, so the controller can tell "stopped on
    purpose" apart from "crashed" purely from the exit code
  - handle /log [n] locally from an in-memory ring buffer

Environment variables
----------------------
SIJBA_URL       sijba WebSocket URL (required) — e.g. wss://<account>.sijba.eu
SIJBA_TOKEN     Auth token (required)
PROJECT_NAME    Registry key / trigger word (required) — e.g. "sijba-web"
WORK_DIR        Working directory for Claude (required)
REGISTRY_FILE   Path to the controller's registry.json (required)
CLAUDE_CMD      Claude CLI command (default: claude)
"""
from __future__ import annotations

import asyncio
import collections
import fcntl
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import websockets

# Exit-code contract with the controller — see sijba-agent-controller/exit_codes.py.
EXIT_ARCHIVED = 90
EXIT_PAUSED = 91

PLUGIN_NAME = "claude"  # this repo *is* the claude plugin — no TOOL switch needed

SIJBA_URL = os.environ.get("SIJBA_URL", "")
if not SIJBA_URL:
    print("ERROR: SIJBA_URL environment variable required (e.g. wss://<account>.sijba.eu)", file=sys.stderr)
    sys.exit(1)

SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
WORK_DIR = Path(os.environ.get("WORK_DIR", ""))
REGISTRY_FILE = os.environ.get("REGISTRY_FILE", "")
CLAUDE_CMD = os.environ.get("CLAUDE_CMD", "claude")

if not SIJBA_TOKEN:
    print("ERROR: SIJBA_TOKEN environment variable required", file=sys.stderr)
    sys.exit(1)
if not PROJECT_NAME:
    print("ERROR: PROJECT_NAME environment variable required", file=sys.stderr)
    sys.exit(1)
if not WORK_DIR.is_dir():
    print(f"ERROR: WORK_DIR {WORK_DIR} does not exist", file=sys.stderr)
    sys.exit(1)
if not REGISTRY_FILE:
    print("ERROR: REGISTRY_FILE environment variable required", file=sys.stderr)
    sys.exit(1)

PLUGIN_ID = f"{PLUGIN_NAME}-{PROJECT_NAME}"
CHANNEL_ID = f"{PLUGIN_NAME}:{PROJECT_NAME}"

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [{PROJECT_NAME}] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _set_registry_status(name: str, status: str) -> None:
    """Read-modify-write REGISTRY_FILE directly — same JSON shape as
    sijba-agent-controller/registry.py, but no import of it: this repo has
    to stand on its own, same as any other git-sourced plugin would."""
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


# ---------------------------------------------------------------------------
# Claude backend (one-shot `claude -p --resume` per turn, streamed)
#
# Earlier versions of this class tried to keep one persistent `claude`
# process alive and feed it a made-up {"request_id", "text"} JSON protocol
# over stdin — claude never spoke that protocol at all (verified live: it
# was actually launching full interactive mode against piped, non-tty
# stdin/stdout, understanding none of what was written to it), so every
# single message timed out after 120s with no exception. The real
# non-interactive interface is `-p --output-format stream-json --verbose`
# (verbose is mandatory for print+stream-json, claude errors out without
# it) plus `--resume <session_id>` for continuation — one invocation per
# turn, same shape as CursorSession below, confirmed against a real
# `claude -p` run.
# ---------------------------------------------------------------------------

# Chat bubbles aren't the place for a raw multi-KB/MB blob (a tool result
# embedding a big file read, a huge diff, ...) — cap what actually reaches
# sijba, independent of how large the underlying event was.
MAX_CHARS_TO_UI = 8000


def _truncate(text: str, limit: int = MAX_CHARS_TO_UI) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…[truncated, {len(text) - limit} more chars]"


async def _iter_json_lines(stream: asyncio.StreamReader):
    """Yield complete newline-delimited lines with no hard size limit.

    `StreamReader.readline()` has a fixed buffer limit (64KiB by default)
    and *raises* LimitOverrunError if a single line exceeds it before the
    separator is found — which happened live: a JSON-lines event with a
    large embedded file/diff blew past that, and the exception replaced
    the whole in-progress reply with an error instead of just being one
    long event. Read raw chunks and split on our own unbounded buffer
    instead — a huge line just accumulates in memory (bounded only by
    what's actually available, not an arbitrary fixed ceiling) rather than
    blowing up the read.
    """
    buf = bytearray()
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            if buf:
                yield bytes(buf)
            return
        buf.extend(chunk)
        while True:
            idx = buf.find(b"\n")
            if idx < 0:
                break
            yield bytes(buf[:idx])
            del buf[: idx + 1]


def _http_base() -> str:
    return SIJBA_URL.replace("wss://", "https://").replace("ws://", "http://")


def _download_attachment(url: str, token: str, dest: Path) -> None:
    # Explicit User-Agent required: urllib's default ("Python-urllib/x.y")
    # gets a 403 from the WAF in front of sijba before the request ever
    # reaches Core's own auth check — verified live (curl with the same
    # bearer token but its own UA reaches the app fine and gets a normal
    # 401/404; the bare Python UA gets 403 regardless of the header).
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "User-Agent": "sijba-claude-plugin/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        dest.write_bytes(resp.read())


async def _fetch_attachments(attachments: list, work_dir: Path) -> list[str]:
    """Download attachments from Core into work_dir/sijba-uploads — this
    plugin's process runs on its own host, no filesystem shared with Core,
    so the file has to be pulled over HTTP before claude can see it."""
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
            _log(f"downloaded attachment: {filename}")
        except (urllib.error.URLError, OSError, KeyError) as exc:
            _log(f"attachment download failed ({filename}): {exc}")
    return fetched


class ClaudeSession:
    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.claude_session_id: str | None = None
        self.current_proc: asyncio.subprocess.Process | None = None

    async def start(self):
        _log("Claude session ready (conversation mode)")

    async def send(self, text: str, request_id: int, on_chunk=None) -> dict:
        cmd = [
            CLAUDE_CMD, "-p", "--dangerously-skip-permissions", "--model", "sonnet",
            "--output-format", "stream-json", "--include-partial-messages", "--verbose",
        ]
        if self.claude_session_id:
            cmd += ["--resume", self.claude_session_id]
        cmd += [text]

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.work_dir,
            )
            self.current_proc = proc
            final_text = ""
            stderr_buf = bytearray()

            async def _pump_stdout() -> None:
                nonlocal final_text
                async for line in _iter_json_lines(proc.stdout):
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    etype = event.get("type")

                    if etype == "system" and event.get("subtype") == "init":
                        if event.get("session_id"):
                            self.claude_session_id = event["session_id"]

                    elif etype == "stream_event":
                        inner = event.get("event") or {}
                        if inner.get("type") == "content_block_delta":
                            delta_text = (inner.get("delta") or {}).get("text", "")
                            if delta_text and on_chunk:
                                await on_chunk(_truncate(delta_text))

                    elif etype == "result":
                        final_text = _truncate(event.get("result", ""))
                        if event.get("session_id"):
                            self.claude_session_id = event["session_id"]

            async def _pump_stderr() -> None:
                while True:
                    chunk = await proc.stderr.read(4096)
                    if not chunk:
                        break
                    stderr_buf.extend(chunk)

            await asyncio.wait_for(
                asyncio.gather(_pump_stdout(), _pump_stderr()), timeout=600
            )
            await proc.wait()

            stderr_text = bytes(stderr_buf).decode(errors="replace").strip()
            if stderr_text:
                _log(f"stderr: {stderr_text}")

            # claude exited non-zero with no `result` (login expired, quota,
            # crash, broken --resume, ...) — surface the real error instead of
            # "(no output)", and drop the session id so the next turn starts
            # clean rather than re-resuming a bad state.
            if proc.returncode != 0 and not final_text:
                self.claude_session_id = None
                detail = stderr_text or f"exited {proc.returncode}, no output"
                _log(f"invocation failed (exit {proc.returncode}) — session reset")
                return {
                    "text": f"Error: claude failed — {_truncate(detail, 800)}",
                    "request_id": request_id,
                }

            return {"text": final_text or "(no output)", "request_id": request_id}

        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": "Error: Claude timeout (600s)", "request_id": request_id}
        except Exception as exc:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": f"Error: {exc}", "request_id": request_id}
        finally:
            self.current_proc = None

    async def close(self):
        _log("Claude session closed")

    async def reset(self) -> None:
        """Drop the remembered session id — the next turn omits --resume,
        so claude starts a brand-new conversation instead of continuing."""
        self.claude_session_id = None
        _log("Claude session reset")

    async def kill(self) -> bool:
        """Abort whatever turn is currently running, then reset — same
        effect as /reset, plus actually terminating a still-running claude
        invocation instead of waiting it out. Returns whether anything was
        actually running to kill."""
        proc = self.current_proc
        killed = bool(proc and proc.returncode is None)
        if killed:
            proc.kill()
        await self.reset()
        return killed


# ---------------------------------------------------------------------------
# sijba plugin protocol (single project channel, self-registered)
# ---------------------------------------------------------------------------

def _strip_trigger(text: str) -> str:
    for trigger in (PROJECT_NAME, CHANNEL_ID):
        if text.lower().startswith(trigger.lower()):
            return text[len(trigger):].strip()
    return text.strip()


def _is_kill_command(text: str) -> bool:
    stripped = _strip_trigger(text)
    if not stripped.startswith("/"):
        return False
    cmd, _, _ = stripped[1:].partition(" ")
    return cmd.lower().strip() == "kill"


class PluginClient:
    def __init__(self):
        self.ws = None
        self.session = ClaudeSession(WORK_DIR)
        self.session_started = False
        self.shutdown_event = asyncio.Event()
        self.exit_code = 0
        self._reconnect_backoff = 5
        self._request_id = 0
        # Keep the WS read loop free to answer pings while a turn runs —
        # blocking `await handle_message` caused keepalive timeouts and
        # dropped follow-up messages when the socket then died.
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._inflight: dict[str, asyncio.Event] = {}
        self._completed_replies: dict[str, str] = {}

    async def connect(self):
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"
        _log(f"Connecting to {ws_url} as {PLUGIN_ID}")
        self.ws = await websockets.connect(
            ws_url, ping_interval=30, ping_timeout=60, close_timeout=5,
        )
        _log("Connected!")

        await self.ws.send(json.dumps({
            "type": "register",
            "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "tools", "files", "code", "agent"],
            "channels": {
                CHANNEL_ID: {
                    "label": f"Claude: {PROJECT_NAME}",
                    "hint": (
                        f"Claude Code agent for project '{PROJECT_NAME}' at {WORK_DIR}. "
                        f"Trigger: '{PROJECT_NAME} ...'. In-channel commands: /archive, /pause, /reset, /kill, /log [n]."
                    ),
                    "triggers": [PROJECT_NAME, CHANNEL_ID],
                    "capabilities": ["code", "analysis", "debugging", "files", "terminal"],
                    "metadata": {"plugin": PLUGIN_NAME, "project": PROJECT_NAME, "working_dir": str(WORK_DIR)},
                }
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

    async def message_loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "registered":
                    _log(f"Plugin registered: {msg['plugin_id']}")
                elif mtype == "channel_registered":
                    _log(f"Channel registered: {msg['channel_id']} (ok={msg.get('ok')})")
                elif mtype == "message":
                    if _is_kill_command(msg["payload"]["text"]):
                        # Bypasses _msg_queue on purpose: the one-turn-at-a-
                        # time worker would otherwise queue /kill behind the
                        # very turn it's meant to abort, making it useless
                        # against a stuck/looping agent. Runs concurrently
                        # with whatever the worker is currently doing.
                        asyncio.create_task(self._handle_kill(msg))
                    else:
                        await self._msg_queue.put(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "error":
                    _log(f"Server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            _log("Connection closed")
        except Exception as exc:
            _log(f"Message loop error: {exc}")

    def _remember_reply(self, envelope_id: str, text: str) -> None:
        self._completed_replies[envelope_id] = text
        while len(self._completed_replies) > 50:
            oldest = next(iter(self._completed_replies))
            self._completed_replies.pop(oldest, None)

    async def _reply(self, channel_id: str, envelope_id: str, text: str) -> None:
        try:
            await self.ws.send(json.dumps({
                "type": "reply", "channel": channel_id, "envelope_id": envelope_id,
                "text": text, "continue": True,
            }))
        except Exception as exc:
            _log(f"Failed to send reply: {exc}")

    async def _handle_kill(self, msg: dict) -> None:
        """See message_loop — runs outside _msg_queue so it takes effect
        immediately against a running turn instead of waiting behind it."""
        channel_id = msg["channel"]
        envelope_id = msg["payload"]["envelope_id"]
        killed = await self.session.kill()
        self.session_started = False
        reply = (
            f"{PROJECT_NAME}: killed running turn, session reset — next message starts fresh."
            if killed else
            f"{PROJECT_NAME}: nothing running, session reset anyway."
        )
        _log(f"killed via /kill (had_running_turn={killed})")
        self._remember_reply(envelope_id, reply)
        await self._reply(channel_id, envelope_id, reply)

    async def _handle_local_command(self, channel_id: str, envelope_id: str, text: str) -> bool:
        cmd, _, rest = text[1:].partition(" ")
        cmd = cmd.lower().strip()

        if cmd == "log":
            n = 10
            if rest.strip().isdigit():
                n = min(int(rest.strip()), len(_LOG_BUFFER) or 10)
            lines = list(_LOG_BUFFER)[-n:]
            reply = "\n".join(lines) or "(no logs yet)"
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd in ("archive", "pause"):
            status = "archived" if cmd == "archive" else "paused"
            code = EXIT_ARCHIVED if cmd == "archive" else EXIT_PAUSED
            try:
                _set_registry_status(PROJECT_NAME, status)
            except Exception as exc:
                reply = f"Error updating registry: {exc}"
                self._remember_reply(envelope_id, reply)
                await self._reply(channel_id, envelope_id, reply)
                return True
            reply = f"{PROJECT_NAME} {status}. Stopping."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            _log(f"{status} via /{cmd} — shutting down (exit {code})")
            self.exit_code = code
            self.shutdown_event.set()
            await self.ws.close()
            return True

        if cmd == "reset":
            await self.session.reset()
            self.session_started = False
            reply = f"{PROJECT_NAME} reset — next message starts fresh, no prior context."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd:
            reply = (
                "Unsupported here: /archive, /pause, /reset, /kill, /log [n]. "
                "Anything else — just plain text."
            )
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True
        return False

    async def handle_message(self, msg: dict):
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = payload["text"]
        attachments = payload.get("attachments") or []

        if envelope_id in self._completed_replies:
            _log(f"redelivery of completed envelope={envelope_id} — resending reply")
            await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return
        if envelope_id in self._inflight:
            _log(f"redelivery of in-flight envelope={envelope_id} — waiting")
            await self._inflight[envelope_id].wait()
            if envelope_id in self._completed_replies:
                await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return

        done = asyncio.Event()
        self._inflight[envelope_id] = done
        try:
            await self._handle_message_inner(channel_id, envelope_id, text, attachments)
        finally:
            done.set()
            self._inflight.pop(envelope_id, None)

    async def _handle_message_inner(
        self, channel_id: str, envelope_id: str, text: str, attachments: list,
    ) -> None:
        text = _strip_trigger(text)
        if not text and not attachments:
            self._remember_reply(envelope_id, "")
            await self._reply(channel_id, envelope_id, "")
            return

        if text.lstrip().startswith("/"):
            if await self._handle_local_command(channel_id, envelope_id, text.lstrip()):
                return

        fetched = await _fetch_attachments(attachments, WORK_DIR)
        note = ""
        if fetched:
            note += f"[Angehängte Datei(en): {', '.join(fetched)}]\n\n"
        if len(fetched) < len(attachments):
            # Surfaced in-chat rather than only in the plugin's own log — a
            # silently-dropped attachment otherwise looks indistinguishable
            # from "the agent just ignored the file".
            note += f"[Achtung: {len(attachments) - len(fetched)} Anhang/Anhänge konnten nicht geladen werden]\n\n"
        text = f"{note}{text}".strip()
        if not text:
            self._remember_reply(envelope_id, "")
            await self._reply(channel_id, envelope_id, "")
            return

        if not self.session_started:
            await self.session.start()
            self.session_started = True

        async def _on_chunk(delta: str) -> None:
            try:
                await self.ws.send(json.dumps({
                    "type": "reply_chunk", "channel": channel_id, "envelope_id": envelope_id, "text": delta,
                }))
            except Exception as exc:
                _log(f"Failed to send reply_chunk: {exc}")

        _log(f"handling envelope={envelope_id} text={text[:120]!r}")
        self._request_id += 1
        reply_text = ""
        try:
            response = await self.session.send(text, request_id=self._request_id, on_chunk=_on_chunk)
            reply_text = response.get("text", "")
        except Exception as exc:
            reply_text = f"Error: {exc}"

        self._remember_reply(envelope_id, reply_text)
        await self._reply(channel_id, envelope_id, reply_text)

    async def run_forever(self):
        loop = asyncio.get_event_loop()

        def _request_shutdown() -> None:
            # Setting the event alone doesn't unblock message_loop()'s
            # `async for raw in self.ws` below — close the socket too, or
            # shutdown just hangs until the connection drops on its own.
            self.shutdown_event.set()
            if self.ws is not None:
                asyncio.ensure_future(self.ws.close())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                pass

        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                _log(f"Connection failed: {exc}")
            if self.shutdown_event.is_set():
                break
            _log(f"Reconnecting in {self._reconnect_backoff}s...")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self._reconnect_backoff)
            except asyncio.TimeoutError:
                pass
            self._reconnect_backoff = min(self._reconnect_backoff * 2, 300)

        await self.session.close()
        _log("Shutdown complete")


async def main() -> int:
    client = PluginClient()
    await client.run_forever()
    return client.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
