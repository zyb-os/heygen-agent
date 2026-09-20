"""
orchestrator_client.py — Connects the heygen-agent to the agent-orchestrator.

Protocol:
  ✓ POST /api/v1/agents/register — stable UUID, capability schema, required_settings
  ✓ WS /ws/{agent_id}           — connect immediately after registration
  ✓ Close code 4004              — re-register then reconnect
  ✓ Exponential-backoff auto-reconnect (cap: 60 s)
  ✓ Heartbeat every 15 s        — status, load, metrics
  ✓ task_request → route to HeyGenClient → task_response
  ✓ settings_push               — live API key / timeout updates
  ✓ task_cancel                 — cancel active task
  ✓ Graceful shutdown on SIGINT/SIGTERM
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import websockets
import websockets.exceptions

from heygen_client import HeyGenClient, HeyGenError

logger = logging.getLogger(__name__)

_AGENT_ID_FILE       = Path(".agent_id")
_HEARTBEAT_INTERVAL  = 15.0
_MAX_BACKOFF         = 60.0
_DRAIN_TIMEOUT       = 120.0
_DEFAULT_TASK_TIMEOUT = 300.0   # video generation can take ~2–3 min


def _stable_agent_id() -> str:
    if _AGENT_ID_FILE.exists():
        aid = _AGENT_ID_FILE.read_text().strip()
        if aid:
            return aid
    aid = str(uuid.uuid4())
    _AGENT_ID_FILE.write_text(aid)
    logger.info("Generated new stable agent ID: %s", aid)
    return aid


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _envelope(
    sender_id: str,
    msg_type: str,
    payload: dict,
    recipient_id: str | None = None,
    correlation_id: str | None = None,
) -> str:
    return json.dumps({
        "id":             str(uuid.uuid4()),
        "type":           msg_type,
        "sender_id":      sender_id,
        "recipient_id":   recipient_id,
        "payload":        payload,
        "timestamp":      _now_iso(),
        "correlation_id": correlation_id,
    })


# ── Registration payload ──────────────────────────────────────────────────────

REGISTRATION_PAYLOAD: dict[str, Any] = {
    "name":        "heygen-agent",
    "description": (
        "HeyGen video generation agent. Creates AI avatar videos from text scripts, "
        "manages avatars, voices and templates, and retrieves download URLs for "
        "completed videos via the HeyGen v2 API."
    ),
    "version": "1.0.0",
    "tags":    ["heygen", "video", "avatar", "tts", "generation", "media"],
    "capabilities": [
        {
            "name": "create_video",
            "description": (
                "Generate an AI avatar video from a text script. "
                "Returns a video_id; poll get_video_status until status=completed."
            ),
            "tags": ["heygen", "video", "generate", "avatar"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "avatar_id":        {"type": "string", "description": "HeyGen avatar ID"},
                    "script_text":      {"type": "string", "description": "Text the avatar will speak"},
                    "voice_id":         {"type": "string", "description": "HeyGen voice ID (optional, uses avatar default if omitted)"},
                    "title":            {"type": "string", "description": "Optional video title"},
                    "width":            {"type": "integer", "description": "Output width in pixels (default 1280)"},
                    "height":           {"type": "integer", "description": "Output height in pixels (default 720)"},
                    "background_color": {"type": "string", "description": "Hex background colour (default #ffffff)"},
                    "speed":            {"type": "number",  "description": "Speaking speed 0.5–1.5 (default 1.0)"},
                    "avatar_style":     {"type": "string",  "description": "Avatar style: normal | circle | closeUp"},
                },
                "required": ["avatar_id", "script_text"],
            },
        },
        {
            "name": "get_video_status",
            "description": (
                "Check the generation status of a HeyGen video. "
                "Returns status (pending | processing | completed | failed) and download_url when done."
            ),
            "tags": ["heygen", "video", "status", "poll"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "video_id": {"type": "string", "description": "Video ID returned by create_video"},
                },
                "required": ["video_id"],
            },
        },
        {
            "name": "download_video",
            "description": (
                "Get the download URL for a completed HeyGen video. "
                "Raises an error if the video is not yet completed."
            ),
            "tags": ["heygen", "video", "download", "url"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "video_id": {"type": "string", "description": "Video ID of a completed video"},
                },
                "required": ["video_id"],
            },
        },
        {
            "name": "list_videos",
            "description": "List previously generated HeyGen videos with their statuses and metadata.",
            "tags": ["heygen", "video", "list"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "limit":      {"type": "integer", "description": "Max results to return (1–100, default 20)"},
                    "next_token": {"type": "string",  "description": "Pagination token from a previous call"},
                },
            },
        },
        {
            "name": "delete_video",
            "description": "Delete a HeyGen video by ID.",
            "tags": ["heygen", "video", "delete"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "video_id": {"type": "string", "description": "Video ID to delete"},
                },
                "required": ["video_id"],
            },
        },
        {
            "name": "list_avatars",
            "description": "List all HeyGen avatars available to this API key.",
            "tags": ["heygen", "avatar", "list"],
            "input_schema": {"type": "object", "properties": {}},
        },
        {
            "name": "list_voices",
            "description": "List HeyGen TTS voices, optionally filtered by language or gender.",
            "tags": ["heygen", "voice", "tts", "list"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "language": {"type": "string", "description": "ISO 639-1 language code filter (e.g. 'en', 'es')"},
                    "gender":   {"type": "string", "description": "Filter by 'male' or 'female'"},
                },
            },
        },
        {
            "name": "list_templates",
            "description": "List available HeyGen video templates.",
            "tags": ["heygen", "template", "list"],
            "input_schema": {"type": "object", "properties": {}},
        },
        {
            "name": "create_video_from_template",
            "description": (
                "Generate a video using a HeyGen template by filling in its variables. "
                "Returns a video_id; poll get_video_status until completed."
            ),
            "tags": ["heygen", "video", "template", "generate"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "template_id": {"type": "string", "description": "Template ID from list_templates"},
                    "variables":   {
                        "type": "object",
                        "description": "Map of variable name → string value (or full variable object)",
                        "additionalProperties": True,
                    },
                    "title": {"type": "string", "description": "Optional video title"},
                },
                "required": ["template_id", "variables"],
            },
        },
        {
            "name": "get_remaining_quota",
            "description": "Return the remaining HeyGen API credits / quota for this account.",
            "tags": ["heygen", "quota", "account"],
            "input_schema": {"type": "object", "properties": {}},
        },
    ],
    "required_settings": [
        {
            "key":         "heygen_api_key",
            "label":       "HeyGen API Key",
            "type":        "string",
            "required":    True,
            "secret":      True,
            "description": "Your HeyGen API key — available at app.heygen.com/settings (API section).",
        },
        {
            "key":         "heygen_task_timeout_s",
            "label":       "Task Timeout (seconds)",
            "type":        "integer",
            "required":    False,
            "description": "Max seconds to wait for any single HeyGen API call (default: 300).",
            "default":     300,
        },
    ],
}


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def dispatch(client: HeyGenClient, capability: str, args: dict) -> dict:
    """Route a capability name + args to the matching HeyGenClient method."""
    if capability == "create_video":
        return await client.create_video(
            avatar_id        = args["avatar_id"],
            script_text      = args["script_text"],
            voice_id         = args.get("voice_id", ""),
            title            = args.get("title", ""),
            width            = int(args.get("width", 1280)),
            height           = int(args.get("height", 720)),
            background_color = args.get("background_color", "#ffffff"),
            speed            = float(args.get("speed", 1.0)),
            avatar_style     = args.get("avatar_style", "normal"),
        )

    if capability == "get_video_status":
        return await client.get_video_status(args["video_id"])

    if capability == "download_video":
        return await client.download_video(args["video_id"])

    if capability == "list_videos":
        return await client.list_videos(
            limit = int(args.get("limit", 20)),
            token = args.get("next_token", ""),
        )

    if capability == "delete_video":
        return await client.delete_video(args["video_id"])

    if capability == "list_avatars":
        return await client.list_avatars()

    if capability == "list_voices":
        return await client.list_voices(
            language = args.get("language", ""),
            gender   = args.get("gender", ""),
        )

    if capability == "list_templates":
        return await client.list_templates()

    if capability == "create_video_from_template":
        return await client.create_video_from_template(
            template_id = args["template_id"],
            variables   = args["variables"],
            title       = args.get("title", ""),
        )

    if capability == "get_remaining_quota":
        return await client.get_remaining_quota()

    raise ValueError(f"Unknown capability: {capability!r}")


# ── Main client ───────────────────────────────────────────────────────────────

class OrchestratorClient:
    def __init__(self, orchestrator_url: str = "http://localhost:8000") -> None:
        self._base   = orchestrator_url.rstrip("/")
        self._ws_base = self._base.replace("http://", "ws://").replace("https://", "wss://")
        self._agent_id = _stable_agent_id()
        self._http   = httpx.AsyncClient(timeout=15.0)
        self._settings: dict[str, Any] = {}

        # HeyGen client is created after registration (needs API key from settings)
        self._heygen: HeyGenClient | None = None
        self._task_timeout = _DEFAULT_TASK_TIMEOUT

        # Concurrency: one task at a time
        self._task_sem   = asyncio.Semaphore(1)
        self._active_task: asyncio.Task | None = None

        # Metrics
        self._tasks_completed = 0
        self._tasks_failed    = 0
        self._total_ms        = 0.0
        self._start_time      = time.monotonic()
        self._status          = "starting"
        self._shutting_down   = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))
        await self._connect_loop()

    # ── Registration ───────────────────────────────────────────────────────────

    async def _register(self) -> None:
        url  = f"{self._base}/api/v1/agents/register"
        payload = {**REGISTRATION_PAYLOAD, "id": self._agent_id}
        resp = await self._http.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._agent_id = data["agent_id"]
        merged = {**data.get("common_settings", {}), **data.get("agent_settings", {})}
        self._apply_settings(merged)
        logger.info("Registered — agent_id=%s", self._agent_id)

    def _apply_settings(self, settings: dict) -> None:
        self._settings.update(settings)
        api_key = settings.get("heygen_api_key") or os.environ.get("HEYGEN_API_KEY", "")
        timeout = float(settings.get("heygen_task_timeout_s") or _DEFAULT_TASK_TIMEOUT)
        self._task_timeout = timeout
        if api_key:
            if self._heygen:
                # Replace client with updated key / timeout
                asyncio.create_task(self._heygen.aclose())
            self._heygen = HeyGenClient(api_key=api_key, timeout=timeout)
            logger.info("HeyGen client initialised (timeout=%.0fs)", timeout)
        elif not self._heygen:
            logger.warning(
                "heygen_api_key not set — tasks will fail until the key is configured "
                "via the orchestrator settings or HEYGEN_API_KEY env var."
            )

    # ── Connection loop ────────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        backoff = 1.0
        while not self._shutting_down:
            try:
                await self._register()
                ws_url = f"{self._ws_base}/ws/{self._agent_id}"
                logger.info("Connecting to WS: %s", ws_url)
                async with websockets.connect(ws_url) as ws:
                    backoff = 1.0
                    await self._run_session(ws)
            except websockets.exceptions.ConnectionClosed as exc:
                code = exc.rcvd.code if exc.rcvd else None
                if self._shutting_down:
                    break
                if code == 4004:
                    logger.warning("Unknown agent_id (4004) — re-registering …")
                elif code == 4003:
                    logger.warning("Agent disabled (4003) — will retry")
                    backoff = max(backoff, 30.0)
                else:
                    logger.warning("WS closed (code=%s) — retry in %.0fs", code, backoff)
            except Exception as exc:
                if self._shutting_down:
                    break
                logger.warning("Connection error: %s — retry in %.0fs", exc, backoff)
            if not self._shutting_down:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)

    # ── Session ────────────────────────────────────────────────────────────────

    async def _run_session(self, ws) -> None:
        self._status = "available"
        logger.info("WebSocket session active")
        try:
            await asyncio.gather(
                self._heartbeat_loop(ws),
                self._recv_loop(ws),
            )
        finally:
            self._status = "offline"

    # ── Heartbeat ──────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            n = self._tasks_completed + self._tasks_failed
            try:
                await ws.send(_envelope(self._agent_id, "heartbeat", {
                    "status":       self._status,
                    "current_load": 1.0 if self._active_task else 0.0,
                    "active_tasks": 1 if self._active_task else 0,
                    "metrics": {
                        "tasks_completed":      self._tasks_completed,
                        "tasks_failed":         self._tasks_failed,
                        "avg_response_time_ms": round(self._total_ms / n, 1) if n else 0.0,
                        "uptime_seconds":       round(time.monotonic() - self._start_time, 1),
                    },
                }))
            except Exception:
                return

    # ── Receive loop ───────────────────────────────────────────────────────────

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            mtype = msg.get("type", "")
            try:
                if mtype == "task_request":
                    asyncio.create_task(self._handle_task(ws, msg))
                elif mtype == "settings_push":
                    self._apply_settings(msg.get("payload", {}).get("settings", {}))
                elif mtype == "task_cancel":
                    if self._active_task and not self._active_task.done():
                        self._active_task.cancel()
                        logger.info("Task cancelled via task_cancel")
                elif mtype == "agent_restart":
                    logger.info("Restart requested by orchestrator — shutting down for restart")
                    import sys
                    asyncio.get_event_loop().call_later(1.0, lambda: sys.exit(0))
                else:
                    logger.debug("← unhandled: %r", mtype)
            except Exception as exc:
                logger.error("Error handling %r: %s", mtype, exc)

    # ── Task handling ──────────────────────────────────────────────────────────

    async def _handle_task(self, ws, msg: dict) -> None:
        payload    = msg.get("payload", {})
        capability = payload.get("capability", "")
        req_id     = msg.get("id")
        sender_id  = msg.get("sender_id")
        input_data = payload.get("input_data", {})
        # Strip internal orchestration keys
        args = {k: v for k, v in input_data.items() if not k.startswith("_")}
        timeout_ms = float(payload.get("timeout_ms") or self._task_timeout * 1000)

        if not self._heygen:
            await self._reply(ws, sender_id, req_id, success=False,
                              error="heygen_api_key is not configured")
            return

        if self._active_task and not self._active_task.done():
            await self._reply(ws, sender_id, req_id, success=False,
                              error="Agent is busy with another task")
            return

        async def _run() -> None:
            async with self._task_sem:
                self._status = "busy"
                await self._send(ws, "status_update", {"status": "busy"})
                t0 = time.monotonic()
                try:
                    result = await asyncio.wait_for(
                        dispatch(self._heygen, capability, args),
                        timeout=timeout_ms / 1000.0,
                    )
                    duration_ms = (time.monotonic() - t0) * 1000
                    self._tasks_completed += 1
                    self._total_ms += duration_ms
                    await self._reply(ws, sender_id, req_id, success=True,
                                      output=result, duration_ms=duration_ms)
                except asyncio.CancelledError:
                    self._tasks_failed += 1
                    await self._reply(ws, sender_id, req_id, success=False,
                                      error="Task cancelled",
                                      duration_ms=(time.monotonic() - t0) * 1000)
                except HeyGenError as exc:
                    self._tasks_failed += 1
                    await self._reply(ws, sender_id, req_id, success=False,
                                      error=str(exc),
                                      duration_ms=(time.monotonic() - t0) * 1000)
                except Exception as exc:
                    self._tasks_failed += 1
                    logger.exception("Unhandled error in capability %r", capability)
                    await self._reply(ws, sender_id, req_id, success=False,
                                      error=str(exc),
                                      duration_ms=(time.monotonic() - t0) * 1000)
                finally:
                    self._active_task = None
                    self._status = "draining" if self._shutting_down else "available"
                    await self._send(ws, "status_update", {"status": self._status})

        self._active_task = asyncio.create_task(_run())

    # ── Graceful shutdown ──────────────────────────────────────────────────────

    async def _shutdown(self) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info("Shutting down …")
        self._status = "draining"
        deadline = time.monotonic() + _DRAIN_TIMEOUT
        while self._active_task and not self._active_task.done():
            if time.monotonic() > deadline:
                self._active_task.cancel()
                break
            await asyncio.sleep(0.5)
        try:
            await self._http.delete(f"{self._base}/api/v1/agents/{self._agent_id}")
        except Exception:
            pass
        if self._heygen:
            await self._heygen.aclose()
        await self._http.aclose()
        logger.info("Shutdown complete")

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _send(self, ws, msg_type: str, payload: dict,
                    recipient_id: str | None = None,
                    correlation_id: str | None = None) -> None:
        try:
            await ws.send(_envelope(self._agent_id, msg_type, payload,
                                    recipient_id, correlation_id))
        except Exception as exc:
            logger.debug("Failed to send %s: %s", msg_type, exc)

    async def _reply(
        self,
        ws,
        recipient_id: str | None,
        correlation_id: str | None,
        success: bool,
        output: dict | None = None,
        error: str = "",
        duration_ms: float = 0.0,
    ) -> None:
        payload: dict[str, Any] = {
            "success":     success,
            "duration_ms": round(duration_ms, 1),
        }
        if success:
            payload["output_data"] = output or {}
        else:
            payload["error"] = error
        await self._send(ws, "task_response", payload,
                         recipient_id=recipient_id, correlation_id=correlation_id)
