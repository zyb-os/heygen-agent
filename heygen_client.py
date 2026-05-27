"""
heygen_client.py — Async HeyGen API v2 client.

Wraps the HeyGen REST API for video generation, avatar/voice/template
discovery, status polling, and quota management.

Authentication: X-Api-Key header (HEYGEN_API_KEY env var).
Base URL: https://api.heygen.com
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.heygen.com"


class HeyGenError(Exception):
    """Raised when the HeyGen API returns an error response."""

    def __init__(self, message: str, status_code: int = 0, code: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class HeyGenClient:
    """
    Async HeyGen API client.

    All methods raise HeyGenError on API-level failures.
    Instantiate once and reuse across requests (shared httpx.AsyncClient).
    """

    def __init__(self, api_key: str, timeout: float = 30.0) -> None:
        if not api_key:
            raise ValueError("HEYGEN_API_KEY is required")
        self._http = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={"X-Api-Key": api_key, "Accept": "application/json"},
            timeout=timeout,
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _get(self, path: str, params: dict | None = None) -> Any:
        resp = await self._http.get(path, params=params)
        return self._unwrap(resp)

    async def _post(self, path: str, body: dict) -> Any:
        resp = await self._http.post(path, json=body)
        return self._unwrap(resp)

    async def _delete(self, path: str) -> Any:
        resp = await self._http.delete(path)
        return self._unwrap(resp)

    @staticmethod
    def _unwrap(resp: httpx.Response) -> Any:
        try:
            data = resp.json()
        except Exception:
            resp.raise_for_status()
            return {}

        # HeyGen uses {error: null, data: ...} for v1 and {data: ...} for v2
        if resp.status_code >= 400:
            err = data.get("error") or data.get("message") or str(resp.status_code)
            raise HeyGenError(str(err), status_code=resp.status_code)

        error = data.get("error")
        if error:
            code = ""
            if isinstance(error, dict):
                code = error.get("code", "")
                msg  = error.get("message", str(error))
            else:
                msg = str(error)
            raise HeyGenError(msg, status_code=resp.status_code, code=code)

        return data.get("data", data)

    # ── Video generation ───────────────────────────────────────────────────────

    async def create_video(
        self,
        avatar_id: str,
        script_text: str,
        voice_id: str = "",
        title: str = "",
        width: int = 1280,
        height: int = 720,
        background_color: str = "#ffffff",
        speed: float = 1.0,
        avatar_style: str = "normal",
    ) -> dict:
        """
        Generate a video from an avatar and text script.

        Returns {"video_id": str} — poll get_video_status() until done.

        avatar_id     : HeyGen avatar ID (from list_avatars)
        script_text   : Text the avatar will speak
        voice_id      : HeyGen voice ID; omit to use avatar default
        title         : Optional video title
        width/height  : Output resolution (default 1280×720)
        background_color: Hex colour string (default #ffffff)
        speed         : Speaking speed 0.5–1.5 (default 1.0)
        avatar_style  : "normal" | "circle" | "closeUp"
        """
        character: dict[str, Any] = {
            "type":        "avatar",
            "avatar_id":   avatar_id,
            "avatar_style": avatar_style,
        }
        voice: dict[str, Any] = {
            "type":       "text",
            "input_text": script_text,
            "speed":      speed,
        }
        if voice_id:
            voice["voice_id"] = voice_id

        body: dict[str, Any] = {
            "video_inputs": [
                {
                    "character": character,
                    "voice":     voice,
                    "background": {
                        "type":  "color",
                        "value": background_color,
                    },
                }
            ],
            "dimension": {"width": width, "height": height},
        }
        if title:
            body["title"] = title

        result = await self._post("/v2/video/generate", body)
        return {"video_id": result.get("video_id", result)}

    async def get_video_status(self, video_id: str) -> dict:
        """
        Get the current status of a video generation job.

        Returns:
          {
            "video_id":    str,
            "status":      "pending" | "processing" | "completed" | "failed",
            "download_url": str | None,   # set when status == "completed"
            "thumbnail_url": str | None,
            "duration":    float | None,  # seconds
            "error":       str | None,    # set when status == "failed"
          }
        """
        data = await self._get("/v1/video_status.get", {"video_id": video_id})
        return {
            "video_id":     data.get("video_id", video_id),
            "status":       data.get("status", "unknown"),
            "download_url": data.get("video_url") or data.get("download_url"),
            "thumbnail_url": data.get("thumbnail_url"),
            "duration":     data.get("duration"),
            "error":        data.get("error"),
        }

    async def download_video(self, video_id: str) -> dict:
        """
        Return the download URL for a completed video.
        Raises HeyGenError if the video is not yet completed.
        """
        status = await self.get_video_status(video_id)
        if status["status"] != "completed":
            raise HeyGenError(
                f"Video {video_id!r} is not completed (status: {status['status']}). "
                "Poll get_video_status first."
            )
        return {
            "video_id":     video_id,
            "download_url": status["download_url"],
            "thumbnail_url": status["thumbnail_url"],
            "duration":     status["duration"],
        }

    async def list_videos(self, limit: int = 20, token: str = "") -> dict:
        """
        List previously generated videos.

        Returns {"videos": [...], "next_token": str | None}
        """
        params: dict[str, Any] = {"limit": min(limit, 100)}
        if token:
            params["token"] = token
        data = await self._get("/v1/video.list", params)
        videos  = data.get("videos") or []
        cleaned = [
            {
                "video_id":    v.get("video_id"),
                "title":       v.get("title"),
                "status":      v.get("status"),
                "duration":    v.get("duration"),
                "thumbnail_url": v.get("thumbnail_url"),
                "created_at":  v.get("created_at"),
            }
            for v in videos
        ]
        return {"videos": cleaned, "next_token": data.get("token")}

    async def delete_video(self, video_id: str) -> dict:
        """Delete a generated video. Returns {"deleted": True}."""
        await self._delete(f"/v1/video/{video_id}")
        return {"deleted": True, "video_id": video_id}

    # ── Avatar management ──────────────────────────────────────────────────────

    async def list_avatars(self) -> dict:
        """
        List all avatars available to this API key.

        Returns {"avatars": [{"avatar_id", "avatar_name", "gender", "preview_image_url",
                               "preview_video_url", "talking_photo_id"}, ...]}
        """
        data = await self._get("/v2/avatars")
        raw = data.get("avatars") or []
        avatars = [
            {
                "avatar_id":        a.get("avatar_id"),
                "avatar_name":      a.get("avatar_name"),
                "gender":           a.get("gender"),
                "preview_image_url": a.get("preview_image_url"),
                "preview_video_url": a.get("preview_video_url"),
                "talking_photo_id": a.get("talking_photo_id"),
            }
            for a in raw
        ]
        return {"avatars": avatars, "total": len(avatars)}

    # ── Voice management ───────────────────────────────────────────────────────

    async def list_voices(
        self,
        language: str = "",
        gender: str = "",
    ) -> dict:
        """
        List available TTS voices.

        language : ISO 639-1 code to filter (e.g. "en", "es", "fr")
        gender   : "male" | "female" to filter

        Returns {"voices": [{"voice_id", "language", "name", "gender",
                              "preview_audio", "support_pause"}, ...]}
        """
        data = await self._get("/v2/voices")
        raw: list[dict] = data.get("voices") or []

        voices = []
        for v in raw:
            lang = v.get("language", "")
            gen  = v.get("gender", "")
            if language and not lang.lower().startswith(language.lower()):
                continue
            if gender and gen.lower() != gender.lower():
                continue
            voices.append({
                "voice_id":      v.get("voice_id"),
                "name":          v.get("name"),
                "language":      lang,
                "gender":        gen,
                "preview_audio": v.get("preview_audio"),
                "support_pause": v.get("support_pause", False),
            })
        return {"voices": voices, "total": len(voices)}

    # ── Template management ────────────────────────────────────────────────────

    async def list_templates(self) -> dict:
        """
        List available video templates.

        Returns {"templates": [{"template_id", "name", "thumbnail_image_url",
                                 "duration", "aspect_ratio"}, ...]}
        """
        data = await self._get("/v1/templates")
        raw: list[dict] = data.get("templates") or []
        templates = [
            {
                "template_id":       t.get("template_id"),
                "name":              t.get("name"),
                "thumbnail_image_url": t.get("thumbnail_image_url"),
                "duration":          t.get("duration"),
                "aspect_ratio":      t.get("aspect_ratio"),
            }
            for t in raw
        ]
        return {"templates": templates, "total": len(templates)}

    async def create_video_from_template(
        self,
        template_id: str,
        variables: dict[str, Any],
        title: str = "",
    ) -> dict:
        """
        Generate a video using a HeyGen template.

        template_id : ID from list_templates()
        variables   : Dict of variable name → value to fill in the template.
                      Each value may be a plain string or a full variable object
                      {"name": str, "type": "text"|"image", "properties": {...}}.
        title       : Optional video title

        Returns {"video_id": str}
        """
        # Accept both plain strings and full variable objects
        formatted: dict[str, Any] = {}
        for var_name, var_value in variables.items():
            if isinstance(var_value, dict):
                formatted[var_name] = var_value
            else:
                formatted[var_name] = {
                    "name":       var_name,
                    "type":       "text",
                    "properties": {"content": str(var_value)},
                }

        body: dict[str, Any] = {"variables": formatted}
        if title:
            body["title"] = title

        result = await self._post(f"/v1/template/{template_id}/generate", body)
        return {"video_id": result.get("video_id", result)}

    # ── Account ────────────────────────────────────────────────────────────────

    async def get_remaining_quota(self) -> dict:
        """
        Return the remaining API credits / quota for this account.

        Returns {"remaining_quota": int, "details": dict}
        """
        data = await self._get("/v1/user/remaining_quota")
        return {
            "remaining_quota": data.get("remaining_quota"),
            "details":         data,
        }
