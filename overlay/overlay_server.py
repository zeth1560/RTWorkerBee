"""
Local HTTP server for the OBS recording countdown overlay.

- Serves ``recording_countdown_overlay.html`` (Browser Source URL: http://127.0.0.1:<port>/).
- Stream Deck / scripts: GET or POST ``/api/start-recording-countdown`` to begin a 20-minute timer.
- When time expires, clients show the max-length message until ``/api/dismiss-ended`` (POST JSON ``{"key":"..."}``)
  or matching configured hotkey.

Environment:
  RECORDING_OVERLAY_HOST (default 127.0.0.1)
  RECORDING_OVERLAY_PORT (default 8765)
  RECORDING_OVERLAY_DURATION_SEC (default 1200 = 20 minutes)
  RECORDING_OVERLAY_DISMISS_KEYS (default "escape,x" — keys accepted when ending message is visible)

Bind is localhost-only for safety.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

_OVERLAY_DIR = Path(__file__).resolve().parent
_HTML_NAME = "recording_countdown_overlay.html"

_state_lock = threading.Lock()
# phase: idle | countdown | ended
_state: dict[str, object] = {"phase": "idle", "ends_at_monotonic": None}


def _duration_sec() -> float:
    raw = os.environ.get("RECORDING_OVERLAY_DURATION_SEC", "1200").strip()
    try:
        v = float(raw)
        return max(1.0, v)
    except ValueError:
        return 1200.0


def _dismiss_keys() -> list[str]:
    raw = os.environ.get("RECORDING_OVERLAY_DISMISS_KEYS", "escape,x").strip()
    if not raw:
        return ["escape", "x"]
    return [k.strip().lower() for k in raw.split(",") if k.strip()]


def _normalize_key(key: str) -> str:
    k = key.strip().lower()
    return k


def _snapshot_state() -> dict:
    now = time.monotonic()
    dismiss = _dismiss_keys()
    with _state_lock:
        phase = _state["phase"]
        ends_at = _state["ends_at_monotonic"]

        if phase == "countdown" and ends_at is not None:
            remaining = max(0.0, ends_at - now)
            if remaining <= 0:
                _state["phase"] = "ended"
                _state["ends_at_monotonic"] = None
                phase = "ended"
                ends_at = None

        if phase == "countdown" and ends_at is not None:
            remaining_wall = max(0.0, ends_at - now)
            ends_at_ms = time.time() + remaining_wall
            return {
                "phase": "countdown",
                "remaining_sec": remaining_wall,
                "ends_at_ms": int(ends_at_ms * 1000),
                "dismiss_keys": dismiss,
            }

        if phase == "ended":
            return {"phase": "ended", "dismiss_keys": dismiss}

        return {"phase": "idle", "dismiss_keys": dismiss}


def _start_countdown() -> None:
    dur = _duration_sec()
    with _state_lock:
        _state["phase"] = "countdown"
        _state["ends_at_monotonic"] = time.monotonic() + dur


def _dismiss_ended(key: str | None, *, force: bool = False) -> bool:
    allowed = {_normalize_key(k) for k in _dismiss_keys()}
    if not force:
        if key is None or _normalize_key(key) not in allowed:
            return False
    with _state_lock:
        if _state["phase"] != "ended":
            return False
        _state["phase"] = "idle"
        _state["ends_at_monotonic"] = None
    return True


class _Handler(BaseHTTPRequestHandler):
    server_version = "RecordingOverlay/1.0"

    def log_message(self, format: str, *args) -> None:
        return

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj: dict) -> None:
        data = json.dumps(obj).encode("utf-8")
        self._send(code, data, "application/json")

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path or "/"

        if path in ("/api/start-recording-countdown", "/api/start"):
            _start_countdown()
            self._json(HTTPStatus.OK, {"ok": True, "phase": "countdown"})
            return

        if path == "/api/dismiss-ended":
            qs = urllib.parse.parse_qs(parsed.query)
            key = (qs.get("key") or [None])[0]
            force_qs = (qs.get("force") or ["0"])[0] in ("1", "true", "yes")
            ok = _dismiss_ended(key, force=force_qs)
            self._json(HTTPStatus.OK, {"ok": ok})
            return

        if path == "/api/overlay-state":
            self._json(HTTPStatus.OK, _snapshot_state())
            return

        if path in ("/", f"/{_HTML_NAME}"):
            html_path = _OVERLAY_DIR / _HTML_NAME
            if not html_path.is_file():
                self._send(HTTPStatus.NOT_FOUND, b"missing overlay html", "text/plain")
                return
            body = html_path.read_bytes()
            self._send(HTTPStatus.OK, body, "text/html; charset=utf-8")
            return

        self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path or "/"
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"

        if path in ("/api/start-recording-countdown", "/api/start"):
            _start_countdown()
            self._json(HTTPStatus.OK, {"ok": True, "phase": "countdown"})
            return

        if path == "/api/dismiss-ended":
            key = None
            force = False
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
                if isinstance(payload, dict):
                    key = payload.get("key")
                    force = bool(payload.get("force"))
            except json.JSONDecodeError:
                pass
            ok = _dismiss_ended(
                str(key) if key is not None else None,
                force=force,
            )
            self._json(HTTPStatus.OK, {"ok": ok})
            return

        self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")


def main() -> None:
    host = os.environ.get("RECORDING_OVERLAY_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.environ.get("RECORDING_OVERLAY_PORT", "8765"))

    if host not in ("127.0.0.1", "::1", "localhost"):
        raise SystemExit("RECORDING_OVERLAY_HOST must be loopback for safety")

    httpd = ThreadingHTTPServer((host, port), _Handler)
    print(f"Recording overlay server http://{host}:{port}/")
    print("Start countdown: GET|POST /api/start-recording-countdown")
    print("Dismiss message: POST /api/dismiss-ended  body: {\"key\":\"escape\"}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
