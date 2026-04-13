"""
Local-only HTTP trigger for replay-buffer processing (stdlib, Windows-friendly).

Binds to loopback by default. Use GET (query params) for Stream Deck / simple launchers,
or POST with JSON body. Responses are JSON; include optional ``request_id`` for log correlation.
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.parse
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable

logger = logging.getLogger(__name__)

ReplayPipeline = Callable[..., tuple[Any, int]]


class ReplayTriggerHTTPServer(HTTPServer):
    """HTTPServer with a shared busy lock for one replay at a time."""

    busy_lock: threading.Lock

    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler],
        busy_lock: threading.Lock,
    ) -> None:
        self.busy_lock = busy_lock
        super().__init__(server_address, RequestHandlerClass)


def _make_handler_class(
    run_pipeline: ReplayPipeline,
    default_timeout: float,
    default_prefix: str,
    default_tolerance: float,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            logger.debug("replay-trigger-http: %s", fmt % args)

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def _parse_post_json(self) -> dict[str, Any] | None:
            length_s = self.headers.get("Content-Length", "0").strip()
            try:
                n = int(length_s)
            except ValueError:
                return None
            if n <= 0:
                return {}
            raw = self.rfile.read(n)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return None
            if not isinstance(data, dict):
                return None
            return data

        def _params_from_query(self) -> dict[str, Any]:
            parsed = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=False)
            out: dict[str, Any] = {}
            if "trigger" in qs and qs["trigger"]:
                out["trigger"] = qs["trigger"][0]
            if "timeout" in qs and qs["timeout"]:
                try:
                    out["timeout"] = float(qs["timeout"][0])
                except ValueError:
                    out["timeout"] = None
            if "request_id" in qs and qs["request_id"]:
                out["request_id"] = qs["request_id"][0]
            if "prefix" in qs and qs["prefix"]:
                out["prefix"] = qs["prefix"][0]
            if "tolerance" in qs and qs["tolerance"]:
                try:
                    out["tolerance"] = float(qs["tolerance"][0])
                except ValueError:
                    out["tolerance"] = None
            return out

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path in ("/health", "/healthz"):
                self._send_json(HTTPStatus.OK, {"ok": True, "service": "replay-trigger-http"})
                return
            if path not in ("/replay", "/"):
                self.send_error(HTTPStatus.NOT_FOUND.value, "Not Found")
                return

            q = self._params_from_query()
            trigger = q.get("trigger")
            if not trigger or not isinstance(trigger, str):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "success": False,
                        "failure_reason": "missing_trigger",
                        "message": "Provide trigger as query parameter (Unix time or ISO-8601)",
                    },
                )
                return

            timeout = q.get("timeout")
            if timeout is None:
                timeout = default_timeout
            elif not isinstance(timeout, (int, float)):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_timeout"},
                )
                return

            prefix = q.get("prefix") if isinstance(q.get("prefix"), str) else default_prefix
            tolerance = q.get("tolerance")
            if tolerance is None:
                tolerance = default_tolerance
            elif not isinstance(tolerance, (int, float)):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_tolerance"},
                )
                return

            request_id = q.get("request_id") if isinstance(q.get("request_id"), str) else None
            self._run_and_respond(
                trigger_raw=trigger,
                timeout_seconds=float(timeout),
                request_id=request_id,
                filename_prefix=prefix,
                tolerance_seconds=float(tolerance),
            )

        def do_POST(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path not in ("/replay", "/"):
                self.send_error(HTTPStatus.NOT_FOUND.value, "Not Found")
                return

            data = self._parse_post_json()
            if data is None:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_json_body"},
                )
                return

            trigger = data.get("trigger")
            if not trigger or not isinstance(trigger, str):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "missing_trigger"},
                )
                return

            timeout = data.get("timeout", default_timeout)
            try:
                timeout_f = float(timeout)
            except (TypeError, ValueError):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_timeout"},
                )
                return

            prefix = data.get("prefix", default_prefix)
            if not isinstance(prefix, str):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_prefix"},
                )
                return

            tolerance = data.get("tolerance", default_tolerance)
            try:
                tolerance_f = float(tolerance)
            except (TypeError, ValueError):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_tolerance"},
                )
                return

            request_id = data.get("request_id")
            if request_id is not None and not isinstance(request_id, str):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "failure_reason": "invalid_request_id"},
                )
                return

            self._run_and_respond(
                trigger_raw=trigger,
                timeout_seconds=timeout_f,
                request_id=request_id,
                filename_prefix=prefix,
                tolerance_seconds=tolerance_f,
            )

        def _run_and_respond(
            self,
            *,
            trigger_raw: str,
            timeout_seconds: float,
            request_id: str | None,
            filename_prefix: str,
            tolerance_seconds: float,
        ) -> None:
            if not self.server.busy_lock.acquire(blocking=False):
                logger.warning(
                    "replay-trigger-http: busy; rejecting request",
                    extra={"structured": {"request_id": request_id}},
                )
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "success": False,
                        "failure_reason": "replay_trigger_busy",
                        "request_id": request_id,
                    },
                )
                return

            logger.info(
                "replay-trigger-http: request received",
                extra={
                    "structured": {
                        "request_id": request_id,
                        "trigger": trigger_raw[:120],
                        "timeout_seconds": timeout_seconds,
                    }
                },
            )
            try:
                result, exit_code = run_pipeline(
                    trigger_raw=trigger_raw,
                    timeout_seconds=timeout_seconds,
                    request_id=request_id,
                    filename_prefix=filename_prefix,
                    tolerance_seconds=tolerance_seconds,
                )
            except Exception as exc:
                logger.exception(
                    "replay-trigger-http: pipeline raised",
                    extra={"structured": {"request_id": request_id}},
                )
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "success": False,
                        "failure_reason": "replay_pipeline_exception",
                        "processing_error": str(exc)[:500],
                        "request_id": request_id,
                    },
                )
                return
            finally:
                self.server.busy_lock.release()

            payload: dict[str, Any] = dict(asdict(result))
            if request_id is not None:
                payload["request_id"] = request_id
            payload["exit_code"] = exit_code
            # Always 200 with JSON body so simple clients can parse; use success + exit_code.
            self._send_json(HTTPStatus.OK, payload)
            logger.info(
                "replay-trigger-http: request finished",
                extra={
                    "structured": {
                        "request_id": request_id,
                        "success": result.success,
                        "exit_code": exit_code,
                        "failure_reason": result.failure_reason,
                    }
                },
            )

    return Handler


def serve_replay_trigger_http_blocking(
    host: str,
    port: int,
    run_pipeline: ReplayPipeline,
    *,
    default_timeout: float = 120.0,
    default_prefix: str = "replay_",
    default_tolerance: float = 10.0,
) -> None:
    """Run the HTTP server in the current thread until interrupted."""
    busy = threading.Lock()
    handler_cls = _make_handler_class(
        run_pipeline,
        default_timeout=default_timeout,
        default_prefix=default_prefix,
        default_tolerance=default_tolerance,
    )
    httpd = ReplayTriggerHTTPServer((host, port), handler_cls, busy_lock=busy)
    logger.info(
        "replay-trigger-http: listening",
        extra={"structured": {"host": host, "port": port}},
    )
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


def run_replay_trigger_http_loop(
    host: str,
    port: int,
    run_pipeline: ReplayPipeline,
    stop: threading.Event,
    *,
    default_timeout: float = 120.0,
    default_prefix: str = "replay_",
    default_tolerance: float = 10.0,
) -> None:
    """Non-blocking-friendly loop: poll ``stop`` between requests (for embedding in worker)."""
    busy = threading.Lock()
    handler_cls = _make_handler_class(
        run_pipeline,
        default_timeout=default_timeout,
        default_prefix=default_prefix,
        default_tolerance=default_tolerance,
    )
    httpd = ReplayTriggerHTTPServer((host, port), handler_cls, busy_lock=busy)
    httpd.timeout = 0.5
    logger.info(
        "replay-trigger-http: listening (embedded)",
        extra={"structured": {"host": host, "port": port}},
    )
    try:
        while not stop.is_set():
            httpd.handle_request()
    finally:
        try:
            httpd.server_close()
        except OSError:
            pass
