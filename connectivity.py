"""
Background connectivity probe: DNS, Supabase host, S3 endpoint.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import Any
from urllib.parse import urlparse

from config import Settings
from lifecycle_events import (
    CONNECTIVITY_RESTORED,
    DNS_RESOLUTION_FAILED,
    ENTERING_OFFLINE_MODE,
    NETWORK_OFFLINE,
    S3_UNAVAILABLE,
    SUPABASE_UNAVAILABLE,
    log_worker_event,
)

logger = logging.getLogger(__name__)


def _resolve_host(hostname: str, *, timeout_seconds: float) -> bool:
    if not hostname:
        return False

    def _run() -> None:
        socket.getaddrinfo(hostname, None, socket.AF_UNSPEC)

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(_run)
        try:
            fut.result(timeout=timeout_seconds)
            return True
        except (FuturesTimeout, OSError, socket.gaierror):
            return False


def _tcp_probe(host: str, port: int, *, timeout_seconds: float) -> bool:
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


class ConnectivityMonitor:
    """
    Tracks coarse connectivity: ONLINE (Supabase + S3 TCP both OK),
    DEGRADED (exactly one OK), OFFLINE (neither OK).
    Emits structured logs only when ``state`` changes.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        interval_seconds: float,
        probe_timeout_seconds: float,
    ) -> None:
        self._settings = settings
        self._interval = max(5.0, interval_seconds)
        self._probe_timeout = max(1.0, probe_timeout_seconds)
        self._lock = threading.Lock()
        self._state = "ONLINE"
        self._last_snapshot: dict[str, Any] = {}
        self._started_offline_logged = False
        self._last_state_change_at = time.time()

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def last_state_change_at(self) -> float:
        with self._lock:
            return float(self._last_state_change_at)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._last_snapshot)

    def probe_once(self) -> tuple[str, dict[str, Any]]:
        sb_url = self._settings.supabase_url
        parsed = urlparse(sb_url)
        sb_host = parsed.hostname or ""
        region = self._settings.aws_region
        s3_host = f"s3.{region}.amazonaws.com"

        sb_dns = _resolve_host(sb_host, timeout_seconds=self._probe_timeout)
        s3_dns = _resolve_host(s3_host, timeout_seconds=self._probe_timeout)

        sb_tcp = _tcp_probe(sb_host, 443, timeout_seconds=self._probe_timeout)
        s3_tcp = _tcp_probe(s3_host, 443, timeout_seconds=self._probe_timeout)

        details: dict[str, Any] = {
            "supabase_host": sb_host,
            "supabase_dns_ok": sb_dns,
            "supabase_tcp_ok": sb_tcp,
            "s3_host": s3_host,
            "s3_dns_ok": s3_dns,
            "s3_tcp_ok": s3_tcp,
        }

        if sb_tcp and s3_tcp:
            state = "ONLINE"
        elif not sb_tcp and not s3_tcp:
            state = "OFFLINE"
        else:
            state = "DEGRADED"

        details["state"] = state
        return state, details

    def _apply_state(self, state: str, details: dict[str, Any]) -> None:
        with self._lock:
            prev = self._state
            self._last_snapshot = details
            self._state = state
            if prev != state:
                self._last_state_change_at = time.time()
        if prev == state:
            return

        if state == "OFFLINE":
            log_worker_event(
                logger,
                logging.WARNING,
                NETWORK_OFFLINE,
                "Network connectivity: OFFLINE",
                details,
            )
            if not details.get("supabase_dns_ok"):
                log_worker_event(
                    logger,
                    logging.WARNING,
                    DNS_RESOLUTION_FAILED,
                    "DNS resolution failed for Supabase host",
                    {"hostname": details.get("supabase_host")},
                )
            if not details.get("s3_dns_ok"):
                log_worker_event(
                    logger,
                    logging.WARNING,
                    DNS_RESOLUTION_FAILED,
                    "DNS resolution failed for S3 endpoint host",
                    {"hostname": details.get("s3_host")},
                )
        elif state == "DEGRADED":
            if details.get("supabase_tcp_ok") and not details.get("s3_tcp_ok"):
                log_worker_event(
                    logger,
                    logging.WARNING,
                    S3_UNAVAILABLE,
                    "S3 endpoint not reachable; Supabase OK (degraded)",
                    details,
                )
            elif details.get("s3_tcp_ok") and not details.get("supabase_tcp_ok"):
                log_worker_event(
                    logger,
                    logging.WARNING,
                    SUPABASE_UNAVAILABLE,
                    "Supabase endpoint not reachable; S3 OK (degraded)",
                    details,
                )
        elif prev in ("OFFLINE", "DEGRADED") and state == "ONLINE":
            log_worker_event(
                logger,
                logging.INFO,
                CONNECTIVITY_RESTORED,
                "Connectivity restored, resuming remote operations",
                {"previous": prev, "current": state, **details},
            )

    def mark_startup_offline_mode(self) -> None:
        """Call when Supabase warm-up fails so we log offline mode once at startup."""
        if self._started_offline_logged:
            return
        self._started_offline_logged = True
        log_worker_event(
            logger,
            logging.WARNING,
            ENTERING_OFFLINE_MODE,
            "Supabase unavailable, starting in offline mode",
            {},
        )
        with self._lock:
            self._state = "OFFLINE"
            self._last_snapshot = {"reason": "startup_supabase_warmup_failed"}
            self._last_state_change_at = time.time()

    def run_loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                state, details = self.probe_once()
                self._apply_state(state, details)
            except Exception:
                logger.exception("Connectivity probe failed")
            if stop.wait(timeout=self._interval):
                break
