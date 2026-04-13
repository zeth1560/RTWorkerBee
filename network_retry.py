"""
Network call retry helpers: exponential backoff with jitter and error classification.
"""

from __future__ import annotations

import logging
import random
import socket
import time
from collections.abc import Callable
from typing import TypeVar

import requests
from botocore.exceptions import ClientError
from postgrest.exceptions import APIError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class NonRetryableDependencyError(RuntimeError):
    """Config, credentials, or client error that should not be retried."""


class TransientNetworkError(RuntimeError):
    """Retryable network/service error that may succeed after connectivity returns."""


def _jitter(seconds: float, jitter_frac: float) -> float:
    if jitter_frac <= 0 or seconds <= 0:
        return seconds
    lo = seconds * (1.0 - jitter_frac)
    hi = seconds * (1.0 + jitter_frac)
    return max(0.0, random.uniform(lo, hi))


def backoff_delay_seconds(
    attempt_index: int,
    *,
    base_seconds: float,
    max_seconds: float,
) -> float:
    """attempt_index 0 → base, then doubles, capped at max (before jitter)."""
    exp = min(max_seconds, base_seconds * (2**attempt_index))
    return float(exp)


def is_retryable_network_error(exc: BaseException) -> bool:
    """True for DNS, timeouts, connection loss, HTTP 5xx (when detectable)."""
    if isinstance(exc, (FileNotFoundError, PermissionError, IsADirectoryError)):
        return False
    if isinstance(exc, socket.gaierror):
        return True
    if isinstance(exc, socket.timeout):
        return True
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, (ConnectionError, BrokenPipeError, ConnectionResetError)):
        return True
    if isinstance(exc, OSError):
        err = getattr(exc, "errno", None)
        win = getattr(exc, "winerror", None)
        if win in (10050, 10051, 10060, 10061, 10064, 10065):
            return True
        if err in (101, 100, 99):
            return True
        if err in (11001, 11002):
            return True
        eai_again = getattr(socket, "EAI_AGAIN", None)
        eai_noname = getattr(socket, "EAI_NONAME", None)
        if eai_again is not None and err == eai_again:
            return True
        if eai_noname is not None and err == eai_noname:
            return True
    if isinstance(exc, requests.exceptions.Timeout):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError):
        return True
    if isinstance(exc, requests.exceptions.ChunkedEncodingError):
        return True
    if isinstance(exc, TransientNetworkError):
        return True

    if isinstance(exc, APIError):
        code = (getattr(exc, "code", None) or "").strip()
        msg = (getattr(exc, "message", None) or str(exc)).lower()
        if code in ("401", "403", "PGRST301", "42501"):
            return False
        if code.isdigit() and code.startswith("5"):
            return True
        if "jwt" in msg or "permission denied" in msg or "unauthorized" in msg:
            return False
        if "timeout" in msg or "timed out" in msg:
            return True
        if "connection" in msg or "network" in msg:
            return True
        return False

    if isinstance(exc, ClientError):
        err = exc.response.get("Error", {}) if exc.response else {}
        code = str(err.get("Code", "") or "")
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if code in (
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "AccessDenied",
            "InvalidToken",
            "TokenRefreshRequired",
        ):
            return False
        if status in (401, 403):
            return False
        if status == 429:
            return True
        if status >= 500 or code in ("ServiceUnavailable", "SlowDown", "RequestTimeout"):
            return True
        if status == 408:
            return True
        return False

    msg = str(exc).lower()
    if "getaddrinfo failed" in msg or "name or service not known" in msg:
        return True
    if "temporary failure in name resolution" in msg:
        return True
    if "timed out" in msg or "timeout" in msg:
        return True
    if "connection reset" in msg or "connection aborted" in msg:
        return True
    if "errno 11001" in msg or "errno 11002" in msg:
        return True
    return False


def is_non_retryable_dependency_error(exc: BaseException) -> bool:
    """Auth, permission, or obvious client misuse — fail without endless retry."""
    if isinstance(exc, NonRetryableDependencyError):
        return True
    if isinstance(exc, APIError):
        code = (getattr(exc, "code", None) or "").strip()
        msg = (getattr(exc, "message", None) or str(exc)).lower()
        if code in ("401", "403", "PGRST301", "42501"):
            return True
        if "jwt" in msg or "permission denied" in msg or "unauthorized" in msg:
            return True
    if isinstance(exc, ClientError):
        err = exc.response.get("Error", {}) if exc.response else {}
        code = str(err.get("Code", "") or "")
        if code in (
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "AccessDenied",
            "InvalidToken",
        ):
            return True
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if status in (401, 403):
            return True
    if isinstance(exc, requests.exceptions.HTTPError):
        resp = exc.response
        if resp is not None and resp.status_code in (401, 403):
            return True
    return False


def classify_network_exception(exc: BaseException) -> str:
    """Return ``retryable`` | ``non_retryable`` | ``unknown``."""
    if is_non_retryable_dependency_error(exc):
        return "non_retryable"
    if is_retryable_network_error(exc):
        return "retryable"
    return "unknown"


def sleep_backoff(
    attempt_index: int,
    *,
    base_seconds: float,
    max_seconds: float,
    jitter_frac: float,
) -> None:
    raw = backoff_delay_seconds(attempt_index, base_seconds=base_seconds, max_seconds=max_seconds)
    delay = _jitter(raw, jitter_frac)
    if delay > 0:
        time.sleep(delay)


def call_with_network_retry(
    fn: Callable[[], T],
    *,
    operation: str,
    base_seconds: float,
    max_seconds: float,
    jitter_frac: float,
    max_rounds: int | None,
    on_retry: Callable[[int, BaseException], None] | None = None,
    infinite: bool = False,
) -> T:
    """
    Retry ``fn`` on retryable errors with exponential backoff + jitter.

    If ``infinite`` is True, ``max_rounds`` is ignored. Otherwise ``max_rounds`` is the
    maximum number of attempts (including the first).
    """
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            if is_non_retryable_dependency_error(exc):
                raise
            if not is_retryable_network_error(exc) and not isinstance(exc, TransientNetworkError):
                raise
            if on_retry is not None:
                on_retry(attempt, exc)
            if not infinite:
                if max_rounds is not None and attempt + 1 >= max_rounds:
                    raise TransientNetworkError(
                        f"{operation} failed after {max_rounds} attempt(s): {exc}"
                    ) from exc
            sleep_backoff(
                attempt,
                base_seconds=base_seconds,
                max_seconds=max_seconds,
                jitter_frac=jitter_frac,
            )
            attempt += 1


def logging_retry_hook(operation: str) -> Callable[[int, BaseException], None]:
    def _hook(attempt: int, exc: BaseException) -> None:
        logger.warning(
            "%s: retrying after retryable error",
            operation,
            extra={
                "structured": {
                    "attempt": attempt + 1,
                    "error": str(exc)[:500],
                    "error_class": type(exc).__name__,
                }
            },
        )

    return _hook