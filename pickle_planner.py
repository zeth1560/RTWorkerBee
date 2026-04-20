"""
Pickle Planner booking match (optional standalone entrypoint).

Runtime configuration must come from :class:`config.Settings`; do not read os.environ here.

Expected successful JSON body from POST ``/bookings/match`` (shape may omit optional fields)::

    {
        "booking_id": "uuid-string",
        "start_time": "2026-04-20T14:00:00Z",
        "end_time": "2026-04-20T16:00:00Z"
    }

``start_time`` and ``end_time`` may be absent, null, or empty; parsing must not fail the worker.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from config import Settings
from network_retry import (
    NonRetryableDependencyError,
    TransientNetworkError,
    call_with_network_retry,
    is_retryable_network_error,
    logging_retry_hook,
)

logger = logging.getLogger(__name__)


def normalize_optional_timestamp(value: Any) -> str | None:
    """
    Normalize an API timestamp to an ISO-8601 string suitable for Postgres ``timestamptz``
    or text columns, or return ``None`` if absent/unparseable.

    - ``Z`` / ``z`` suffix is treated as UTC.
    - Naive datetimes are interpreted as UTC.
    - Blank, null-like strings, and booleans yield ``None``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return None
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
        return dt.isoformat()
    s = str(value).strip()
    if not s:
        return None
    low = s.lower()
    if low in ("null", "none", "nil", "undefined"):
        return None

    norm = s.replace("Z", "+00:00").replace("z", "+00:00")
    try:
        dt = datetime.fromisoformat(norm)
    except ValueError:
        logger.debug(
            "normalize_optional_timestamp: unparseable value",
            extra={"structured": {"raw": s[:160]}},
        )
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


@dataclass(frozen=True)
class BookingMatchResult:
    """Outcome of a Pickle Planner booking match call."""

    booking_id: str | None
    start_time: str | None
    end_time: str | None


def parse_booking_match_response(data: Any) -> BookingMatchResult:
    if not isinstance(data, dict):
        return BookingMatchResult(None, None, None)

    raw_id = data.get("booking_id")
    booking_id: str | None = None
    if raw_id is not None and isinstance(raw_id, str):
        booking_id = raw_id.strip() or None

    start_time = normalize_optional_timestamp(data.get("start_time"))
    end_time = normalize_optional_timestamp(data.get("end_time"))

    return BookingMatchResult(
        booking_id=booking_id,
        start_time=start_time,
        end_time=end_time,
    )


def get_booking_match_for_clip(
    settings: Settings, recorded_at: str
) -> BookingMatchResult:
    """
    Ask Pickle Planner which booking this clip belongs to and optionally receive
    booking window timestamps.

    Returns a :class:`BookingMatchResult` with ``booking_id`` set or all fields empty
    when the API responded but no booking matched.

    Raises :class:`TransientNetworkError` when the API could not be reached after retries.
    Raises :class:`NonRetryableDependencyError` on auth / permission failures.
    """
    url = settings.pickle_planner_match_url
    api_key = settings.pickle_planner_api_key
    api_key_header = settings.pickle_planner_api_key_header or "x-api-key"
    max_attempts = settings.booking_match_http_attempts

    def _request_once() -> BookingMatchResult:
        logger.info(
            "Starting Pickle Planner lookup",
            extra={
                "structured": {
                    "recorded_at": recorded_at,
                    "url": url,
                    "club_id": settings.club_id,
                    "court_id": settings.court_id,
                }
            },
        )
        try:
            response = requests.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    api_key_header: api_key,
                },
                json={
                    "recorded_at": recorded_at,
                    "club_id": settings.club_id,
                    "court_id": settings.court_id,
                },
                timeout=15,
            )
        except requests.exceptions.RequestException as exc:
            if is_retryable_network_error(exc):
                raise TransientNetworkError(str(exc)) from exc
            raise

        if response.status_code in (401, 403):
            raise NonRetryableDependencyError(
                f"Pickle Planner auth failed (HTTP {response.status_code})"
            )
        if response.status_code >= 500:
            raise TransientNetworkError(
                f"Pickle Planner server error HTTP {response.status_code}"
            )
        if not response.ok:
            logger.warning(
                "Pickle Planner lookup returned non-OK response",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "status_code": response.status_code,
                        "response_text": response.text[:2000],
                    }
                },
            )
            return BookingMatchResult(None, None, None)

        try:
            data = response.json()
        except ValueError:
            logger.warning(
                "Pickle Planner lookup returned non-JSON body",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "response_text": response.text[:2000],
                    }
                },
            )
            return BookingMatchResult(None, None, None)

        result = parse_booking_match_response(data)

        logger.info(
            "Pickle Planner lookup completed",
            extra={
                "structured": {
                    "recorded_at": recorded_at,
                    "booking_id": result.booking_id,
                    "start_time": result.start_time,
                    "end_time": result.end_time,
                }
            },
        )

        return result

    return call_with_network_retry(
        _request_once,
        operation="pickle_planner_booking_match",
        base_seconds=settings.network_retry_base_seconds,
        max_seconds=settings.network_retry_max_seconds,
        jitter_frac=settings.network_retry_jitter_fraction,
        max_rounds=max_attempts,
        on_retry=logging_retry_hook("Pickle Planner booking match"),
    )


def get_booking_id_for_clip(settings: Settings, recorded_at: str) -> str | None:
    """
    Ask Pickle Planner which booking this clip belongs to.
    Returns ``booking_id`` or ``None`` when the API responded but no booking matched.

    Prefer :func:`get_booking_match_for_clip` when start/end times are needed.

    Raises :class:`TransientNetworkError` when the API could not be reached after retries.
    Raises :class:`NonRetryableDependencyError` on auth / permission failures.
    """
    return get_booking_match_for_clip(settings, recorded_at).booking_id
