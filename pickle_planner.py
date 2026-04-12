"""
Pickle Planner booking match (optional standalone entrypoint).

Runtime configuration must come from :class:`config.Settings`; do not read os.environ here.
"""

from __future__ import annotations

import logging
import time

import requests

from config import Settings

logger = logging.getLogger(__name__)


def get_booking_id_for_clip(settings: Settings, recorded_at: str) -> str | None:
    """
    Ask Pickle Planner which booking this clip belongs to.
    Returns ``booking_id`` or ``None`` (unmatched / empty response).
    """
    url = settings.pickle_planner_match_url
    api_key = settings.pickle_planner_api_key
    api_key_header = settings.pickle_planner_api_key_header or "x-api-key"
    max_attempts = settings.booking_match_http_attempts

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(
                "Starting Pickle Planner lookup",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "url": url,
                        "attempt": attempt,
                        "club_id": settings.club_id,
                        "court_id": settings.court_id,
                    }
                },
            )

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

            if response.ok:
                data = response.json()
                booking_id = data.get("booking_id")

                logger.info(
                    "Pickle Planner lookup completed",
                    extra={
                        "structured": {
                            "recorded_at": recorded_at,
                            "booking_id": booking_id,
                            "attempt": attempt,
                        }
                    },
                )

                if booking_id and isinstance(booking_id, str):
                    return booking_id.strip()

                return None

            logger.warning(
                "Pickle Planner lookup returned non-OK response",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "status_code": response.status_code,
                        "response_text": response.text[:2000],
                        "attempt": attempt,
                    }
                },
            )

        except Exception as exc:
            logger.warning(
                "Pickle Planner request failed",
                extra={
                    "structured": {
                        "recorded_at": recorded_at,
                        "error": str(exc),
                        "attempt": attempt,
                    }
                },
            )

        if attempt < max_attempts:
            time.sleep(2 * attempt)

    return None
