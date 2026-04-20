"""
Supabase (PostgREST) insert/update helpers for clip rows.
"""

from __future__ import annotations

import logging
from typing import Any

from supabase import Client, create_client
from postgrest.exceptions import APIError

from config import Settings
from network_retry import (
    call_with_network_retry,
    is_retryable_network_error,
    logging_retry_hook,
)

logger = logging.getLogger(__name__)


def create_supabase_client(settings: Settings) -> Client:
    return create_client(settings.supabase_url, settings.supabase_key)


def _clip_row(
    settings: Settings,
    *,
    title: str,
    slug: str,
    s3_key: str,
    preview_s3_key: str,
    recorded_at: str,
    duration_seconds: float | None = None,
    worker_job_identity: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "title": title,
        "slug": slug,
        "s3_key": s3_key,
        "preview_s3_key": preview_s3_key,
        "recorded_at": recorded_at,
        "club_id": settings.club_id,
        "court_id": settings.court_id,
        "published": settings.published,
    }
    if duration_seconds is not None:
        row["duration_seconds"] = duration_seconds
    col = settings.supabase_clip_worker_identity_column.strip()
    if col and worker_job_identity:
        row[col] = worker_job_identity
    return row


def upsert_clip_record(
    client: Client,
    settings: Settings,
    *,
    title: str,
    slug: str,
    s3_key: str,
    preview_s3_key: str,
    recorded_at: str,
    duration_seconds: float | None = None,
    worker_job_identity: str | None = None,
) -> dict[str, Any]:
    """
    Insert or update on ``s3_key`` conflict so restarts never create duplicate rows.
    """
    row = _clip_row(
        settings,
        title=title,
        slug=slug,
        s3_key=s3_key,
        preview_s3_key=preview_s3_key,
        recorded_at=recorded_at,
        duration_seconds=duration_seconds,
        worker_job_identity=worker_job_identity,
    )

    logger.info(
        "Supabase upsert",
        extra={
            "structured": {
                "table": settings.supabase_clips_table,
                "slug": slug,
                "s3_key": s3_key,
                "recorded_at": recorded_at,
                "duration_seconds": duration_seconds,
                "worker_job_identity": worker_job_identity,
            }
        },
    )

    def _execute_upsert() -> dict[str, Any]:
        try:
            response = (
                client.table(settings.supabase_clips_table)
                .upsert(row, on_conflict="s3_key")
                .execute()
            )
            data = getattr(response, "data", None)
            if data and isinstance(data, list) and len(data) > 0:
                return data[0]
            return row
        except APIError as e:
            err = str(e).lower()
            if "duration_seconds" in row and (
                "column" in err and "duration_seconds" in err and "does not exist" in err
            ):
                logger.warning(
                    "Supabase upsert retried without duration_seconds (column missing)",
                    extra={"structured": {"table": settings.supabase_clips_table}},
                )
                row_without_duration = dict(row)
                row_without_duration.pop("duration_seconds", None)
                response = (
                    client.table(settings.supabase_clips_table)
                    .upsert(row_without_duration, on_conflict="s3_key")
                    .execute()
                )
                data = getattr(response, "data", None)
                if data and isinstance(data, list) and len(data) > 0:
                    return data[0]
                return row_without_duration
            if "duplicate key value violates unique constraint" in str(e):
                logger.warning(
                    "Upsert fell back to select",
                    extra={"structured": {"s3_key": s3_key}},
                )
                existing = (
                    client.table(settings.supabase_clips_table)
                    .select("*")
                    .eq("s3_key", s3_key)
                    .limit(1)
                    .execute()
                )
                data = getattr(existing, "data", None)
                if data and len(data) > 0:
                    return data[0]
            if is_retryable_network_error(e):
                raise
            raise

    return call_with_network_retry(
        _execute_upsert,
        operation="supabase_upsert",
        base_seconds=settings.network_retry_base_seconds,
        max_seconds=settings.network_retry_max_seconds,
        jitter_frac=settings.network_retry_jitter_fraction,
        max_rounds=settings.network_retry_rounds_per_tick,
        on_retry=logging_retry_hook("Supabase upsert"),
    )


def insert_clip_record(
    client: Client,
    settings: Settings,
    *,
    title: str,
    slug: str,
    s3_key: str,
    preview_s3_key: str,
    recorded_at: str,
    duration_seconds: float | None = None,
    worker_job_identity: str | None = None,
) -> dict[str, Any]:
    """Backward-compatible alias for :func:`upsert_clip_record`."""
    return upsert_clip_record(
        client,
        settings,
        title=title,
        slug=slug,
        s3_key=s3_key,
        preview_s3_key=preview_s3_key,
        recorded_at=recorded_at,
        duration_seconds=duration_seconds,
        worker_job_identity=worker_job_identity,
    )


def update_clip_booking_id(
    client: Client,
    settings: Settings,
    *,
    clip_id: str,
    booking_id: str,
) -> dict[str, Any]:
    """
    Update an existing clip row with the resolved booking_id.

    Returns the updated row dict when available.
    """
    logger.info(
        "Supabase clip booking update",
        extra={
            "structured": {
                "table": settings.supabase_clips_table,
                "clip_id": clip_id,
                "booking_id": booking_id,
            }
        },
    )

    def _do_update() -> dict[str, Any]:
        response = (
            client.table(settings.supabase_clips_table)
            .update({"booking_id": booking_id})
            .eq("id", clip_id)
            .execute()
        )
        data = getattr(response, "data", None)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return {"id": clip_id, "booking_id": booking_id}

    out = call_with_network_retry(
        _do_update,
        operation="supabase_booking_update",
        base_seconds=settings.network_retry_base_seconds,
        max_seconds=settings.network_retry_max_seconds,
        jitter_frac=settings.network_retry_jitter_fraction,
        max_rounds=settings.network_retry_rounds_per_tick,
        on_retry=logging_retry_hook("Supabase booking update"),
    )

    logger.info(
        "Supabase clip booking update completed",
        extra={
            "structured": {
                "table": settings.supabase_clips_table,
                "clip_id": clip_id,
                "booking_id": booking_id,
            }
        },
    )
    return out


def upsert_booking_from_match(
    client: Client,
    settings: Settings,
    *,
    booking_id: str,
    start_time: str | None,
    end_time: str | None,
) -> dict[str, Any]:
    """
    Insert or update a row in the ``bookings`` table when a clip matches a booking.

    ``start_time`` and ``end_time`` are only included in the payload when non-None so
    existing DB values are not overwritten with nulls when the API omits them.

    Upserts on ``booking_id`` conflict. ``club_id`` and ``court_id`` are always set
    from worker settings for the matched row.
    """
    row: dict[str, Any] = {
        "booking_id": booking_id,
        "club_id": settings.club_id,
        "court_id": settings.court_id,
    }
    if start_time is not None:
        row["start_time"] = start_time
    if end_time is not None:
        row["end_time"] = end_time

    logger.info(
        "Supabase booking upsert",
        extra={
            "structured": {
                "table": settings.supabase_bookings_table,
                "booking_id": booking_id,
                "club_id": settings.club_id,
                "court_id": settings.court_id,
                "start_time": start_time,
                "end_time": end_time,
            }
        },
    )

    def _execute_upsert() -> dict[str, Any]:
        try:
            response = (
                client.table(settings.supabase_bookings_table)
                .upsert(row, on_conflict="booking_id")
                .execute()
            )
            data = getattr(response, "data", None)
            if data and isinstance(data, list) and len(data) > 0:
                return data[0]
            return row
        except APIError as e:
            err_l = str(e).lower()
            had_times = "start_time" in row or "end_time" in row
            time_col_missing = had_times and (
                (
                    "start_time" in err_l
                    and (
                        "does not exist" in err_l
                        or "not found" in err_l
                        or "unknown" in err_l
                    )
                )
                or (
                    "end_time" in err_l
                    and (
                        "does not exist" in err_l
                        or "not found" in err_l
                        or "unknown" in err_l
                    )
                )
            )
            if time_col_missing:
                logger.warning(
                    "Supabase booking upsert retried without start_time/end_time",
                    extra={"structured": {"error": str(e)[:500]}},
                )
                row_min: dict[str, Any] = {
                    "booking_id": booking_id,
                    "club_id": settings.club_id,
                    "court_id": settings.court_id,
                }
                response = (
                    client.table(settings.supabase_bookings_table)
                    .upsert(row_min, on_conflict="booking_id")
                    .execute()
                )
                data = getattr(response, "data", None)
                if data and isinstance(data, list) and len(data) > 0:
                    return data[0]
                return row_min
            if is_retryable_network_error(e):
                raise
            raise

    out = call_with_network_retry(
        _execute_upsert,
        operation="supabase_booking_upsert",
        base_seconds=settings.network_retry_base_seconds,
        max_seconds=settings.network_retry_max_seconds,
        jitter_frac=settings.network_retry_jitter_fraction,
        max_rounds=settings.network_retry_rounds_per_tick,
        on_retry=logging_retry_hook("Supabase booking upsert"),
    )

    logger.debug(
        "Supabase booking upsert completed",
        extra={
            "structured": {
                "booking_id": booking_id,
                "table": settings.supabase_bookings_table,
            }
        },
    )
    return out