"""
Supabase (PostgREST) insert/update helpers for clip rows.
"""

from __future__ import annotations

import logging
from typing import Any

from supabase import Client, create_client
from postgrest.exceptions import APIError

from config import Settings

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
                "worker_job_identity": worker_job_identity,
            }
        },
    )

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
        raise


def insert_clip_record(
    client: Client,
    settings: Settings,
    *,
    title: str,
    slug: str,
    s3_key: str,
    preview_s3_key: str,
    recorded_at: str,
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

    response = (
        client.table(settings.supabase_clips_table)
        .update({"booking_id": booking_id})
        .eq("id", clip_id)
        .execute()
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

    data = getattr(response, "data", None)
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]

    return {"id": clip_id, "booking_id": booking_id}