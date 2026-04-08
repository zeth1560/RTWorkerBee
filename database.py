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


def insert_clip_record(
    client: Client,
    settings: Settings,
    *,
    title: str,
    slug: str,
    s3_key: str,
    preview_s3_key: str,
    recorded_at: str,
) -> dict[str, Any]:
    """
    Insert one clip row.

    If a duplicate s3_key exists, return the existing row instead of failing.
    """

    row = {
        "title": title,
        "slug": slug,
        "s3_key": s3_key,
        "preview_s3_key": preview_s3_key,
        "recorded_at": recorded_at,
        "club_id": settings.club_id,
        "court_id": settings.court_id,
        "published": True,
    }

    logger.info(
        "Supabase insert",
        extra={
            "structured": {
                "table": settings.supabase_clips_table,
                "slug": slug,
                "s3_key": s3_key,
                "recorded_at": recorded_at,
            }
        },
    )

    try:
        response = (
            client.table(settings.supabase_clips_table)
            .insert(row)
            .execute()
        )

        logger.info(
            "Supabase insert completed",
            extra={
                "structured": {
                    "table": settings.supabase_clips_table,
                    "slug": slug,
                    "recorded_at": recorded_at,
                }
            },
        )

        data = getattr(response, "data", None)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]

        return row

    except APIError as e:
        error_message = str(e)

        # 👇 HANDLE DUPLICATE s3_key CLEANLY
        if "duplicate key value violates unique constraint" in error_message:
            logger.warning(
                "Duplicate clip detected, fetching existing record",
                extra={
                    "structured": {
                        "s3_key": s3_key,
                    }
                },
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

            # fallback if somehow not found
            return row

        # 🚨 REAL ERROR → re-raise
        raise


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