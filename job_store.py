"""
SQLite-backed job tracking for restart-safe clip processing.

Immutable clip identity: ``idempotency_key`` is a content fingerprint (see :mod:`clip_fingerprint`),
fixed at first claim; ``job_uuid`` is a separate stable correlation ID for logs and Supabase.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from clip_fingerprint import compute_clip_idempotency_key
from paths import normalize_storage_path


class JobIdempotencyCollisionError(Exception):
    """SQLite rejected a new job row (same idempotency key as an existing job)."""


# Step bitmask (order matches pipeline)
STEP_RENAMED_UTC = 1 << 0
STEP_PREVIEW = 1 << 1
STEP_UPLOAD_ORIGINAL = 1 << 2
STEP_UPLOAD_PREVIEW = 1 << 3
STEP_DB_UPSERT = 1 << 4
STEP_BOOKING = 1 << 5
STEP_FINALIZED = 1 << 6
STEP_DETECTED = 1 << 7
STEP_MOVED_TO_PROCESSING = 1 << 8

REMOTE_STEP_UPLOAD_ORIGINAL = "upload_original"
REMOTE_STEP_UPLOAD_PREVIEW = "upload_preview"
REMOTE_STEP_DB_UPSERT = "db_upsert"
REMOTE_STEP_BOOKING = "booking_match"


@dataclass
class ClipJob:
    idempotency_key: str
    job_uuid: str
    status: str
    incoming_basename: str
    incoming_path: str | None
    processing_path: str
    file_size: int
    step_flags: int
    utc_filename: str | None
    preview_relpath: str | None
    s3_original_key: str | None
    s3_preview_key: str | None
    slug: str | None
    clip_id: str | None
    recorded_at: str | None
    last_error: str | None
    failure_category: str | None
    failure_reason_code: str | None
    current_stage: str | None
    retry_preview: int
    retry_upload_original: int
    retry_upload_preview: int
    retry_db_upsert: int
    retry_booking: int
    last_step_started_at: float | None
    last_step_completed_at: float | None
    original_s3_bucket: str | None
    original_s3_etag: str | None
    preview_s3_bucket: str | None
    preview_s3_etag: str | None
    original_uploaded_at: float | None
    preview_uploaded_at: float | None
    booking_match_attempts: int
    booking_matched_at: float | None
    booking_next_attempt_at: float | None
    created_at: float
    updated_at: float


_SCHEMA = """
CREATE TABLE IF NOT EXISTS clip_jobs (
    idempotency_key TEXT PRIMARY KEY,
    job_uuid TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    incoming_basename TEXT NOT NULL,
    incoming_path TEXT,
    processing_path TEXT NOT NULL UNIQUE,
    file_size INTEGER NOT NULL,
    step_flags INTEGER NOT NULL DEFAULT 0,
    utc_filename TEXT,
    preview_relpath TEXT,
    s3_original_key TEXT,
    s3_preview_key TEXT,
    slug TEXT,
    clip_id TEXT,
    recorded_at TEXT,
    last_error TEXT,
    failure_category TEXT,
    failure_reason_code TEXT,
    current_stage TEXT,
    retry_preview INTEGER NOT NULL DEFAULT 0,
    retry_upload_original INTEGER NOT NULL DEFAULT 0,
    retry_upload_preview INTEGER NOT NULL DEFAULT 0,
    retry_db_upsert INTEGER NOT NULL DEFAULT 0,
    retry_booking INTEGER NOT NULL DEFAULT 0,
    last_step_started_at REAL,
    last_step_completed_at REAL,
    original_s3_bucket TEXT,
    original_s3_etag TEXT,
    preview_s3_bucket TEXT,
    preview_s3_etag TEXT,
    original_uploaded_at REAL,
    preview_uploaded_at REAL,
    booking_match_attempts INTEGER NOT NULL DEFAULT 0,
    booking_matched_at REAL,
    booking_next_attempt_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clip_jobs_processing_path ON clip_jobs(processing_path);
CREATE INDEX IF NOT EXISTS idx_clip_jobs_status ON clip_jobs(status);
"""

_REMOTE_SYNC_SCHEMA = """
CREATE TABLE IF NOT EXISTS remote_sync_queue (
    job_uuid TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    processing_path TEXT NOT NULL,
    failed_step TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_time REAL NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_remote_sync_next ON remote_sync_queue(next_retry_time);
"""


def _migrate_remote_sync_queue(conn: sqlite3.Connection) -> None:
    conn.executescript(_REMOTE_SYNC_SCHEMA)
    try:
        rows = conn.execute("PRAGMA table_info(remote_sync_queue)").fetchall()
    except sqlite3.OperationalError:
        return
    existing = {str(r[1]) for r in rows}
    if "drain_attempts" not in existing:
        conn.execute(
            "ALTER TABLE remote_sync_queue ADD COLUMN drain_attempts INTEGER NOT NULL DEFAULT 0"
        )


_MIGRATION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("job_uuid", "TEXT"),
    ("incoming_path", "TEXT"),
    ("failure_category", "TEXT"),
    ("failure_reason_code", "TEXT"),
    ("current_stage", "TEXT"),
    ("retry_preview", "INTEGER NOT NULL DEFAULT 0"),
    ("retry_upload_original", "INTEGER NOT NULL DEFAULT 0"),
    ("retry_upload_preview", "INTEGER NOT NULL DEFAULT 0"),
    ("retry_db_upsert", "INTEGER NOT NULL DEFAULT 0"),
    ("retry_booking", "INTEGER NOT NULL DEFAULT 0"),
    ("last_step_started_at", "REAL"),
    ("last_step_completed_at", "REAL"),
    ("original_s3_bucket", "TEXT"),
    ("original_s3_etag", "TEXT"),
    ("preview_s3_bucket", "TEXT"),
    ("preview_s3_etag", "TEXT"),
    ("original_uploaded_at", "REAL"),
    ("preview_uploaded_at", "REAL"),
    ("booking_match_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("booking_matched_at", "REAL"),
    ("booking_next_attempt_at", "REAL"),
)


def _existing_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(clip_jobs)").fetchall()
    return {str(r[1]) for r in rows}


def _migrate_clip_jobs(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    existing = _existing_columns(conn)
    for col, decl in _MIGRATION_COLUMNS:
        if col not in existing:
            conn.execute(f"ALTER TABLE clip_jobs ADD COLUMN {col} {decl}")
            existing.add(col)

    rows = conn.execute(
        "SELECT idempotency_key FROM clip_jobs WHERE job_uuid IS NULL OR job_uuid = ''"
    ).fetchall()
    for (idem_key,) in rows:
        conn.execute(
            "UPDATE clip_jobs SET job_uuid = ? WHERE idempotency_key = ?",
            (str(uuid.uuid4()), idem_key),
        )

    for (idem_key, proc) in conn.execute(
        "SELECT idempotency_key, processing_path FROM clip_jobs"
    ).fetchall():
        np = normalize_storage_path(proc)
        if np != proc:
            try:
                conn.execute(
                    "UPDATE clip_jobs SET processing_path = ? WHERE idempotency_key = ?",
                    (np, idem_key),
                )
            except sqlite3.IntegrityError:
                pass

    if "job_uuid" in _existing_columns(conn):
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_clip_jobs_job_uuid ON clip_jobs(job_uuid)"
        )

    _migrate_remote_sync_queue(conn)
    conn.execute(
        f"""
        UPDATE clip_jobs
        SET step_flags = step_flags | {STEP_DETECTED | STEP_MOVED_TO_PROCESSING}
        WHERE status IN ('processing', 'pending_remote_sync', 'completed', 'failed', 'stale')
          AND (step_flags & {STEP_MOVED_TO_PROCESSING}) = 0
        """
    )


@dataclass
class RemoteSyncEntry:
    job_uuid: str
    idempotency_key: str
    processing_path: str
    failed_step: str
    retry_count: int
    drain_attempts: int
    last_error: str | None
    next_retry_time: float
    created_at: float
    updated_at: float


class JobStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()

    def init_schema(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._connect() as conn:
                _migrate_clip_jobs(conn)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.commit()

    def _normalize_step_flags(self, flags: int, status: str) -> int:
        sf = int(flags)
        if status in (
            "processing",
            "pending_remote_sync",
            "completed",
            "failed",
            "stale",
        ):
            if not (sf & STEP_MOVED_TO_PROCESSING):
                sf |= STEP_DETECTED | STEP_MOVED_TO_PROCESSING
        return sf

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _row_to_job(self, row: sqlite3.Row) -> ClipJob:
        flags = self._normalize_step_flags(int(row["step_flags"]), str(row["status"]))
        return ClipJob(
            idempotency_key=row["idempotency_key"],
            job_uuid=row["job_uuid"] or str(uuid.uuid4()),
            status=row["status"],
            incoming_basename=row["incoming_basename"],
            incoming_path=row["incoming_path"],
            processing_path=row["processing_path"],
            file_size=int(row["file_size"]),
            step_flags=flags,
            utc_filename=row["utc_filename"],
            preview_relpath=row["preview_relpath"],
            s3_original_key=row["s3_original_key"],
            s3_preview_key=row["s3_preview_key"],
            slug=row["slug"],
            clip_id=row["clip_id"],
            recorded_at=row["recorded_at"],
            last_error=row["last_error"],
            failure_category=row["failure_category"],
            failure_reason_code=row["failure_reason_code"],
            current_stage=row["current_stage"],
            retry_preview=int(row["retry_preview"] or 0),
            retry_upload_original=int(row["retry_upload_original"] or 0),
            retry_upload_preview=int(row["retry_upload_preview"] or 0),
            retry_db_upsert=int(row["retry_db_upsert"] or 0),
            retry_booking=int(row["retry_booking"] or 0),
            last_step_started_at=row["last_step_started_at"],
            last_step_completed_at=row["last_step_completed_at"],
            original_s3_bucket=row["original_s3_bucket"],
            original_s3_etag=row["original_s3_etag"],
            preview_s3_bucket=row["preview_s3_bucket"],
            preview_s3_etag=row["preview_s3_etag"],
            original_uploaded_at=row["original_uploaded_at"],
            preview_uploaded_at=row["preview_uploaded_at"],
            booking_match_attempts=int(row["booking_match_attempts"] or 0),
            booking_matched_at=row["booking_matched_at"],
            booking_next_attempt_at=row["booking_next_attempt_at"],
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def get(self, idempotency_key: str) -> ClipJob | None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT * FROM clip_jobs WHERE idempotency_key = ?",
                    (idempotency_key,),
                )
                row = cur.fetchone()
                return self._row_to_job(row) if row else None

    def get_by_job_uuid(self, job_uuid: str) -> ClipJob | None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT * FROM clip_jobs WHERE job_uuid = ?",
                    (job_uuid,),
                )
                row = cur.fetchone()
                return self._row_to_job(row) if row else None

    def get_by_processing_path(self, path: Path | str) -> ClipJob | None:
        key = normalize_storage_path(path)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT * FROM clip_jobs WHERE processing_path = ?",
                    (key,),
                )
                row = cur.fetchone()
                return self._row_to_job(row) if row else None

    def insert_after_claim(
        self,
        *,
        idempotency_key: str,
        incoming_basename: str,
        incoming_path: str | None,
        processing_path: Path,
        file_size: int,
        job_uuid: str | None = None,
    ) -> ClipJob:
        now = time.time()
        proc_str = normalize_storage_path(processing_path)
        ju = job_uuid or str(uuid.uuid4())
        with self._lock:
            with self._connect() as conn:
                try:
                    initial_flags = STEP_DETECTED | STEP_MOVED_TO_PROCESSING
                    conn.execute(
                        """
                        INSERT INTO clip_jobs (
                            idempotency_key, job_uuid, status, incoming_basename, incoming_path,
                            processing_path, file_size, step_flags, created_at, updated_at
                        ) VALUES (?, ?, 'processing', ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            idempotency_key,
                            ju,
                            incoming_basename,
                            incoming_path,
                            proc_str,
                            file_size,
                            initial_flags,
                            now,
                            now,
                        ),
                    )
                    conn.commit()
                except sqlite3.IntegrityError as exc:
                    conn.rollback()
                    raise JobIdempotencyCollisionError(str(exc)) from exc
        job = self.get(idempotency_key)
        assert job is not None
        return job

    def ensure_job_for_processing_file(
        self,
        path: Path,
        *,
        chunk_bytes: int,
        include_mtime: bool,
        full_hash_max_bytes: int,
    ) -> tuple[ClipJob, str]:
        """
        Return ``(job, reason)`` where reason is ``resumed`` | ``recovery_inserted`` | ``rebound``.
        """
        path = path.resolve(strict=False)
        normalized = normalize_storage_path(path)
        job = self.get_by_processing_path(normalized)
        if job is not None:
            return job, "resumed"

        st = path.stat()
        key = compute_clip_idempotency_key(
            path,
            chunk_bytes=chunk_bytes,
            include_mtime=include_mtime,
            full_hash_max_bytes=full_hash_max_bytes,
        )
        prior = self.get(key)
        if prior is not None:
            if prior.status == "completed":
                return prior, "resumed"
            self.update_job(prior.idempotency_key, processing_path=normalized)
            j = self.get(key)
            assert j is not None
            return j, "rebound"
        try:
            j = self.insert_after_claim(
                idempotency_key=key,
                incoming_basename=path.name,
                incoming_path=None,
                processing_path=path,
                file_size=st.st_size,
            )
            return j, "recovery_inserted"
        except JobIdempotencyCollisionError:
            j = self.get(key)
            if j is None:
                raise
            if j.status == "completed":
                return j, "resumed"
            self.update_job(j.idempotency_key, processing_path=normalized)
            out = self.get(key)
            assert out is not None
            return out, "rebound"

    def iter_booking_retry_paths(self, before_mono: float) -> list[str]:
        """Paths ready for another booking match attempt (wall clock in ``before_mono`` — use time.time())."""
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT processing_path FROM clip_jobs
                    WHERE status IN ('processing', 'pending_remote_sync')
                      AND (step_flags & ?) = ?
                      AND (step_flags & ?) = 0
                      AND clip_id IS NOT NULL
                      AND booking_next_attempt_at IS NOT NULL
                      AND booking_next_attempt_at <= ?
                    """,
                    (
                        STEP_DB_UPSERT,
                        STEP_DB_UPSERT,
                        STEP_BOOKING,
                        before_mono,
                    ),
                )
                return [str(r[0]) for r in cur.fetchall() if r[0]]

    def count_rows_by_status(self) -> dict[str, int]:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT status, COUNT(*) FROM clip_jobs GROUP BY status"
                )
                return {str(s): int(c) for s, c in cur.fetchall()}

    def count_stale_processing(self, idle_seconds: float, now: float | None = None) -> int:
        if idle_seconds <= 0:
            return 0
        now = now if now is not None else time.time()
        cutoff = now - idle_seconds
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT COUNT(*) FROM clip_jobs
                    WHERE status = 'processing'
                      AND updated_at < ?
                      AND (step_flags & ?) = 0
                    """,
                    (cutoff, STEP_FINALIZED),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0

    def list_stale_processing_jobs(
        self, idle_seconds: float, now: float | None = None
    ) -> list[ClipJob]:
        if idle_seconds <= 0:
            return []
        now = now if now is not None else time.time()
        cutoff = now - idle_seconds
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT * FROM clip_jobs
                    WHERE status = 'processing'
                      AND updated_at < ?
                      AND (step_flags & ?) = 0
                    """,
                    (cutoff, STEP_FINALIZED),
                )
                return [self._row_to_job(r) for r in cur.fetchall()]

    def update_job(
        self,
        idempotency_key: str,
        *,
        step_flags: int | None = None,
        utc_filename: str | None = None,
        preview_relpath: str | None = None,
        s3_original_key: str | None = None,
        s3_preview_key: str | None = None,
        slug: str | None = None,
        clip_id: str | None = None,
        recorded_at: str | None = None,
        status: str | None = None,
        last_error: str | None = None,
        failure_category: str | None = None,
        failure_reason_code: str | None = None,
        current_stage: str | None = None,
        merge_steps: bool = False,
        processing_path: str | None = None,
        incoming_path: str | None = None,
        retry_preview: int | None = None,
        retry_upload_original: int | None = None,
        retry_upload_preview: int | None = None,
        retry_db_upsert: int | None = None,
        retry_booking: int | None = None,
        last_step_started_at: float | None = None,
        last_step_completed_at: float | None = None,
        original_s3_bucket: str | None = None,
        original_s3_etag: str | None = None,
        preview_s3_bucket: str | None = None,
        preview_s3_etag: str | None = None,
        original_uploaded_at: float | None = None,
        preview_uploaded_at: float | None = None,
        booking_match_attempts: int | None = None,
        booking_matched_at: float | None = None,
        booking_next_attempt_at: float | None = None,
        clear_booking_next_attempt_at: bool = False,
        clear_last_error: bool = False,
        clear_failure_metadata: bool = False,
    ) -> None:
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                if merge_steps and step_flags is not None:
                    cur = conn.execute(
                        "SELECT step_flags FROM clip_jobs WHERE idempotency_key = ?",
                        (idempotency_key,),
                    )
                    row = cur.fetchone()
                    if row:
                        step_flags = int(row["step_flags"]) | step_flags
                fields: list[str] = ["updated_at = ?"]
                values: list[Any] = [now]
                if step_flags is not None:
                    fields.append("step_flags = ?")
                    values.append(step_flags)
                if utc_filename is not None:
                    fields.append("utc_filename = ?")
                    values.append(utc_filename)
                if preview_relpath is not None:
                    fields.append("preview_relpath = ?")
                    values.append(preview_relpath)
                if s3_original_key is not None:
                    fields.append("s3_original_key = ?")
                    values.append(s3_original_key)
                if s3_preview_key is not None:
                    fields.append("s3_preview_key = ?")
                    values.append(s3_preview_key)
                if slug is not None:
                    fields.append("slug = ?")
                    values.append(slug)
                if clip_id is not None:
                    fields.append("clip_id = ?")
                    values.append(clip_id)
                if recorded_at is not None:
                    fields.append("recorded_at = ?")
                    values.append(recorded_at)
                if status is not None:
                    fields.append("status = ?")
                    values.append(status)
                if clear_failure_metadata:
                    fields.append("failure_reason_code = NULL")
                    fields.append("failure_category = NULL")
                if clear_last_error:
                    fields.append("last_error = NULL")
                elif last_error is not None:
                    fields.append("last_error = ?")
                    values.append(last_error)
                if failure_category is not None:
                    fields.append("failure_category = ?")
                    values.append(failure_category)
                if failure_reason_code is not None:
                    fields.append("failure_reason_code = ?")
                    values.append(failure_reason_code)
                if current_stage is not None:
                    fields.append("current_stage = ?")
                    values.append(current_stage)
                if processing_path is not None:
                    fields.append("processing_path = ?")
                    values.append(normalize_storage_path(processing_path))
                if incoming_path is not None:
                    fields.append("incoming_path = ?")
                    values.append(incoming_path)
                if retry_preview is not None:
                    fields.append("retry_preview = ?")
                    values.append(retry_preview)
                if retry_upload_original is not None:
                    fields.append("retry_upload_original = ?")
                    values.append(retry_upload_original)
                if retry_upload_preview is not None:
                    fields.append("retry_upload_preview = ?")
                    values.append(retry_upload_preview)
                if retry_db_upsert is not None:
                    fields.append("retry_db_upsert = ?")
                    values.append(retry_db_upsert)
                if retry_booking is not None:
                    fields.append("retry_booking = ?")
                    values.append(retry_booking)
                if last_step_started_at is not None:
                    fields.append("last_step_started_at = ?")
                    values.append(last_step_started_at)
                if last_step_completed_at is not None:
                    fields.append("last_step_completed_at = ?")
                    values.append(last_step_completed_at)
                if original_s3_bucket is not None:
                    fields.append("original_s3_bucket = ?")
                    values.append(original_s3_bucket)
                if original_s3_etag is not None:
                    fields.append("original_s3_etag = ?")
                    values.append(original_s3_etag)
                if preview_s3_bucket is not None:
                    fields.append("preview_s3_bucket = ?")
                    values.append(preview_s3_bucket)
                if preview_s3_etag is not None:
                    fields.append("preview_s3_etag = ?")
                    values.append(preview_s3_etag)
                if original_uploaded_at is not None:
                    fields.append("original_uploaded_at = ?")
                    values.append(original_uploaded_at)
                if preview_uploaded_at is not None:
                    fields.append("preview_uploaded_at = ?")
                    values.append(preview_uploaded_at)
                if booking_match_attempts is not None:
                    fields.append("booking_match_attempts = ?")
                    values.append(booking_match_attempts)
                if booking_matched_at is not None:
                    fields.append("booking_matched_at = ?")
                    values.append(booking_matched_at)
                if clear_booking_next_attempt_at:
                    fields.append("booking_next_attempt_at = NULL")
                elif booking_next_attempt_at is not None:
                    fields.append("booking_next_attempt_at = ?")
                    values.append(booking_next_attempt_at)
                values.append(idempotency_key)
                sql = f"UPDATE clip_jobs SET {', '.join(fields)} WHERE idempotency_key = ?"
                conn.execute(sql, values)
                conn.commit()

    def upsert_remote_sync_pending(
        self,
        *,
        job_uuid: str,
        idempotency_key: str,
        processing_path: Path | str,
        failed_step: str,
        last_error: str | None,
        next_retry_time: float,
    ) -> None:
        now = time.time()
        proc = normalize_storage_path(processing_path)
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT retry_count FROM remote_sync_queue WHERE job_uuid = ?",
                    (job_uuid,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO remote_sync_queue (
                            job_uuid, idempotency_key, processing_path, failed_step,
                            retry_count, drain_attempts, last_error, next_retry_time, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?)
                        """,
                        (
                            job_uuid,
                            idempotency_key,
                            proc,
                            failed_step,
                            last_error,
                            next_retry_time,
                            now,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE remote_sync_queue SET
                            idempotency_key = ?,
                            processing_path = ?,
                            failed_step = ?,
                            retry_count = retry_count + 1,
                            last_error = ?,
                            next_retry_time = ?,
                            updated_at = ?
                        WHERE job_uuid = ?
                        """,
                        (
                            idempotency_key,
                            proc,
                            failed_step,
                            last_error,
                            next_retry_time,
                            now,
                            job_uuid,
                        ),
                    )
                conn.commit()

    def _row_to_remote(self, row: sqlite3.Row) -> RemoteSyncEntry:
        try:
            drain = int(row["drain_attempts"] or 0)
        except (KeyError, IndexError, TypeError, ValueError):
            drain = 0
        return RemoteSyncEntry(
            job_uuid=str(row["job_uuid"]),
            idempotency_key=str(row["idempotency_key"]),
            processing_path=str(row["processing_path"]),
            failed_step=str(row["failed_step"]),
            retry_count=int(row["retry_count"] or 0),
            drain_attempts=drain,
            last_error=row["last_error"],
            next_retry_time=float(row["next_retry_time"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def _abort_remote_sync_locked(
        self,
        conn: sqlite3.Connection,
        *,
        job_uuid: str,
        idempotency_key: str,
        reason_code: str,
        human_message: str,
        now: float,
    ) -> None:
        conn.execute(
            "DELETE FROM remote_sync_queue WHERE job_uuid = ?",
            (job_uuid,),
        )
        conn.execute(
            """
            UPDATE clip_jobs SET
                status = 'failed',
                failure_category = 'terminal',
                failure_reason_code = ?,
                last_error = ?,
                current_stage = NULL,
                updated_at = ?
            WHERE idempotency_key = ?
            """,
            (reason_code, human_message[:1000], now, idempotency_key),
        )

    def try_begin_remote_sync_drain(
        self,
        job_uuid: str,
        *,
        now: float,
        max_total_attempts: int,
        max_age_seconds: float,
    ) -> tuple[bool, str | None, str | None]:
        """
        Enforce remote-sync zombie limits and reserve one drain attempt.

        Returns ``(proceed, abort_reason, idempotency_key)`` where ``abort_reason`` is
        ``max_attempts``, ``max_age``, or ``None`` when ``proceed`` is True.
        When limits are exceeded, the queue row is removed and the clip job is marked failed.
        """
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM remote_sync_queue WHERE job_uuid = ?",
                    (job_uuid,),
                ).fetchone()
                if row is None:
                    return False, "missing", None
                idem = str(row["idempotency_key"])
                created_at = float(row["created_at"])
                drain_attempts = int(row["drain_attempts"] or 0)

                if max_age_seconds > 0 and (now - created_at) > max_age_seconds:
                    self._abort_remote_sync_locked(
                        conn,
                        job_uuid=job_uuid,
                        idempotency_key=idem,
                        reason_code="remote_sync_max_age",
                        human_message=(
                            f"Remote sync abandoned: queued longer than "
                            f"{max_age_seconds:.0f}s (first queued at {created_at})"
                        ),
                        now=now,
                    )
                    conn.commit()
                    return False, "max_age", idem

                if max_total_attempts > 0 and drain_attempts >= max_total_attempts:
                    self._abort_remote_sync_locked(
                        conn,
                        job_uuid=job_uuid,
                        idempotency_key=idem,
                        reason_code="remote_sync_max_attempts",
                        human_message=(
                            f"Remote sync abandoned after {drain_attempts} drain attempt(s) "
                            f"(limit {max_total_attempts})"
                        ),
                        now=now,
                    )
                    conn.commit()
                    return False, "max_attempts", idem

                conn.execute(
                    """
                    UPDATE remote_sync_queue
                    SET drain_attempts = drain_attempts + 1, updated_at = ?
                    WHERE job_uuid = ?
                    """,
                    (now, job_uuid),
                )
                conn.commit()
                return True, None, idem

    def list_due_remote_sync(
        self, before_mono: float, *, limit: int = 50
    ) -> list[RemoteSyncEntry]:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT * FROM remote_sync_queue
                    WHERE next_retry_time <= ?
                    ORDER BY next_retry_time ASC
                    LIMIT ?
                    """,
                    (before_mono, max(1, limit)),
                )
                return [self._row_to_remote(r) for r in cur.fetchall()]

    def delete_remote_sync(self, job_uuid: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM remote_sync_queue WHERE job_uuid = ?",
                    (job_uuid,),
                )
                conn.commit()

    def count_remote_sync_pending(self) -> int:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM remote_sync_queue"
                ).fetchone()
                return int(row[0]) if row else 0

    def bump_remote_sync_retry(
        self,
        job_uuid: str,
        *,
        last_error: str | None,
        next_retry_time: float,
    ) -> None:
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE remote_sync_queue
                    SET retry_count = retry_count + 1,
                        last_error = ?,
                        next_retry_time = ?,
                        updated_at = ?
                    WHERE job_uuid = ?
                    """,
                    (last_error, next_retry_time, now, job_uuid),
                )
                conn.commit()
