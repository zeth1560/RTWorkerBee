"""
SQLite-backed job tracking for restart-safe clip processing.

Idempotency key: SHA-256 hex of ``basename|file_size`` at claim time.
"""

from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class JobIdempotencyCollisionError(Exception):
    """SQLite rejected a new job row (same idempotency key as an existing job)."""


def make_idempotency_key(basename: str, size_bytes: int) -> str:
    raw = f"{basename}|{size_bytes}".encode("utf-8", errors="replace")
    return hashlib.sha256(raw).hexdigest()


# Step bitmask (order matches pipeline)
STEP_RENAMED_UTC = 1 << 0
STEP_PREVIEW = 1 << 1
STEP_UPLOAD_ORIGINAL = 1 << 2
STEP_UPLOAD_PREVIEW = 1 << 3
STEP_DB_UPSERT = 1 << 4
STEP_BOOKING = 1 << 5
STEP_FINALIZED = 1 << 6


@dataclass
class ClipJob:
    idempotency_key: str
    status: str
    incoming_basename: str
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
    created_at: float
    updated_at: float


_SCHEMA = """
CREATE TABLE IF NOT EXISTS clip_jobs (
    idempotency_key TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    incoming_basename TEXT NOT NULL,
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
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clip_jobs_processing_path ON clip_jobs(processing_path);
CREATE INDEX IF NOT EXISTS idx_clip_jobs_status ON clip_jobs(status);
"""


class JobStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()

    def init_schema(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _row_to_job(self, row: sqlite3.Row) -> ClipJob:
        return ClipJob(
            idempotency_key=row["idempotency_key"],
            status=row["status"],
            incoming_basename=row["incoming_basename"],
            processing_path=row["processing_path"],
            file_size=row["file_size"],
            step_flags=row["step_flags"],
            utc_filename=row["utc_filename"],
            preview_relpath=row["preview_relpath"],
            s3_original_key=row["s3_original_key"],
            s3_preview_key=row["s3_preview_key"],
            slug=row["slug"],
            clip_id=row["clip_id"],
            recorded_at=row["recorded_at"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
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

    def get_by_processing_path(self, resolved_path: str | Path) -> ClipJob | None:
        key = str(Path(resolved_path).resolve(strict=False)).lower()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("SELECT * FROM clip_jobs")
                for row in cur.fetchall():
                    p = str(Path(row["processing_path"]).resolve(strict=False)).lower()
                    if p == key:
                        return self._row_to_job(row)
        return None

    def insert_after_claim(
        self,
        *,
        idempotency_key: str,
        incoming_basename: str,
        processing_path: Path,
        file_size: int,
    ) -> ClipJob:
        now = time.time()
        proc_str = str(processing_path.resolve(strict=False))
        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO clip_jobs (
                            idempotency_key, status, incoming_basename, processing_path,
                            file_size, step_flags, created_at, updated_at
                        ) VALUES (?, 'processing', ?, ?, ?, 0, ?, ?)
                        """,
                        (idempotency_key, incoming_basename, proc_str, file_size, now, now),
                    )
                    conn.commit()
                except sqlite3.IntegrityError as exc:
                    conn.rollback()
                    raise JobIdempotencyCollisionError(str(exc)) from exc
        job = self.get(idempotency_key)
        assert job is not None
        return job

    def ensure_job_for_processing_file(self, path: Path) -> ClipJob:
        """
        Load or create a job row for a file sitting under ``processing`` (restart recovery).
        """
        path = path.resolve(strict=False)
        job = self.get_by_processing_path(path)
        if job is not None:
            return job
        st = path.stat()
        key = make_idempotency_key(path.name, st.st_size)
        prior = self.get(key)
        if prior is not None:
            if prior.status == "completed":
                return prior
            self.update_job(prior.idempotency_key, processing_path=str(path))
            j = self.get(key)
            assert j is not None
            return j
        try:
            return self.insert_after_claim(
                idempotency_key=key,
                incoming_basename=path.name,
                processing_path=path,
                file_size=st.st_size,
            )
        except JobIdempotencyCollisionError:
            j = self.get(key)
            if j is None:
                raise
            if j.status == "completed":
                return j
            self.update_job(j.idempotency_key, processing_path=str(path))
            out = self.get(key)
            assert out is not None
            return out

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
        merge_steps: bool = False,
        processing_path: str | None = None,
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
                        step_flags = row["step_flags"] | step_flags
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
                if last_error is not None:
                    fields.append("last_error = ?")
                    values.append(last_error)
                if processing_path is not None:
                    fields.append("processing_path = ?")
                    values.append(processing_path)
                values.append(idempotency_key)
                sql = f"UPDATE clip_jobs SET {', '.join(fields)} WHERE idempotency_key = ?"
                conn.execute(sql, values)
                conn.commit()
