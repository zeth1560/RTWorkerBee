"""
Inspect and lightly repair the worker SQLite job database.

Examples::

    python jobs_cli.py --env .env list
    python jobs_cli.py show <idempotency_key_or_job_uuid>
    python jobs_cli.py retry-booking <idempotency_key>
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

from config import ConfigError, load_settings
from job_store import STEP_BOOKING, JobStore


def _open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_list(conn: sqlite3.Connection, limit: int) -> None:
    cur = conn.execute(
        "SELECT * FROM clip_jobs ORDER BY updated_at DESC LIMIT ?",
        (max(1, min(int(limit), 10_000)),),
    )
    rows = cur.fetchall()
    for r in rows:
        print(json.dumps({k: r[k] for k in r.keys()}, default=str))


def cmd_show(conn: sqlite3.Connection, key: str) -> None:
    cur = conn.execute("SELECT * FROM clip_jobs WHERE idempotency_key = ?", (key,))
    row = cur.fetchone()
    if row is None:
        cur = conn.execute("SELECT * FROM clip_jobs WHERE job_uuid = ?", (key,))
        row = cur.fetchone()
    if row is None:
        print("Not found", file=sys.stderr)
        sys.exit(1)
    print(json.dumps({k: row[k] for k in row.keys()}, indent=2, default=str))


def cmd_retry_booking(store: JobStore, idem: str) -> None:
    job = store.get(idem)
    if job is None:
        print("Not found", file=sys.stderr)
        sys.exit(1)
    new_flags = job.step_flags & ~STEP_BOOKING
    store.update_job(
        idem,
        step_flags=new_flags,
        booking_next_attempt_at=time.time(),
        status="processing",
    )
    print("Updated: cleared STEP_BOOKING and scheduled immediate booking retry")


def main() -> int:
    parser = argparse.ArgumentParser(description="ReplayTrove job DB tools")
    parser.add_argument("--env", type=Path, default=None, help="Path to .env")
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite path (defaults to WORKER_JOB_DB from settings)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="Print recent jobs as JSON lines")
    p_list.add_argument("--limit", type=int, default=50)

    p_show = sub.add_parser("show", help="Print one job as JSON")
    p_show.add_argument("key", help="idempotency_key or job_uuid")

    p_rb = sub.add_parser(
        "retry-booking",
        help="Clear booking step flag and enqueue next match (requeue file separately)",
    )
    p_rb.add_argument("idempotency_key")

    args = parser.parse_args()

    try:
        settings = load_settings(env_file=args.env)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2

    db_path = args.db or settings.job_db_path

    if args.command == "retry-booking":
        store = JobStore(db_path)
        store.init_schema()
        cmd_retry_booking(store, args.idempotency_key)
        return 0

    conn = _open_db(db_path)
    if args.command == "list":
        cmd_list(conn, args.limit)
    elif args.command == "show":
        cmd_show(conn, args.key)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
