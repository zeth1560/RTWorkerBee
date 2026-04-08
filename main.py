from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path

from config import ConfigError, Settings, load_settings
from database import create_supabase_client
from logger import setup_logging
from processor import process_clip
from uploader import S3Uploader
from watcher import ClipFileHandler, ClipJobQueue, scan_existing_clips, start_observer

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ReplayTrove clip worker")
    parser.add_argument(
        "--env",
        type=Path,
        default=None,
        help="Path to .env file (optional)",
    )
    return parser.parse_args(argv)


def _ensure_directories(settings: Settings) -> None:
    for folder in (
        settings.clips_folder,
        settings.preview_folder,
        settings.processed_folder,
        settings.failed_folder,
        settings.log_folder,
    ):
        folder.mkdir(parents=True, exist_ok=True)


def _warm_supabase(settings: Settings) -> None:
    client = create_supabase_client(settings)
    client.table(settings.supabase_clips_table).select("*").limit(1).execute()


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    try:
        settings = load_settings(env_file=args.env)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    _ensure_directories(settings)
    setup_logging(settings.log_folder)

    logger.info(
        "Worker initialized",
        extra={
            "structured": {
                "clips_folder": str(settings.clips_folder),
                "preview_folder": str(settings.preview_folder),
                "processed_folder": str(settings.processed_folder),
                "failed_folder": str(settings.failed_folder),
                "log_folder": str(settings.log_folder),
                "clips_table": settings.supabase_clips_table,
                "club_id": settings.club_id,
                "court_id": settings.court_id,
                "primary_bucket": settings.s3_bucket,
            }
        },
    )

    try:
        _warm_supabase(settings)
    except Exception:
        logger.exception("Supabase connectivity check failed")
        return 3

    supabase = create_supabase_client(settings)

    primary_uploader = S3Uploader(
        bucket=settings.s3_bucket,
        region=settings.aws_region,
        access_key_id=settings.aws_access_key_id,
        secret_access_key=settings.aws_secret_access_key,
        upload_retries=settings.upload_retries,
        upload_retry_delay_seconds=settings.upload_retry_delay_seconds,
        label="primary",
    )

    job_queue = ClipJobQueue()
    stop = threading.Event()

    def worker_loop() -> None:
        while not stop.is_set():
            path = job_queue.get(timeout=0.5)
            if path is None:
                continue

            try:
                process_clip(path, settings, primary_uploader, supabase)
            except Exception:
                logger.exception(
                    "Worker failed processing clip",
                    extra={"structured": {"path": str(path)}},
                )
            finally:
                job_queue.mark_done(path)
                job_queue.task_done()

    worker = threading.Thread(target=worker_loop, name="clip-worker", daemon=True)
    worker.start()

    handler = ClipFileHandler(settings, job_queue.submit)
    observer = start_observer(settings, handler)
    scan_existing_clips(settings, job_queue.submit)

    def handle_signal(signum: int, _frame: object | None) -> None:
        logger.info(
            "Shutdown signal received",
            extra={"structured": {"signal": signum}},
        )
        stop.set()
        observer.stop()

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    logger.info("ReplayTrove worker running; press Ctrl+C to stop")

    try:
        while observer.is_alive():
            time.sleep(0.25)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; shutting down")
        stop.set()
        observer.stop()

    observer.join(timeout=30)
    stop.set()
    logger.info("ReplayTrove worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())