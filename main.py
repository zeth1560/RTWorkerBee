from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from config import ConfigError, Settings, load_settings
from connectivity import ConnectivityMonitor
from database import create_supabase_client
from ingest import (
    instant_replay_ingest_loop,
    instant_replay_trigger_loop,
    long_clips_ingest_loop,
)
from logger import setup_logging
from job_store import JobStore
from lifecycle_events import (
    REMOTE_SYNC_ABORTED,
    REMOTE_SYNC_RESUMED,
    STALE_JOB_DETECTED,
    WORKER_HEALTH_SUMMARY,
    WORKER_STARTUP_SUMMARY,
    log_worker_event,
)
from network_retry import backoff_delay_seconds
from processor import is_copying_temp_clip, is_video_file, process_clip
from replay_buffer_command import (
    ProcessLatestReplayResult,
    replay_scoreboard_auto_sync_loop,
    run_process_latest_replay_cli,
)
from replay_trigger_http import (
    run_replay_trigger_http_loop,
    serve_replay_trigger_http_blocking,
)
from uploader import S3Uploader
from worker_status import WorkerStatusReporter
from watcher import (
    ClipFileHandler,
    ClipJobQueue,
    scan_existing_clips,
    scan_processing_resume,
    start_observer,
)

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ReplayTrove clip worker")
    parser.add_argument(
        "--env",
        type=Path,
        default=None,
        help="Path to .env file (optional)",
    )
    sub = parser.add_subparsers(dest="command", required=False)
    sub.add_parser("run", help="Run the full worker (default when no subcommand is given)")
    rp = sub.add_parser(
        "process-latest-replay",
        aliases=["replay-latest"],
        help=(
            "Replay-buffer only: newest replay_*.mkv under LONG_CLIPS, stabilize, "
            "remux to MP4 for incoming, copy to INSTANTREPLAY.mkv (scoreboard), "
            "verify, delete or fail-move source, then process_clip once on the incoming MP4"
        ),
    )
    rp.add_argument(
        "--trigger",
        required=True,
        help="Trigger time: Unix epoch seconds or ISO-8601 (e.g. 2026-04-13T12:00:00Z)",
    )
    rp.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Max seconds to find a file and finish stabilization (default: 120)",
    )
    rp.add_argument(
        "--prefix",
        default="replay_",
        help="Replay basename prefix (default: replay_)",
    )
    rp.add_argument(
        "--tolerance",
        type=float,
        default=10.0,
        help="Accept files with mtime up to this many seconds before --trigger (default: 10)",
    )
    rth = sub.add_parser(
        "replay-trigger-http",
        help=(
            "Local-only HTTP server (loopback): trigger replay-buffer processing via GET/POST /replay"
        ),
    )
    rth.add_argument(
        "--host",
        default=None,
        help="Bind address (default: REPLAY_TRIGGER_HTTP_HOST or 127.0.0.1)",
    )
    rth.add_argument(
        "--port",
        type=int,
        default=None,
        help="TCP port (default: REPLAY_TRIGGER_HTTP_PORT env; required if unset)",
    )
    return parser.parse_args(argv)


def _replay_pipeline_structured(request_id: str | None, **kwargs: object) -> dict[str, object]:
    out: dict[str, object] = {k: v for k, v in kwargs.items() if v is not None}
    if request_id:
        out["request_id"] = request_id
    return out


def run_process_latest_replay_pipeline(
    settings: Settings,
    *,
    trigger_raw: str,
    timeout_seconds: float,
    filename_prefix: str,
    tolerance_seconds: float,
    request_id: str | None = None,
    connectivity: ConnectivityMonitor | None = None,
    supabase: Any | None = None,
    primary_uploader: S3Uploader | None = None,
    job_store: JobStore | None = None,
    status_reporter: WorkerStatusReporter | None = None,
    long_clip_semaphore: threading.BoundedSemaphore | None = None,
) -> tuple[ProcessLatestReplayResult, int]:
    """
    Full replay-buffer path: scan/stabilize/dual-copy, then ``process_clip`` on the incoming file.

    When ``connectivity`` and related deps are omitted, builds fresh clients (CLI / standalone HTTP).
    When provided (embedded in running worker), reuses them.
    """
    logger.info(
        "replay-processing: pipeline starting",
        extra={
            "structured": _replay_pipeline_structured(
                request_id,
                trigger=trigger_raw[:120],
                timeout_seconds=timeout_seconds,
                prefix=filename_prefix,
                tolerance_seconds=tolerance_seconds,
            )
        },
    )

    result = run_process_latest_replay_cli(
        settings,
        trigger_raw=trigger_raw,
        timeout_seconds=timeout_seconds,
        filename_prefix=filename_prefix,
        tolerance_seconds=tolerance_seconds,
        request_id=request_id,
    )

    if not result.success:
        logger.warning(
            "replay-processing: replay-buffer stage failed",
            extra={
                "structured": _replay_pipeline_structured(
                    request_id,
                    failure_reason=result.failure_reason,
                )
            },
        )
        return result, 1

    use_shared = connectivity is not None
    if not use_shared:
        connectivity = ConnectivityMonitor(
            settings,
            interval_seconds=settings.connectivity_check_interval_seconds,
            probe_timeout_seconds=settings.connectivity_probe_timeout_seconds,
        )
        try:
            _warm_supabase(settings)
        except Exception as exc:
            logger.warning(
                "replay-processing: Supabase warm-up failed (continuing): %s",
                exc,
                extra={
                    "structured": _replay_pipeline_structured(
                        request_id,
                        error_class=type(exc).__name__,
                    )
                },
            )
            connectivity.mark_startup_offline_mode()

    if supabase is None:
        supabase = create_supabase_client(settings)
    if primary_uploader is None:
        primary_uploader = S3Uploader(
            bucket=settings.s3_bucket,
            region=settings.aws_region,
            access_key_id=settings.aws_access_key_id,
            secret_access_key=settings.aws_secret_access_key,
            upload_retries=settings.upload_retries,
            upload_retry_delay_seconds=settings.upload_retry_delay_seconds,
            label="primary",
            multipart_threshold_bytes=settings.s3_multipart_threshold_bytes,
            multipart_chunksize_bytes=settings.s3_multipart_chunksize_bytes,
            network_retry_base_seconds=settings.network_retry_base_seconds,
            network_retry_max_seconds=settings.network_retry_max_seconds,
            network_retry_jitter_fraction=settings.network_retry_jitter_fraction,
        )
    if job_store is None:
        job_store = JobStore(settings.job_db_path)
        job_store.init_schema()
    if status_reporter is None:
        status_reporter = WorkerStatusReporter(settings.worker_status_json_path)

    assert result.incoming_path is not None
    incoming = Path(result.incoming_path)
    try:
        process_clip(
            incoming,
            settings,
            primary_uploader,
            supabase,
            job_store,
            long_clip_semaphore=long_clip_semaphore,
            connectivity=connectivity,
            on_original_upload_complete=status_reporter.record_original_upload_success,
        )
    except Exception as exc:
        logger.exception(
            "replay-processing: process_clip failed",
            extra={
                "structured": _replay_pipeline_structured(
                    request_id,
                    path=str(incoming),
                )
            },
        )
        failed = ProcessLatestReplayResult(
            success=False,
            selected_source_path=result.selected_source_path,
            incoming_path=result.incoming_path,
            detected_at=result.detected_at,
            stability_confirmed=result.stability_confirmed,
            failure_reason="processing_exception",
            scoreboard_replay_path=result.scoreboard_replay_path,
            processing_error=str(exc)[:500],
            source_deleted=result.source_deleted,
        )
        return failed, 1

    logger.info(
        "replay-processing: pipeline completed",
        extra={
            "structured": _replay_pipeline_structured(
                request_id,
                selected_source_path=result.selected_source_path,
                incoming_path=result.incoming_path,
                scoreboard_replay_path=result.scoreboard_replay_path,
                source_deleted=result.source_deleted,
            )
        },
    )
    return result, 0


def _run_process_latest_replay_command(settings: Settings, args: argparse.Namespace) -> int:
    """Minimal worker deps + replay scan/stabilize/copy + single process_clip. Exits process."""
    _ensure_directories(settings)
    setup_logging(settings.log_folder)

    logger.info(
        "process-latest-replay command",
        extra={
            "structured": {
                "trigger": args.trigger,
                "timeout": args.timeout,
                "prefix": args.prefix,
                "tolerance": args.tolerance,
            }
        },
    )

    result, exit_code = run_process_latest_replay_pipeline(
        settings,
        trigger_raw=args.trigger,
        timeout_seconds=args.timeout,
        filename_prefix=args.prefix,
        tolerance_seconds=args.tolerance,
        request_id=None,
    )
    print(result.to_json(), flush=True)
    return exit_code


def _run_replay_trigger_http_command(settings: Settings, args: argparse.Namespace) -> int:
    """Run a blocking local HTTP server that triggers :func:`run_process_latest_replay_pipeline`."""
    _ensure_directories(settings)
    setup_logging(settings.log_folder)

    host = args.host or settings.replay_trigger_http_host
    port = args.port if args.port is not None else settings.replay_trigger_http_port
    if port is None:
        print(
            "replay-trigger-http: set REPLAY_TRIGGER_HTTP_PORT or pass --port",
            file=sys.stderr,
        )
        return 2

    def run_pipeline(
        *,
        trigger_raw: str,
        timeout_seconds: float,
        request_id: str | None,
        filename_prefix: str,
        tolerance_seconds: float,
    ) -> tuple[ProcessLatestReplayResult, int]:
        return run_process_latest_replay_pipeline(
            settings,
            trigger_raw=trigger_raw,
            timeout_seconds=timeout_seconds,
            filename_prefix=filename_prefix,
            tolerance_seconds=tolerance_seconds,
            request_id=request_id,
        )

    logger.info(
        "replay-trigger-http: starting standalone server",
        extra={"structured": {"host": host, "port": port}},
    )
    try:
        serve_replay_trigger_http_blocking(
            host,
            port,
            run_pipeline,
            default_timeout=120.0,
            default_prefix=settings.replay_buffer_filename_prefix,
            default_tolerance=10.0,
        )
    except KeyboardInterrupt:
        logger.info("replay-trigger-http: interrupted")
    return 0


def _ensure_directories(settings: Settings) -> None:
    folders = [
        settings.clips_incoming_folder,
        settings.clips_processing_folder,
        settings.preview_folder,
        settings.processed_folder,
        settings.failed_folder,
        settings.log_folder,
    ]
    if settings.long_clips_folder is not None:
        folders.append(settings.long_clips_folder)
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)

    if settings.instant_replay_trigger_file is not None:
        p = settings.instant_replay_trigger_file
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.touch()
    if settings.long_clips_trigger_file is not None:
        p = settings.long_clips_trigger_file
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.touch()
    if settings.instant_replay_source is not None:
        ir = settings.instant_replay_source.expanduser().resolve(strict=False)
        if ir.is_dir():
            ir.mkdir(parents=True, exist_ok=True)
        else:
            ir.parent.mkdir(parents=True, exist_ok=True)


def _warm_supabase(settings: Settings) -> None:
    client = create_supabase_client(settings)
    client.table(settings.supabase_clips_table).select("*").limit(1).execute()


def _normalize_path(path: Path) -> Path:
    return path.resolve(strict=False)


def _count_clip_videos(folder: Path, settings: Settings) -> int:
    if not folder.is_dir():
        return 0
    n = 0
    for entry in folder.iterdir():
        if not entry.is_file():
            continue
        if is_copying_temp_clip(entry, settings):
            continue
        if is_video_file(entry, settings):
            n += 1
    return n


def main(argv: list[str] | None = None) -> int:
    argv_list = sys.argv[1:] if argv is None else argv
    args = _parse_args(argv_list)

    try:
        settings = load_settings(env_file=args.env)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    command = args.command
    if command is None:
        command = "run"
    if command in ("process-latest-replay", "replay-latest"):
        return _run_process_latest_replay_command(settings, args)
    if command == "replay-trigger-http":
        return _run_replay_trigger_http_command(settings, args)
    if command != "run":
        print(f"Unknown command: {command}", file=sys.stderr)
        return 2

    _ensure_directories(settings)
    setup_logging(settings.log_folder)

    logger.info(
        "Worker initialized",
        extra={
            "structured": {
                "clips_incoming_folder": str(settings.clips_incoming_folder),
                "clips_processing_folder": str(settings.clips_processing_folder),
                "job_db_path": str(settings.job_db_path),
                "preview_folder": str(settings.preview_folder),
                "processed_folder": str(settings.processed_folder),
                "failed_folder": str(settings.failed_folder),
                "log_folder": str(settings.log_folder),
                "clips_table": settings.supabase_clips_table,
                "club_id": settings.club_id,
                "court_id": settings.court_id,
                "primary_bucket": settings.s3_bucket,
                "instant_replay_source": str(settings.instant_replay_source)
                if settings.instant_replay_source
                else None,
                "long_clips_folder": str(settings.long_clips_folder)
                if settings.long_clips_folder
                else None,
                "long_clip_stable_seconds": settings.long_clip_stable_seconds,
                "instant_replay_post_copy_delay_seconds": settings.instant_replay_post_copy_delay_seconds,
                "clip_readiness_stable_rounds": settings.clip_readiness_stable_rounds,
                "clip_readiness_max_cycles": settings.clip_readiness_max_cycles,
                "ffmpeg_decode_max_soft_fails": settings.ffmpeg_decode_max_soft_fails,
                "ffmpeg_decode_retry_delay_seconds": settings.ffmpeg_decode_retry_delay_seconds,
                "recent_completed_suppress_seconds": settings.recent_completed_suppress_seconds,
                "instant_replay_source_min_age_seconds": settings.instant_replay_source_min_age_seconds,
                "instant_replay_source_check_seconds": settings.instant_replay_source_check_seconds,
                "instant_replay_source_retries": settings.instant_replay_source_retries,
                "instant_replay_trigger_file": str(settings.instant_replay_trigger_file)
                if settings.instant_replay_trigger_file
                else None,
                "instant_replay_trigger_settle_seconds": settings.instant_replay_trigger_settle_seconds,
                "long_clips_trigger_file": str(settings.long_clips_trigger_file)
                if settings.long_clips_trigger_file
                else None,
                "long_clips_scan_interval_seconds": settings.long_clips_scan_interval_seconds,
                "replay_trigger_http_host": settings.replay_trigger_http_host,
                "replay_trigger_http_port": settings.replay_trigger_http_port,
                "replay_buffer_filename_prefix": settings.replay_buffer_filename_prefix,
                "replay_scoreboard_auto_sync_interval_seconds": settings.replay_scoreboard_auto_sync_interval_seconds,
                "replay_buffer_stable_check_seconds": settings.replay_buffer_stable_check_seconds,
                "replay_buffer_stable_min_age_seconds": settings.replay_buffer_stable_min_age_seconds,
                "replay_buffer_stable_rounds_required": settings.replay_buffer_stable_rounds_required,
                "replay_buffer_delete_source_after_success": settings.replay_buffer_delete_source_after_success,
                "replay_buffer_remux_max_attempts": settings.replay_buffer_remux_max_attempts,
                "replay_buffer_remux_retry_delay_seconds": settings.replay_buffer_remux_retry_delay_seconds,
            }
        },
    )

    connectivity = ConnectivityMonitor(
        settings,
        interval_seconds=settings.connectivity_check_interval_seconds,
        probe_timeout_seconds=settings.connectivity_probe_timeout_seconds,
    )

    try:
        _warm_supabase(settings)
    except Exception as exc:
        logger.warning(
            "Supabase warm-up failed (worker continues): %s",
            exc,
            extra={"structured": {"error_class": type(exc).__name__}},
        )
        connectivity.mark_startup_offline_mode()

    supabase = create_supabase_client(settings)

    primary_uploader = S3Uploader(
        bucket=settings.s3_bucket,
        region=settings.aws_region,
        access_key_id=settings.aws_access_key_id,
        secret_access_key=settings.aws_secret_access_key,
        upload_retries=settings.upload_retries,
        upload_retry_delay_seconds=settings.upload_retry_delay_seconds,
        label="primary",
        multipart_threshold_bytes=settings.s3_multipart_threshold_bytes,
        multipart_chunksize_bytes=settings.s3_multipart_chunksize_bytes,
        network_retry_base_seconds=settings.network_retry_base_seconds,
        network_retry_max_seconds=settings.network_retry_max_seconds,
        network_retry_jitter_fraction=settings.network_retry_jitter_fraction,
    )

    job_store = JobStore(settings.job_db_path)
    job_store.init_schema()

    status_reporter = WorkerStatusReporter(settings.worker_status_json_path)

    if settings.stale_job_idle_seconds > 0:
        stale_jobs = job_store.list_stale_processing_jobs(settings.stale_job_idle_seconds)
        for sj in stale_jobs:
            log_worker_event(
                logger,
                logging.WARNING,
                STALE_JOB_DETECTED,
                "Stale processing job detected on startup",
                {
                    "job_uuid": sj.job_uuid,
                    "clip_identity": sj.idempotency_key,
                    "processing_path": sj.processing_path,
                    "status": sj.status,
                    "idle_seconds": settings.stale_job_idle_seconds,
                    "policy": settings.stale_job_policy,
                },
            )
            if settings.stale_job_policy == "flag":
                job_store.update_job(
                    sj.idempotency_key,
                    status="stale",
                    last_error="stale_processing_idle",
                    failure_category="retryable",
                    failure_reason_code="stale_idle",
                )

    long_clip_sem: threading.BoundedSemaphore | None = None
    if settings.long_clip_max_concurrent > 0:
        long_clip_sem = threading.BoundedSemaphore(settings.long_clip_max_concurrent)

    job_queue = ClipJobQueue(settings)
    stop = threading.Event()
    replay_trigger_thread: threading.Thread | None = None
    replay_auto_thread: threading.Thread | None = None

    connectivity_thread = threading.Thread(
        target=lambda: connectivity.run_loop(stop),
        name="connectivity-monitor",
        daemon=True,
    )
    connectivity_thread.start()

    if settings.replay_trigger_http_port is not None:
        bind_host = settings.replay_trigger_http_host
        bind_port = settings.replay_trigger_http_port

        def _replay_http_pipeline(
            *,
            trigger_raw: str,
            timeout_seconds: float,
            request_id: str | None,
            filename_prefix: str,
            tolerance_seconds: float,
        ) -> tuple[ProcessLatestReplayResult, int]:
            return run_process_latest_replay_pipeline(
                settings,
                trigger_raw=trigger_raw,
                timeout_seconds=timeout_seconds,
                filename_prefix=filename_prefix,
                tolerance_seconds=tolerance_seconds,
                request_id=request_id,
                connectivity=connectivity,
                supabase=supabase,
                primary_uploader=primary_uploader,
                job_store=job_store,
                status_reporter=status_reporter,
                long_clip_semaphore=long_clip_sem,
            )

        def _replay_trigger_http_main() -> None:
            try:
                run_replay_trigger_http_loop(
                    bind_host,
                    bind_port,
                    _replay_http_pipeline,
                    stop,
                    default_prefix=settings.replay_buffer_filename_prefix,
                )
            except Exception:
                logger.exception("replay-trigger-http: embedded server thread exited")

        replay_trigger_thread = threading.Thread(
            target=_replay_trigger_http_main,
            name="replay-trigger-http",
            daemon=True,
        )
        replay_trigger_thread.start()

    def submit_job(path: Path) -> None:
        normalized = _normalize_path(path)
        if is_copying_temp_clip(normalized, settings):
            logger.debug(
                "Submit skipped: ingest temp file (.copying)",
                extra={"structured": {"path": str(normalized)}},
            )
            return
        job_queue.submit(normalized)

    def worker_loop() -> None:
        logger.info("Worker thread started")
        while not stop.is_set():
            path = job_queue.get(timeout=0.5)
            if path is None:
                continue

            try:
                process_clip(
                    path,
                    settings,
                    primary_uploader,
                    supabase,
                    job_store,
                    long_clip_semaphore=long_clip_sem,
                    connectivity=connectivity,
                    on_original_upload_complete=status_reporter.record_original_upload_success,
                )
            except Exception:
                logger.exception(
                    "Worker failed processing clip",
                    extra={"structured": {"path": str(path)}},
                )
            finally:
                try:
                    job_queue.mark_done(path)
                finally:
                    job_queue.task_done()

        logger.info("Worker thread exiting")

    workers: list[threading.Thread] = []
    for wi in range(settings.worker_concurrency):
        t = threading.Thread(
            target=worker_loop,
            name=f"clip-worker-{wi}",
            daemon=True,
        )
        t.start()
        workers.append(t)

    if (
        settings.replay_scoreboard_auto_sync_interval_seconds > 0
        and settings.long_clips_folder is not None
    ):

        def _replay_auto_main() -> None:
            try:
                replay_scoreboard_auto_sync_loop(settings, submit_job, stop)
            except Exception:
                logger.exception("replay-buffer: auto-sync thread exited")

        replay_auto_thread = threading.Thread(
            target=_replay_auto_main,
            name="replay-scoreboard-auto-sync",
            daemon=True,
        )
        replay_auto_thread.start()

    booking_thread: threading.Thread | None = None
    if settings.unmatched_booking_retry_seconds > 0:

        def _booking_retry_poll() -> None:
            while not stop.is_set():
                if stop.wait(timeout=settings.unmatched_booking_poll_seconds):
                    break
                for pstr in job_store.iter_booking_retry_paths(time.time()):
                    p = Path(pstr)
                    if p.is_file():
                        submit_job(p)

        booking_thread = threading.Thread(
            target=_booking_retry_poll,
            name="booking-retry-poll",
            daemon=True,
        )
        booking_thread.start()

    remote_sync_thread: threading.Thread | None = None

    def _remote_sync_drain_loop() -> None:
        def _between_drain_jobs() -> None:
            base = settings.remote_sync_inter_job_delay_seconds
            jit = settings.remote_sync_inter_job_jitter_seconds
            if base <= 0 and jit <= 0:
                return
            low = max(0.0, base - jit)
            high = max(low, base + jit)
            time.sleep(random.uniform(low, high))

        upload_cb = status_reporter.record_original_upload_success
        while not stop.is_set():
            if stop.wait(timeout=settings.remote_sync_drain_interval_seconds):
                break
            if connectivity.state == "OFFLINE":
                continue
            pending_n = job_store.count_remote_sync_pending()
            if pending_n == 0:
                continue
            due = job_store.list_due_remote_sync(
                time.time(),
                limit=settings.remote_sync_max_jobs_per_cycle,
            )
            for idx, ent in enumerate(due):
                if stop.is_set():
                    break
                path = Path(ent.processing_path)
                if not path.is_file():
                    continue
                now_ts = time.time()
                ok, abort_reason, idem_key = job_store.try_begin_remote_sync_drain(
                    ent.job_uuid,
                    now=now_ts,
                    max_total_attempts=settings.remote_sync_max_total_attempts,
                    max_age_seconds=settings.remote_sync_max_age_seconds,
                )
                if not ok:
                    if abort_reason in ("max_age", "max_attempts"):
                        log_worker_event(
                            logger,
                            logging.WARNING,
                            REMOTE_SYNC_ABORTED,
                            "Remote sync abandoned after retry limits",
                            {
                                "reason": abort_reason,
                                "job_uuid": ent.job_uuid,
                                "clip_identity": idem_key,
                                "max_total_attempts": settings.remote_sync_max_total_attempts,
                                "max_age_seconds": settings.remote_sync_max_age_seconds,
                            },
                        )
                    continue
                log_worker_event(
                    logger,
                    logging.INFO,
                    REMOTE_SYNC_RESUMED,
                    "Retrying deferred remote sync from queue",
                    {
                        "job_uuid": ent.job_uuid,
                        "failed_step": ent.failed_step,
                        "path": str(path),
                        "pending_remote_sync_jobs": pending_n,
                        "jobs_this_cycle": len(due),
                        "max_jobs_per_cycle": settings.remote_sync_max_jobs_per_cycle,
                    },
                )
                try:
                    process_clip(
                        path,
                        settings,
                        primary_uploader,
                        supabase,
                        job_store,
                        long_clip_semaphore=long_clip_sem,
                        connectivity=connectivity,
                        on_original_upload_complete=upload_cb,
                    )
                except Exception as exc:
                    logger.exception(
                        "Remote sync drain pass failed for clip",
                        extra={"structured": {"path": str(path)}},
                    )
                    raw = backoff_delay_seconds(
                        ent.retry_count,
                        base_seconds=settings.network_retry_base_seconds,
                        max_seconds=settings.network_retry_max_seconds,
                    )
                    jf = settings.network_retry_jitter_fraction
                    jittered = raw * (
                        1.0 + random.uniform(-jf, jf) if jf > 0 else 1.0
                    )
                    job_store.bump_remote_sync_retry(
                        ent.job_uuid,
                        last_error=str(exc)[:1000],
                        next_retry_time=time.time() + max(1.0, jittered),
                    )
                if idx + 1 < len(due) and not stop.is_set():
                    _between_drain_jobs()

    remote_sync_thread = threading.Thread(
        target=_remote_sync_drain_loop,
        name="remote-sync-drain",
        daemon=True,
    )
    remote_sync_thread.start()

    status_thread: threading.Thread | None = None
    if settings.worker_status_write_interval_seconds > 0:

        def _status_json_loop() -> None:
            while not stop.is_set():
                if stop.wait(timeout=settings.worker_status_write_interval_seconds):
                    break
                status_reporter.write(
                    settings=settings,
                    connectivity=connectivity,
                    job_store=job_store,
                    worker_running=True,
                )

        status_thread = threading.Thread(
            target=_status_json_loop,
            name="worker-status-json",
            daemon=True,
        )
        status_thread.start()

    health_thread: threading.Thread | None = None
    if settings.worker_health_summary_interval_seconds > 0:

        def _health_loop() -> None:
            while not stop.is_set():
                if stop.wait(timeout=settings.worker_health_summary_interval_seconds):
                    break
                by_status = job_store.count_rows_by_status()
                stale_n = job_store.count_stale_processing(settings.stale_job_idle_seconds)
                failed_n = by_status.get("failed", 0)
                log_worker_event(
                    logger,
                    logging.INFO,
                    WORKER_HEALTH_SUMMARY,
                    "Worker health summary",
                    {
                        "jobs_by_status": by_status,
                        "stale_processing_estimate": stale_n,
                        "network_state": connectivity.state,
                        "pending_remote_sync_queue": job_store.count_remote_sync_pending(),
                        "failed_jobs": failed_n,
                        "worker_running": True,
                    },
                )

        health_thread = threading.Thread(
            target=_health_loop,
            name="worker-health",
            daemon=True,
        )
        health_thread.start()

    ingest_threads: list[threading.Thread] = []

    if settings.instant_replay_source is not None:

        def _instant_poll() -> None:
            try:
                instant_replay_ingest_loop(settings, submit_job, stop)
            except Exception:
                logger.exception("Instant replay poll ingest thread exited with error")

        def _instant_trigger() -> None:
            try:
                instant_replay_trigger_loop(settings, submit_job, stop)
            except Exception:
                logger.exception("Instant replay trigger ingest thread exited with error")

        if settings.instant_replay_trigger_file is not None:
            ir_thread = threading.Thread(
                target=_instant_trigger,
                name="instant-replay-trigger",
                daemon=True,
            )
        else:
            ir_thread = threading.Thread(
                target=_instant_poll,
                name="instant-replay-poll",
                daemon=True,
            )
        ir_thread.start()
        ingest_threads.append(ir_thread)

    if settings.long_clips_folder is not None:
        if (
            settings.long_clips_scan_interval_seconds <= 0
            and settings.long_clips_trigger_file is None
        ):
            logger.warning(
                "Long clips folder configured but ingest disabled "
                "(set LONG_CLIPS_SCAN_INTERVAL_SECONDS > 0 and/or LONG_CLIPS_TRIGGER_FILE)",
                extra={"structured": {"folder": str(settings.long_clips_folder)}},
            )
        else:

            def _long_ingest() -> None:
                try:
                    long_clips_ingest_loop(settings, submit_job, stop)
                except Exception:
                    logger.exception("Long clips ingest thread exited with error")

            lc_thread = threading.Thread(
                target=_long_ingest,
                name="long-clips-ingest",
                daemon=True,
            )
            lc_thread.start()
            ingest_threads.append(lc_thread)

    handler = ClipFileHandler(settings, submit_job)
    observer = start_observer(settings, handler)

    logger.info(
        "Scanning existing clips on startup",
        extra={
            "structured": {
                "clips_incoming_folder": str(settings.clips_incoming_folder),
                "clips_processing_folder": str(settings.clips_processing_folder),
            }
        },
    )
    scan_existing_clips(settings, submit_job)
    scan_processing_resume(settings, submit_job)

    incoming_n = _count_clip_videos(settings.clips_incoming_folder, settings)
    processing_n = _count_clip_videos(settings.clips_processing_folder, settings)
    by_status = job_store.count_rows_by_status()
    stale_n = job_store.count_stale_processing(settings.stale_job_idle_seconds)
    log_worker_event(
        logger,
        logging.INFO,
        WORKER_STARTUP_SUMMARY,
        "Startup scan and job store summary",
        {
            "incoming_clip_files": incoming_n,
            "processing_clip_files": processing_n,
            "jobs_by_status": by_status,
            "stale_processing_jobs": stale_n,
            "network_state": connectivity.state,
            "pending_remote_sync_queue": job_store.count_remote_sync_pending(),
            "failed_jobs": by_status.get("failed", 0),
            "worker_running": True,
        },
    )
    status_reporter.write(
        settings=settings,
        connectivity=connectivity,
        job_store=job_store,
        worker_running=True,
    )

    def handle_signal(signum: int, _frame: object | None) -> None:
        logger.info(
            "Shutdown signal received",
            extra={"structured": {"signal": signum}},
        )
        stop.set()
        try:
            observer.stop()
        except Exception:
            logger.exception("Failed stopping observer during shutdown")

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    logger.info("ReplayTrove worker running; press Ctrl+C to stop")

    try:
        while observer.is_alive() and not stop.is_set():
            time.sleep(0.25)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; shutting down")
        stop.set()
        observer.stop()
    finally:
        stop.set()
        try:
            observer.stop()
        except Exception:
            logger.exception("Failed stopping observer in final shutdown block")
        try:
            status_reporter.write(
                settings=settings,
                connectivity=connectivity,
                job_store=job_store,
                worker_running=False,
            )
        except Exception:
            logger.exception("Failed final worker status JSON write")

    observer.join(timeout=30)
    for w in workers:
        w.join(timeout=10)
    if booking_thread is not None:
        booking_thread.join(timeout=2)
    if health_thread is not None:
        health_thread.join(timeout=2)
    if remote_sync_thread is not None:
        remote_sync_thread.join(timeout=2)
    if status_thread is not None:
        status_thread.join(timeout=2)
    connectivity_thread.join(timeout=2)
    if replay_trigger_thread is not None:
        replay_trigger_thread.join(timeout=2)
    if replay_auto_thread is not None:
        replay_auto_thread.join(timeout=2)
    for t in ingest_threads:
        t.join(timeout=5)

    logger.info("ReplayTrove worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())