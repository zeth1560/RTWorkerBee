"""
S3 uploads via boto3 with retries.

``upload_file`` returns persisted metadata (bucket, key, ETag) for SQLite. Uses boto3's
transfer manager (multipart when over threshold). A future multipart-resume layer can
wrap the client while keeping this surface area stable.
"""

from __future__ import annotations

import logging
import mimetypes
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

from network_retry import (
    call_with_network_retry,
    is_non_retryable_dependency_error,
    is_retryable_network_error,
    logging_retry_hook,
)

logger = logging.getLogger(__name__)


def _guess_content_type(path: Path) -> str | None:
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype


class S3Uploader:
    """Upload local files to a single bucket with configurable retry behavior."""

    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        access_key_id: str,
        secret_access_key: str,
        upload_retries: int,
        upload_retry_delay_seconds: float,
        label: str = "s3",
        multipart_threshold_bytes: int = 8 * 1024 * 1024,
        multipart_chunksize_bytes: int = 128 * 1024 * 1024,
        network_retry_base_seconds: float = 5.0,
        network_retry_max_seconds: float = 60.0,
        network_retry_jitter_fraction: float = 0.2,
    ) -> None:
        self.bucket = bucket
        self.upload_retries = max(1, upload_retries)
        self.upload_retry_delay_seconds = upload_retry_delay_seconds
        self.label = label
        self._net_base = network_retry_base_seconds
        self._net_max = network_retry_max_seconds
        self._net_jitter = network_retry_jitter_fraction
        self._transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold_bytes,
            multipart_chunksize=multipart_chunksize_bytes,
            max_concurrency=10,
            use_threads=True,
        )

        self._client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def upload_file(self, local_path: Path, object_key: str) -> dict[str, Any]:
        """
        Upload ``local_path`` to ``object_key``. Returns ``bucket``, ``key``, ``etag`` (may be empty).
        """
        extra_args: dict[str, Any] = {}
        ctype = _guess_content_type(local_path)
        if ctype:
            extra_args["ContentType"] = ctype

        attempt_holder = {"n": 0}

        def _upload_once() -> dict[str, Any]:
            attempt_holder["n"] += 1
            logger.info(
                "S3 upload starting",
                extra={
                    "structured": {
                        "target": self.label,
                        "attempt": attempt_holder["n"],
                        "bucket": self.bucket,
                        "key": object_key,
                        "path": str(local_path),
                    }
                },
            )
            try:
                self._client.upload_file(
                    str(local_path),
                    self.bucket,
                    object_key,
                    ExtraArgs=extra_args or None,
                    Config=self._transfer_config,
                )
            except (ClientError, BotoCoreError, OSError) as exc:
                if is_non_retryable_dependency_error(exc):
                    raise
                if not is_retryable_network_error(exc):
                    raise
                raise

            etag = ""
            try:
                head = self._client.head_object(Bucket=self.bucket, Key=object_key)
                raw = head.get("ETag")
                if raw:
                    etag = str(raw).strip('"')
            except (ClientError, BotoCoreError) as head_exc:
                logger.warning(
                    "S3 head_object after upload failed (ETag unavailable)",
                    extra={
                        "structured": {
                            "bucket": self.bucket,
                            "key": object_key,
                            "error": str(head_exc),
                        }
                    },
                )
            logger.info(
                "S3 upload completed",
                extra={
                    "structured": {
                        "target": self.label,
                        "bucket": self.bucket,
                        "key": object_key,
                        "path": str(local_path),
                        "attempt": attempt_holder["n"],
                        "etag": etag,
                    }
                },
            )
            return {
                "bucket": self.bucket,
                "key": object_key,
                "etag": etag,
            }

        return call_with_network_retry(
            _upload_once,
            operation=f"s3_upload:{self.label}:{object_key}",
            base_seconds=self._net_base,
            max_seconds=self._net_max,
            jitter_frac=self._net_jitter,
            max_rounds=self.upload_retries,
            on_retry=logging_retry_hook(f"S3 upload {self.label}"),
        )
