"""
S3 uploads via boto3 with retries.
"""

from __future__ import annotations

import logging
import mimetypes
import time
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

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
    ) -> None:
        self.bucket = bucket
        self.upload_retries = max(1, upload_retries)
        self.upload_retry_delay_seconds = upload_retry_delay_seconds
        self.label = label
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

    def upload_file(self, local_path: Path, object_key: str) -> None:
        extra_args: dict[str, Any] = {}
        ctype = _guess_content_type(local_path)
        if ctype:
            extra_args["ContentType"] = ctype

        last_error: Exception | None = None

        for attempt in range(1, self.upload_retries + 1):
            try:
                logger.info(
                    "S3 upload starting",
                    extra={
                        "structured": {
                            "target": self.label,
                            "attempt": attempt,
                            "bucket": self.bucket,
                            "key": object_key,
                            "path": str(local_path),
                        }
                    },
                )
                self._client.upload_file(
                    str(local_path),
                    self.bucket,
                    object_key,
                    ExtraArgs=extra_args or None,
                    Config=self._transfer_config,
                )
                logger.info(
                    "S3 upload completed",
                    extra={
                        "structured": {
                            "target": self.label,
                            "bucket": self.bucket,
                            "key": object_key,
                            "path": str(local_path),
                            "attempt": attempt,
                        }
                    },
                )
                return
            except (ClientError, BotoCoreError, OSError) as exc:
                last_error = exc
                logger.warning(
                    "S3 upload failed",
                    extra={
                        "structured": {
                            "target": self.label,
                            "attempt": attempt,
                            "bucket": self.bucket,
                            "key": object_key,
                            "error": str(exc),
                        }
                    },
                    exc_info=attempt == self.upload_retries,
                )
                if attempt < self.upload_retries and self.upload_retry_delay_seconds > 0:
                    time.sleep(self.upload_retry_delay_seconds)

        assert last_error is not None
        raise RuntimeError(
            f"S3 upload failed after {self.upload_retries} attempts: "
            f"{self.bucket}/{object_key}"
        ) from last_error