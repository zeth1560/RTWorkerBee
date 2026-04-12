"""
Strong idempotency fingerprint: size + first/last chunk hashes (+ optional mtime, optional bounded full-file hash).
"""

from __future__ import annotations

import hashlib
import struct
from pathlib import Path


def compute_clip_idempotency_key(
    path: Path,
    *,
    chunk_bytes: int,
    include_mtime: bool,
    full_hash_max_bytes: int,
) -> str:
    """
    Return a SHA-256 hex digest identifying clip *content* (not basename).

    For files larger than ``full_hash_max_bytes`` (when > 0), only the first and last
    ``chunk_bytes`` are hashed unless the file fits under ``full_hash_max_bytes``.
    """
    st = path.stat()
    size = st.st_size
    chunk_bytes = max(4096, int(chunk_bytes))
    full_hash_max = max(0, int(full_hash_max_bytes))

    h = hashlib.sha256()
    h.update(str(size).encode("ascii"))

    if include_mtime:
        h.update(struct.pack(">q", st.st_mtime_ns))

    use_full = full_hash_max > 0 and size <= full_hash_max

    with path.open("rb") as f:
        if use_full:
            while True:
                block = f.read(1024 * 1024)
                if not block:
                    break
                h.update(block)
        else:
            read_first = min(chunk_bytes, size)
            if read_first:
                h.update(f.read(read_first))
            if size > read_first:
                tail_len = min(chunk_bytes, size)
                f.seek(max(0, size - tail_len))
                h.update(f.read(tail_len))

    return h.hexdigest()
