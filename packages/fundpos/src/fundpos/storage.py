from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd


def json_default(value):
    if isinstance(value, (datetime, date, pd.Timestamp, Path)):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value).__name__)


def atomic_json(path: Path, content: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(
                content, stream, ensure_ascii=False, indent=2, default=json_default, allow_nan=False
            )
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def atomic_parquet(path: Path, data: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp = tempfile.mkstemp(dir=path.parent, suffix=".parquet")
    os.close(handle)
    try:
        data.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def frame_hash(frame: pd.DataFrame) -> str:
    if frame.empty:
        return hashlib.sha256("|".join(sorted(frame.columns)).encode()).hexdigest()
    canonical = frame.reindex(sorted(frame.columns), axis=1).copy()
    for col in canonical:
        if canonical[col].dtype == "object":
            canonical[col] = canonical[col].map(
                lambda x: (
                    json.dumps(x, sort_keys=True, default=json_default)
                    if isinstance(x, (dict, list))
                    else str(x)
                )
            )
    rows = pd.util.hash_pandas_object(canonical, index=False).to_numpy()
    return hashlib.sha256(
        np.sort(rows).tobytes() + "|".join(canonical.columns).encode()
    ).hexdigest()


def code_fingerprint(root: Path) -> str:
    files = sorted((root / "src").rglob("*.py"))
    files += sorted((root / "config").glob("*.json"))
    lock = root / "uv.lock"
    if lock.exists():
        files.append(lock)
    digest = hashlib.sha256()
    for path in files:
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def git_revision(root: Path) -> str | None:
    frozen = os.environ.get("FUNDPOS_SOURCE_REVISION")
    if frozen:
        return frozen
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, stderr=subprocess.DEVNULL, text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None
