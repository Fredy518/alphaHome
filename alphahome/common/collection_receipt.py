"""Credential-free, atomic receipts for the production collection entrypoint."""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping


def safe_reason(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if re.search(r"[a-z][a-z0-9+.-]*://|\b(password|passwd|pwd|token|secret|api[_-]?key|authorization)\s*[:=]", value, re.I):
        return "[redacted reason containing connection or credential data]"
    return value[:500]


def task_receipt(result: Mapping[str, Any]) -> dict[str, Any]:
    """Whitelist operational fields; never persist arbitrary API/error payloads."""
    payload = result.get("result")
    payload = payload if isinstance(payload, Mapping) else result
    record = {"task_name": result.get("task_name"), "status": result.get("status")}
    for key in ("attempts", "execution_time"):
        value = result.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            record[key] = value
    for key in ("rows", "processed_rows", "saved_rows", "committed_rows", "error_records", "failed_batches"):
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            record[key] = value
    if result.get("status") in {"expected_no_data", "expected_skip"}:
        record["reason"] = safe_reason(payload.get("reason"))
    if result.get("error") or payload.get("error"):
        record["error_present"] = True
    return record


def write_receipt(path: Path, record: Mapping[str, Any]) -> str:
    """Publish a complete JSON file atomically without touching business data."""
    encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return hashlib.sha256(encoded).hexdigest()
