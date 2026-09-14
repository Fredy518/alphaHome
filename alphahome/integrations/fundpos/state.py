"""Provision and validate AlphaHome-owned mutable fundpos state."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any


class FundposStateError(RuntimeError):
    """Raised when the managed supplement state is incomplete or inconsistent."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_supplement_directory(directory: Path) -> dict[str, Any]:
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        raise FundposStateError(f"Missing supplement manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise FundposStateError("Supplement manifest has no files")
    verified = []
    for item in files:
        path = directory / str(item["file"])
        if not path.exists():
            raise FundposStateError(f"Missing supplement file: {path}")
        actual = _sha256(path)
        if actual != item.get("sha256"):
            raise FundposStateError(f"Supplement hash differs: {path.name}")
        verified.append({"file": path.name, "sha256": actual})
    return {
        "directory": str(directory.resolve()),
        "manifest_sha256": _sha256(manifest_path),
        "files": verified,
    }


def bootstrap_fundpos_state(engine_root: Path, state_root: Path) -> dict[str, Any]:
    engine_root, state_root = engine_root.resolve(), state_root.resolve()
    seed = engine_root / "resources" / "supplements_seed"
    validate_supplement_directory(seed)
    state_root.mkdir(parents=True, exist_ok=True)
    for name in ("data", "outputs"):
        (state_root / name).mkdir(exist_ok=True)
    target = state_root / "supplements"
    if target.exists():
        result = validate_supplement_directory(target)
        result["status"] = "existing_valid_state"
        return result
    if state_root not in target.parents:
        raise FundposStateError("Supplement target escapes the configured state root")
    shutil.copytree(seed, target)
    result = validate_supplement_directory(target)
    result["status"] = "initialized_from_versioned_seed"
    return result
