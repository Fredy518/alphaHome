"""Local parquet cache for tinydata datasets."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .config import get_config


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return str(value)


def make_cache_key(dataset: str, params: Dict[str, Any]) -> str:
    payload = {"dataset": dataset, "params": params}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_json_default)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class CacheManager:
    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = Path(cache_dir or get_config().cache_dir).expanduser()

    def path_for(self, dataset: str, key: str) -> Path:
        safe_dataset = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in dataset)
        return self.cache_dir / safe_dataset / f"{key}.parquet"

    def read(self, dataset: str, key: str) -> Optional[pd.DataFrame]:
        path = self.path_for(dataset, key)
        if not path.exists():
            return None
        return pd.read_parquet(path)

    def write(self, dataset: str, key: str, df: pd.DataFrame) -> Path:
        path = self.path_for(dataset, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        return path
