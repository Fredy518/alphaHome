"""Code normalization and local code-pool helpers."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from .config import get_config


def ts_code_to_tinysoft_symbol(value: object) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    if "." in raw:
        code, suffix = raw.rsplit(".", 1)
        if re.fullmatch(r"\d{6}", code) and suffix in {"SH", "SZ", "BJ", "OF"}:
            return f"{suffix}{code}"
    if re.fullmatch(r"(SH|SZ|BJ|OF)\d{6}", raw):
        return raw
    return raw if raw else None


def tinysoft_symbol_to_ts_code(value: object) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    match = re.fullmatch(r"(SH|SZ|BJ|OF)(\d{6})", raw)
    if match:
        return f"{match.group(2)}.{match.group(1)}"
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ|OF)", raw):
        return raw
    return None


def normalize_codes(codes: Optional[Iterable[object]]) -> List[str]:
    if codes is None:
        return []
    if isinstance(codes, (str, bytes)):
        raw_items: Iterable[object] = re.split(r"[\s,;]+", str(codes))
    else:
        raw_items = codes
    normalized: List[str] = []
    for item in raw_items:
        symbol = ts_code_to_tinysoft_symbol(item)
        if symbol:
            normalized.append(symbol)
    return list(dict.fromkeys(normalized))


def _read_codes_from_csv(path: Path, columns: Sequence[str]) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            for col in columns:
                value = row.get(col)
                symbol = ts_code_to_tinysoft_symbol(value)
                if symbol:
                    out.append(symbol)
                    break
    return list(dict.fromkeys(out))


def load_code_pool(name: str, *, columns: Sequence[str] = ("ts_code", "code", "symbol")) -> List[str]:
    cfg = get_config()
    candidates = [
        cfg.code_dir / f"{name}.csv",
        cfg.code_dir / f"{name}_codes.csv",
    ]
    for path in candidates:
        codes = _read_codes_from_csv(path, columns)
        if codes:
            return codes
    return []
