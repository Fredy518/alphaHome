#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def _hikyuu_dir_from_env() -> str | None:
    v = os.environ.get("HIKYUU_DATA_DIR")
    if v and v.strip():
        return v.strip()
    return None


def _iter_market_keys(h5_path: Path) -> Iterable[str]:
    import h5py

    with h5py.File(h5_path.as_posix(), "r") as f:
        grp = f.get("data")
        if grp is None:
            return []
        # keys like SH600000 / SZ000001 / BJ430047
        return list(grp.keys())


def _symbol_to_ts_code(symbol: str) -> str:
    symbol = (symbol or "").strip().upper()
    if len(symbol) < 3:
        raise ValueError(f"Invalid symbol: {symbol!r}")
    market = symbol[:2]
    code = symbol[2:]
    if market not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"Unsupported market in symbol: {symbol!r}")
    if not code.isdigit():
        raise ValueError(f"Invalid digits in symbol: {symbol!r}")
    return f"{code}.{market}"


def _write_lines(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def build_lists(hikyuu_dir: Path) -> Dict[str, List[str]]:
    markets = {"sh": "sh_5min.h5", "sz": "sz_5min.h5", "bj": "bj_5min.h5"}

    by_market: Dict[str, List[str]] = {"sh": [], "sz": [], "bj": []}
    for mkt, fname in markets.items():
        h5_path = hikyuu_dir / fname
        if not h5_path.exists():
            continue
        ts_codes = sorted({_symbol_to_ts_code(k) for k in _iter_market_keys(h5_path)})
        by_market[mkt] = ts_codes

    out: Dict[str, List[str]] = {}
    for mkt, codes in by_market.items():
        out[f"{mkt}_all"] = codes
        # group by first digit
        groups: Dict[str, List[str]] = {str(i): [] for i in range(10)}
        for ts_code in codes:
            code, _ = ts_code.split(".", 1)
            groups[code[0]].append(ts_code)
        for digit, items in groups.items():
            if items:
                out[f"{mkt}_{digit}"] = items

    all_codes = sorted(set(by_market["sh"]) | set(by_market["sz"]) | set(by_market["bj"]))
    out["all"] = all_codes
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate ts_code lists from Hikyuu 5min HDF5")
    parser.add_argument(
        "--hikyuu-dir",
        default=None,
        help="Hikyuu data dir (default: env HIKYUU_DATA_DIR, typical E:/stock)",
    )
    parser.add_argument("--output-dir", default="scripts/tickers", help="Output dir")
    args = parser.parse_args()

    hikyuu_dir = args.hikyuu_dir or _hikyuu_dir_from_env() or "E:/stock"
    hikyuu_dir_path = Path(hikyuu_dir)

    lists = build_lists(hikyuu_dir_path)
    out_dir = Path(args.output_dir)

    # Write files
    for name, lines in sorted(lists.items()):
        _write_lines(out_dir / f"{name}.txt", lines)

    print(f"Wrote {len(lists)} tickers files to {out_dir}")
    for k in ["sh_all", "sz_all", "bj_all", "all"]:
        if k in lists:
            print(f"- {k}: {len(lists[k])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

