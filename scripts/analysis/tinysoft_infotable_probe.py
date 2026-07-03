#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""探测天软表44(合并资产负债表)列名，用于补 balance 缺口。"""
from __future__ import annotations
import asyncio, sys, json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alphahome.common.config_manager import ConfigManager
from alphahome.fetchers.sources.tinysoft.tinysoft_opi_api import TinySoftOPIAPI
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import ts_code_to_tinysoft_symbol


async def main(stock_code: str, table_id: int, label: str) -> int:
    cfg = ConfigManager().get_tinysoft_config() or {}
    api = TinySoftOPIAPI(
        user=cfg.get("user"), password=cfg.get("password"),
        opi_url=cfg.get("opi_url") or "https://opi.tinysoft.com.cn",
        auth_mode=str(cfg.get("opi_auth_mode") or "basic"),
        session_key=cfg.get("session_key"), session_password=cfg.get("session_password"),
        service=str(cfg.get("service") or ""),
        timeout_ms=120000, request_interval=0.2,
    )
    stock = ts_code_to_tinysoft_symbol(stock_code)
    try:
        df = await api.call_dataframe("infoarray", table_id, stock=stock, where_clause=None, fields=None, timeout_ms=120000)
        if df is None or df.empty:
            print(f"[{label}] 表{table_id} {stock_code} 返回空")
            return 1
        cols = list(df.columns)
        out = ROOT / "tmp" / "tinysoft_probe" / f"table{table_id}_columns.json"
        out.write_text(json.dumps(cols, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[{label}] 表{table_id} {stock_code}: {len(df)}行 {len(cols)}列 -> {out}")
        # 显示前5行截止日
        cutoff = [c for c in cols if "截止" in str(c)]
        if cutoff:
            print(f"  截止日列: {cutoff[0]}, 取值: {df[cutoff[0]].tolist()[:8]}")
        return 0
    finally:
        await api.close()


if __name__ == "__main__":
    sc = sys.argv[1] if len(sys.argv) > 1 else "002549.SZ"
    tid = int(sys.argv[2]) if len(sys.argv) > 2 else 44
    lb = sys.argv[3] if len(sys.argv) > 3 else "balancesheet"
    raise SystemExit(asyncio.run(main(sc, tid, lb)))
