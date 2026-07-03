#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
探测 Tinysoft 表48（股票.现金流量表）对 002549.SZ 2022 年的覆盖情况。

门控目的：确认天软表48是否真的有 2022-06-30（Q2 半年报）这期现金流数据——
该期在 tushare.fina_cashflow / rawdata.fina_cashflow 双双缺失（source_missing），
但 tushare 利润表/资产负债表都有该期，说明公司披露了半年报，仅现金流单表漏采。

用法：
    python scripts/analysis/tinysoft_cashflow48_probe.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# 确保工程根目录在 sys.path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alphahome.common.config_manager import ConfigManager
from alphahome.fetchers.sources.tinysoft.tinysoft_opi_api import TinySoftOPIAPI
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import (
    tinysoft_symbol_to_ts_code,
    ts_code_to_tinysoft_symbol,
)

TS_CODE = "002549.SZ"
TABLE_ID = 48  # 股票.现金流量表


def _build_opi_api() -> TinySoftOPIAPI:
    cfg = ConfigManager().get_tinysoft_config() or {}
    return TinySoftOPIAPI(
        user=cfg.get("user"),
        password=cfg.get("password"),
        opi_url=cfg.get("opi_url") or "https://opi.tinysoft.com.cn",
        auth_mode=str(cfg.get("opi_auth_mode") or "basic"),
        session_key=cfg.get("session_key"),
        session_password=cfg.get("session_password"),
        service=str(cfg.get("service") or ""),
        timeout_ms=int(cfg.get("timeout_ms", 60000) or 60000),
        request_interval=float(cfg.get("request_interval", 0.2) or 0.2),
    )


async def probe_full_schema(api: TinySoftOPIAPI, stock: str) -> Optional[pd.DataFrame]:
    """全量拉取表48，不带 where，看返回的全部列名与行。"""
    print(f"\n=== [1] 全量拉取表48: {TS_CODE} ({stock}) ===")
    df = await api.call_dataframe(
        "infoarray",
        TABLE_ID,
        stock=stock,
        where_clause=None,
        fields=None,
        timeout_ms=120000,
    )
    if df is None or df.empty:
        print("  返回空。")
        return None
    print(f"  行数: {len(df)}")
    print(f"  列数: {len(df.columns)}")
    print(f"  列名: {list(df.columns)}")
    # 找日期相关列
    date_like = [c for c in df.columns if any(k in str(c) for k in ("日", "date", "Date", "截止", "公布"))]
    print(f"  日期相关列: {date_like}")
    return df


async def probe_filtered_2022(api: TinySoftOPIAPI, stock: str) -> Optional[pd.DataFrame]:
    """按截止日过滤 2022 年，确认 2022-06-30 这期是否存在。"""
    print(f"\n=== [2] 按截止日过滤 2022 年 ===")
    where = '["截止日"]>="20220101" and ["截止日"]<="20230101"'
    df = await api.call_dataframe(
        "infoarray",
        TABLE_ID,
        stock=stock,
        where_clause=where,
        fields=None,
        timeout_ms=120000,
    )
    if df is None or df.empty:
        print("  返回空（2022 年无数据）。")
        return None
    print(f"  行数: {len(df)}")
    # 显示截止日列
    cutoff_col = None
    for c in df.columns:
        if "截止日" in str(c) or str(c).lower() == "截止日":
            cutoff_col = c
            break
    if cutoff_col is not None:
        print(f"  各行截止日:")
        for _, row in df.iterrows():
            print(f"    {row[cutoff_col]}")
    return df


async def main() -> int:
    stock = ts_code_to_tinysoft_symbol(TS_CODE)
    print(f"目标: {TS_CODE} -> 天软代码 {stock}")
    print(f"表: infotable {TABLE_ID} (股票.现金流量表)")

    api = _build_opi_api()
    try:
        # 全量探测列名
        full = await probe_full_schema(api, stock)

        # 过滤 2022 年
        filtered = await probe_filtered_2022(api, stock)

        if filtered is not None and not filtered.empty:
            print(f"\n=== [3] 2022 年数据完整列预览 ===")
            # 截断显示，避免列过多
            show_cols = [c for c in filtered.columns if c is not None][:20]
            print(filtered[show_cols].to_string())

        # 把全量结果落盘备查
        out_dir = ROOT / "tmp" / "tinysoft_probe"
        out_dir.mkdir(parents=True, exist_ok=True)
        if full is not None:
            full.to_csv(out_dir / f"{TS_CODE}_table48_full.csv", index=False, encoding="utf-8-sig")
            print(f"\n全量结果已保存: {out_dir / f'{TS_CODE}_table48_full.csv'}")
        if filtered is not None:
            filtered.to_csv(out_dir / f"{TS_CODE}_table48_2022.csv", index=False, encoding="utf-8-sig")
            print(f"2022 过滤结果已保存: {out_dir / f'{TS_CODE}_table48_2022.csv'}")

        return 0
    finally:
        await api.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
