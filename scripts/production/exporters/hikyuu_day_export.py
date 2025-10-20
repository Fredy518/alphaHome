#!/usr/bin/env python3
"""
Hikyuu 日线数据导出器（生产）

功能：
- 从 AlphaHome 数据库读取日频 OHLCV 数据
- 标准化并导出到 Hikyuu 数据目录（data_dir）
- 支持增量（按标的的最新日期水位）

使用：
  python scripts/production/exporters/hikyuu_day_export.py --start 2010-01-01 --end 2025-12-31 --symbols 000001.SZ,000002.SZ

注意：实际 Hikyuu data_dir 结构与文件格式可根据您本地 Hikyuu 配置调整。
"""

import argparse
import os
from pathlib import Path
from typing import List
import pandas as pd

from alphahome.common.config_manager import get_hikyuu_data_dir, get_database_url
from alphahome.common.db_manager import create_sync_manager
from alphahome.providers import AlphaDataTool
from alphahome.providers.tools.hikyuu_h5_exporter import HikyuuH5Exporter


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="导出日频数据到 Hikyuu 数据目录")
    p.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    p.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    p.add_argument("--symbols", help="逗号分隔的 ts_code 列表，如 000001.SZ,000002.SZ")
    p.add_argument("--all-listed", action="store_true", help="导出全部在市股票")
    return p.parse_args()


def load_symbols(data_tool: AlphaDataTool, symbols_arg: str, all_listed: bool) -> List[str]:
    if symbols_arg:
        return [s.strip() for s in symbols_arg.split(',') if s.strip()]
    if all_listed:
        # 获取所有股票（包括退市），避免回测时的未来信息泄露
        info = data_tool.get_stock_info(active_only=False)
        return sorted(info['ts_code'].tolist())
    raise SystemExit("请通过 --symbols 指定标的，或使用 --all-listed 导出全部股票")


def export_daily(data_tool: AlphaDataTool, symbols: List[str], start: str, end: str, data_dir: str):
    # 获取原始价格数据
    raw = data_tool.get_stock_data(symbols, start, end)
    if raw.empty:
        print("未查询到数据，跳过导出")
        return
    
    # 获取复权因子数据
    adj_factor = data_tool.get_adj_factor_data(symbols, start, end)
    
    # 标准化数据格式
    sdf = AlphaDataTool.standardize_to_hikyuu_ohlcv(raw)
    
    # 导出到 Hikyuu（包含前复权计算）
    exporter = HikyuuH5Exporter(data_dir)
    exporter.export_day_incremental(sdf, adj_factor)


def main():
    args = parse_args()
    data_dir = get_hikyuu_data_dir()
    if not data_dir:
        raise SystemExit("未配置 HIKYUU_DATA_DIR（配置文件或环境变量）")

    # 连接数据库（使用配置管理器）
    conn = get_database_url()
    if not conn:
        raise SystemExit("未配置数据库连接URL（配置文件或环境变量）")
    db = create_sync_manager(conn)
    data_tool = AlphaDataTool(db)

    symbols = load_symbols(data_tool, args.symbols, args.all_listed)
    export_daily(data_tool, symbols, args.start, args.end, data_dir)


if __name__ == "__main__":
    main()


