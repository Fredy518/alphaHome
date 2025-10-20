"""
演示：如何使用 AlphaDataTool 导出日频数据到 Hikyuu 数据目录

仅示例用法，不作为生产脚本。
"""

import os
import pandas as pd
from pathlib import Path
from alphahome.common.config_manager import get_hikyuu_data_dir
from alphahome.common.db_manager import create_sync_manager
from alphahome.providers import AlphaDataTool, map_ts_code_to_hikyuu


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def export_daily_csv(df: pd.DataFrame, data_dir: str):
    """将标准化后的 DataFrame 导出为按股票拆分的 CSV（示意）
    实际生产脚本请参考 scripts/production/exporters/hikyuu_day_export.py
    """
    if df.empty:
        return
    base = Path(data_dir) / "day"
    ensure_dir(base)
    for ts_code, g in df.groupby('ts_code'):
        sym = map_ts_code_to_hikyuu(ts_code)
        out = base / f"{sym}.csv"
        g_sorted = g.sort_values('trade_date')
        g_sorted.to_csv(out, index=False)


def main():
    data_dir = get_hikyuu_data_dir()
    if not data_dir:
        raise RuntimeError("未配置 HIKYUU_DATA_DIR")

    # 仅示例：从环境或固定连接串创建 DB 管理器
    # connection_string = os.environ.get('DATABASE_URL')
    # db = create_sync_manager(connection_string)
    # data_tool = AlphaDataTool(db)
    # df = data_tool.get_stock_data(['000001.SZ'], '2020-01-01', '2024-12-31')
    # sdf = AlphaDataTool.standardize_to_hikyuu_ohlcv(df)
    # export_daily_csv(sdf, data_dir)
    pass


if __name__ == "__main__":
    main()


