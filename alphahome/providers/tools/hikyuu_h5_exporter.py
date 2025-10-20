"""
Hikyuu HDF5（日频）导出器

目标：
- 将 AlphaHome 的日频 OHLCV DataFrame 增量写入 Hikyuu 的 HDF5 数据文件
- 严格按照现有 H5 文件结构：/data/{MKT}{CODE} 数据集
- 支持前复权数据导出，避免回测时的未来信息泄露

依赖：
- h5py（需要：`pip install h5py`）

约定：
- 目标文件：E:/stock/{sh|sz|bj}_day.h5（由上层传入 data_dir）
- 数据集路径：/data/{MKT}{CODE}（如 /data/SZ000001）
- 字段顺序：["closePrice", "datetime", "highPrice", "lowPrice", "openPrice", "transAmount", "transCount"]
- 数据类型：价格字段为 uint32，金额/成交量/时间戳为 uint64
- datetime 格式：yyyymmdd0000（日频 HHMM 固定为 0000）
- 价格缩放因子：基于源码分析，价格字段使用 1000 倍缩放，成交金额使用 10 倍缩放
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np

import pandas as pd
import h5py

from .. import map_ts_code_to_hikyuu


class HikyuuH5Exporter:
    """Hikyuu HDF5 导出器（严格按照现有结构增量写入）"""

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Hikyuu H5 文件结构常量
        self.DATA_GROUP = 'data'
        # 基于源码分析的正确缩放因子
        self.PRICE_SCALE = 1000      # 价格 * 1000 转为整数
        self.AMOUNT_SCALE = 10       # 成交金额 * 10 转为整数
        
        # 目标 dtype 结构
        self.DTYPE = np.dtype([
            ('closePrice', '<u4'),    # uint32
            ('datetime', '<u8'),      # uint64
            ('highPrice', '<u4'),     # uint32
            ('lowPrice', '<u4'),      # uint32
            ('openPrice', '<u4'),     # uint32
            ('transAmount', '<u8'),   # uint64
            ('transCount', '<u8')     # uint64
        ])

    # ----------------------------- 公共方法 -----------------------------
    def export_day_incremental(self, df: pd.DataFrame, adj_factor_df: Optional[pd.DataFrame] = None) -> None:
        """将标准化日频数据增量写入 HDF5

        期望 df 列：['ts_code','trade_date','open','high','low','close','vol','amount']
        多标的混合写入时会被按市场拆分到不同 H5 文件。
        
        Args:
            df: 原始价格数据
            adj_factor_df: 复权因子数据，如果提供则计算前复权价格
        """
        if df is None or df.empty:
            return

        # 规范化日期
        df = df.copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 如果有复权因子数据，计算前复权价格
        if adj_factor_df is not None and not adj_factor_df.empty:
            df = self._calculate_forward_adj(df, adj_factor_df)

        # 按市场分组
        by_market: Dict[str, pd.DataFrame] = {}
        for ts_code, g in df.groupby('ts_code'):
            market = self._market_of_ts_code(ts_code)
            by_market.setdefault(market, pd.DataFrame())
            by_market[market] = pd.concat([by_market[market], g], ignore_index=True)

        for market, mdf in by_market.items():
            h5_path = self._market_day_file(market)
            self._write_market_day(h5_path, market, mdf)

    # ----------------------------- 内部实现 -----------------------------
    def _calculate_forward_adj(self, df: pd.DataFrame, adj_factor_df: pd.DataFrame) -> pd.DataFrame:
        """计算前复权价格
        
        Args:
            df: 原始价格数据
            adj_factor_df: 复权因子数据
            
        Returns:
            前复权后的价格数据
        """
        # 确保复权因子数据有正确的列
        if 'adj_factor' not in adj_factor_df.columns:
            raise ValueError("复权因子数据缺少 adj_factor 列")
        
        # 确保数据类型一致
        adj_factor_df = adj_factor_df.copy()
        adj_factor_df['trade_date'] = pd.to_datetime(adj_factor_df['trade_date'])

        # 合并数据
        merged = df.merge(
            adj_factor_df[['ts_code', 'trade_date', 'adj_factor']],
            on=['ts_code', 'trade_date'],
            how='left'
        )
        
        # 填充缺失的复权因子（使用前向填充）
        merged['adj_factor'] = merged.groupby('ts_code')['adj_factor'].ffill()
        
        # 如果仍然有缺失值，使用 1.0（不复权）
        merged['adj_factor'] = merged['adj_factor'].fillna(1.0)
        
        # 计算前复权价格
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in merged.columns:
                merged[col] = merged[col] * merged['adj_factor']
        
        # 删除复权因子列
        merged = merged.drop('adj_factor', axis=1)
        
        return merged
    
    def _market_of_ts_code(self, ts_code: str) -> str:
        """从 ts_code 提取市场代码（大写）"""
        try:
            _, market = ts_code.split('.')
            market = market.upper()
            if market in ['SH', 'SZ', 'BJ']:
                return market.lower()
        except Exception:
            pass
        return 'sh'  # 默认

    def _market_day_file(self, market: str) -> Path:
        return self.data_dir / f"{market}_day.h5"

    def _ts_code_to_hikyuu_symbol(self, ts_code: str) -> str:
        """将 ts_code 转换为 Hikyuu 符号格式"""
        try:
            code, market = ts_code.split('.')
            return f"{market.upper()}{code}"
        except Exception:
            return ts_code

    def _prepare_hikyuu_data(self, df: pd.DataFrame) -> np.ndarray:
        """将 DataFrame 转换为 Hikyuu 格式的 numpy 数组"""
        # 确保列存在
        required_cols = ['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必需的列: {missing_cols}")

        # 转换 datetime 为 yyyymmdd0000 格式
        dates = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d') + '0000'
        datetime_values = dates.astype(np.uint64)

        # 转换价格字段（放大并转为 uint32）
        price_fields = ['close', 'high', 'low', 'open']
        price_data = {}
        for field in price_fields:
            price_data[field] = (df[field] * self.PRICE_SCALE).astype(np.uint32)

        # 转换金额和成交量（转为 uint64）
        amount_values = (df['amount'] * self.AMOUNT_SCALE).astype(np.uint64)
        volume_values = df['vol'].astype(np.uint64)

        # 按照 Hikyuu dtype 顺序构建数组
        records = []
        for i in range(len(df)):
            record = (
                price_data['close'].iloc[i],    # closePrice
                datetime_values.iloc[i],         # datetime
                price_data['high'].iloc[i],     # highPrice
                price_data['low'].iloc[i],      # lowPrice
                price_data['open'].iloc[i],     # openPrice
                amount_values.iloc[i],          # transAmount
                volume_values.iloc[i]           # transCount
            )
            records.append(record)

        return np.array(records, dtype=self.DTYPE)

    def _write_market_day(self, h5_path: Path, market: str, mdf: pd.DataFrame) -> None:
        """写入单个市场的日频数据"""
        h5_path.parent.mkdir(parents=True, exist_ok=True)

        with h5py.File(h5_path.as_posix(), 'a') as f:
            # 确保 data 组存在
            if self.DATA_GROUP not in f:
                f.create_group(self.DATA_GROUP)

            data_group = f[self.DATA_GROUP]

            for ts_code, g in mdf.groupby('ts_code'):
                symbol = self._ts_code_to_hikyuu_symbol(ts_code)
                dataset_path = f"/{self.DATA_GROUP}/{symbol}"

                # 准备写入数据
                new_data = self._prepare_hikyuu_data(g)

                if symbol in data_group:
                    # 增量写入：读取现有数据，去重后追加
                    existing_dataset = data_group[symbol]
                    existing_data = existing_dataset[:]
                    
                    # 基于 datetime 去重（保留最新数据）
                    existing_datetimes = set(existing_data['datetime'])
                    new_mask = ~np.isin(new_data['datetime'], existing_datetimes)
                    new_data = new_data[new_mask]

                    if len(new_data) > 0:
                        # 合并数据并排序
                        combined_data = np.concatenate([existing_data, new_data])
                        combined_data = np.sort(combined_data, order='datetime')
                        
                        # 删除旧数据集，创建新的
                        del data_group[symbol]
                        data_group.create_dataset(symbol, data=combined_data, dtype=self.DTYPE)
                        print(f"增量更新 {symbol}: +{len(new_data)} 条记录")
                else:
                    # 新数据集：直接创建
                    data_group.create_dataset(symbol, data=new_data, dtype=self.DTYPE)
                    print(f"新建 {symbol}: {len(new_data)} 条记录")


