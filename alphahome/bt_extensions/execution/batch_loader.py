#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量数据加载器 - Backtrader增强工具

提供高效的批量数据加载功能，优化大规模回测的数据获取性能。
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ...common.db_manager import create_sync_manager
from ...common.logging_utils import get_logger
from ..utils.cache_manager import CacheManager


class BatchDataLoader:
    """
    批量数据加载器

    为Backtrader提供高效的批量数据加载功能，支持：
    - 批量SQL查询，减少数据库连接次数
    - 智能缓存，避免重复查询
    - 数据预处理和验证
    - 内存优化管理
    """

    def __init__(self, db_manager, cache_manager: Optional[CacheManager] = None):
        """
        初始化批量数据加载器

        Args:
            db_manager: 同步数据库管理器
            cache_manager: 可选的缓存管理器
        """
        self.db_manager = db_manager
        self.cache_manager = cache_manager or CacheManager()
        self.logger = get_logger("batch_loader")

    def load_stocks_data(
        self,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
        table_name: str = "tushare_stock_daily",
        use_cache: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载多只股票的数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            table_name: 数据表名
            use_cache: 是否使用缓存

        Returns:
            字典，键为股票代码，值为对应的DataFrame
        """
        self.logger.info(f"批量加载 {len(stock_codes)} 只股票数据")

        result = {}
        uncached_codes = []

        # 1. 检查缓存
        if use_cache:
            for code in stock_codes:
                cache_key = self._get_cache_key(code, start_date, end_date, table_name)
                cached_data = self.cache_manager.get(cache_key)
                if cached_data is not None:
                    result[code] = cached_data
                    self.logger.debug(f"从缓存加载: {code}")
                else:
                    uncached_codes.append(code)
        else:
            uncached_codes = stock_codes

        # 2. 批量查询未缓存的数据
        if uncached_codes:
            batch_data = self._batch_query(
                uncached_codes, start_date, end_date, table_name
            )

            # 3. 数据预处理和缓存
            for code, df in batch_data.items():
                processed_df = self._preprocess_data(df)
                result[code] = processed_df

                # 缓存处理后的数据
                if use_cache:
                    cache_key = self._get_cache_key(
                        code, start_date, end_date, table_name
                    )
                    self.cache_manager.set(cache_key, processed_df)

        self.logger.info(f"批量加载完成: {len(result)}/{len(stock_codes)} 只股票")
        return result

    def _batch_query(
        self, stock_codes: List[str], start_date: date, end_date: date, table_name: str
    ) -> Dict[str, pd.DataFrame]:
        """
        执行批量SQL查询
        """
        if not stock_codes:
            return {}

        # 构建批量查询SQL（同步模式使用 %s 占位符）
        placeholders = ",".join(["%s" for _ in range(len(stock_codes))])
        sql = f"""
        SELECT ts_code, trade_date, open, high, low, close, volume, amount
        FROM {table_name} 
        WHERE ts_code IN ({placeholders})
          AND trade_date BETWEEN %s AND %s
        ORDER BY ts_code, trade_date
        """

        params = list(stock_codes) + [start_date, end_date]

        try:
            records = self.db_manager.fetch(sql, tuple(params))
            self.logger.debug(f"查询到 {len(records)} 条记录")
        except Exception as e:
            self.logger.error(f"批量查询失败: {e}")
            return {}

        # 按股票代码分组
        data_by_stock = {}
        for record in records:
            ts_code = record["ts_code"]
            if ts_code not in data_by_stock:
                data_by_stock[ts_code] = []

            data_by_stock[ts_code].append(
                {
                    "trade_date": record["trade_date"],
                    "open": float(record["open"]),
                    "high": float(record["high"]),
                    "low": float(record["low"]),
                    "close": float(record["close"]),
                    "volume": float(record["volume"]),
                    "amount": float(record.get("amount", 0)),
                }
            )

        # 转换为DataFrame
        result = {}
        for ts_code, data_list in data_by_stock.items():
            df = pd.DataFrame(data_list)
            result[ts_code] = df

        return result

    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据预处理
        """
        if df.empty:
            return df

        # 确保日期列为datetime类型
        df["datetime"] = pd.to_datetime(df["trade_date"])

        # 确保数值列为float类型
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 处理缺失值
        df = df.dropna(subset=numeric_columns)

        # 按日期排序
        df = df.sort_values("datetime").reset_index(drop=True)

        # 数据验证
        self._validate_data(df)

        return df

    def _validate_data(self, df: pd.DataFrame):
        """
        数据验证
        """
        if df.empty:
            return

        # 检查OHLC数据的合理性
        invalid_ohlc = ~(
            (df["low"] <= df["open"])
            & (df["open"] <= df["high"])
            & (df["low"] <= df["close"])
            & (df["close"] <= df["high"])
        )

        if invalid_ohlc.any():
            invalid_count = invalid_ohlc.sum()
            self.logger.warning(f"发现 {invalid_count} 条异常OHLC数据")

        # 检查负成交量
        negative_volume = df["volume"] < 0
        if negative_volume.any():
            negative_count = negative_volume.sum()
            self.logger.warning(f"发现 {negative_count} 条负成交量数据")

    def _get_cache_key(
        self, ts_code: str, start_date: date, end_date: date, table_name: str
    ) -> str:
        """
        生成缓存键
        """
        return f"{table_name}_{ts_code}_{start_date}_{end_date}"

    def preload_market_data(
        self,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
        batch_size: int = 100,
        table_name: str = "tushare_stock_daily",
    ) -> Dict[str, pd.DataFrame]:
        """
        预加载市场数据（分批处理大量股票）

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 每批处理的股票数量
            table_name: 数据表名

        Returns:
            所有股票的数据字典
        """
        self.logger.info(
            f"预加载 {len(stock_codes)} 只股票数据，批次大小: {batch_size}"
        )

        all_data = {}

        # 分批处理
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(stock_codes) + batch_size - 1) // batch_size

            self.logger.info(
                f"处理批次 {batch_num}/{total_batches}: {len(batch_codes)} 只股票"
            )

            try:
                batch_data = self.load_stocks_data(
                    batch_codes, start_date, end_date, table_name
                )
                all_data.update(batch_data)

                self.logger.info(f"批次 {batch_num} 完成: {len(batch_data)} 只股票")

            except Exception as e:
                self.logger.error(f"批次 {batch_num} 处理失败: {e}")
                continue

        self.logger.info(f"预加载完成: {len(all_data)} 只股票")
        return all_data

    def clear_cache(self):
        """清理缓存"""
        self.cache_manager.clear()
        self.logger.info("批量加载器缓存已清理")

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        return self.cache_manager.get_stats()
