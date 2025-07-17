#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量数据加载器 - Backtrader增强工具

提供高效的批量数据加载功能，优化大规模回测的数据获取性能。
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ...common.logging_utils import get_logger
from ...providers import AlphaDataTool
from ..utils.cache_manager import CacheManager
from ..utils.exceptions import BatchLoadingError


class BatchDataLoader:
    """
    批量数据加载器

    为Backtrader提供高效的批量数据加载功能，支持：
    - 统一通过AlphaDataTool进行数据获取
    - 智能缓存，避免重复查询
    - 数据预处理和验证
    - 内存优化管理
    """

    def __init__(
        self,
        alpha_data_tool: AlphaDataTool,
        cache_manager: Optional[CacheManager] = None,
    ):
        """
        初始化批量数据加载器

        Args:
            alpha_data_tool: AlphaDataTool实例，用于统一数据访问
            cache_manager: 可选的缓存管理器
        """
        self.alpha_data_tool = alpha_data_tool
        self.cache_manager = cache_manager or CacheManager()
        self.logger = get_logger("batch_loader")

    def load_stocks_data(
        self,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
        use_cache: bool = True,
        **kwargs,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载多只股票的数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存
            **kwargs: 传递给alpha_data_tool.get_stock_data的其他参数 (如 adjust)

        Returns:
            字典，键为股票代码，值为对应的DataFrame
        """
        self.logger.info(f"开始为 {len(stock_codes)} 只股票批量加载数据...")

        try:
            # 调用AlphaDataTool的批量数据获取方法
            all_stocks_data = self.alpha_data_tool.get_stock_data_batch(
                symbols=stock_codes,
                start_date=start_date,
                end_date=end_date,
                use_cache=use_cache,
                **kwargs,
            )

            self.logger.info(f"成功加载 {len(all_stocks_data)} 只股票的数据")
            return all_stocks_data

        except BatchLoadingError as e:
            self.logger.error(f"批量数据加载失败: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"批量数据加载失败: {e}", exc_info=True)
            raise BatchLoadingError(f"批量数据加载过程中发生错误: {e}") from e

    def _batch_query(
        self, stock_codes: List[str], start_date: date, end_date: date, **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        通过AlphaDataTool执行批量查询
        """
        if not stock_codes:
            return {}

        self.logger.debug(f"通过 AlphaDataTool 查询 {len(stock_codes)} 只股票...")

        try:
            # 日期格式转换为 'YYYY-MM-DD'
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            # 使用AlphaDataTool进行批量查询
            all_data_df = self.alpha_data_tool.get_stock_data(
                symbols=stock_codes,
                start_date=start_date_str,
                end_date=end_date_str,
                **kwargs,
            )
            self.logger.debug(f"查询到 {len(all_data_df)} 条记录")

        except Exception as e:
            self.logger.error(f"通过AlphaDataTool进行批量查询失败: {e}")
            return {}

        # 按股票代码分组
        if all_data_df.empty:
            return {}

        data_by_stock = {
            code: group_df
            for code, group_df in all_data_df.groupby("ts_code")
        }

        return data_by_stock

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

    def _get_cache_key(self, ts_code: str, start_date: date, end_date: date) -> str:
        """
        生成缓存键
        """
        # 移除 table_name, 因为数据源是统一的
        return f"stock_data_{ts_code}_{start_date}_{end_date}"

    def preload_market_data(
        self,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
        batch_size: int = 100,
        **kwargs,
    ) -> Dict[str, pd.DataFrame]:
        """
        预加载市场数据（分批处理大量股票）

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 每批处理的股票数量
            **kwargs: 传递给alpha_data_tool.get_stock_data的其他参数

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
                f"正在处理批次 {batch_num}/{total_batches} ({len(batch_codes)} 只股票)"
            )

            batch_data = self.load_stocks_data(
                batch_codes, start_date, end_date, **kwargs
            )
            all_data.update(batch_data)

        self.logger.info(f"预加载完成: {len(all_data)} 只股票")
        return all_data

    def clear_cache(self):
        """清理缓存"""
        self.cache_manager.clear()
        self.logger.info("批量加载器缓存已清理")

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        return self.cache_manager.get_stats()
