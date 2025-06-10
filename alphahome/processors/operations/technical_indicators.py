#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
技术指标计算操作

提供各种金融技术指标的计算操作，如移动平均线、相对强弱指标等。
"""

from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from .base_operation import Operation


class MovingAverageOperation(Operation):
    """移动平均线计算操作

    计算数据的简单移动平均线(SMA)。
    可以计算价格、成交量或其他数值列的移动平均线。
    """

    def __init__(
        self,
        window: int = 5,
        column: str = "close",
        result_column: Optional[str] = None,
        min_periods: Optional[int] = None,
        center: bool = False,
        group_by: Optional[List[str]] = None,
    ):
        """初始化移动平均线操作

        Args:
            window: 窗口大小(天数)
            column: 要计算移动平均的列名
            result_column: 结果列名，默认为'{column}_ma{window}'
            min_periods: 最小观测值数量，默认为window
            center: 是否使用居中窗口
            group_by: 分组计算的列名列表，常用于按股票代码分组
        """
        super().__init__(name=f"MA{window}")
        self.window = window
        self.column = column
        self.result_column = result_column or f"{column}_ma{window}"
        self.min_periods = min_periods or window
        self.center = center
        self.group_by = group_by

    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用移动平均线计算

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 添加了移动平均线的数据框
        """
        if data is None or data.empty:
            return pd.DataFrame()

        # 创建数据副本
        result = data.copy()

        # 检查输入列是否存在
        if self.column not in result.columns:
            self.logger.error(f"列 '{self.column}' 不存在")
            return result

        # 按分组计算
        if self.group_by:
            # 检查分组列是否存在
            missing_columns = [
                col for col in self.group_by if col not in result.columns
            ]
            if missing_columns:
                self.logger.error(f"分组列不存在: {missing_columns}")
                return result

            # 分组计算移动平均线
            grouped = result.groupby(self.group_by)
            result[self.result_column] = np.nan  # 初始化结果列

            for name, group in grouped:
                ma_values = (
                    group[self.column]
                    .rolling(
                        window=self.window,
                        min_periods=self.min_periods,
                        center=self.center,
                    )
                    .mean()
                )

                # 更新结果列
                result.loc[group.index, self.result_column] = ma_values

        else:
            # 不分组，直接计算
            result[self.result_column] = (
                result[self.column]
                .rolling(
                    window=self.window, min_periods=self.min_periods, center=self.center
                )
                .mean()
            )

        # 记录结果
        null_count = result[self.result_column].isna().sum()
        valid_count = len(result) - null_count
        self.logger.info(
            f"计算了 {valid_count} 个 {self.result_column} 值，{null_count} 个空值"
        )

        return result


class RSIOperation(Operation):
    """相对强弱指标(RSI)计算操作

    计算相对强弱指数(Relative Strength Index)。
    RSI = 100 - 100 / (1 + RS)
    其中RS是一段时间内平均上涨幅度与平均下跌幅度的比值。
    """

    def __init__(
        self,
        window: int = 14,
        column: str = "close",
        result_column: Optional[str] = None,
        group_by: Optional[List[str]] = None,
    ):
        """初始化RSI计算操作

        Args:
            window: 周期(天数)
            column: 要计算RSI的价格列名
            result_column: 结果列名，默认为'rsi{window}'
            group_by: 分组计算的列名列表
        """
        super().__init__(name=f"RSI{window}")
        self.window = window
        self.column = column
        self.result_column = result_column or f"rsi{window}"
        self.group_by = group_by

    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用RSI计算

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 添加了RSI的数据框
        """
        if data is None or data.empty:
            return pd.DataFrame()

        # 创建数据副本
        result = data.copy()

        # 检查输入列是否存在
        if self.column not in result.columns:
            self.logger.error(f"列 '{self.column}' 不存在")
            return result

        # 初始化结果列
        result[self.result_column] = np.nan

        # RSI计算函数
        def calculate_rsi(prices):
            # 计算价格变化
            deltas = prices.diff()
            # 分离上涨和下跌
            gain = deltas.where(deltas > 0, 0)
            loss = -deltas.where(deltas < 0, 0)

            # 计算平均上涨和下跌
            avg_gain = gain.rolling(window=self.window).mean()
            avg_loss = loss.rolling(window=self.window).mean()

            # 计算相对强度
            rs = avg_gain / avg_loss

            # 计算RSI
            rsi = 100 - (100 / (1 + rs))
            return rsi

        # 按分组计算
        if self.group_by:
            # 检查分组列是否存在
            missing_columns = [
                col for col in self.group_by if col not in result.columns
            ]
            if missing_columns:
                self.logger.error(f"分组列不存在: {missing_columns}")
                return result

            # 分组计算RSI
            for name, group in result.groupby(self.group_by):
                # 按时间排序
                if "trade_date" in group.columns:
                    group = group.sort_values("trade_date")

                # 计算RSI
                rsi_values = calculate_rsi(group[self.column])

                # 更新结果列
                result.loc[group.index, self.result_column] = rsi_values

        else:
            # 不分组，直接计算
            # 先按时间排序(如果有日期列)
            if "trade_date" in result.columns:
                result = result.sort_values("trade_date")

            # 计算RSI
            result[self.result_column] = calculate_rsi(result[self.column])

        # 记录结果
        null_count = result[self.result_column].isna().sum()
        valid_count = len(result) - null_count
        self.logger.info(
            f"计算了 {valid_count} 个 {self.result_column} 值，{null_count} 个空值"
        )

        return result


class MACDOperation(Operation):
    """MACD指标计算操作

    计算平滑异同移动平均线(Moving Average Convergence Divergence)。
    MACD由三部分组成:
    1. MACD线: 快速EMA - 慢速EMA
    2. 信号线: MACD线的EMA
    3. 柱状图: MACD线 - 信号线
    """

    def __init__(
        self,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        column: str = "close",
        macd_col: Optional[str] = None,
        signal_col: Optional[str] = None,
        hist_col: Optional[str] = None,
        group_by: Optional[List[str]] = None,
    ):
        """初始化MACD计算操作

        Args:
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期
            column: 要计算MACD的价格列名
            macd_col: MACD线结果列名，默认为'macd'
            signal_col: 信号线结果列名，默认为'macd_signal'
            hist_col: 柱状图结果列名，默认为'macd_hist'
            group_by: 分组计算的列名列表
        """
        super().__init__(name=f"MACD")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.column = column
        self.macd_col = macd_col or "macd"
        self.signal_col = signal_col or "macd_signal"
        self.hist_col = hist_col or "macd_hist"
        self.group_by = group_by

    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用MACD计算

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 添加了MACD指标的数据框
        """
        if data is None or data.empty:
            return pd.DataFrame()

        # 创建数据副本
        result = data.copy()

        # 检查输入列是否存在
        if self.column not in result.columns:
            self.logger.error(f"列 '{self.column}' 不存在")
            return result

        # 初始化结果列
        result[self.macd_col] = np.nan
        result[self.signal_col] = np.nan
        result[self.hist_col] = np.nan

        # MACD计算函数
        def calculate_macd(prices):
            # 计算快速和慢速EMA
            ema_fast = prices.ewm(span=self.fast_period, adjust=False).mean()
            ema_slow = prices.ewm(span=self.slow_period, adjust=False).mean()

            # 计算MACD线
            macd_line = ema_fast - ema_slow

            # 计算信号线
            signal_line = macd_line.ewm(span=self.signal_period, adjust=False).mean()

            # 计算柱状图
            histogram = macd_line - signal_line

            return macd_line, signal_line, histogram

        # 按分组计算
        if self.group_by:
            # 检查分组列是否存在
            missing_columns = [
                col for col in self.group_by if col not in result.columns
            ]
            if missing_columns:
                self.logger.error(f"分组列不存在: {missing_columns}")
                return result

            # 分组计算MACD
            for name, group in result.groupby(self.group_by):
                # 按时间排序
                if "trade_date" in group.columns:
                    group = group.sort_values("trade_date")

                # 计算MACD
                macd_line, signal_line, histogram = calculate_macd(group[self.column])

                # 更新结果列
                result.loc[group.index, self.macd_col] = macd_line
                result.loc[group.index, self.signal_col] = signal_line
                result.loc[group.index, self.hist_col] = histogram

        else:
            # 不分组，直接计算
            # 先按时间排序(如果有日期列)
            if "trade_date" in result.columns:
                result = result.sort_values("trade_date")

            # 计算MACD
            macd_line, signal_line, histogram = calculate_macd(result[self.column])

            # 更新结果列
            result[self.macd_col] = macd_line
            result[self.signal_col] = signal_line
            result[self.hist_col] = histogram

        # 记录结果
        null_count = result[self.macd_col].isna().sum()
        valid_count = len(result) - null_count
        self.logger.info(f"计算了 {valid_count} 个 MACD 指标值，{null_count} 个空值")

        return result
