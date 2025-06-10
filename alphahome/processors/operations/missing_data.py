#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
缺失值处理操作

提供各种缺失值处理策略，包括填充、删除等。
"""

from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from .base_operation import Operation


class FillNAOperation(Operation):
    """缺失值填充操作

    用于填充数据框中的缺失值(NaN)。

    支持多种填充方法:
    - 'value': 使用指定值填充
    - 'mean': 使用列平均值填充
    - 'median': 使用列中位数填充
    - 'mode': 使用列众数填充
    - 'ffill': 使用前向填充
    - 'bfill': 使用后向填充
    - 'interpolate': 使用插值法填充
    """

    def __init__(
        self,
        method: str = "value",
        columns: Optional[List[str]] = None,
        value: Any = 0,
        limit: Optional[int] = None,
        group_by: Optional[List[str]] = None,
    ):
        """初始化缺失值填充操作

        Args:
            method: 填充方法，可选值: 'value', 'mean', 'median', 'mode', 'ffill', 'bfill', 'interpolate'
            columns: 要处理的列名列表，None表示处理所有列
            value: 使用method='value'时的填充值
            limit: 限制填充的最大连续缺失值数量
            group_by: 分组计算的列名列表(适用于method='mean', 'median', 'mode')
        """
        super().__init__(name="FillNA")
        self.method = method
        self.columns = columns
        self.value = value
        self.limit = limit
        self.group_by = group_by

    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用缺失值填充操作

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 填充缺失值后的数据框
        """
        if data is None or data.empty:
            return pd.DataFrame()

        # 创建数据副本
        result = data.copy()

        # 确定要处理的列
        target_columns = self.columns
        if target_columns is None:
            # 默认处理所有数值列
            target_columns = result.select_dtypes(include=["number"]).columns.tolist()

        # 检查目标列是否存在
        existing_columns = [col for col in target_columns if col in result.columns]
        if len(existing_columns) == 0:
            self.logger.warning("没有找到要处理的列")
            return result

        # 检查是否有缺失值
        na_counts = result[existing_columns].isna().sum()
        if na_counts.sum() == 0:
            self.logger.info("数据中没有缺失值，无需处理")
            return result

        # 记录缺失值数量
        for col, count in na_counts.items():
            if count > 0:
                self.logger.info(f"列 '{col}' 有 {count} 个缺失值")

        # 根据不同方法填充缺失值
        if self.method == "value":
            result[existing_columns] = result[existing_columns].fillna(self.value)

        elif self.method in ["mean", "median", "mode"]:
            # 按分组处理
            if self.group_by:
                for col in existing_columns:
                    for name, group in result.groupby(self.group_by):
                        if self.method == "mean":
                            fill_value = group[col].mean()
                        elif self.method == "median":
                            fill_value = group[col].median()
                        else:  # mode
                            fill_value = (
                                group[col].mode().iloc[0]
                                if not group[col].mode().empty
                                else None
                            )

                        if pd.notna(fill_value):
                            group_idx = group.index
                            na_idx = result.loc[group_idx, col].isna()
                            result.loc[group_idx[na_idx], col] = fill_value
            else:
                # 不分组处理
                for col in existing_columns:
                    if self.method == "mean":
                        fill_value = result[col].mean()
                    elif self.method == "median":
                        fill_value = result[col].median()
                    else:  # mode
                        fill_value = (
                            result[col].mode().iloc[0]
                            if not result[col].mode().empty
                            else None
                        )

                    if pd.notna(fill_value):
                        result[col] = result[col].fillna(fill_value)

        elif self.method == "ffill":
            result[existing_columns] = result[existing_columns].fillna(
                method="ffill", limit=self.limit
            )

        elif self.method == "bfill":
            result[existing_columns] = result[existing_columns].fillna(
                method="bfill", limit=self.limit
            )

        elif self.method == "interpolate":
            for col in existing_columns:
                result[col] = result[col].interpolate(limit=self.limit)

        else:
            self.logger.warning(f"未知的填充方法: {self.method}")

        # 计算处理后的缺失值数量
        post_na_counts = result[existing_columns].isna().sum()
        filled_counts = na_counts - post_na_counts

        # 记录处理结果
        for col, count in filled_counts.items():
            if count > 0:
                self.logger.info(f"列 '{col}' 填充了 {count} 个缺失值")

        return result


class DropNAOperation(Operation):
    """缺失值删除操作

    删除包含缺失值的行或列
    """

    def __init__(
        self,
        axis: int = 0,
        how: str = "any",
        columns: Optional[List[str]] = None,
        thresh: Optional[int] = None,
    ):
        """初始化缺失值删除操作

        Args:
            axis: 0表示删除行，1表示删除列
            how: 'any'表示存在任何NA就删除，'all'表示全部为NA才删除
            columns: 要考虑的列名列表，None表示所有列
            thresh: 非NA值的最小数量阈值，低于此值则删除
        """
        super().__init__(name="DropNA")
        self.axis = axis
        self.how = how
        self.columns = columns
        self.thresh = thresh

    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用缺失值删除操作

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 删除缺失值后的数据框
        """
        if data is None or data.empty:
            return pd.DataFrame()

        # 创建数据副本
        result = data.copy()

        # 确定要考虑的列
        subset = self.columns

        # 检查是否有缺失值
        if subset:
            existing_columns = [col for col in subset if col in result.columns]
            na_counts = result[existing_columns].isna().sum()
        else:
            na_counts = result.isna().sum()

        if na_counts.sum() == 0:
            self.logger.info("数据中没有缺失值，无需处理")
            return result

        # 记录删除前的行数和列数
        rows_before = len(result)
        cols_before = len(result.columns)

        # 删除缺失值
        result = result.dropna(
            axis=self.axis, how=self.how, subset=subset, thresh=self.thresh
        )

        # 记录删除后的行数和列数
        rows_after = len(result)
        cols_after = len(result.columns)

        # 记录处理结果
        if self.axis == 0:
            rows_removed = rows_before - rows_after
            self.logger.info(
                f"删除了 {rows_removed} 行 ({rows_removed/rows_before:.2%})"
            )
        else:
            cols_removed = cols_before - cols_after
            self.logger.info(
                f"删除了 {cols_removed} 列 ({cols_removed/cols_before:.2%})"
            )

        return result
