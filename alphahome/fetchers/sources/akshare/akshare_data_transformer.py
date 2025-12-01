#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 数据转换器

负责处理 akshare 返回数据的转换工作，包括：
- 中文表头转英文
- 宽表转长表（melt 操作）
- 日期格式标准化
- 数据类型转换
"""

import logging
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from .akshare_task import AkShareTask


class AkShareDataTransformer:
    """
    AkShare 数据转换器

    处理 akshare 返回数据的标准化工作，支持：
    1. 中文列名到英文列名的映射
    2. 宽表格式转换为长表格式（适合数据库存储）
    3. 日期格式标准化
    4. 数据类型转换
    """

    def __init__(self, task_instance: Any) -> None:
        """
        初始化转换器

        Args:
            task_instance: 关联的 AkShareTask 实例，用于获取转换配置
        """
        self.task = task_instance
        self.logger = (
            self.task.logger
            if hasattr(self.task, "logger")
            else logging.getLogger(__name__)
        )

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理 akshare 返回的原始数据

        处理流程：
        1. 应用中文列名映射
        2. 宽表转长表（如果配置了 melt 参数）
        3. 日期格式标准化
        4. 应用自定义转换规则

        Args:
            data: akshare 返回的原始 DataFrame
            **kwargs: 额外参数

        Returns:
            处理后的 DataFrame
        """
        if data is None or data.empty:
            self.logger.info("process_data: 输入数据为空，跳过处理")
            return pd.DataFrame()

        self.logger.debug(f"开始数据转换，原始数据形状: {data.shape}")
        self.logger.debug(f"原始列名: {list(data.columns)}")

        # 1. 应用中文列名映射
        data = self._apply_column_mapping(data)

        # 2. 宽表转长表（如果配置了）
        if hasattr(self.task, "melt_config") and self.task.melt_config:
            data = self._melt_wide_to_long(data, self.task.melt_config)

        # 3. 日期格式标准化
        data = self._convert_date_columns(data)

        # 4. 应用自定义转换规则
        data = self._apply_transformations(data)

        self.logger.debug(f"数据转换完成，最终数据形状: {data.shape}")
        return data

    def _apply_column_mapping(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        应用中文到英文的列名映射

        Args:
            data: 原始数据

        Returns:
            列名映射后的数据
        """
        if not hasattr(self.task, "column_mapping") or not self.task.column_mapping:
            self.logger.debug("没有配置 column_mapping，跳过列名映射")
            return data

        # 获取列名映射
        mapping = self.task.column_mapping
        self.logger.debug(f"开始列名映射，映射规则数量: {len(mapping)}")

        # 检查哪些列存在于数据中
        existing_cols = {col for col in mapping.keys() if col in data.columns}
        missing_cols = set(mapping.keys()) - existing_cols

        self.logger.debug(f"数据中的列数量: {len(data.columns)}")
        self.logger.debug(f"找到的匹配列数量: {len(existing_cols)}")
        if missing_cols:
            self.logger.debug(f"列名映射中有 {len(missing_cols)} 个列不在数据中")

        # 只映射存在的列
        rename_map = {k: v for k, v in mapping.items() if k in data.columns}

        if rename_map:
            self.logger.debug(f"应用列名映射: {len(rename_map)} 个列")
            # 记录前几个映射示例
            sample_maps = list(rename_map.items())[:3]
            for old, new in sample_maps:
                self.logger.debug(f"  {repr(old)} -> {repr(new)}")

            data = data.rename(columns=rename_map)
            self.logger.debug("列名映射完成")
        else:
            self.logger.debug("没有找到需要映射的列")

        return data

    def _melt_wide_to_long(
        self,
        data: pd.DataFrame,
        melt_config: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        将宽表格式转换为长表格式

        melt_config 配置说明：
        {
            "id_vars": ["date"],           # 保持不变的列（标识列）
            "value_vars": ["col1", "col2"], # 要转换的列（如果为 None，则自动推断）
            "var_name": "category",         # 转换后的变量列名
            "value_name": "value",          # 转换后的值列名
            "var_parser": callable,         # 可选：解析变量名的函数
        }

        Args:
            data: 宽表格式的数据
            melt_config: melt 配置

        Returns:
            长表格式的数据
        """
        id_vars = melt_config.get("id_vars", [])
        value_vars = melt_config.get("value_vars")
        var_name = melt_config.get("var_name", "variable")
        value_name = melt_config.get("value_name", "value")
        var_parser = melt_config.get("var_parser")

        # 如果没有指定 value_vars，使用除 id_vars 之外的所有列
        if value_vars is None:
            value_vars = [col for col in data.columns if col not in id_vars]

        self.logger.debug(
            f"宽表转长表: id_vars={id_vars}, value_vars={len(value_vars)}列, "
            f"var_name={var_name}, value_name={value_name}"
        )

        # 执行 melt 操作
        melted = pd.melt(
            data,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name
        )

        # 如果提供了变量解析器，应用它
        if var_parser and callable(var_parser):
            try:
                # 解析器应返回一个 DataFrame 或 Series
                parsed_result = var_parser(melted[var_name])

                if isinstance(parsed_result, pd.DataFrame):
                    # 如果返回 DataFrame，合并到结果中
                    for col in parsed_result.columns:
                        melted[col] = parsed_result[col]
                    # 删除原始的变量列
                    melted = melted.drop(columns=[var_name])
                elif isinstance(parsed_result, pd.Series):
                    # 如果返回 Series，替换变量列
                    melted[var_name] = parsed_result
                else:
                    self.logger.warning(f"var_parser 返回了意外的类型: {type(parsed_result)}")

                self.logger.debug("已应用变量解析器")
            except Exception as e:
                self.logger.error(f"变量解析器执行失败: {e}", exc_info=True)

        self.logger.info(f"宽表转长表完成: {data.shape} -> {melted.shape}")
        return melted

    def _convert_date_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        根据 schema_def 自动转换日期列

        Args:
            data: 数据

        Returns:
            日期列转换后的数据
        """
        if not hasattr(self.task, "schema_def") or not self.task.schema_def:
            return data

        for col, definition in self.task.schema_def.items():
            col_type = definition.get("type", "").upper()

            if col in data.columns and (col_type.startswith("DATE") or col_type.startswith("TIMESTAMP")):
                if pd.api.types.is_datetime64_any_dtype(data[col]):
                    continue

                self.logger.debug(f"自动转换日期列: {col}")

                original_nan_count = data[col].isna().sum()

                # 替换空字符串为 None
                if data[col].dtype == 'object':
                    data[col] = data[col].replace('', None)

                # 尝试转换日期
                converted_col = pd.to_datetime(data[col], errors='coerce')

                new_nan_count = converted_col.isna().sum()

                if new_nan_count > original_nan_count:
                    self.logger.warning(
                        f"列 '{col}' 在日期转换中有 {new_nan_count - original_nan_count} 个值无法解析"
                    )

                data[col] = converted_col

        return data

    def _apply_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        应用任务定义的数据转换规则

        Args:
            data: 数据

        Returns:
            转换后的数据
        """
        if not hasattr(self.task, "transformations") or not self.task.transformations:
            return data

        self.logger.debug(f"开始应用 {len(self.task.transformations)} 个转换规则")

        for column, transform_func in self.task.transformations.items():
            if column not in data.columns:
                continue

            try:
                # 定义安全转换函数
                def safe_transform(x, func=transform_func):
                    if pd.isna(x):
                        return np.nan
                    try:
                        return func(x)
                    except Exception as e:
                        self.logger.warning(f"转换值 '{x}' 时失败: {e}")
                        return np.nan

                data[column] = data[column].apply(safe_transform)
                self.logger.debug(f"已转换列: {column}")

            except Exception as e:
                self.logger.error(f"处理列 '{column}' 时发生错误: {e}", exc_info=True)

        return data

    @staticmethod
    def parse_bond_column_name(column_series: pd.Series) -> pd.DataFrame:
        """
        解析债券收益率列名，提取国家和期限信息

        支持两种格式：
        1. 中文格式： "中国国债收益率2年" -> country="CN", term="2y"
        2. 英文格式： "CN_10y" -> country="CN", term="10y"

        Args:
            column_series: 包含原始列名的 Series

        Returns:
            包含 country 和 term 列的 DataFrame
        """
        result = pd.DataFrame(index=column_series.index)

        def parse_single(col_name: str) -> Tuple[str, str]:
            col_name = str(col_name)

            # 首先尝试解析英文格式 (CN_10y, US_2y)
            english_match = re.match(r'^([A-Z]{2})_(\d+)([my])$', col_name)
            if english_match:
                country = english_match.group(1)
                num = english_match.group(2)
                unit = english_match.group(3)
                term = f"{num}{unit}"
                return country, term

            # 回退到中文格式解析
            # 提取国家
            if "中国" in col_name:
                country = "CN"
            elif "美国" in col_name:
                country = "US"
            else:
                country = "OTHER"

            # 提取期限
            term_match = re.search(r'(\d+)(年|月)', col_name)
            if term_match:
                num = term_match.group(1)
                unit = term_match.group(2)
                if unit == "年":
                    term = f"{num}y"
                else:
                    term = f"{num}m"
            else:
                term = "unknown"

            return country, term

        parsed = column_series.apply(parse_single)
        result["country"] = parsed.apply(lambda x: x[0])
        result["term"] = parsed.apply(lambda x: x[1])

        return result

    @staticmethod
    def create_bond_var_parser() -> Callable[[pd.Series], pd.DataFrame]:
        """
        创建债券列名解析器

        Returns:
            用于 melt_config.var_parser 的解析函数
        """
        return AkShareDataTransformer.parse_bond_column_name

