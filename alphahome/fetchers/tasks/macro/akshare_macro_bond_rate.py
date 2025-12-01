#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中美国债收益率数据任务

使用 akshare.bond_zh_us_rate 获取中美国债收益率历史数据。
数据从 1990-12-19 开始，包含多个期限的国债收益率。

原始数据为宽表格式（每列一个期限），转换为长表格式存储：
- date: 日期
- country: 国家 (CN/US)
- term: 期限 (3m/6m/1y/2y/3y/5y/7y/10y/20y/30y)
- yield: 收益率
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareSingleBatchTask
from ...sources.akshare.akshare_data_transformer import AkShareDataTransformer
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroBondRateTask(AkShareSingleBatchTask):
    """
    获取中美国债收益率数据

    数据来源：东方财富
    更新频率：每日更新
    数据范围：1990-12-19 至今
    """

    # 1. 核心属性
    domain = "macro"
    name = "akshare_macro_bond_rate"
    description = "中美国债收益率数据（AkShare）"
    table_name = "macro_bond_rate"
    primary_keys = ["date", "country", "term"]
    date_column = "date"
    default_start_date = "19901219"

    # 2. AkShare 特有属性
    api_name = "bond_zh_us_rate"

    # 固定的 API 参数（start_date 会在运行时传入）
    api_params = {}

    # 3. 列名映射（已在 AkShareAPI 中处理，这里保留为空）
    column_mapping = {}

    # 4. 宽表转长表配置
    # 原始数据格式：date | CN_2y | CN_5y | ... | US_2y | ...
    # 目标格式：date | country | term | yield
    melt_config = {
        "id_vars": ["date"],  # 保持不变的列
        "value_vars": [  # 明确指定需要转换的收益率列（GDP 列不参与转换）
            "CN_2y", "CN_5y", "CN_10y", "CN_30y",
            "US_2y", "US_5y", "US_10y", "US_30y"
        ],
        "var_name": "_original_col",  # 临时变量名
        "value_name": "yield",  # 值列名
        "var_parser": AkShareDataTransformer.create_bond_var_parser(),  # 解析列名
    }

    # 5. 数据类型转换
    transformations = {
        "yield": float,
    }

    # 6. 数据库表结构
    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL"},
        "country": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "term": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "yield": {"type": "NUMERIC(10,4)"},
        # update_time 会自动添加
    }

    # 7. 自定义索引
    indexes = [
        {"name": "idx_bond_rate_date", "columns": "date"},
        {"name": "idx_bond_rate_country", "columns": "country"},
        {"name": "idx_bond_rate_term", "columns": "term"},
        {"name": "idx_bond_rate_update_time", "columns": "update_time"},
    ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成批次参数列表

        bond_zh_us_rate 接口只需要 start_date 参数，一次返回所有历史数据。
        为了支持增量更新，我们传入起始日期。

        Args:
            **kwargs: 包含 start_date, end_date 等参数

        Returns:
            批次参数列表（单批次）
        """
        # 获取起始日期
        start_date = kwargs.get("start_date", self.default_start_date)

        # 确保日期格式正确（akshare 期望 YYYYMMDD 格式）
        if start_date:
            try:
                # 标准化日期格式
                start_dt = pd.to_datetime(start_date)
                start_date = start_dt.strftime("%Y%m%d")
            except Exception as e:
                self.logger.warning(f"日期格式转换失败: {e}，使用默认日期")
                start_date = self.default_start_date

        batch_params = {
            "start_date": start_date,
        }

        self.logger.info(
            f"任务 {self.name}: 生成批次参数: start_date={start_date}"
        )

        return [batch_params]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理数据的额外逻辑

        主要的转换（列映射、宽转长）已在 AkShareDataTransformer 中完成。
        这里进行一些额外的数据清洗。

        Args:
            data: 经过转换器处理后的数据
            **kwargs: 额外参数

        Returns:
            最终处理后的数据
        """
        if data is None or data.empty:
            self.logger.info(f"任务 {self.name}: 输入数据为空")
            return data

        # 调用基类处理
        data = super().process_data(data, **kwargs)

        # 过滤掉无效数据
        initial_rows = len(data)

        # 1. 移除 country 或 term 为空的行
        data = data.dropna(subset=["country", "term"])

        # 2. 移除 country 为 OTHER 的行（无法识别的列）
        data = data[data["country"] != "OTHER"]

        # 3. 移除 term 为 unknown 的行
        data = data[data["term"] != "unknown"]

        # 4. 移除 yield 为 NULL 的行
        data = data.dropna(subset=["yield"])

        # 5. 过滤掉 yield <= 0 的数据（国债收益率通常为正值）
        data = data[data["yield"] > 0]

        dropped_rows = initial_rows - len(data)
        if dropped_rows > 0:
            self.logger.info(f"任务 {self.name}: 过滤掉 {dropped_rows} 行无效数据")

        # 4. 确保日期列格式正确
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"]).dt.date

        self.logger.info(
            f"任务 {self.name}: 数据处理完成，共 {len(data)} 行"
        )

        return data

    # 8. 数据验证规则
    validations = [
        (lambda df: df["date"].notna(), "日期不能为空"),
        (lambda df: df["country"].notna(), "国家不能为空"),
        (lambda df: df["term"].notna(), "期限不能为空"),
        (lambda df: df["country"].isin(["CN", "US"]), "国家应为 CN 或 US"),
        # 收益率合理性验证（允许为空，但如果有值应在合理范围内）
        (
            lambda df: df["yield"].isna() | ((df["yield"] >= -10) & (df["yield"] <= 50)),
            "收益率应在 -10% 到 50% 范围内"
        ),
    ]

    # 9. 验证模式配置
    validation_mode = "report"

