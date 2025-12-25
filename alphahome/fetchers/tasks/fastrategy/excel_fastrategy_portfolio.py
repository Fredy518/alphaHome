#!/usr/bin/env python
# -*- coding: utf-8 -*-

r"""
基金投顾策略组合信息数据采集任务

数据来源: E:\stock\Excel\fastrategy_portfolio.xlsx
数据说明: 基金投顾策略的组合信息数据
保存表名: fastrategy_portfolio (在 excel schema 中)

数据字段:
- channel_code: 渠道代码
- channel_name: 渠道名称
- strategy_code: 策略代码
- strategy_name: 策略名称
- fund_code: 基金代码
- weight: 权重
- rebalancing_date: 调仓日期
"""

import pandas as pd
from typing import Optional

from alphahome.fetchers.sources.excel import ExcelTask
from alphahome.common.task_system.task_decorator import task_register


@task_register()
class ExcelFastategyPortfolioTask(ExcelTask):
    """
    从 Excel 文件读取基金投顾策略组合信息数据。

    数据说明：
    - 包含基金投顾策略的组合信息
    - 数据量: ~42672行
    - 用途: 策略组合分析、权重跟踪、调仓分析等
    """

    # 任务标识
    name = "excel_fastrategy_portfolio"
    description = "基金投顾策略组合信息数据（从Excel读取）"

    # 数据表配置
    table_name = "fastrategy_portfolio"
    schema_name = "excel"  # 使用 excel schema 存储
    primary_keys = ["channel_code", "strategy_code", "fund_code", "rebalancing_date"]
    date_column = "rebalancing_date"
    data_source = "excel"
    domain = "fastrategy"

    # Excel 文件配置
    excel_file_path = r"E:\stock\Excel\fastrategy_portfolio.xlsx"
    header_row = 0  # 第一行为表头

    # 日期列配置
    date_columns = ["rebalancing_date"]

    # 数据表结构定义（使用字典格式）
    schema_def = {
        "channel_code": {"type": "VARCHAR(50)", "constraints": "NOT NULL", "comment": "渠道代码"},
        "channel_name": {"type": "VARCHAR(200)", "comment": "渠道名称"},
        "strategy_code": {"type": "VARCHAR(50)", "constraints": "NOT NULL", "comment": "策略代码"},
        "strategy_name": {"type": "VARCHAR(200)", "comment": "策略名称"},
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL", "comment": "基金代码"},
        "weight": {"type": "DECIMAL(10,6)", "comment": "权重"},
        "rebalancing_date": {"type": "DATE", "constraints": "NOT NULL", "comment": "调仓日期"},
    }

    # 自定义索引
    indexes = [
        {"name": "idx_channel_code", "columns": "channel_code"},
        {"name": "idx_strategy_code", "columns": "strategy_code"},
        {"name": "idx_fund_code", "columns": "fund_code"},
        {"name": "idx_rebalancing_date", "columns": "rebalancing_date"},
        {"name": "idx_channel_strategy", "columns": ["channel_code", "strategy_code"]},
        {"name": "idx_strategy_fund", "columns": ["strategy_code", "fund_code"]},
    ]

    # 数据验证规则
    validation_rules = [
        {
            "name": "渠道代码不能为空",
            "condition": lambda df: df["channel_code"].notna(),
        },
        {
            "name": "策略代码不能为空",
            "condition": lambda df: df["strategy_code"].notna(),
        },
        {
            "name": "基金代码不能为空",
            "condition": lambda df: df["fund_code"].notna(),
        },
        {
            "name": "调仓日期不能为空",
            "condition": lambda df: df["rebalancing_date"].notna(),
        },
        {
            "name": "权重应在0-1范围内",
            "condition": lambda df: (df["weight"].isna()) | ((df["weight"] >= 0) & (df["weight"] <= 1)),
        },
        {
            "name": "相同策略和日期的权重总和应接近1",
            "condition": lambda df: True,  # 此规则需要分组验证，在process_data中实现
        },
    ]

    def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """
        处理基金投顾策略组合信息数据。

        处理步骤:
        1. 调用父类的基础处理
        2. 转换日期格式
        3. 处理数值字段
        4. 清理文本字段
        5. 验证权重合理性

        Args:
            data: 原始数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数

        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 调用父类的基础处理
        data = super().process_data(data, stop_event=stop_event, **kwargs)

        # 转换日期格式为 DATE 类型
        for date_col in self.date_columns:
            if date_col in data.columns:
                # 转换为 DATE 类型，格式为 YYYY-MM-DD
                data[date_col] = pd.to_datetime(data[date_col], errors='coerce').dt.strftime('%Y-%m-%d')

        # 处理数值字段，确保类型正确
        if 'weight' in data.columns:
            data['weight'] = pd.to_numeric(data['weight'], errors='coerce')

        # 清理文本字段，去除多余的空白字符
        text_columns = ['channel_name', 'strategy_name']
        for col in text_columns:
            if col in data.columns:
                data[col] = data[col].apply(
                    lambda x: ' '.join(str(x).split()) if pd.notna(x) else None
                )

        # 验证权重合理性：检查每个策略在每个日期的权重总和
        if 'weight' in data.columns and all(col in data.columns for col in ['channel_code', 'strategy_code', 'rebalancing_date']):
            # 按渠道、策略、调仓日期分组，计算权重总和
            weight_sums = data.groupby(['channel_code', 'strategy_code', 'rebalancing_date'])['weight'].sum()

            # 找出权重总和不接近1的组合（允许0.01的误差）
            invalid_weights = weight_sums[~((weight_sums - 1.0).abs() <= 0.01)]
            if len(invalid_weights) > 0:
                self.logger.warning(f"发现 {len(invalid_weights)} 个权重总和异常的组合:")
                for (channel, strategy, date), total_weight in invalid_weights.items():
                    self.logger.warning(f"  渠道:{channel}, 策略:{strategy}, 日期:{date}, 权重总和:{total_weight:.6f}")

        self.logger.info(f"数据处理完成: {len(data)} 行")

        return data
