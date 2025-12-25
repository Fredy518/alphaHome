#!/usr/bin/env python
# -*- coding: utf-8 -*-

r"""
基金投顾策略基本信息数据采集任务

数据来源: E:\stock\Excel\fastrategy_basic.xlsx
数据说明: 基金投顾策略的基本信息数据
保存表名: fastrategy_basic (在 excel schema 中)

数据字段:
- strategy_code: 策略代码
- strategy_name: 策略名称
- channel_code: 渠道代码
- channel_name: 渠道名称
- fee_rate: 费率
- setup_date: 成立日期
- is_regulation: 是否报送监管
- risk_type: 风险类型
- risk_level: 风险等级
- is_tracking: 是否跟踪
- redemption_days_strategy: 策略赎回天数
- redemption_days_fund: 基金赎回天数
- channel_sort: 渠道排序
- strategy_sort: 策略排序
- is_official: 是否官方
- docking_source_channel: 对接渠道来源
- docking_source_strategy: 对接策略来源
- docking_nav_date: 对接净值日期
- main_strategy_code: 主策略代码
"""

import pandas as pd
from typing import Optional

from alphahome.fetchers.sources.excel import ExcelTask
from alphahome.common.task_system.task_decorator import task_register


@task_register()
class ExcelFastategyBasicTask(ExcelTask):
    """
    从 Excel 文件读取基金投顾策略基本信息数据。

    数据说明：
    - 包含基金投顾策略的基本信息
    - 数据量: ~2300行
    - 用途: 策略信息查询、风险分析等
    """

    # 任务标识
    name = "excel_fastrategy_basic"
    description = "基金投顾策略基本信息数据（从Excel读取）"

    # 数据表配置
    table_name = "fastrategy_basic"
    schema_name = "excel"  # 使用 excel schema 存储
    primary_keys = ["strategy_code"]
    date_column = "setup_date"
    data_source = "excel"
    domain = "fastrategy"

    # Excel 文件配置
    excel_file_path = r"E:\stock\Excel\fastrategy_basic.xlsx"
    header_row = 0  # 第一行为表头

    # 日期列配置
    date_columns = ["setup_date", "docking_nav_date"]

    # 数据表结构定义（使用字典格式）
    schema_def = {
        "strategy_code": {"type": "VARCHAR(50)", "constraints": "NOT NULL", "comment": "策略代码"},
        "strategy_name": {"type": "VARCHAR(200)", "comment": "策略名称"},
        "channel_code": {"type": "VARCHAR(50)", "comment": "渠道代码"},
        "channel_name": {"type": "VARCHAR(200)", "comment": "渠道名称"},
        "fee_rate": {"type": "DECIMAL(10,6)", "comment": "费率"},
        "setup_date": {"type": "DATE", "comment": "成立日期"},
        "is_regulation": {"type": "VARCHAR(10)", "comment": "是否报送监管"},
        "risk_type": {"type": "VARCHAR(50)", "comment": "风险类型"},
        "risk_level": {"type": "VARCHAR(50)", "comment": "风险等级"},
        "is_tracking": {"type": "VARCHAR(10)", "comment": "是否跟踪"},
        "redemption_days_strategy": {"type": "INTEGER", "comment": "策略赎回天数"},
        "redemption_days_fund": {"type": "INTEGER", "comment": "基金赎回天数"},
        "channel_sort": {"type": "DECIMAL(10,2)", "comment": "渠道排序"},
        "strategy_sort": {"type": "DECIMAL(10,2)", "comment": "策略排序"},
        "is_official": {"type": "VARCHAR(10)", "comment": "是否官方"},
        "docking_source_channel": {"type": "VARCHAR(200)", "comment": "对接渠道来源"},
        "docking_source_strategy": {"type": "VARCHAR(200)", "comment": "对接策略来源"},
        "docking_nav_date": {"type": "DATE", "comment": "对接净值日期"},
        "main_strategy_code": {"type": "VARCHAR(50)", "comment": "主策略代码"},
    }

    # 自定义索引
    indexes = [
        {"name": "idx_channel_code", "columns": "channel_code"},
        {"name": "idx_setup_date", "columns": "setup_date"},
        {"name": "idx_risk_type", "columns": "risk_type"},
        {"name": "idx_risk_level", "columns": "risk_level"},
    ]

    # 数据验证规则
    validation_rules = [
        {
            "name": "策略代码不能为空",
            "condition": lambda df: df["strategy_code"].notna(),
        },
        {
            "name": "策略名称不能为空",
            "condition": lambda df: df["strategy_name"].notna(),
        },
        {
            "name": "费率应在合理范围内",
            "condition": lambda df: (df["fee_rate"].isna()) | ((df["fee_rate"] >= 0) & (df["fee_rate"] <= 1)),
        },
        {
            "name": "赎回天数应为正整数",
            "condition": lambda df: (df["redemption_days_strategy"].isna()) | (df["redemption_days_strategy"] >= 0),
        },
        {
            "name": "基金赎回天数应为正整数",
            "condition": lambda df: (df["redemption_days_fund"].isna()) | (df["redemption_days_fund"] >= 0),
        },
    ]

    def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """
        处理基金投顾策略基本信息数据。

        处理步骤:
        1. 调用父类的基础处理
        2. 转换日期格式
        3. 处理数值字段
        4. 清理文本字段

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
        numeric_columns = ['fee_rate', 'channel_sort', 'strategy_sort']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')

        # 处理整数字段
        int_columns = ['redemption_days_strategy', 'redemption_days_fund']
        for col in int_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce', downcast='integer')

        # 清理文本字段，去除多余的空白字符
        text_columns = ['strategy_name', 'channel_name', 'risk_type', 'risk_level',
                       'docking_source_channel', 'docking_source_strategy']
        for col in text_columns:
            if col in data.columns:
                data[col] = data[col].apply(
                    lambda x: ' '.join(str(x).split()) if pd.notna(x) else None
                )

        self.logger.info(f"数据处理完成: {len(data)} 行")

        return data
