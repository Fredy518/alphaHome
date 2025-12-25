#!/usr/bin/env python
# -*- coding: utf-8 -*-

r"""
基金投顾策略FOF基金对比信息数据采集任务

数据来源: E:\stock\Excel\fastrategy_fof_versus.xlsx
数据说明: 基金投顾策略中FOF基金的对比信息数据
保存表名: fastrategy_fof_versus (在 excel schema 中)

数据字段:
- fund_code: 基金代码
- fund_name: 基金名称
- fund_full_name: 基金全称
- setup_date: 成立日期
- basic_info: 基本信息
- is_versus: 是否对比
- risk_type: 风险类型
"""

import pandas as pd
from typing import Optional

from alphahome.fetchers.sources.excel import ExcelTask
from alphahome.common.task_system.task_decorator import task_register


@task_register()
class ExcelFastategyFofVersusTask(ExcelTask):
    """
    从 Excel 文件读取基金投顾策略FOF基金对比信息数据。

    数据说明：
    - 包含基金投顾策略中FOF基金的对比信息
    - 数据量: ~586行
    - 用途: FOF基金对比分析、风险评估等
    """

    # 任务标识
    name = "excel_fastrategy_fof_versus"
    description = "基金投顾策略FOF基金对比信息数据（从Excel读取）"

    # 数据表配置
    table_name = "fastrategy_fof_versus"
    schema_name = "excel"  # 使用 excel schema 存储
    primary_keys = ["fund_code"]
    date_column = "setup_date"
    data_source = "excel"
    domain = "fastrategy"

    # Excel 文件配置
    excel_file_path = r"E:\stock\Excel\fastrategy_fof_versus.xlsx"
    header_row = 0  # 第一行为表头

    # 日期列配置
    date_columns = ["setup_date"]

    # 数据表结构定义（使用字典格式）
    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL", "comment": "基金代码"},
        "fund_name": {"type": "VARCHAR(200)", "comment": "基金名称"},
        "fund_full_name": {"type": "VARCHAR(300)", "comment": "基金全称"},
        "setup_date": {"type": "DATE", "comment": "成立日期"},
        "basic_info": {"type": "TEXT", "comment": "基本信息"},
        "is_versus": {"type": "VARCHAR(10)", "comment": "是否对比"},
        "risk_type": {"type": "VARCHAR(50)", "comment": "风险类型"},
    }

    # 自定义索引
    indexes = [
        {"name": "idx_fund_name", "columns": "fund_name"},
        {"name": "idx_setup_date", "columns": "setup_date"},
        {"name": "idx_risk_type", "columns": "risk_type"},
        {"name": "idx_is_versus", "columns": "is_versus"},
    ]

    # 数据验证规则
    validation_rules = [
        {
            "name": "基金代码不能为空",
            "condition": lambda df: df["fund_code"].notna(),
        },
        {
            "name": "基金名称不能为空",
            "condition": lambda df: df["fund_name"].notna(),
        },
        {
            "name": "成立日期不能为空",
            "condition": lambda df: df["setup_date"].notna(),
        },
        {
            "name": "是否对比字段不能为空",
            "condition": lambda df: df["is_versus"].notna(),
        },
    ]

    def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """
        处理基金投顾策略FOF基金对比信息数据。

        处理步骤:
        1. 调用父类的基础处理
        2. 转换日期格式
        3. 清理文本字段
        4. 标准化字段值

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

        # 清理文本字段，去除多余的空白字符
        text_columns = ['fund_name', 'fund_full_name', 'basic_info', 'risk_type']
        for col in text_columns:
            if col in data.columns:
                data[col] = data[col].apply(
                    lambda x: ' '.join(str(x).split()) if pd.notna(x) else None
                )

        # 标准化is_versus字段
        if 'is_versus' in data.columns:
            data['is_versus'] = data['is_versus'].apply(
                lambda x: '是' if str(x).strip().lower() in ['是', 'yes', '1', 'true'] else
                         ('否' if str(x).strip().lower() in ['否', 'no', '0', 'false'] else str(x).strip())
            )

        self.logger.info(f"数据处理完成: {len(data)} 行")

        return data
