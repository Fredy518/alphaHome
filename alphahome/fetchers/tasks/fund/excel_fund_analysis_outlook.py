#!/usr/bin/env python
# -*- coding: utf-8 -*-

r"""
基金市场分析与展望数据采集任务

数据来源: E:\stock\Excel\fund_analysis_outloook.xlsx
数据说明: 基金定期报告中的市场分析和市场展望文本数据
保存表名: fund_analysis_outlook (在 excel schema 中)

数据字段:
- ts_code: 基金代码
- end_date: 报告期
- report_type: 报告类型（Q1/Q2/Q3/Q4等）
- ann_date: 公告日期
- market_analysis: 市场分析文本
- market_outlook: 市场展望文本
"""

import pandas as pd
from typing import Optional

from alphahome.fetchers.sources.excel import ExcelTask
from alphahome.common.task_system.task_decorator import task_register


@task_register()
class ExcelFundAnalysisOutlookTask(ExcelTask):
    """
    从 Excel 文件读取基金市场分析与展望数据。
    
    数据说明：
    - 包含基金定期报告中的市场分析和市场展望
    - 数据量: ~23万行
    - 用途: 文本分析、情绪分析、市场观点研究等
    """
    
    # 任务标识
    name = "excel_fund_analysis_outlook"
    description = "基金市场分析与展望数据（从Excel读取）"
    
    # 数据表配置
    table_name = "fund_analysis_outlook"
    schema_name = "excel"  # 使用 excel schema 存储
    primary_keys = ["ts_code", "end_date", "report_type"]
    date_column = "ann_date"
    data_source = "excel"
    domain = "fund"
    
    # Excel 文件配置
    excel_file_path = r"E:\stock\Excel\fund_analysis_outlook.xlsx"
    sheet_name = 0  # 第一个sheet
    header_row = 0  # 第一行为表头
    
    # 日期列配置
    date_columns = ["end_date", "ann_date"]
    
    # 数据表结构定义（使用字典格式）
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL", "comment": "基金代码"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL", "comment": "报告期"},
        "report_type": {"type": "VARCHAR(10)", "constraints": "NOT NULL", "comment": "报告类型（Q1/Q2/Q3/Q4等）"},
        "ann_date": {"type": "DATE", "comment": "公告日期"},
        "market_analysis": {"type": "TEXT", "comment": "市场分析文本"},
        "market_outlook": {"type": "TEXT", "comment": "市场展望文本"},
    }
    
    # 自定义索引
    indexes = [
        {"name": "idx_ts_code", "columns": "ts_code"},
        {"name": "idx_ann_date", "columns": "ann_date"},
        {"name": "idx_end_date", "columns": "end_date"},
    ]
    
    # 数据验证规则
    validation_rules = [
        {
            "name": "基金代码不能为空",
            "condition": lambda df: df["ts_code"].notna(),
        },
        {
            "name": "报告期不能为空",
            "condition": lambda df: df["end_date"].notna(),
        },
        {
            "name": "报告类型不能为空",
            "condition": lambda df: df["report_type"].notna(),
        },
        {
            "name": "公告日期不能为空",
            "condition": lambda df: df["ann_date"].notna(),
        },
        {
            "name": "至少有市场分析或市场展望",
            "condition": lambda df: df["market_analysis"].notna() | df["market_outlook"].notna(),
        },
    ]
    
    def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """
        处理基金分析数据。
        
        处理步骤:
        1. 调用父类的基础处理
        2. 转换日期格式
        3. 填充缺失的ann_date（使用end_date替代）
        4. 处理文本字段（去除多余空格）
        5. 处理market_outlook字段（从数字转为文本或设为None）
        
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
        
        # 填充缺失的ann_date（使用end_date替代，因为它们通常在同一天）
        if 'ann_date' in data.columns and 'end_date' in data.columns:
            missing_ann_date = data['ann_date'].isna()
            data.loc[missing_ann_date, 'ann_date'] = data.loc[missing_ann_date, 'end_date']
            self.logger.info(f"填充了 {missing_ann_date.sum()} 条缺失的ann_date")
        
        # 处理 market_outlook 字段
        # 如果是数字0，转换为 None（表示无展望）
        if 'market_outlook' in data.columns:
            data['market_outlook'] = data['market_outlook'].apply(
                lambda x: None if (pd.notna(x) and str(x).strip() == '0') else x
            )
        
        # 清理文本字段，去除多余的空白字符
        text_columns = ['market_analysis', 'market_outlook']
        for col in text_columns:
            if col in data.columns:
                data[col] = data[col].apply(
                    lambda x: ' '.join(str(x).split()) if pd.notna(x) else None
                )
        
        self.logger.info(f"数据处理完成: {len(data)} 行")
        
        return data
