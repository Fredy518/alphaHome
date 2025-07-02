#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货及股票期权合约基础信息 (opt_basic) 更新任务
每次执行时，获取所有交易所的期权合约基础信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareOptionBasicTask(TushareTask):
    """获取期货及股票期权合约基础信息 (全量更新，按交易所分批)"""

    # 1. 核心属性
    domain = "option"  # 业务域标识
    name = "tushare_option_basic"
    description = "获取期货及股票期权合约基础信息"
    table_name = "option_basic"
    primary_keys = ["ts_code"]  # 期权代码是唯一主键
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = '19700101'  # 全量任务不需要起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) ---
    # 考虑到需要按交易所分批，可以适当增加并发限制
    default_concurrent_limit = 10
    default_page_size = 10000  # 试验最大单次返回12000行

    # 2. TushareTask 特有属性
    api_name = "opt_basic"
    # Tushare opt_basic 接口实际返回的字段 (根据文档 https://tushare.pro/document/2?doc_id=158)
    fields = [
        "ts_code",
        "exchange",
        "name",
        "per_unit",
        "opt_code",
        "opt_type",
        "call_put",
        "exercise_type",
        "exercise_price",
        "s_month",
        "maturity_date",
        "list_price",
        "list_date",
        "delist_date",
        "last_edate",
        "last_ddate",
        "quote_unit",
        "min_price_chg",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理，其他需要转换的列可以在这里添加)
    # exercise_price, list_price 需要转换为 numeric
    transformations = {
        "exercise_price": lambda x: pd.to_numeric(x, errors="coerce"),
        "list_price": lambda x: pd.to_numeric(x, errors="coerce"),
        # per_unit, quote_unit, min_price_chg 是 str，暂不转换
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "exchange": {"type": "VARCHAR(10)"},
        "name": {"type": "VARCHAR(100)"},
        "per_unit": {
            "type": "FLOAT"
        },  # 文档是str，暂定FLOAT，若API返回str需调整或在process_data处理
        "opt_code": {"type": "VARCHAR(20)"},
        "opt_type": {"type": "VARCHAR(10)"},
        "call_put": {"type": "VARCHAR(10)"},
        "exercise_type": {"type": "VARCHAR(10)"},
        "exercise_price": {"type": "FLOAT"},
        "s_month": {"type": "VARCHAR(10)"},
        "maturity_date": {"type": "DATE"},
        "list_price": {"type": "FLOAT"},
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "last_edate": {"type": "DATE"},
        "last_ddate": {"type": "DATE"},
        "quote_unit": {"type": "VARCHAR(10)"},
        "min_price_chg": {"type": "VARCHAR(10)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_option_basic_ts_code", "columns": "ts_code"},
        {"name": "idx_option_basic_exchange", "columns": "exchange"},
        {"name": "idx_option_basic_opt_code", "columns": "opt_code"},
        {"name": "idx_option_basic_update_time", "columns": "update_time"},
    ]# --- This __init__ was commented out for code simplification. ---
# 
# 
# def __init__(self, db_connection, api_token=None, api=None, **kwargs):
# """初始化任务"""
# super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
# self.logger.info(f"任务 {self.name} 已配置初始化。")
# 
    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 opt_basic，按交易所分批获取。
        使用包含常见期货和股票期权交易所的列表。
        """
        # 包含常见期货和股票期权交易所的列表 (待验证哪些支持 opt_basic)
        exchanges = ["CFFEX", "SSE", "SZSE"] # 暂时只获取金融期权合约
        batch_list = [{"exchange": exc} for exc in exchanges]
        self.logger.info(
            f"任务 {self.name}: 按交易所分批获取模式，生成 {len(batch_list)} 个批次。"
        )
        return batch_list

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "期权代码不能为空"),
        (lambda df: df['name'].notna(), "期权名称不能为空"),
        (lambda df: df['exchange'].notna(), "交易所不能为空"),
        (lambda df: df['call_put'].notna(), "看涨看跌标识不能为空"),
        (lambda df: df['exercise_price'].notna(), "行权价不能为空"),
        (lambda df: df['maturity_date'].notna(), "到期日不能为空"),
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "期权代码不能为空字符串"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "期权名称不能为空字符串"),
        (lambda df: ~(df['exchange'].astype(str).str.strip().eq('') | df['exchange'].isna()), "交易所不能为空字符串"),
        (lambda df: ~(df['call_put'].astype(str).str.strip().eq('') | df['call_put'].isna()), "看涨看跌标识不能为空字符串"),
        (lambda df: df['call_put'].isin(['C', 'P']), "看涨看跌标识必须为C或P"),
        (lambda df: df['exercise_price'] > 0, "行权价必须为正数"),
    ]
