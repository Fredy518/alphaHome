#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETF基础信息 (etf_basic) 全量更新任务
获取国内ETF基础信息，包括了QDII。数据来源与沪深交易所公开披露信息。
继承自 TushareTask。

Tushare API文档: https://tushare.pro/document/2?doc_id=385
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable, Tuple

import pandas as pd
import numpy as np

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register


@task_register()
class TushareFundEtfBasicTask(TushareTask):
    """获取ETF基础信息数据 (全量更新)"""

    # 1. 核心属性
    name = "tushare_fund_etf_basic"
    description = "获取ETF基础信息 (含QDII)"
    table_name = "fund_etf_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 全量更新任务，无日期列
    default_start_date = "19700101"  # 全量更新
    data_source = "tushare"
    domain = "fund"  # 业务域标识

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 全量更新，设置为串行执行
    default_page_size = 5000
    update_type = "full"  # 明确指定为全量更新任务类型

    # 2. TushareTask 特有属性
    api_name = "etf_basic"
    # Tushare etf_basic 接口返回的字段 (根据2024年新版文档)
    fields = [
        "ts_code",          # 基金交易代码
        "csname",           # ETF中文简称
        "extname",          # ETF扩位简称
        "cname",            # 基金中文全称
        "index_code",       # ETF基准指数代码
        "index_name",       # ETF基准指数中文全称
        "setup_date",       # 设立日期
        "list_date",        # 上市日期
        "list_status",      # 存续状态 (L上市 D退市 P待上市)
        "exchange",         # 交易所 (SH上交所 SZ深交所)
        "mgr_name",         # 基金管理人简称
        "custod_name",      # 基金托管人名称
        "mgt_fee",          # 基金管理人收取的费用
        "etf_type",         # 基金投资通道类型 (境内、QDII)
    ]

    # 3. 列名映射 (新API字段 -> 数据库列名)
    column_mapping = {
        "cname": "name",
        "setup_date": "found_date",
        "list_status": "status",
        "exchange": "market",
        "mgr_name": "management",
        "custod_name": "custodian",
        "mgt_fee": "m_fee",
        "index_name": "benchmark"
    }

    # 4. 数据类型转换
    transformations = {
        "mgt_fee": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},            # from cname
        "csname": {"type": "VARCHAR(100)"},          # new
        "extname": {"type": "VARCHAR(100)"},         # new
        "management": {"type": "VARCHAR(100)"},      # from mgr_name
        "custodian": {"type": "VARCHAR(100)"},       # from custod_name
        "found_date": {"type": "DATE"},              # from setup_date
        "list_date": {"type": "DATE"},
        "status": {"type": "VARCHAR(10)"},           # from list_status
        "market": {"type": "VARCHAR(10)"},           # from exchange
        "benchmark": {"type": "TEXT"},               # from index_name
        "index_code": {"type": "VARCHAR(20)"},       # new
        "etf_type": {"type": "VARCHAR(50)"},         # new
        "m_fee": {"type": "NUMERIC(8,4)"},            # from mgt_fee
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {
            "name": "idx_fund_etf_basic_name",
            "columns": "name",
        },
        {
            "name": "idx_fund_etf_basic_management",
            "columns": "management",
        },
        {
            "name": "idx_fund_etf_basic_status",
            "columns": "status",
        },
        {
            "name": "idx_fund_etf_basic_market",
            "columns": "market",
        },
        {
            "name": "idx_fund_etf_basic_index_code",
            "columns": "index_code",
        },
        {
            "name": "idx_fund_etf_basic_update_time",
            "columns": "update_time",
        },
    ]

    # 7. 数据验证规则
    validations: Optional[List[Union[Callable, Tuple[Callable, str]]]] = [
        (lambda df: df['ts_code'].notna(), "ETF代码不能为空"),
        (lambda df: df['name'].notna(), "ETF名称不能为空"),
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "ETF代码不能为空字符串"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "ETF名称不能为空字符串"),
        (lambda df: df['ts_code'].str.match(r'^\d{6}\.(SH|SZ)$'), "ETF代码格式检查（6位数字.SH/SZ）"),
        (lambda df: df['status'].isin(['L', 'D', 'P']) if 'status' in df.columns else True, "存续状态必须为L/D/P (上市/退市/待上市)"),
        (lambda df: df['market'].isin(['SH', 'SZ']) if 'market' in df.columns else True, "市场类型必须为SH/SZ (上交所/深交所)"),
        (lambda df: df['m_fee'].fillna(0) >= 0, "管理费率不能为负数"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 etf_basic 全量获取，返回空参数字典列表。
        
        Args:
            **kwargs: 查询参数（全量更新时通常不需要）
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 返回包含一个空字典的列表，触发一次不带参数的 API 调用
        return [{}]
