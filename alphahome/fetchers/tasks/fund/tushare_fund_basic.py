#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金列表 (fund_basic) 全量更新任务
每次执行时，获取所有基金的基本信息并替换数据库中的旧数据。
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
class TushareFundBasicTask(TushareTask):
    """获取公募基金列表 (全量更新)"""

    # 1. 核心属性
    name = "tushare_fund_basic"
    description = "获取公募基金基本信息"
    table_name = "fund_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 全量更新
    default_start_date = "19700101"  # 全量更新
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "fund_basic"
    # Tushare fund_basic 接口实际返回的字段
    fields = [
        "ts_code",
        "name",
        "management",
        "custodian",
        "fund_type",
        "found_date",
        "due_date",
        "list_date",
        "issue_date",
        "delist_date",
        "issue_amount",
        "m_fee",
        "c_fee",
        "duration_year",
        "p_value",
        "min_amount",
        "exp_return",
        "benchmark",
        "status",
        "invest_type",
        "type",
        "trustee",
        "purc_startdate",
        "redm_startdate",
        "market",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理, 数值列在此处定义)
    transformations = {
        "issue_amount": lambda x: pd.to_numeric(x, errors="coerce"),
        "m_fee": lambda x: pd.to_numeric(x, errors="coerce"),
        "c_fee": lambda x: pd.to_numeric(x, errors="coerce"),
        "duration_year": lambda x: pd.to_numeric(x, errors="coerce"),
        "p_value": lambda x: pd.to_numeric(x, errors="coerce"),
        "min_amount": lambda x: pd.to_numeric(x, errors="coerce"),
        "exp_return": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "management": {"type": "VARCHAR(100)"},
        "custodian": {"type": "VARCHAR(100)"},
        "fund_type": {"type": "VARCHAR(50)"},
        "found_date": {"type": "DATE"},
        "due_date": {"type": "DATE"},
        "list_date": {"type": "DATE"},
        "issue_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "issue_amount": {"type": "FLOAT"},
        "m_fee": {"type": "FLOAT"},
        "c_fee": {"type": "FLOAT"},
        "duration_year": {"type": "FLOAT"},
        "p_value": {"type": "FLOAT"},
        "min_amount": {"type": "FLOAT"},
        "exp_return": {"type": "FLOAT"},
        "benchmark": {"type": "TEXT"},  # 业绩基准可能很长
        "status": {"type": "VARCHAR(1)"},
        "invest_type": {"type": "VARCHAR(100)"},
        "type": {"type": "VARCHAR(100)"},  # 基金类型
        "trustee": {"type": "VARCHAR(100)"},
        "purc_startdate": {"type": "DATE"},
        "redm_startdate": {"type": "DATE"},
        "market": {"type": "VARCHAR(1)"},  # E场内 O场外
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_basic_fund_type", "columns": "fund_type"},
        {"name": "idx_fund_basic_market", "columns": "market"},
        {"name": "idx_fund_basic_status", "columns": "status"},
        {"name": "idx_fund_basic_list_date", "columns": "list_date"},
        {
            "name": "idx_fund_basic_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['name'].notna(), "基金名称不能为空"),
        (lambda df: df['fund_type'].notna(), "基金类型不能为空"),
        (lambda df: df['status'].isin(['D', 'I', 'L']), "存续状态必须为D/I/L（终止/发行/上市）"),
        (lambda df: df['market'].isin(['E', 'O']), "市场类型必须为E/O（场内/场外）"),
    ]

    # def __init__(self, db_connection, api_token=None, api=None, **kwargs):
    #     """初始化任务"""
    #     super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
    #     # 全量更新，设置为串行执行以简化
    #     self.concurrent_limit = 1
    #     self.logger.info(f"任务 {self.name} 已配置为串行执行 (concurrent_limit=1)")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 fund_basic 全量获取，返回空参数字典列表。
        可以考虑按 market 分批: return [{'market': 'E'}, {'market': 'O'}]
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        return [{'market': 'E'}, {'market': 'O'}]


