#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金列表 (fund_basic) 全量更新任务
每次执行时，获取所有基金的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)

@task_register()
class TushareFundBasicTask(TushareTask):
    """获取公募基金列表 (全量更新)"""

    # 1. 核心属性
    name = "tushare_fund_basic"
    description = "获取公募基金基础信息"
    table_name = "tushare_fund_basic"
    primary_keys = ["ts_code"]
    date_column = None # 全量更新
    default_start_date = None # 全量更新

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "fund_basic"
    # Tushare fund_basic 接口实际返回的字段
    fields = [
        'ts_code', 'name', 'management', 'custodian', 'fund_type', 'found_date',
        'due_date', 'list_date', 'issue_date', 'delist_date', 'issue_amount',
        'm_fee', 'c_fee', 'duration_year', 'p_value', 'min_amount', 'exp_return',
        'benchmark', 'status', 'invest_type', 'type', 'trustee', 'purc_startdate',
        'redm_startdate', 'market'
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理, 数值列在此处定义)
    transformations = {
        "issue_amount": lambda x: pd.to_numeric(x, errors='coerce'),
        "m_fee": lambda x: pd.to_numeric(x, errors='coerce'),
        "c_fee": lambda x: pd.to_numeric(x, errors='coerce'),
        "duration_year": lambda x: pd.to_numeric(x, errors='coerce'),
        "p_value": lambda x: pd.to_numeric(x, errors='coerce'),
        "min_amount": lambda x: pd.to_numeric(x, errors='coerce'),
        "exp_return": lambda x: pd.to_numeric(x, errors='coerce'),
    }

    # 5. 数据库表结构
    schema = {
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
        "benchmark": {"type": "TEXT"}, # 业绩基准可能很长
        "status": {"type": "VARCHAR(1)"},
        "invest_type": {"type": "VARCHAR(100)"},
        "type": {"type": "VARCHAR(100)"}, # 基金类型
        "trustee": {"type": "VARCHAR(100)"},
        "purc_startdate": {"type": "DATE"},
        "redm_startdate": {"type": "DATE"},
        "market": {"type": "VARCHAR(1)"} # E场内 O场外
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_basic_fund_type", "columns": "fund_type"},
        {"name": "idx_fund_basic_market", "columns": "market"},
        {"name": "idx_fund_basic_status", "columns": "status"},
        {"name": "idx_fund_basic_list_date", "columns": "list_date"},
        {"name": "idx_fund_basic_update_time", "columns": "update_time"} # 新增 update_time 索引
    ]

    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        # 全量更新，设置为串行执行以简化
        self.concurrent_limit = 1
        self.logger.info(f"任务 {self.name} 已配置为串行执行 (concurrent_limit=1)")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 fund_basic 全量获取，返回空参数字典列表。
        可以考虑按 market 分批: return [{'market': 'E'}, {'market': 'O'}]
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        return [{}]

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段 ('name') 是否全部为空。
        """
        if df.empty:
            self.logger.warning(f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。")
            return df

        critical_cols = ['name']
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace('', pd.NA)
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。")
        return df 

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        TushareDataTransformer 已处理通用日期转换。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        # 所有必要的处理已由 TushareDataTransformer 完成。
        # 此方法可以保留用于未来可能的、此任务真正特有的转换。
        # 目前，它只检查并返回df。
        self.logger.info(f"任务 {self.name}: process_data 被调用，返回 DataFrame (行数: {len(df)}). TushareDataTransformer 已完成主要处理。")
        return df 