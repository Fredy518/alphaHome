#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
申万行业成分 (分级) 数据任务 (tushare: index_member_all)

获取最新的申万行业成分数据 (is_new='Y')，并使用 UPSERT 更新到数据库。
命名遵循 tushare_index_swmember 模式。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

@task_register()
class TushareIndexSwmemberTask(TushareTask): # <-- 类名改回
    """获取申万行业成分 (分级) - tushare_index_swmember"""

    # 1. 核心属性
    name = "tushare_index_swmember" # <-- 改回
    description = "获取最新的申万(SW)行业成分 (分级) 数据" # 描述可以保留
    table_name = "tushare_index_swmember" # <-- 改回
    primary_keys = ["ts_code", "l3_code", "in_date"] # <-- 增加 in_date
    date_column = None # 全量任务
    default_start_date = None # 全量任务

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 2
    default_page_size = 3000

    # 2. TushareTask 特有属性
    api_name = "index_member_all" # API 名称保持不变
    fields = [
        "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name", 
        "ts_code", "name", "in_date", "out_date", "is_new"
    ]
    
    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {}

    # 5. 数据库表结构
    schema = {
        "l1_code": {"type": "VARCHAR(20)"},
        "l1_name": {"type": "VARCHAR(50)"},
        "l2_code": {"type": "VARCHAR(20)"},
        "l2_name": {"type": "VARCHAR(50)"},
        "l3_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "l3_name": {"type": "VARCHAR(100)"},
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"}, # <-- 增加 NOT NULL
        "out_date": {"type": "DATE"},
        "is_new": {"type": "VARCHAR(1)"}
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_index_swmember_l1", "columns": "l1_code"}, # <-- 改回
        {"name": "idx_index_swmember_l2", "columns": "l2_code"}, # <-- 改回
        {"name": "idx_index_swmember_l3", "columns": "l3_code"}, # <-- 新增
    ]

    def __init__(self, db_connection, api_token=None):
        """初始化任务，使用父类默认并发设置"""
        super().__init__(db_connection, api_token=api_token)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        获取最新的 (is_new='Y') 和历史的 (is_new='N') 成员信息。
        返回两个批次参数，分别对应 'Y' 和 'N'。
        """
        self.logger.info(f"任务 {self.name}: 生成获取最新(Y)和历史(N)行业成分的批次。")
        return [{'is_new': 'Y'}, {'is_new': 'N'}] # <-- 返回两个批次 

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的申万行业成分数据。
        - 检查 DataFrame 是否为空。
        - 检查关键标识字段 (ts_code, l3_code, in_date) 是否全部为空。
        """
        if df.empty:
            self.logger.warning(f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。")
            return df

        # 1. 检查关键字段是否存在
        critical_cols = ['ts_code', 'l3_code', 'in_date']
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 2. 检查关键字段是否在所有行中都为空值
        df_check = df[critical_cols].replace('', pd.NA) # 将空字符串视为空
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键字段 ({', '.join(critical_cols)}) 均为空值。可能数据源返回异常数据。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效成分记录。")
        return df 