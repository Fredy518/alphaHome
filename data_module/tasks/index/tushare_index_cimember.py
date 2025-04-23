#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中信行业成分 (ci_index_member) 数据任务
获取最新的 (is_new='Y') 和历史的 (is_new='N') 中信行业成分信息，
并使用 UPSERT 更新到数据库。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

@task_register()
class TushareIndexCiMemberTask(TushareTask):
    """获取中信(CITIC)行业成分数据 (UPSERT)"""

    # 1. 核心属性
    name = "tushare_index_cimember"
    description = "获取中信(CITIC)行业成分数据 (含历史, UPSERT)"
    table_name = "tushare_index_cimember"
    primary_keys = ["ts_code", "l3_code", "in_date"] # <-- 修改主键为 l3_code
    date_column = None # <-- 明确一下没有主日期列用于增量

    # 2. TushareTask 特有属性
    api_name = "ci_index_member"
    # 根据 Tushare 文档更新字段列表
    fields = [
        "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
        "ts_code", "name", "in_date", "out_date", "is_new"
    ]
    
    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列由基类处理)
    transformations = {}

    # 5. 数据库表结构 (添加缺失字段，更新约束)
    schema = {
        "l1_code": {"type": "VARCHAR(20)"},
        "l1_name": {"type": "VARCHAR(50)"},
        "l2_code": {"type": "VARCHAR(20)"}, # <-- 移除 NOT NULL
        "l2_name": {"type": "VARCHAR(50)"},
        "l3_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"}, # <-- 添加 NOT NULL
        "l3_name": {"type": "VARCHAR(100)"}, # 新增
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"}, # 主键部分
        "name": {"type": "VARCHAR(100)"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"}, # 主键部分, NOT NULL
        "out_date": {"type": "DATE"}, # 新增
        "is_new": {"type": "VARCHAR(1)"} # 新增
        # update_time 会自动添加 (默认)
    }

    # 6. 自定义索引
    indexes = [
        # 主键 ("ts_code", "l3_code", "in_date") 已自动创建索引
        {"name": "idx_index_cimember_l1", "columns": "l1_code"},
        {"name": "idx_index_cimember_l3", "columns": "l3_code"}
    ]

    def __init__(self, db_connection, api_token=None):
        """初始化任务，使用默认并发"""
        super().__init__(db_connection, api_token=api_token)
        # 移除 self.concurrent_limit = 1
        # self.logger.info(f"任务 {self.name} 使用默认并发设置")

    # 移除 pre_execute 方法
    # async def pre_execute(self, **kwargs: Any) -> None:
    #     ...

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        获取最新的 (is_new='Y') 和历史的 (is_new='N') 成员信息。
        返回两个批次参数。
        """
        self.logger.info(f"任务 {self.name}: 生成获取最新(Y)和历史(N)行业成分的批次。")
        return [{'is_new': 'Y'}, {'is_new': 'N'}] # <-- 返回两个批次 

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的指数成分数据。
        - 检查 DataFrame 是否为空。
        - 检查关键标识字段 (ts_code, l2_code, in_date) 是否全部为空。
        """
        if df.empty:
            self.logger.warning(f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。")
            return df

        # 1. 检查关键字段是否存在 (映射后的字段名)
        # 注意: column_mapping 将 index_code 映射为 ts_code
        critical_cols = ['ts_code', 'l3_code', 'in_date']
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 2. 检查关键字段是否在所有行中都为空值
        df_check = df[critical_cols].replace('', pd.NA)
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键字段 ({', '.join(critical_cols)}) 均为空值。可能数据源返回异常数据。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效成分记录。")
        return df

    # 可选：如果需要在保存前进行额外处理，可以覆盖 process_data
    # async def process_data(self, df: pd.DataFrame, batch_params: Dict) -> pd.DataFrame:
    #     df = await super().process_data(df, batch_params) # 调用基类处理
    #     # 在这里添加特定于此任务的处理
    #     return df

    # 可选：如果需要在任务执行后进行操作，可以覆盖 post_execute
    # async def post_execute(self, results: List[pd.DataFrame], **kwargs: Any) -> None:
    #     self.logger.info(f"任务 {self.name} 完成。") 