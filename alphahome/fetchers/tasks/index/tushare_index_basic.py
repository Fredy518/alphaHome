#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
指数基本信息 (index_basic) 全量更新任务
每次执行时，获取所有指数基本信息并替换数据库中的旧数据。
继承自 TushareTask，利用 pre_execute 清空表。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 假设的数据库异常类，可以根据实际使用的库替换，例如 asyncpg.exceptions.PostgresError
# from asyncpg.exceptions import PostgresError

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareIndexBasicTask(TushareTask):
    """获取指数基本信息 (UPSERT 更新)"""

    # 1. 核心属性
    name = "tushare_index_basic"
    description = "获取指数基本信息"
    table_name = "index_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 全量任务
    default_start_date = "19900101"  # 全量任务，设置一个早期默认起始日期
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "index_basic"
    # Tushare index_basic 接口实际返回的字段 (根据用户反馈更新)
    fields = [
        "ts_code",
        "name",
        "market",
        "publisher",
        "category",
        "base_date",
        "base_point",
        "list_date",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (根据 Tushare 文档和数据库类型定义)
    transformations = {
        "base_point": lambda x: pd.to_numeric(x, errors="coerce"),
        # list_date 由覆盖的 process_data 方法处理
        # "list_date": lambda x: pd.to_datetime(x, errors='coerce'),
        # base_date 仍可由基类 process_data 默认处理
        # "base_date": lambda x: pd.to_datetime(x, errors='coerce'),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "market": {"type": "VARCHAR(50)"},
        "publisher": {"type": "VARCHAR(100)"},
        "category": {"type": "VARCHAR(100)"},
        "base_date": {"type": "DATE"},
        "base_point": {"type": "FLOAT"},
        "list_date": {"type": "DATE"},
        # 移除不再获取的字段定义
        # "index_type": {"type": "VARCHAR(50)"},
        # "fullname": {"type": "VARCHAR(255)"},
        # "cn_spell": {"type": "VARCHAR(100)"},
        # "desc": {"type": "TEXT"},
        # "exp_date": {"type": "DATE"},
        # "update_flag": {"type": "VARCHAR(1)"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 主键 ts_code 已自动创建索引
        {"name": "idx_index_basic_market", "columns": "market"},
        {"name": "idx_index_basic_publisher", "columns": "publisher"},
        {"name": "idx_index_basic_category", "columns": "category"},
        {"name": "idx_index_basic_list_date", "columns": "list_date"},
        {
            "name": "idx_index_basic_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        对于 index_basic 全量获取，不需要分批，返回一个空参数字典的列表。
        基类的 fetch_batch 会使用这个空字典调用 Tushare API。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 返回包含一个空字典的列表，触发一次不带参数的 API 调用
        return [{}]

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段是否全部为空，防止无效数据覆盖。
        """
        if df.empty:
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        # 1. 检查关键业务字段是否存在
        critical_cols = ["name", "market", "publisher", "category"]
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 2. 检查关键业务字段是否在所有行中都为空值
        # 注意: isnull() 检查 None 和 NaN。对于空字符串，需要额外处理或确保它们在转换时变为NaN/None。
        # Tushare 返回空字符串 '' 很常见，将其视为空值
        df_check = df[critical_cols].replace("", pd.NA)  # 将空字符串替换为 NA
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。可能数据源返回异常数据。"
            self.logger.critical(error_msg)  # 使用 critical 级别日志
            raise ValueError(error_msg)

        self.logger.info(
            f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。"
        )
        return df


# --- 移除之前的模拟类和旧的 execute 方法 ---
# (已在上次编辑中移除)

# --- 移除之前的用法注释 ---
# (已在上次编辑中移除)

"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'ts_index_basic' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. (可能需要) 在 TaskFactory 中注册此任务。
5. 使用 scripts/tasks/index/run_index_basic.py 脚本（下一步创建）来运行此任务。
"""
