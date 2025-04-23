#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
指数基本信息 (index_basic) 全量更新任务
每次执行时，获取所有指数基本信息并替换数据库中的旧数据。
继承自 TushareTask，利用 pre_execute 清空表。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register
# 假设的数据库异常类，可以根据实际使用的库替换，例如 asyncpg.exceptions.PostgresError
# from asyncpg.exceptions import PostgresError 

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)

@task_register()
class TushareIndexBasicTask(TushareTask):
    """获取指数基本信息 (UPSERT 更新)"""

    # 1. 核心属性
    name = "tushare_index_basic"
    description = "获取指数基本信息 (UPSERT 更新)"
    table_name = "tushare_index_basic"  # 请确认实际表名
    primary_keys = ["ts_code"]
    date_column = "list_date" # 明确有 list_date 列，但不用作增量更新
    
    # TushareTask 继承自 Task，默认 auto_add_update_time=True
    # 如果不需要自动添加 update_time 列，可以设置：
    # auto_add_update_time = False
    
    # 2. TushareTask 特有属性
    api_name = "index_basic"
    # Tushare index_basic 接口实际返回的字段 (根据用户反馈更新)
    fields = [
        'ts_code', 'name', 'market', 'publisher', 'category', 'base_date',
        'base_point', 'list_date'
    ]
    
    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (根据 Tushare 文档和数据库类型定义)
    transformations = {
        "base_point": lambda x: pd.to_numeric(x, errors='coerce'),
        # list_date 由覆盖的 process_data 方法处理
        # "list_date": lambda x: pd.to_datetime(x, errors='coerce'), 
        # base_date 仍可由基类 process_data 默认处理
        # "base_date": lambda x: pd.to_datetime(x, errors='coerce'),
    }

    # 5. 数据库表结构 (根据更新后的字段调整)
    # 类型应与数据库兼容，例如 PostgreSQL, MySQL, SQLite
    schema = {
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
        {"name": "idx_index_basic_list_date", "columns": "list_date"}
    ]

    def __init__(self, db_connection, api_token=None):
        """初始化任务"""
        # 调用父类 TushareTask 的 __init__ 并传递必要的参数
        super().__init__(db_connection, api_token=api_token)
        # 禁用并发执行，因为此任务数据量小且为全量更新
        self.concurrent_limit = 1
        self.logger.info(f"任务 {self.name} 已配置为串行执行 (concurrent_limit=1)")
        # 此处可以添加 TushareIndexBasicTask 特有的其他初始化逻辑（如果需要）

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        对于 index_basic 全量获取，不需要分批，返回一个空参数字典的列表。
        基类的 fetch_batch 会使用这个空字典调用 Tushare API。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 返回包含一个空字典的列表，触发一次不带参数的 API 调用
        return [{}]

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        覆盖基类的数据处理方法，特别处理 list_date 列。
        1. 使用 errors='coerce' 转换 list_date，允许无效格式。
        2. 将转换失败产生的 NaT 填充为 '1970-01-01'。
        3. 调用基类的 process_data 完成剩余处理。
        """
        if 'list_date' in df.columns:
            original_count = len(df)
            df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce')
            nat_count = df['list_date'].isnull().sum()
            if nat_count > 0:
                fill_date = pd.Timestamp('1970-01-01')
                df['list_date'].fillna(fill_date, inplace=True)
                self.logger.info(f"任务 {self.name}: 将 {nat_count} 个无效或缺失的 'list_date' 填充为 {fill_date.date()}")
            else:
                self.logger.info(f"任务 {self.name}: 'list_date' 列无需填充默认日期。")
        else:
            self.logger.warning(f"任务 {self.name}: DataFrame 中未找到 'list_date' 列，跳过预处理。")

        # 调用基类方法完成其他处理 (如 base_date 转换, transformations 应用, 排序等)
        df = await super().process_data(df, **kwargs)
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段是否全部为空，防止无效数据覆盖。
        """
        if df.empty:
            self.logger.warning(f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。")
            return df

        # 1. 检查关键业务字段是否存在
        critical_cols = ['name', 'market', 'publisher', 'category']
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 2. 检查关键业务字段是否在所有行中都为空值
        # 注意: isnull() 检查 None 和 NaN。对于空字符串，需要额外处理或确保它们在转换时变为NaN/None。
        # Tushare 返回空字符串 '' 很常见，将其视为空值
        df_check = df[critical_cols].replace('', pd.NA) # 将空字符串替换为 NA
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。可能数据源返回异常数据。"
            self.logger.critical(error_msg) # 使用 critical 级别日志
            raise ValueError(error_msg)

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。")
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