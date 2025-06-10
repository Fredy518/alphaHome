#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票基本信息 (stock_basic) 全量更新任务
每次执行时，获取所有股票的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockBasicTask(TushareTask):
    """获取所有股票基础信息 (全量更新)"""

    # 1. 核心属性
    name = "tushare_stock_basic"
    description = "获取上市公司基础信息"
    table_name = "tushare_stock_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = None  # 全量任务不需要起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "stock_basic"
    # Tushare stock_basic 接口实际返回的字段
    fields = [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理)
    transformations = {
        # 其他需要转换的列可以在这里添加
        # "some_numeric_string_col": lambda x: pd.to_numeric(x, errors='coerce'),
    }

    # 5. 数据库表结构
    schema = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "symbol": {"type": "VARCHAR(10)"},
        "name": {"type": "VARCHAR(50)"},
        "area": {"type": "VARCHAR(50)"},
        "industry": {"type": "VARCHAR(100)"},  # 行业名称可能较长
        "fullname": {"type": "VARCHAR(255)"},
        "enname": {"type": "VARCHAR(255)"},
        "cnspell": {"type": "VARCHAR(50)"},
        "market": {"type": "VARCHAR(50)"},
        "exchange": {"type": "VARCHAR(10)"},
        "curr_type": {"type": "VARCHAR(5)"},
        "list_status": {"type": "VARCHAR(1)"},
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "is_hs": {"type": "VARCHAR(1)"},
        "act_name": {"type": "VARCHAR(100)"},
        "act_ent_type": {"type": "VARCHAR(100)"},  # 控制人性质可能较长
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_basic_name", "columns": "name"},
        {"name": "idx_stock_basic_industry", "columns": "industry"},
        {"name": "idx_stock_basic_market", "columns": "market"},
        {"name": "idx_stock_basic_update_time", "columns": "update_time"},
    ]

    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        # 全量更新，设置为串行执行以简化
        self.concurrent_limit = 1
        self.logger.info(f"任务 {self.name} 已配置为串行执行 (concurrent_limit=1)")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 stock_basic 全量获取，返回空参数字典列表。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 触发一次不带参数的 API 调用以获取所有股票
        # Tushare stock_basic 支持 list_status 参数过滤，如果需要可以分批获取
        # 例如： return [{'list_status': 'L'}, {'list_status': 'D'}, {'list_status': 'P'}]
        # 但全量获取通常更简单
        return [{}]

    async def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        TushareDataTransformer 已处理通用日期转换。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        # 调用基类方法完成其他处理 (应用 transformations, 排序等)
        # df = super().process_data(df, **kwargs)

        # 所有必要的处理已由 TushareDataTransformer 完成。
        # 此方法可以保留用于未来可能的、此任务真正特有的转换。
        # 目前，它只检查并返回df。
        self.logger.info(
            f"任务 {self.name}: process_data 被调用，返回 DataFrame (行数: {len(df)}). TushareDataTransformer 已完成主要处理。"
        )
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段 ('symbol', 'name') 是否全部为空。
        """
        if df.empty:
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        critical_cols = ["symbol", "name"]
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace("", pd.NA)
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(
            f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。"
        )
        return df
