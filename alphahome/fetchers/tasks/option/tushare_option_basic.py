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
from ...task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareOptionBasicTask(TushareTask):
    """获取期货及股票期权合约基础信息 (全量更新，按交易所分批)"""

    # 1. 核心属性
    name = "tushare_option_basic"
    description = "获取期货及股票期权合约基础信息"
    table_name = "tushare_option_basic"
    primary_keys = ["ts_code"]  # 期权代码是唯一主键
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = None  # 全量任务不需要起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) ---
    # 考虑到需要按交易所分批，可以适当增加并发限制
    default_concurrent_limit = 5
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

    # 5. 数据库表结构 (根据 fields 和文档类型说明定义)
    schema = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "exchange": {"type": "VARCHAR(10)"},
        "name": {"type": "VARCHAR(50)"},
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
    ]

    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        self.logger.info(f"任务 {self.name} 已配置初始化。")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 opt_basic，按交易所分批获取。
        使用包含常见期货和股票期权交易所的列表。
        """
        # 包含常见期货和股票期权交易所的列表 (待验证哪些支持 opt_basic)
        exchanges = ["CFFEX", "DCE", "CZCE", "SHFE", "SSE", "SZSE"]
        batch_list = [{"exchange": exc} for exc in exchanges]
        self.logger.info(
            f"任务 {self.name}: 按交易所分批获取模式，生成 {len(batch_list)} 个批次。"
        )
        return batch_list

    async def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        主要依赖基类方法完成通用处理。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            exchange = kwargs.get("exchange", "未知交易所")
            self.logger.info(
                f"任务 {self.name}: process_data 接收到 {exchange} 的空 DataFrame，跳过处理。"
            )
            return df

        # 调用基类方法完成其他处理 (应用 transformations, 排序等)
        # 基类已经处理了 transformations 和日期转换。
        # 此处可以添加 opt_basic 特有的额外处理，如果需要的话。

        # 目前，没有 opt_basic 特有的额外处理，直接返回。
        self.logger.info(
            f"任务 {self.name}: process_data 被调用，返回 DataFrame (行数: {len(df)}). TushareDataTransformer 已完成主要处理。"
        )
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空 (注意空是允许的，如果某个交易所没有合约)。
        - 检查关键业务字段是否全部为空或缺失。
        """
        if df.empty:
            exchange = kwargs.get("exchange", "未知交易所")
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取 {exchange} 的 DataFrame 为空，无需验证。"
            )
            return df

        # 关键业务字段列表
        critical_cols = [
            "ts_code",
            "name",
            "exchange",
            "call_put",
            "exercise_price",
            "maturity_date",
        ]

        # 检查关键列是否存在
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 检查关键列是否所有行都为空
        # 替换空字符串为 NA以便 isnull() 检测
        df_check = df[critical_cols].replace("", pd.NA)
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值或缺失。"  # 修正错误信息，包含缺失
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        # 检查 exercise_price 和 maturity_date 是否有非空值 (至少有一行有效数据)
        if df["exercise_price"].isnull().all() or df["maturity_date"].isnull().all():
            self.logger.warning(
                f"任务 {self.name}: 数据验证警告 - exercise_price 或 maturity_date 列全为空，请检查数据源或API调用。"
            )

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条记录。")
        return df
