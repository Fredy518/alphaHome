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
        直接实现数据处理逻辑，避免与TushareDataTransformer形成循环调用。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            exchange = kwargs.get("exchange", "未知交易所")
            self.logger.info(
                f"任务 {self.name}: process_data 接收到 {exchange} 的空 DataFrame，跳过处理。"
            )
            return df

        # 手动执行TushareDataTransformer的处理步骤，但不调用其process_data方法
        data = df.copy()

        # 1. 应用列名映射
        data = self.data_transformer._apply_column_mapping(data)

        # 2. 处理主要的 date_column (如果定义)
        data = self.data_transformer._process_date_column(data)

        # 3. 应用通用数据类型转换
        data = self.data_transformer._apply_transformations(data)

        # 4. 手动处理 schema_def 中定义的其他 DATE/TIMESTAMP 列
        if hasattr(self, "schema_def") and self.schema_def:
            date_columns_to_process = []
            # 识别需要处理的日期列
            for col_name, col_def in self.schema_def.items():
                col_type = (
                    col_def.get("type", "").upper()
                    if isinstance(col_def, dict)
                    else str(col_def).upper()
                )
                if (
                    ("DATE" in col_type or "TIMESTAMP" in col_type)
                    and col_name in data.columns
                    and col_name != getattr(self, "date_column", None)
                ):
                    # 仅处理尚未是日期时间类型的列
                    if data[col_name].dtype == "object" or pd.api.types.is_string_dtype(
                        data[col_name]
                    ):
                        date_columns_to_process.append(col_name)

            # 批量处理识别出的日期列
            if date_columns_to_process:
                self.logger.info(
                    f"转换以下列为日期时间格式: {', '.join(date_columns_to_process)}"
                )
                
                for col_name in date_columns_to_process:
                    try:
                        # 首先尝试直接转换
                        converted_col = pd.to_datetime(data[col_name], errors="coerce")
                        
                        # 检查是否大部分转换成功
                        success_rate = converted_col.notna().sum() / len(converted_col)
                        
                        if success_rate < 0.5:  # 如果成功率低于50%，尝试其他格式
                            self.logger.info(f"列 {col_name} 直接转换成功率较低 ({success_rate:.2%})，尝试YYYYMMDD格式")
                            # 尝试 YYYYMMDD 格式
                            converted_col = pd.to_datetime(
                                data[col_name], format="%Y%m%d", errors="coerce"
                            )
                            
                        # 检查最终转换结果
                        final_success_rate = converted_col.notna().sum() / len(converted_col)
                        invalid_count = converted_col.isna().sum()
                        
                        if invalid_count > 0:
                            self.logger.warning(
                                f"列 {col_name}: {invalid_count} 个值无法转换为日期，将设为NaT"
                            )
                        
                        # 将转换后的列赋值回原数据
                        data[col_name] = converted_col
                        
                        self.logger.info(f"列 {col_name} 成功转换为datetime类型，成功率: {final_success_rate:.2%}")
                            
                    except Exception as e:
                        self.logger.error(f"转换列 {col_name} 时发生错误: {e}")
                        continue

        # 5. 对数据进行排序
        data = self.data_transformer._sort_data(data)

        self.logger.info(
            f"任务 {self.name}: process_data 处理完成，返回 DataFrame (行数: {len(data)})."
        )
        return data

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
