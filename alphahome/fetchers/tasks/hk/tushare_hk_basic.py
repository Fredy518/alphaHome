#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
港股股票基本信息 (hk_basic) 全量更新任务
每次执行时，获取所有港股的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareHKBasicTask(TushareTask):
    """获取所有港股基础信息 (全量更新)"""

    # 1. 核心属性
    name = "tushare_hk_basic"
    description = "获取港股上市公司基础信息"
    table_name = "tushare_hk_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = "19700101"  # 全量任务，此日期仅为满足基类全量模式的日期要求，实际API调用不使用此日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000  # Tushare hk_basic 接口可能没有分页，但保留

    # 2. TushareTask 特有属性
    api_name = "hk_basic"
    # Tushare hk_basic 接口实际返回的字段 (请根据Tushare文档核实和调整)
    fields = [
        "ts_code",
        "name",
        "fullname",
        "enname",
        "cn_spell",
        "market",
        "list_status",
        "list_date",
        "delist_date",
        "trade_unit",
        "isin",
        "curr_type",  # 确保与API返回一致
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理)
    transformations = {
        # "trade_unit": int, # Example if conversion needed
        # "min_tick": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "fullname": {"type": "VARCHAR(200)"},
        "enname": {"type": "VARCHAR(255)"},
        "cn_spell": {"type": "VARCHAR(50)"},
        "market": {"type": "VARCHAR(10)"},
        "list_status": {"type": "VARCHAR(1)"},
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "trade_unit": {"type": "NUMERIC(10,0)"},  # 每手股数
        "isin": {"type": "VARCHAR(20)"},  # 新增 ISIN 码, VARCHAR(12) 通常也够用
        "curr_type": {"type": "VARCHAR(10)"},  # 货币类型, 从 'currency' 修改而来
        # "min_tick": {"type": "NUMERIC(10,5)"}, # 从 fields 列表来看，此字段不由API提供，移除
        # "security_type": {"type": "VARCHAR(20)"} # 从 fields 列表来看，此字段不由API提供，移除
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_hk_basic_name", "columns": "name"},
        {"name": "idx_hk_basic_market", "columns": "market"},
        {"name": "idx_hk_basic_list_status", "columns": "list_status"},
        {"name": "idx_hk_basic_update_time", "columns": "update_time"},
        # 移除了 security_type 相关的索引
        # {"name": "idx_hk_basic_security_type", "columns": "security_type"}
    ]

    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        # 全量更新，设置为串行执行以简化
        self.concurrent_limit = 1
        self.logger.info(f"任务 {self.name} 已配置为串行执行 (concurrent_limit=1)")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 hk_basic 全量获取，返回空参数字典列表。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # Tushare hk_basic 接口可能支持按 list_status 过滤，但全量通常一次获取
        return [{}]  # 触发一次不带参数的 API 调用

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        直接实现数据处理逻辑，避免与TushareDataTransformer形成循环调用。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
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
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段 ('name') 是否全部为空 (ts_code is primary key, should always exist)。
        """
        if df.empty:
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        critical_cols = ["name"]  # ts_code is already handled as PK
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace("", pd.NA)
        # Check if all critical columns are null for any row
        if df_check.isnull().all(axis=1).any():
            # More precise logging: find rows where all critical cols are null
            all_null_rows = df[df_check.isnull().all(axis=1)]
            self.logger.warning(
                f"任务 {self.name}: 数据验证发现 {len(all_null_rows)} 行的关键业务字段 ({', '.join(critical_cols)}) 同时为空: {all_null_rows['ts_code'].tolist() if 'ts_code' in all_null_rows else 'ts_code not available'}"
            )
            # Depending on strictness, one might raise ValueError here or just warn.
            # For hk_basic, a stock must have a name.
            if (
                df_check.isnull().all(axis=1).all()
            ):  # if ALL rows have all critical_cols as null
                error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
                self.logger.critical(error_msg)
                raise ValueError(error_msg)

        self.logger.info(
            f"任务 {self.name}: 数据验证通过（或有警告），获取了 {len(df)} 条记录。"
        )
        return df
