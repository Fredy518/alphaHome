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
from alphahome.common.task_system.task_decorator import task_register

# 假设的数据库异常类，可以根据实际使用的库替换，例如 asyncpg.exceptions.PostgresError
# from asyncpg.exceptions import PostgresError

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareIndexBasicTask(TushareTask):
    """获取指数基本信息 (UPSERT 更新)"""

    # 1. 核心属性
    name = "tushare_index_basic"
    description = "获取指数基础信息"
    table_name = "tushare_index_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 全量任务
    default_start_date = "19900101"  # 全量任务，设置一个早期默认起始日期

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
        
        # 任务特有的额外处理：对list_date进行特殊填充
        if "list_date" in data.columns:
            nat_count = data["list_date"].isnull().sum()
            if nat_count > 0:
                fill_date = pd.Timestamp("1970-01-01")
                data["list_date"].fillna(fill_date, inplace=True)
                self.logger.info(
                    f"任务 {self.name}: 将 {nat_count} 个无效或缺失的 'list_date' 填充为 {fill_date.date()}"
                )
            else:
                self.logger.info(f"任务 {self.name}: 'list_date' 列无需填充默认日期。")
        else:
            self.logger.warning(
                f"任务 {self.name}: DataFrame 中未找到 'list_date' 列，跳过预处理。"
            )

        self.logger.info(
            f"任务 {self.name}: process_data 处理完成，返回 DataFrame (行数: {len(data)})."
        )
        return data

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
