#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金列表 (fund_basic) 全量更新任务
每次执行时，获取所有基金的基本信息并替换数据库中的旧数据。
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
class TushareFundBasicTask(TushareTask):
    """获取公募基金列表 (全量更新)"""

    # 1. 核心属性
    name = "tushare_fund_basic"
    description = "获取公募基金基本信息"
    table_name = "fund_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 全量更新
    default_start_date = None  # 全量更新
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "fund_basic"
    # Tushare fund_basic 接口实际返回的字段
    fields = [
        "ts_code",
        "name",
        "management",
        "custodian",
        "fund_type",
        "found_date",
        "due_date",
        "list_date",
        "issue_date",
        "delist_date",
        "issue_amount",
        "m_fee",
        "c_fee",
        "duration_year",
        "p_value",
        "min_amount",
        "exp_return",
        "benchmark",
        "status",
        "invest_type",
        "type",
        "trustee",
        "purc_startdate",
        "redm_startdate",
        "market",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理, 数值列在此处定义)
    transformations = {
        "issue_amount": lambda x: pd.to_numeric(x, errors="coerce"),
        "m_fee": lambda x: pd.to_numeric(x, errors="coerce"),
        "c_fee": lambda x: pd.to_numeric(x, errors="coerce"),
        "duration_year": lambda x: pd.to_numeric(x, errors="coerce"),
        "p_value": lambda x: pd.to_numeric(x, errors="coerce"),
        "min_amount": lambda x: pd.to_numeric(x, errors="coerce"),
        "exp_return": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
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
        "benchmark": {"type": "TEXT"},  # 业绩基准可能很长
        "status": {"type": "VARCHAR(1)"},
        "invest_type": {"type": "VARCHAR(100)"},
        "type": {"type": "VARCHAR(100)"},  # 基金类型
        "trustee": {"type": "VARCHAR(100)"},
        "purc_startdate": {"type": "DATE"},
        "redm_startdate": {"type": "DATE"},
        "market": {"type": "VARCHAR(1)"},  # E场内 O场外
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_basic_fund_type", "columns": "fund_type"},
        {"name": "idx_fund_basic_market", "columns": "market"},
        {"name": "idx_fund_basic_status", "columns": "status"},
        {"name": "idx_fund_basic_list_date", "columns": "list_date"},
        {
            "name": "idx_fund_basic_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    def __init__(self, db_connection, api_token=None, api=None, **kwargs):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
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
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        critical_cols = ["name"]
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
