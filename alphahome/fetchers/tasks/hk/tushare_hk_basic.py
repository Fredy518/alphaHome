#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
港股股票基本信息 (hk_basic) 全量更新任务
每次执行时，获取所有港股的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

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
    date_column = None # 该任务不以日期为主，全量更新
    default_start_date = None # 全量任务不需要起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000 # Tushare hk_basic 接口可能没有分页，但保留

    # 2. TushareTask 特有属性
    api_name = "hk_basic"
    # Tushare hk_basic 接口实际返回的字段 (请根据Tushare文档核实和调整)
    fields = [
        'ts_code', 'name', 'fullname', 'enname', 'cn_spell', 'market', 'list_status', 
        'list_date', 'delist_date', 'trade_unit', 'isin', 'curr_type' # 确保与API返回一致
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理)
    transformations = {
        # "trade_unit": int, # Example if conversion needed
        # "min_tick": float,
    }

    # 5. 数据库表结构
    schema = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"}, 
        "name": {"type": "VARCHAR(100)"}, 
        "fullname": {"type": "VARCHAR(255)"},
        "enname": {"type": "VARCHAR(255)"},
        "cn_spell": {"type": "VARCHAR(50)"}, 
        "market": {"type": "VARCHAR(10)"}, 
        "list_status": {"type": "VARCHAR(1)"}, 
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "trade_unit": {"type": "NUMERIC(10,0)"}, # 每手股数
        "isin": {"type": "VARCHAR(20)"},           # 新增 ISIN 码, VARCHAR(12) 通常也够用
        "curr_type": {"type": "VARCHAR(10)"}      # 货币类型, 从 'currency' 修改而来
        # "min_tick": {"type": "NUMERIC(10,5)"}, # 从 fields 列表来看，此字段不由API提供，移除
        # "security_type": {"type": "VARCHAR(20)"} # 从 fields 列表来看，此字段不由API提供，移除
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_hk_basic_name", "columns": "name"},
        {"name": "idx_hk_basic_market", "columns": "market"},
        {"name": "idx_hk_basic_list_status", "columns": "list_status"},
        {"name": "idx_hk_basic_update_time", "columns": "update_time"}
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
        return [{}] # 触发一次不带参数的 API 调用

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        覆盖基类的数据处理方法，特别处理日期列 list_date 和 delist_date。
        1. 使用 errors='coerce' 转换日期列，允许无效格式。
        2. list_date (如果作为 date_column) 的 NaT 填充 (当前 date_column is None)。
        3. delist_date 的 NaT 保持 NaT (将被保存为 NULL)。
        4. 调用基类的 process_data 完成剩余处理。
        """
        date_cols_to_process = ['list_date', 'delist_date']
        # fill_date = pd.Timestamp('1970-01-01') # Not used as self.date_column is None

        for col_name in date_cols_to_process:
            if col_name in df.columns:
                original_dtype = df[col_name].dtype
                try:
                    df[col_name] = pd.to_datetime(df[col_name].astype(str), format='%Y%m%d', errors='coerce')
                except Exception:
                    self.logger.warning(f"任务 {self.name}: 列 '{col_name}' 使用 YYYYMMDD 格式转换失败，尝试通用解析。")
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')

                nat_count = df[col_name].isnull().sum()
                
                if nat_count > 0:
                    # For non-date_column date fields, just log NaNs. They'll be saved as NULL.
                    self.logger.info(f"任务 {self.name}: 列 '{col_name}' 包含 {nat_count} 个无效或缺失日期 (将保存为 NULL)")
            else:
                self.logger.warning(f"任务 {self.name}: DataFrame 中未找到日期列 '{col_name}'，跳过预处理。")

        # 调用基类方法完成其他处理 (应用 transformations, 排序等)
        df = await super().process_data(df, **kwargs)
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段 ('name') 是否全部为空 (ts_code is primary key, should always exist)。
        """
        if df.empty:
            self.logger.warning(f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。")
            return df

        critical_cols = ['name'] # ts_code is already handled as PK
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace('', pd.NA)
        # Check if all critical columns are null for any row
        if df_check.isnull().all(axis=1).any():
            # More precise logging: find rows where all critical cols are null
            all_null_rows = df[df_check.isnull().all(axis=1)]
            self.logger.warning(f"任务 {self.name}: 数据验证发现 {len(all_null_rows)} 行的关键业务字段 ({', '.join(critical_cols)}) 同时为空: {all_null_rows['ts_code'].tolist() if 'ts_code' in all_null_rows else 'ts_code not available'}")
            # Depending on strictness, one might raise ValueError here or just warn.
            # For hk_basic, a stock must have a name.
            if df_check.isnull().all(axis=1).all(): # if ALL rows have all critical_cols as null
                error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
                self.logger.critical(error_msg)
                raise ValueError(error_msg)


        self.logger.info(f"任务 {self.name}: 数据验证通过（或有警告），获取了 {len(df)} 条记录。")
        return df 