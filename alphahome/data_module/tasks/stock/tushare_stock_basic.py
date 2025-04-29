#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票基本信息 (stock_basic) 全量更新任务
每次执行时，获取所有股票的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

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
    date_column = None # 该任务不以日期为主，全量更新
    default_start_date = None # 全量任务不需要起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "stock_basic"
    # Tushare stock_basic 接口实际返回的字段
    fields = [
        'ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname',
        'cnspell', 'market', 'exchange', 'curr_type', 'list_status', 'list_date',
        'delist_date', 'is_hs', 'act_name', 'act_ent_type'
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
        "industry": {"type": "VARCHAR(100)"}, # 行业名称可能较长
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
        "act_ent_type": {"type": "VARCHAR(100)"} # 控制人性质可能较长
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_basic_name", "columns": "name"},
        {"name": "idx_stock_basic_industry", "columns": "industry"},
        {"name": "idx_stock_basic_market", "columns": "market"}
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

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        覆盖基类的数据处理方法，特别处理日期列 list_date 和 delist_date。
        1. 使用 errors='coerce' 转换日期列，允许无效格式。
        2. list_date (date_column) 的 NaT 填充为 '1970-01-01'。
        3. delist_date 的 NaT 保持 NaT (将被保存为 NULL)。
        4. 调用基类的 process_data 完成剩余处理。
        """
        date_cols_to_process = ['list_date', 'delist_date']
        fill_date = pd.Timestamp('1970-01-01') # 仅用于 date_column

        for col_name in date_cols_to_process:
            if col_name in df.columns:
                original_dtype = df[col_name].dtype
                # 尝试直接用 YYYYMMDD 转换，如果失败再用通用转换
                try:
                    # 确保输入是字符串类型，避免因混合类型（如整数）导致 format='%Y%m%d' 失败
                    df[col_name] = pd.to_datetime(df[col_name].astype(str), format='%Y%m%d', errors='coerce')
                except Exception:
                    # 如果 format='%Y%m%d' 出错（例如数据不是纯粹的YYYYMMDD字符串），尝试通用转换
                    self.logger.warning(f"任务 {self.name}: 列 '{col_name}' 使用 YYYYMMDD 格式转换失败，尝试通用解析。")
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')

                nat_count = df[col_name].isnull().sum()
                
                # 只对主要的日期列 (self.date_column) 填充默认值
                if col_name == self.date_column:
                    if nat_count > 0:
                        df[col_name].fillna(fill_date, inplace=True)
                        self.logger.info(f"任务 {self.name}: 将 {nat_count} 个无效或缺失的 '{col_name}' (主日期列) 填充为 {fill_date.date()}")
                # 对于其他日期列 (如 delist_date)，记录 NaT 数量但不填充
                elif nat_count > 0:
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
        - 检查关键业务字段 ('symbol', 'name') 是否全部为空。
        """
        if df.empty:
            self.logger.warning(f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。")
            return df

        critical_cols = ['symbol', 'name']
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace('', pd.NA)
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。")
        return df 