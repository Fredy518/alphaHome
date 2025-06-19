#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
居民消费价格指数 (cn_cpi) 更新任务
获取中国居民消费价格指数(CPI)数据。
继承自 TushareTask。
使用月度批次生成。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入月份批次生成工具函数
from ...tools.batch_utils import generate_month_batches


@task_register()
class TushareMacroCpiTask(TushareTask):
    """获取中国居民消费价格指数(CPI)数据"""

    # 1. 核心属性
    name = "tushare_macro_cpi"
    description = "获取中国居民消费价格指数(CPI)"
    table_name = "macro_cpi"
    primary_keys = ["month_end_date"]  # 修改: 使用 month_end_date 作为主键
    date_column = "month_end_date"  # 修改: 使用 month_end_date 作为主要日期列
    default_start_date = "19960101"  # API 支持YYYYMM，但为与 month_end_date 保持一致性
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 单次最大5000行

    # 启用单批次处理模式
    single_batch = True

    # 2. TushareTask 特有属性
    api_name = "cn_cpi"
    # Tushare cn_cpi 接口实际返回的字段 (根据文档 [https://tushare.pro/document/2?doc_id=228])
    fields = [
        "month",
        "nt_val",
        "nt_yoy",
        "nt_mom",
        "nt_accu",
        "town_val",
        "town_yoy",
        "town_mom",
        "town_accu",
        "cnt_val",
        "cnt_yoy",
        "cnt_mom",
        "cnt_accu",
    ]

    # 3. 列名映射 (API字段名与数据库列名不完全一致时，进行映射)
    column_mapping = {}  # 无需映射

    # 4. 数据类型转换 (日期列在基类处理，数值列转换为 float)
    # 所有字段转换为 float，除了 month
    transformations = {field: float for field in fields if field != "month"}

    # 5. 数据库表结构
    schema_def = {
        "month": {
            "type": "VARCHAR(10)",
            "constraints": "NOT NULL",
        },  # YYYYMM 格式, 保留原始月份
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},  # 新增: 月末日期
        "nt_val": {"type": "NUMERIC(15,4)"},  # 全国当月值
        "nt_yoy": {"type": "NUMERIC(15,4)"},  # 全国同比（%）
        "nt_mom": {"type": "NUMERIC(15,4)"},  # 全国环比（%）
        "nt_accu": {"type": "NUMERIC(15,4)"},  # 全国累计值
        "town_val": {"type": "NUMERIC(15,4)"},  # 城市当月值
        "town_yoy": {"type": "NUMERIC(15,4)"},  # 城市同比（%）
        "town_mom": {"type": "NUMERIC(15,4)"},  # 城市环比（%）
        "town_accu": {"type": "NUMERIC(15,4)"},  # 城市累计值
        "cnt_val": {"type": "NUMERIC(15,4)"},  # 农村当月值
        "cnt_yoy": {"type": "NUMERIC(15,4)"},  # 农村同比（%）
        "cnt_mom": {"type": "NUMERIC(15,4)"},  # 农村环比（%）
        "cnt_accu": {"type": "NUMERIC(15,4)"},  # 农村累计值
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        # {"name": "idx_macro_cpi_month", "columns": "month"}, # 移除或注释掉
        {
            "name": "idx_cpi_month_orig",
            "columns": "month",
        },  # 可选：为原始 month 列保留索引
        {"name": "idx_macro_cpi_update_time", "columns": "update_time"},
    ]

    # 7. 分批配置
    # 批次大小，每次获取12个月的数据（1年）
    batch_month_size = 12

    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        self.logger.info(f"任务 {self.name} 已配置初始化。")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表，使用月份批次。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        # 转换为YYYYMM格式
        if len(start_date) == 8:  # YYYYMMDD格式
            start_date = start_date[:6]  # 取YYYYMM部分
        if len(end_date) == 8:  # YYYYMMDD格式
            end_date = end_date[:6]  # 取YYYYMM部分

        self.logger.info(
            f"任务 {self.name}: 使用月份批次生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用月份批次工具函数，每个批次12个月（1年）
            batch_list = await generate_month_batches(
                start_m=start_date,
                end_m=end_date,
                batch_size=self.batch_month_size,
                logger=self.logger,
            )
            self.logger.info(
                f"任务 {self.name}: 成功生成 {len(batch_list)} 个月度批次。"
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成月度批次时出错: {e}", exc_info=True
            )
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成月度批次失败") from e

    async def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        调用基类方法完成通用处理。
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.info(
                f"任务 {self.name}: process_data 接收到空 DataFrame，跳过处理。"
            )
            return df

        # 新增: 计算 month_end_date
        try:
            df["month_end_date"] = pd.to_datetime(
                df["month"], format="%Y%m"
            ) + pd.offsets.MonthEnd(0)
            # 确保转换为纯日期对象，如果数据库列是 DATE 类型
            df["month_end_date"] = df["month_end_date"].dt.date
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 计算 month_end_date 时出错: {e}", exc_info=True
            )
            raise ValueError(
                f"任务 {self.name}: month_end_date 计算失败，停止处理。"
            ) from e

        self.logger.info(
            f"任务 {self.name}: process_data 被调用，已添加 month_end_date，返回 DataFrame (行数: {len(df)})。"
        )
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段 ('month', 'nt_val', 'nt_yoy') 是否全部为空。
        """
        if df.empty:
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        critical_cols = [
            "month",
            "nt_val",
            "nt_yoy",
        ]  # 最关键的字段：月份、全国当月值和同比
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
