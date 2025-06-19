#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金持仓数据 (fund_portfolio) 更新任务
获取公募基金季度末持仓明细数据。
继承自 TushareTask，按 ann_date (公告日期) 增量更新。
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具
from ...tools.batch_utils import generate_natural_day_batches


@task_register()
class TushareFundPortfolioTask(TushareTask):
    """获取公募基金持仓数据"""

    # 1. 核心属性
    name = "tushare_fund_portfolio"
    description = "获取公募基金持仓明细"
    table_name = "fund_portfolio"
    data_source = "tushare"
    # 主键：基金代码 + 公告日期 + 股票代码 + 报告期 能够唯一确定一条持仓记录
    primary_keys = ["ts_code", "ann_date", "symbol", "end_date"]
    date_column = "ann_date"  # 使用公告日期进行增量更新
    default_start_date = "19980101"  # 基金持仓数据大致起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 4000

    # 2. TushareTask 特有属性
    api_name = "fund_portfolio"  # Tushare API 名称
    # 根据 https://tushare.pro/document/2?doc_id=121 列出字段
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "symbol",
        "mkv",
        "amount",
        "stk_mkv_ratio",
        "stk_float_ratio",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "mkv": lambda x: pd.to_numeric(x, errors="coerce"),  # 持有股票市值(元)
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),  # 持有股票数量（股）
        "stk_mkv_ratio": lambda x: pd.to_numeric(x, errors="coerce"),  # 占股票市值比
        "stk_float_ratio": lambda x: pd.to_numeric(
            x, errors="coerce"
        ),  # 占流通股本比例
        # ann_date 和 end_date 由基类 process_data 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},  # 报告期也加入非空
        "symbol": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},  # 股票代码
        "mkv": {"type": "FLOAT"},
        "amount": {"type": "FLOAT"},
        "stk_mkv_ratio": {"type": "FLOAT"},
        "stk_float_ratio": {"type": "FLOAT"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_portfolio_ts_code", "columns": "ts_code"},
        {"name": "idx_fund_portfolio_ann_date", "columns": "ann_date"},
        {"name": "idx_fund_portfolio_end_date", "columns": "end_date"},
        {"name": "idx_fund_portfolio_symbol", "columns": "symbol"},
        {
            "name": "idx_fund_portfolio_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 7. 分批配置 (按自然日，约3个月一批)
    batch_size_days = 90

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用自然日批次工具, 基于 ann_date)。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选参数

        if not start_date:
            latest_db_date = await self.get_latest_date()
            start_date = (
                (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                if latest_db_date
                else self.default_start_date
            )
            self.logger.info(
                f"未提供 start_date，使用数据库最新公告日期+1天或默认起始日期: {start_date}"
            )
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用当前日期: {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 (基于 ann_date)，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 使用自然日批次生成函数
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_size_days,
                date_format="%Y%m%d",  # 指定API参数中的日期字段名
                ts_code=ts_code,  # 传递 ts_code (如果提供)
                logger=self.logger,
            )
            # generate_natural_day_batches 返回的字典包含 start_date 和 end_date
            # 需要将其映射到 API 需要的 ann_date 参数 (如果API只接受单个日期)
            # 或者 start_date/end_date (如果API接受日期范围)
            # fund_portfolio API 接受 ann_date, start_date, end_date
            # generate_natural_day_batches 返回的批次参数可以直接使用
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    # prepare_params 可以使用基类默认实现，它会传递 batch_list 中的所有参数

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证基金持仓数据。
        """
        if df.empty:
            return df

        # 检查关键列是否存在
        critical_cols = ["ts_code", "ann_date", "end_date", "symbol"]
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 检查数值列是否为负数 (理论上不应为负)
        numeric_cols_check = ["mkv", "amount", "stk_mkv_ratio", "stk_float_ratio"]
        for col in numeric_cols_check:
            if col in df.columns:
                negative_count = (df[col].dropna() < 0).sum()
                if negative_count > 0:
                    self.logger.warning(
                        f"任务 {self.name}: 列 '{col}' 发现 {negative_count} 条负值记录。"
                    )

        return df
