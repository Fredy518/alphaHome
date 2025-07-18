#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中信行业指数日线行情 (ci_daily) 更新任务
获取中信行业指数日线行情数据。
继承自 TushareTask，按日期增量更新。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_trade_day_batches

# logger 由 Task 基类提供


@task_register()
class TushareIndexCiDailyTask(TushareTask):
    """获取中信行业指数日线行情数据"""

    # 1. 核心属性
    name = "tushare_index_cidaily"
    description = "获取中信行业指数日线行情数据"
    table_name = "index_cidaily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    # 中信指数发布日期可能也较早，暂定与申万一致
    default_start_date = "20050101"
    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 4000
    data_source = "tushare"

    # 2. TushareTask 特有属性
    api_name = "ci_daily"  # Tushare API 名称
    fields = [  # 根据文档列出所有需要的字段
        "ts_code",
        "trade_date",
        "open",
        "low",
        "high",
        "close",
        "pre_close",
        "change",
        "pct_change",
        "vol",
        "amount",
    ]

    # 3. 列名映射 (保持与 sw_daily 一致，vol -> volume)
    column_mapping = {"vol": "volume"}

    # 4. 数据类型转换
    transformations = {
        "open": lambda x: pd.to_numeric(x, errors="coerce"),
        "low": lambda x: pd.to_numeric(x, errors="coerce"),
        "high": lambda x: pd.to_numeric(x, errors="coerce"),
        "close": lambda x: pd.to_numeric(x, errors="coerce"),
        "pre_close": lambda x: pd.to_numeric(x, errors="coerce"),
        "change": lambda x: pd.to_numeric(x, errors="coerce"),
        "pct_change": lambda x: pd.to_numeric(x, errors="coerce"),
        "vol": lambda x: pd.to_numeric(x, errors="coerce"),  # 成交量（万股）
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),  # 成交额（万元）
        # trade_date 由基类 process_data 中的 _process_date_column 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "FLOAT"},
        "low": {"type": "FLOAT"},
        "high": {"type": "FLOAT"},
        "close": {"type": "FLOAT"},
        "pre_close": {"type": "FLOAT"},
        "change": {"type": "FLOAT"},
        "pct_change": {"type": "FLOAT"},
        "volume": {"type": "FLOAT"},  # 使用映射后的列名
        "amount": {"type": "FLOAT"},
        # update_time 会自动添加
        # 主键 ("ts_code", "trade_date") 索引由基类自动处理
    }

    # 6. 自定义索引 (主键已包含，无需额外添加)
    indexes = [
        {
            "name": "idx_tushare_index_cidaily_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 7. 分批配置 (与 sw_daily 保持一致)
    batch_trade_days_single_code = 360
    batch_trade_days_all_codes = 30

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用交易日批次工具)。
        与 sw_daily 逻辑一致。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的指数代码

        # 检查必要的日期参数
        if not start_date:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date = (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = self.default_start_date
            self.logger.info(f"未提供 start_date，使用: {start_date}")

        if not end_date:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")  # 默认到今天
            self.logger.info(f"未提供 end_date，使用: {end_date}")

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 指数代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=(
                    self.batch_trade_days_single_code
                    if ts_code
                    else self.batch_trade_days_all_codes
                ),
                ts_code=ts_code,
                logger=self.logger,
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True
            )
            return []

    # prepare_params 方法使用基类默认实现

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "中信行业指数代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['close'].notna(), "收盘价不能为空"),
        (lambda df: df['close'] > 0, "收盘价必须为正数"),
        (lambda df: df['open'] > 0, "开盘价必须为正数"),
        (lambda df: df['high'] > 0, "最高价必须为正数"),
        (lambda df: df['low'] > 0, "最低价必须为正数"),
        (lambda df: df['high'] >= df['low'], "最高价不能低于最低价"),
        (lambda df: df['high'] >= df['open'], "最高价不能低于开盘价"),
        (lambda df: df['high'] >= df['close'], "最高价不能低于收盘价"),
        (lambda df: df['low'] <= df['open'], "最低价不能高于开盘价"),
        (lambda df: df['low'] <= df['close'], "最低价不能高于收盘价"),
        (lambda df: df['volume'] >= 0, "成交量不能为负数"),
        (lambda df: df['amount'] >= 0, "成交额不能为负数"),
    ]
