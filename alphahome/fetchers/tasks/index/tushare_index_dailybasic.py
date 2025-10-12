#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
大盘指数每日指标 (index_dailybasic) 更新任务
获取指定12个主要指数的每日指标数据，包括市值、换手率、市盈率等。

全量更新模式：直接使用ts_code分批，只需要获取以下12个代码的全量数据：
000001.SH, 000005.SH, 000006.SH, 000016.SH, 000300.SH,
000905.SH, 399001.SZ, 399005.SZ, 399006.SZ, 399016.SZ,
399300.SZ, 399905.SZ

增量模式：使用交易日分批。
单次限量3000条。

继承自 TushareTask，支持全量更新和增量更新两种模式。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_trade_day_batches


@task_register()
class TushareIndexDailyBasicTask(TushareTask):
    """获取大盘指数每日指标数据"""

    # 1. 核心属性
    name = "tushare_index_dailybasic"
    description = "获取大盘指数每日指标数据"
    table_name = "index_dailybasic"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20040101"  # 根据文档，从2004年1月开始提供数据
    data_source = "tushare"
    domain = "index"  # 业务域标识

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 3000  # 单次限量3000条

    # 2. TushareTask 特有属性
    api_name = "index_dailybasic"
    fields = [
        "ts_code",
        "trade_date",
        "total_mv",
        "float_mv",
        "total_share",
        "float_share",
        "free_share",
        "turnover_rate",
        "turnover_rate_f",
        "pe",
        "pe_ttm",
        "pb",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "total_mv": lambda x: pd.to_numeric(x, errors="coerce"),
        "float_mv": lambda x: pd.to_numeric(x, errors="coerce"),
        "total_share": lambda x: pd.to_numeric(x, errors="coerce"),
        "float_share": lambda x: pd.to_numeric(x, errors="coerce"),
        "free_share": lambda x: pd.to_numeric(x, errors="coerce"),
        "turnover_rate": lambda x: pd.to_numeric(x, errors="coerce"),
        "turnover_rate_f": lambda x: pd.to_numeric(x, errors="coerce"),
        "pe": lambda x: pd.to_numeric(x, errors="coerce"),
        "pe_ttm": lambda x: pd.to_numeric(x, errors="coerce"),
        "pb": lambda x: pd.to_numeric(x, errors="coerce"),
        # trade_date 由基类 process_data 中的 _process_date_column 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "total_mv": {"type": "NUMERIC(20,2)"},  # 当日总市值（元）
        "float_mv": {"type": "NUMERIC(20,2)"},  # 当日流通市值（元）
        "total_share": {"type": "NUMERIC(15,2)"},  # 当日总股本（股）
        "float_share": {"type": "NUMERIC(15,2)"},  # 当日流通股本（股）
        "free_share": {"type": "NUMERIC(15,2)"},  # 当日自由流通股本（股）
        "turnover_rate": {"type": "NUMERIC(10,4)"},  # 换手率
        "turnover_rate_f": {"type": "NUMERIC(10,4)"},  # 换手率(基于自由流通股本)
        "pe": {"type": "NUMERIC(15,4)"},  # 市盈率
        "pe_ttm": {"type": "NUMERIC(15,4)"},  # 市盈率TTM
        "pb": {"type": "NUMERIC(15,4)"},  # 市净率
        # update_time 会自动添加
        # 主键 ("ts_code", "trade_date") 索引由基类自动处理
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_index_dailybasic_trade_date", "columns": "trade_date"},
        {"name": "idx_index_dailybasic_ts_code", "columns": "ts_code"},
        {"name": "idx_index_dailybasic_ts_code_trade_date", "columns": ["ts_code", "trade_date"]},
        {
            "name": "idx_index_dailybasic_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 7. 指定的12个指数代码
    target_index_codes = [
        "000001.SH",  # 上证综指
        "000005.SH",  # 上证180
        "000006.SH",  # 上证50
        "000016.SH",  # 上证50
        "000300.SH",  # 沪深300
        "000905.SH",  # 中证500
        "399001.SZ",  # 深证成指
        "399005.SZ",  # 中小板指
        "399006.SZ",  # 创业板指
        "399016.SZ",  # 创业板指
        "399300.SZ",  # 沪深300
        "399905.SZ",  # 中证500
    ]

    # 8. 分批配置
    batch_trade_days_single_code = 360  # 单个指数的交易日批次大小
    batch_trade_days_all_codes = 30     # 所有指数的交易日批次大小

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。

        全量更新模式：直接使用ts_code分批，针对指定的12个指数
        增量模式：使用交易日分批
        """
        update_type = kwargs.get("update_type", "incremental")

        if update_type == UpdateTypes.FULL:
            # 全量更新模式：直接使用ts_code分批
            return await self._get_backfill_batch_list(**kwargs)
        else:
            # 增量模式：使用交易日分批
            return await self._get_incremental_batch_list(**kwargs)

    async def _get_backfill_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成全量更新模式的批处理参数列表。
        直接使用ts_code分批，针对指定的12个指数，每个批次包含一个指数的全量数据。
        """
        self.logger.info(f"任务 {self.name}: 全量更新模式，使用ts_code分批处理。")

        # 为每个指定的指数生成一个批次
        batch_list = []
        for ts_code in self.target_index_codes:
            batch_params = {
                "ts_code": ts_code,
                "start_date": self.default_start_date,
                "end_date": pd.Timestamp.now().strftime("%Y%m%d")
            }
            batch_list.append(batch_params)

        self.logger.info(f"生成了 {len(batch_list)} 个批次，每个批次处理一个指数的全量数据。")
        return batch_list

    async def _get_incremental_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成增量更新模式的批处理参数列表。
        为每个指数代码生成单独的批次，使用交易日分批。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

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
            f"任务 {self.name}: 增量更新模式，范围: {start_date} 到 {end_date}"
        )

        try:
            # 为每个指定的指数生成批次
            batch_list = []
            for ts_code in self.target_index_codes:
                # 为每个指数生成交易日批次
                index_batches = await generate_trade_day_batches(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=self.batch_trade_days_single_code,
                    ts_code=ts_code,
                    logger=self.logger,
                )
                batch_list.extend(index_batches)

            self.logger.info(f"生成了 {len(batch_list)} 个增量批次，覆盖 {len(self.target_index_codes)} 个指数。")
            return batch_list

        except Exception as e:
            self.logger.error(f"生成增量批次列表失败: {e}")
            return []


    # 9. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "指数代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['ts_code'].isin([
            "000001.SH", "000005.SH", "000006.SH", "000016.SH", "000300.SH",
            "000905.SH", "399001.SZ", "399005.SZ", "399006.SZ", "399016.SZ",
            "399300.SZ", "399905.SZ"
        ]), "只允许指定的12个指数代码"),
    ]

    async def pre_execute(self):
        """
        预执行处理。
        对于全量更新模式，清空表数据。
        """
        update_type = getattr(self, 'update_type', 'incremental')

        if update_type == UpdateTypes.FULL:
            self.logger.info(f"任务 {self.name}: 全量更新模式，预先清空表数据。")
            await self.clear_table()
        else:
            self.logger.info(f"任务 {self.name}: 增量更新模式，保留现有数据。")

"""
使用方法:
1. 全量更新：设置 update_type="full" 或通过GUI选择"全量更新"
   - 会清空现有数据，重新获取所有指定指数的全量数据
2. 增量更新：设置 update_type="incremental/smart/manual" (默认)
   - 只获取最新的数据，追加到现有数据中
3. 单次限量3000条，确保符合API限制

注意事项:
- 只处理指定的12个主要指数
- 全量更新模式使用ts_code分批，每个指数一个批次
- 增量模式使用交易日分批，所有指数统一处理
"""
