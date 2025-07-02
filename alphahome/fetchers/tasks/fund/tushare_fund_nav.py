#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金净值 (fund_nav) 更新任务
获取公募基金净值数据。
继承自 TushareTask，按 nav_date 增量更新。
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable, Tuple

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_single_date_batches

# 导入日历工具
from ...tools.calendar import get_trade_days_between


@task_register()
class TushareFundNavTask(TushareTask):
    """获取公募基金净值数据"""

    # 1. 核心属性
    name = "tushare_fund_nav"
    description = "获取公募基金净值数据"
    table_name = "fund_nav"
    primary_keys = ["ts_code", "nav_date"]  # 使用净值日期作为主键之一
    date_column = "nav_date"  # 主要日期列用于增量更新
    default_start_date = "20000101"  # 设定一个较早的默认开始日期
    data_source = "tushare"
    domain = "fund"  # 业务域标识
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "fund_nav"
    fields = [
        "ts_code",
        "ann_date",
        "nav_date",
        "unit_nav",
        "accum_nav",
        "accum_div",
        "net_asset",
        "total_netasset",
        "adj_nav",
    ]

    # 3. 列名映射 (无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "unit_nav": lambda x: pd.to_numeric(x, errors="coerce"),
        "accum_nav": lambda x: pd.to_numeric(x, errors="coerce"),
        "accum_div": lambda x: pd.to_numeric(x, errors="coerce"),
        "net_asset": lambda x: pd.to_numeric(x, errors="coerce"),
        "total_netasset": lambda x: pd.to_numeric(x, errors="coerce"),
        "adj_nav": lambda x: pd.to_numeric(x, errors="coerce"),
        # 日期列 ann_date 和 nav_date 由基类 process_data 处理
        # 特别注意：如果 process_data 中只处理 self.date_column ('nav_date')
        # 并且我们希望 ann_date 也被当作日期处理，需要在 process_data 中添加逻辑
        # 或者在基类 TushareTask.process_data 中使其能处理 schema 定义的所有 DATE 类型
        # 目前基类 TushareTask.process_data 已包含处理 schema 中 DATE/TIMESTAMP 列的逻辑
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "nav_date": {"type": "DATE", "constraints": "NOT NULL"},
        "unit_nav": {"type": "NUMERIC(15,6)"},
        "accum_nav": {"type": "NUMERIC(15,6)"},
        "accum_div": {"type": "NUMERIC(15,6)"},
        "net_asset": {"type": "NUMERIC(20,3)"},
        "total_netasset": {"type": "NUMERIC(20,3)"},
        "adj_nav": {"type": "NUMERIC(15,6)"},
        # update_time 会自动添加
        # 主键 ("ts_code", "nav_date") 索引由基类自动处理
    }

    # 6. 自定义索引
    indexes = [
        # 可以考虑在 ann_date 上添加索引，如果经常按公告日期查询
        {"name": "idx_fund_nav_ann_date", "columns": "ann_date"},
        {
            "name": "idx_fund_nav_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 7. 数据验证规则
    validations: Optional[List[Union[Callable, Tuple[Callable, str]]]] = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['nav_date'].notna(), "净值日期不能为空"),
        (lambda df: df['unit_nav'] > 0, "单位净值必须为正"),
        (lambda df: df['accum_nav'] >= df['unit_nav'], "累计净值应大于等于单位净值"),
        (lambda df: df['net_asset'] >= 0, "基金资产净值非负"),
    ]

    # 8. 分批配置
    batch_trade_days_single_code = 360  # 单基金查询时，约1.5年
    batch_trade_days_all_codes = 5  # 全市场查询

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用单日期批次工具)。
        为每个交易日生成单独的批次，使用nav_date参数而不是start_date/end_date范围。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")
        market = kwargs.get("market")  # fund_nav 支持 market 参数

        if not start_date:
            latest_db_date = (
                await self.get_latest_date()
            )  # 基类方法获取 self.date_column 的最新日期
            start_date = (
                (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                if latest_db_date
                else self.default_start_date
            )
            self.logger.info(f"未提供 start_date，使用: {start_date}")
        if not end_date:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用: {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，日期范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}, 市场: {market if market else '所有'}"
        )

        try:
            # 使用专用的单日期批次生成函数
            additional_params = {}
            if market:
                additional_params["market"] = market

            batch_list = await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field="nav_date",
                ts_code=ts_code,
                additional_params=additional_params,
                logger=self.logger,
            )

            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    async def prepare_params(self, batch_params: Dict) -> Dict:
        """
        准备 API 调用参数。
        将批次中的 nav_date 直接映射到 API 参数中。
        fund_nav API 需要 nav_date 或 ts_code 至少提供一个。
        """
        api_params = {}

        # 传递必要的参数：ts_code、market 和 nav_date
        if "ts_code" in batch_params and batch_params["ts_code"]:
            api_params["ts_code"] = batch_params["ts_code"]

        if "market" in batch_params and batch_params["market"]:
            api_params["market"] = batch_params["market"]

        if "nav_date" in batch_params and batch_params["nav_date"]:
            api_params["nav_date"] = batch_params["nav_date"]

        return api_params

    # validate_data 可以使用基类或自定义
