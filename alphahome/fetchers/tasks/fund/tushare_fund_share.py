#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基金规模数据 (fund_share) 更新任务
获取基金规模数据，包含上海和深圳ETF基金。
继承自 TushareTask，按日期增量更新。

特别说明：
- Tushare fund_share 在部分时段存在 SZ 日期错位 1 天的问题。
- 深市 ETF 日期按每条交易所历史记录核验；沪深最新日期差不作为历史纠偏依据。
- REITs/LOF 口径不同，保留供应商日期，不套用 ETF 的日期修正。
- 缺少交易所记录的整只产品保留原始标签并明确警告；不认证其历史可得性。
- 缺少日期或数值证据时中止该批次，保留现有记录；新收件版本留档。
"""

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.calendar import get_last_trade_day, get_next_trade_day, get_trade_cal
from .szse_share_reference import load_szse_reference, normalize_share_rows, reconcile_share_dates

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_trade_day_batches


@task_register()
class TushareFundShareTask(TushareTask):
    """获取基金规模数据 (含ETF)"""

    # 1. 核心属性
    domain = "fund"  # 业务域标识
    name = "tushare_fund_share"
    description = "获取基金规模数据 (含ETF)"
    table_name = "fund_share"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20000101"  # 根据实际情况调整

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "fund_share"
    fields = ["ts_code", "trade_date", "fd_share"]
    archive_source_versions = True

    # 3. 列名映射 (无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "fd_share": lambda x: pd.to_numeric(x, errors="coerce")
        # trade_date 由基类 process_data 中的 _process_date_column 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "fd_share": {"type": "NUMERIC(20,2)"},  # 单位：万份
        # update_time 会自动添加
        # 主键 ("ts_code", "trade_date") 索引由基类根据 primary_keys 自动处理
    }

    # 6. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['fd_share'] >= 0, "基金份额不能为负数"),
    ]

    # 7. 自定义索引 (主键已包含，无需额外添加)
    indexes = [
        {
            "name": "idx_tushare_fund_share_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 7. 分批配置 (根据接口特性和数据量调整)
    batch_trade_days_single_code = 360  # 单基金查询时，每个批次的交易日数量 (约1.5年)
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用交易日批次工具)。
        支持按日期范围和可选的 ts_code 进行批处理。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的基金代码

        # 检查必要的日期参数
        if not start_date:
            # 如果未提供 start_date，尝试从数据库获取最新日期 + 1天作为开始日期
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
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                # 根据是否提供 ts_code 选择不同的批次大小
                batch_size=(
                    self.batch_trade_days_single_code
                    if ts_code
                    else self.batch_trade_days_all_codes
                ),
                # 将 ts_code 传递给批处理函数，以便在参数中包含它
                ts_code=ts_code,
                logger=self.logger,
                # 注意：fund_share API 可能需要 market 参数，但 generate_trade_day_batches 目前不直接支持
                # 如果需要按 market 分批，需要自定义 get_batch_list 逻辑或扩展工具函数
            )
            # 批处理函数返回的字典已包含 start_date 和 end_date
            # 如果提供了 ts_code，它也会包含在字典中，可以直接用于 API 调用
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True
            )
            return []

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        """Use per-record exchange evidence; never extrapolate a current offset."""
        if stop_event is not None and stop_event.is_set():
            raise asyncio.CancelledError()
        single_day = params.get("trade_date")
        start = single_day or params.get("start_date")
        end = single_day or params.get("end_date")
        if not start or not end:
            raise ValueError("fund_share_reference_requires_bounded_dates")
        start_ts, end_ts = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
        if start_ts > end_ts:
            raise ValueError("fund_share_invalid_date_range")
        single_code = str(params.get("ts_code") or "")
        only_native = bool(single_code) and not (single_code.startswith("15") and single_code.endswith(".SZ"))
        query = dict(params)
        if not only_native:
            previous = await get_last_trade_day(start_ts.strftime("%Y%m%d"), n=1, exchange="SZSE")
            following = await get_next_trade_day(end_ts.strftime("%Y%m%d"), n=1, exchange="SZSE")
            if not previous or not following:
                raise ValueError("fund_share_reference_calendar_boundary_missing")
            query.pop("trade_date", None)
            query.update(start_date=previous, end_date=following)
        data = await super().fetch_batch(query, stop_event=stop_event)
        if data is None or data.empty:
            return data
        data = normalize_share_rows(data)
        sz_codes = set(data.loc[data.ts_code.str.endswith(".SZ") & data.ts_code.str.startswith("15"), "ts_code"])
        if not sz_codes:
            return data[data.trade_date.between(start_ts, end_ts)].reset_index(drop=True)
        reference = await asyncio.to_thread(load_szse_reference, start_ts, end_ts, sz_codes)
        calendar = await get_trade_cal(start_date=query["start_date"], end_date=query["end_date"], exchange="SZSE")
        if calendar.empty or not {"cal_date", "is_open"}.issubset(calendar.columns):
            raise ValueError("fund_share_reference_calendar_missing")
        days = pd.to_datetime(calendar.loc[pd.to_numeric(calendar.is_open, errors="coerce") == 1, "cal_date"], errors="raise")
        if stop_event is not None and stop_event.is_set():
            raise asyncio.CancelledError()
        result = reconcile_share_dates(data, reference, days, start_ts, end_ts, retain_unreferenced=True)
        unverified = result.attrs.get('unverified_sz_etf_codes', [])
        if unverified:
            self.logger.warning("fund_share %s 只深市产品无交易所记录，保留供应商原始日期且未认证: %s", len(unverified), ','.join(unverified))
        self.logger.info("fund_share 深市 ETF 逐日交易所证据核验完成: %s 至 %s, %s 条；仅认证当前历史数值，其他品种日期保持供应商口径", start, end, len(result))
        return result
