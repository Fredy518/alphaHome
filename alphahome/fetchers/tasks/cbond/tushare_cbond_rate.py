#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债票面利率 (cb_rate) 任务
获取可转债票面利率，支持全量更新和增量更新。
继承自 TushareTask。

接口文档: https://tushare.pro/document/2?doc_id=305
权限要求: 需要至少5000积分
限量: 单次最大2000条数据，总量不限制
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ...tools.calendar import is_trade_day
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareCBondRateTask(TushareTask):
    """获取可转债票面利率

    支持两种更新模式：
    1. 全量更新：获取所有可转债票面利率（ts_code 为空，但 fields 必须显式传入）
    2. 增量更新：获取指定日期范围内有更新的转债票面利率（通过 ts_code 分批查询）
    """

    # 1. 核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_rate"
    description = "获取可转债票面利率"
    table_name = "cbond_rate"
    primary_keys = ["ts_code", "rate_start_date"]  # 主键：转债代码 + 付息开始日期
    date_column = "rate_start_date"  # 增量更新使用的日期字段
    default_start_date = "19900101"  # 默认起始日期
    smart_lookback_days = 30  # 智能增量模式下，回看30天（付息期通常较长）

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 默认并发限制
    default_page_size = 2000  # 单次最大2000条

    # 2. TushareTask 特有属性
    api_name = "cb_rate"
    # Tushare cb_rate 接口返回的字段 (根据文档 https://tushare.pro/document/2?doc_id=305)
    fields = [
        "ts_code",  # 转债代码
        "rate_freq",  # 付息频率(次/年)
        "rate_start_date",  # 付息开始日期
        "rate_end_date",  # 付息结束日期
        "coupon_rate",  # 票面利率(%)
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "rate_freq": lambda x: pd.to_numeric(x, errors="coerce", downcast="integer"),
        "coupon_rate": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},  # 转债代码
        "rate_freq": {"type": "INTEGER"},  # 付息频率(次/年)
        "rate_start_date": {"type": "DATE", "constraints": "NOT NULL"},  # 付息开始日期
        "rate_end_date": {"type": "DATE"},  # 付息结束日期
        "coupon_rate": {"type": "NUMERIC(10,4)"},  # 票面利率(%)
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_rate_ts_code", "columns": "ts_code"},
        {"name": "idx_cbond_rate_start_date", "columns": "rate_start_date"},
        {"name": "idx_cbond_rate_end_date", "columns": "rate_end_date"},
        {"name": "idx_cbond_rate_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "转债代码不能为空"),
        (lambda df: df["rate_start_date"].notna(), "付息开始日期不能为空"),
        (lambda df: df["rate_freq"].fillna(0) >= 0 if "rate_freq" in df.columns else True, "付息频率不能为负数"),
        (lambda df: df["coupon_rate"].fillna(0).between(0, 100) if "coupon_rate" in df.columns else True, "票面利率应在0-100之间"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def _determine_date_range(self) -> Optional[Dict[str, str]]:
        """
        智能增量模式：仅在满足“超过1个月未更新且当天为非交易日”时执行全量更新。
        """
        if self.update_type != UpdateTypes.SMART:
            return await super()._determine_date_range()

        should_update = await self._should_perform_full_update()
        # 记录本次智能增量是否允许全量更新，供 get_batch_list 使用
        self._smart_full_update_allowed = should_update
        if not should_update:
            self.logger.info(
                f"任务 {self.name}: 智能增量 - 不满足全量更新条件（超过1个月未更新且当天为非交易日），跳过执行"
            )
            return None

        self.logger.info(f"任务 {self.name}: 智能增量 - 满足全量更新条件，转为全量更新")
        end_date = datetime.now().strftime("%Y%m%d")
        return {"start_date": self.default_start_date, "end_date": end_date}

    async def _should_perform_full_update(self) -> bool:
        """
        检查是否满足全量更新条件：
        1) 超过1个月未更新（以 max(update_time) 为准）
        2) 当天为非交易日
        """
        try:
            query = f'SELECT MAX(update_time) as last_update FROM "{self.data_source}"."{self.table_name}"'
            result = await self.db.fetch(query)

            if not result or not result[0]["last_update"]:
                self.logger.info(f"表 {self.table_name} 没有更新时间记录，需要执行全量更新")
                return True

            last_update = result[0]["last_update"]
            current_time = datetime.now()
            time_diff = current_time - last_update
            is_over_one_month = time_diff > timedelta(days=30)

            today_str = current_time.strftime("%Y%m%d")
            is_trading_day = await is_trade_day(today_str)
            is_non_trading_day = not is_trading_day

            self.logger.info(
                f"表 {self.table_name} 最后更新时间为 {last_update}，"
                f"距离现在 {time_diff.days} 天，是否超过1个月: {is_over_one_month}，"
                f"当天 {today_str} 是否为非交易日: {is_non_trading_day}"
            )

            return bool(is_over_one_month and is_non_trading_day)
        except Exception as e:
            self.logger.error(f"检查表 {self.table_name} 更新时间失败: {e}", exc_info=True)
            # 检查失败时，为安全起见，默认允许更新（避免长期不更新）
            return True

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        生成批处理参数列表。

        策略说明:
        1. 全量更新模式：返回空 ts_code 的参数字典，但必须显式传入 fields 参数
        2. 增量更新模式：获取所有 ts_code，然后分批查询（单一批次，但显式传入 fields）
        """
        update_type = kwargs.get("update_type")

        # fields 参数必须显式传入
        fields_str = ",".join(self.fields or [])

        if update_type in (UpdateTypes.FULL, UpdateTypes.SMART):
            # 全量更新模式：ts_code 为空（不传），但 fields 必须显式传入
            self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次（ts_code 为空，fields 显式传入）。")
            return [
                {
                    "fields": fields_str,
                }
            ]
        else:
            # 增量更新模式（智能增量或手动增量）
            # 由于 cb_rate 接口没有日期参数，需要通过 ts_code 来查询
            # 获取所有 ts_code，然后分批查询
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")

            # 如果没有提供日期，使用默认值或从数据库获取最新日期
            if not start_date:
                if update_type == "smart":
                    # 智能增量模式：从数据库获取最新 rate_start_date，回看 smart_lookback_days 天
                    latest_date = await self.get_latest_date()
                    if latest_date:
                        from datetime import timedelta
                        start_date_obj = latest_date - timedelta(days=self.smart_lookback_days)
                        start_date = start_date_obj.strftime("%Y%m%d")
                    else:
                        start_date = self.default_start_date
                else:
                    start_date = self.default_start_date
                self.logger.info(
                    f"任务 {self.name}: 未提供 start_date，使用: {start_date}"
                )

            if not end_date:
                from datetime import datetime
                end_date = datetime.now().strftime("%Y%m%d")
                self.logger.info(
                    f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}"
                )

            # 增量模式：获取所有 ts_code，然后查询
            # 从 cbond_basic 表获取所有转债代码
            query = 'SELECT DISTINCT ts_code FROM "tushare"."cbond_basic" ORDER BY ts_code'
            records = await self.db.fetch(query)
            
            if not records:
                self.logger.warning(f"任务 {self.name}: 未找到任何转债代码，返回空批次列表。")
                return []

            ts_codes = [r["ts_code"] for r in records]
            self.logger.info(
                f"任务 {self.name}: 增量更新模式，找到 {len(ts_codes)} 个转债代码，日期范围: {start_date} ~ {end_date}"
            )

            # 由于接口支持多值输入，可以一次性查询所有 ts_code
            # 但为了安全，如果数量太多，可以分批
            # 这里先尝试单一批次，如果数据量太大，后续可以优化为分批
            ts_codes_str = ",".join(ts_codes)
            
            return [
                {
                    "fields": fields_str,
                    "ts_code": ts_codes_str,
                }
            ]

    async def pre_execute(self, stop_event: Optional[Any] = None, **kwargs):
        """
        预执行处理。
        对于全量更新模式，清空表数据。
        """
        update_type = kwargs.get("update_type")
        if update_type == UpdateTypes.FULL:
            self.logger.info(
                f"任务 {self.name}: 全量更新模式，预先清空表数据。"
            )
            await self.clear_table()

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理数据，过滤增量更新模式下的日期范围。
        """
        update_type = kwargs.get("update_type")
        
        if update_type != UpdateTypes.FULL and "rate_start_date" in df.columns:
            # 增量更新模式：根据日期范围过滤数据
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")
            
            if start_date or end_date:
                # 将 rate_start_date 转换为日期类型进行比较
                df["rate_start_date"] = pd.to_datetime(df["rate_start_date"], errors="coerce")
                
                if start_date:
                    start_date_obj = pd.to_datetime(start_date, format="%Y%m%d")
                    df = df[df["rate_start_date"] >= start_date_obj]
                
                if end_date:
                    end_date_obj = pd.to_datetime(end_date, format="%Y%m%d")
                    df = df[df["rate_start_date"] <= end_date_obj]
                
                self.logger.info(
                    f"任务 {self.name}: 增量更新模式，过滤后剩余 {len(df)} 条记录。"
                )
        
        return df


"""
使用方法:
1. 全量更新：设置 update_type="full" 或通过GUI选择"全量更新"
   - 会清空现有数据，重新获取所有可转债票面利率
   - ts_code 参数为空（不传），但 fields 参数必须显式传入
2. 增量更新：设置 update_type="smart" 或 "incremental"
   - 基于 rate_start_date（付息开始日期）进行增量更新
   - 智能增量模式会自动计算日期范围（最新日期回看 smart_lookback_days 天）
   - 手动增量模式需要提供 start_date 和 end_date
   - 会获取所有转债代码，然后查询，最后根据日期范围过滤
3. 权限要求：需要至少5000积分
4. 单次限量：2000条，总量不限制

注意事项:
- 主键为 (ts_code, rate_start_date)，同一个转债可能有多个付息期记录
- 全量更新模式：ts_code 为空，但 fields 必须显式传入
- 增量更新模式：fields 也必须显式传入
- 数据包含每个付息期的票面利率信息
"""

