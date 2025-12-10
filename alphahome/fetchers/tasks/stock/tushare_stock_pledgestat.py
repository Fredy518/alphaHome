#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股权质押统计数据任务

接口文档: https://tushare.pro/document/2?doc_id=110
数据说明:
- 获取股票质押统计数据
- 包含质押次数、无限售股质押数量、限售股质押数量、质押比例等
- API 只支持按 ts_code 查询，需要遍历所有股票

权限要求: 需要至少120积分
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...sources.tushare.tushare_task import TushareTask
from ...tools.calendar import is_trade_day
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes


@task_register()
class TushareStockPledgeStatTask(TushareTask):
    """获取股权质押统计数据 (pledge_stat)

    实现要求:
    - API 只支持按 ts_code 查询
    - 全量更新: 遍历所有股票代码获取数据
    - 智能增量: 检查是否超过1个月未更新且当天为非交易日
    """

    # 1. 核心属性
    name = "tushare_stock_pledgestat"
    description = "获取股权质押统计数据"
    table_name = "stock_pledgestat"
    primary_keys = ["ts_code", "end_date"]
    date_column = "end_date"
    default_start_date = "20140101"
    data_source = "tushare"
    domain = "stock"

    # --- 默认配置 ---
    default_concurrent_limit = 3
    default_page_size = 5000

    # 2. TushareTask 特有属性
    api_name = "pledge_stat"
    fields = [
        "ts_code",           # TS代码
        "end_date",          # 截止日期
        "pledge_count",      # 质押次数
        "unrest_pledge",     # 无限售股质押数量（万股）
        "rest_pledge",       # 限售股质押数量（万股）
        "total_share",       # 总股本（万股）
        "pledge_ratio",      # 质押比例（%）
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "pledge_count": int,
        "unrest_pledge": float,
        "rest_pledge": float,
        "total_share": float,
        "pledge_ratio": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pledge_count": {"type": "INTEGER"},
        "unrest_pledge": {"type": "NUMERIC(20,4)"},
        "rest_pledge": {"type": "NUMERIC(20,4)"},
        "total_share": {"type": "NUMERIC(20,4)"},
        "pledge_ratio": {"type": "NUMERIC(10,4)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_pledgestat_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_pledgestat_end_date", "columns": "end_date"},
        {"name": "idx_stock_pledgestat_pledge_ratio", "columns": "pledge_ratio"},
        {"name": "idx_stock_pledgestat_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["end_date"].notna(), "截止日期不能为空"),
        (lambda df: (df["pledge_count"] >= 0) | df["pledge_count"].isna(), "质押次数必须非负"),
        (lambda df: (df["pledge_ratio"] >= 0) | df["pledge_ratio"].isna(), "质押比例必须非负"),
        (lambda df: (df["pledge_ratio"] <= 100) | df["pledge_ratio"].isna(), "质押比例不能超过100%"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成批处理参数列表。

        - 全量更新：遍历所有股票代码
        - 智能增量：检查是否超过1个月未更新且当天为非交易日
        - 手动增量：跳过执行（API不支持日期范围查询）
        """
        update_type = kwargs.get("update_type", UpdateTypes.FULL)

        if update_type == UpdateTypes.SMART:
            should_update = await self._should_perform_full_update()
            if not should_update:
                self.logger.info(f"任务 {self.name}: 智能增量 - 不满足全量更新条件，跳过执行")
                return []
            else:
                self.logger.info(f"任务 {self.name}: 智能增量 - 满足全量更新条件，转为全量更新")
        elif update_type != UpdateTypes.FULL:
            self.logger.warning(
                f"任务 {self.name}: 此任务仅支持全量更新 (FULL) 和智能增量 (SMART)。"
                f"当前更新类型为 '{update_type}'，任务将跳过执行。"
            )
            return []

        # 获取所有股票代码
        ts_codes = await self._get_all_stock_codes()

        if not ts_codes:
            self.logger.warning("未找到任何股票代码，任务将跳过执行")
            return []

        # 为每个 ts_code 生成批处理参数
        batch_list = [{"ts_code": ts_code} for ts_code in ts_codes]

        self.logger.info(f"任务 {self.name}: 为 {len(batch_list)} 只股票生成批处理列表")
        return batch_list

    async def _get_all_stock_codes(self) -> List[str]:
        """
        从 tushare.stock_basic 表获取所有股票代码。

        Returns:
            List[str]: 股票代码列表
        """
        try:
            # 查询所有上市和退市的股票（L上市 D退市 P暂停上市）
            query = """
            SELECT ts_code FROM tushare.stock_basic 
            WHERE list_status IN ('L', 'D', 'P')
            ORDER BY ts_code
            """
            result = await self.db.fetch(query)

            if result:
                ts_codes = [row["ts_code"] for row in result]
                self.logger.info(f"从 tushare.stock_basic 表获取到 {len(ts_codes)} 只股票代码")
                return ts_codes
            else:
                self.logger.warning("tushare.stock_basic 表为空，未找到任何股票代码")
                return []

        except Exception as e:
            self.logger.error(f"查询 tushare.stock_basic 表失败: {e}", exc_info=True)
            return []

    async def _should_perform_full_update(self) -> bool:
        """
        检查数据表是否满足全量更新的条件：
        1. 数据表超过1个月未更新
        2. 当天为非交易日

        只有同时满足两个条件才返回True。
        """
        try:
            query = f"SELECT MAX(update_time) as last_update FROM {self.data_source}.{self.table_name}"
            result = await self.db.fetch(query)

            if not result or not result[0]["last_update"]:
                self.logger.info(f"表 {self.table_name} 没有更新时间记录，需要执行全量更新")
                return True

            last_update = result[0]["last_update"]
            current_time = datetime.now()
            time_diff = current_time - last_update

            # 检查是否超过1个月 (30天)
            is_over_one_month = time_diff > timedelta(days=30)

            # 检查当天是否为非交易日
            today_str = current_time.strftime("%Y%m%d")
            is_trading_day = await is_trade_day(today_str)
            is_non_trading_day = not is_trading_day

            self.logger.info(
                f"表 {self.table_name} 最后更新时间为 {last_update}，"
                f"距离现在 {time_diff.days} 天，是否超过1个月: {is_over_one_month}，"
                f"当天 {today_str} 是否为非交易日: {is_non_trading_day}"
            )

            if is_over_one_month and is_non_trading_day:
                self.logger.info(f"表 {self.table_name} 满足全量更新条件")
                return True
            else:
                return False

        except Exception as e:
            self.logger.error(f"检查表 {self.table_name} 更新时间失败: {e}", exc_info=True)
            return True


__all__ = ["TushareStockPledgeStatTask"]
