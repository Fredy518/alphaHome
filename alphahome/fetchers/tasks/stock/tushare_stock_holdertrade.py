#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股东增减持数据任务

接口文档: https://tushare.pro/document/2?doc_id=175
数据说明:
- 获取上市公司增减持数据，了解重要股东近期及历史上的股份增减变化
- 包含股东名称、变动数量、变动比例等信息
- 单次最大提取3000行记录，总量不限制

权限要求: 需要至少2000积分，5000积分以上无明显限制
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_quarter_range_batches
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockHolderTradeTask(TushareTask):
    """获取股东增减持数据 (stk_holdertrade)

    实现要求:
    - 全量更新: 按季度范围分批获取全部历史数据
    - 增量模式: 根据时间跨度智能选择批处理策略
    """

    # 1. 核心属性
    name = "tushare_stock_holdertrade"
    description = "获取股东增减持数据"
    table_name = "stock_holdertrade"
    primary_keys = ["ts_code", "ann_date", "holder_name"]
    date_column = "ann_date"  # 公告日期
    default_start_date = "20040101"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 7

    # --- 默认配置 ---
    default_concurrent_limit = 3
    default_page_size = 3000

    # 2. TushareTask 特有属性
    api_name = "stk_holdertrade"
    fields = [
        "ts_code",        # TS代码
        "ann_date",       # 公告日期
        "holder_name",    # 股东名称
        "holder_type",    # 股东类型（G高管P个人C公司）
        "in_de",          # 增减持类型（IN增持DE减持）
        "change_vol",     # 变动数量（股）
        "change_ratio",   # 占流通比例（%）
        "after_share",    # 变动后持股（股）
        "after_ratio",    # 变动后占流通比例（%）
        "avg_price",      # 平均价格
        "total_share",    # 持股总数（股）
        "begin_date",     # 增减持开始日期
        "close_date",     # 增减持结束日期
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "change_vol": float,
        "change_ratio": float,
        "after_share": float,
        "after_ratio": float,
        "avg_price": float,
        "total_share": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "holder_name": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "holder_type": {"type": "VARCHAR(10)"},
        "in_de": {"type": "VARCHAR(10)"},
        "change_vol": {"type": "NUMERIC(20,2)"},
        "change_ratio": {"type": "NUMERIC(15,6)"},
        "after_share": {"type": "NUMERIC(20,2)"},
        "after_ratio": {"type": "NUMERIC(15,6)"},
        "avg_price": {"type": "NUMERIC(15,4)"},
        "total_share": {"type": "NUMERIC(20,2)"},
        "begin_date": {"type": "DATE"},
        "close_date": {"type": "DATE"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_holdertrade_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_holdertrade_ann_date", "columns": "ann_date"},
        {"name": "idx_stock_holdertrade_holder_type", "columns": "holder_type"},
        {"name": "idx_stock_holdertrade_in_de", "columns": "in_de"},
        {"name": "idx_stock_holdertrade_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["ann_date"].notna(), "公告日期不能为空"),
        (lambda df: df["holder_name"].notna(), "股东名称不能为空"),
        (lambda df: df["holder_type"].isin(["G", "P", "C"]) | df["holder_type"].isna(), 
         "股东类型必须为G/P/C"),
        (lambda df: df["in_de"].isin(["IN", "DE"]) | df["in_de"].isna(), 
         "增减持类型必须为IN/DE"),
        (lambda df: (df["avg_price"] >= 0) | df["avg_price"].isna(), "平均价格必须非负"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表"""
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        current_date = datetime.now().strftime("%Y%m%d")
        is_full_mode = start_date == self.default_start_date and end_date == current_date

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 - is_full_mode: {is_full_mode}, "
            f"start_date: {start_date}, end_date: {end_date}"
        )

        try:
            if is_full_mode:
                self.logger.info("全量模式：按季度范围分批获取股东增减持数据")
                return await generate_quarter_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])}
                )
            else:
                if not start_date:
                    latest_db_date = await self.get_latest_date()
                    if latest_db_date:
                        start_date = (latest_db_date + timedelta(days=1)).strftime("%Y%m%d")
                    else:
                        start_date = self.default_start_date

                if not end_date:
                    end_date = current_date

                if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
                    self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
                    return []

                days_span = (datetime.strptime(str(end_date), "%Y%m%d") - 
                            datetime.strptime(str(start_date), "%Y%m%d")).days

                if days_span < 30:
                    self.logger.info(f"增量模式：时间跨度 {days_span} 天，使用单批次获取")
                    return [{
                        "start_date": start_date,
                        "end_date": end_date,
                        "fields": ",".join(self.fields or [])
                    }]
                else:
                    self.logger.info(f"增量模式：时间跨度 {days_span} 天，使用季度分批")
                    return await generate_quarter_range_batches(
                        start_date=start_date,
                        end_date=end_date,
                        logger=self.logger,
                        additional_params={"fields": ",".join(self.fields or [])}
                    )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []


__all__ = ["TushareStockHolderTradeTask"]
