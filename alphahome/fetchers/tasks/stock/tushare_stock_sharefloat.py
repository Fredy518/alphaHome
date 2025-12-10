#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 限售股解禁数据任务

接口文档: https://tushare.pro/document/2?doc_id=160
数据说明:
- 获取限售股解禁数据
- 支持按公告日期或解禁日期查询
- 单次最大6000条，总量不限制

权限要求: 需要至少120积分，超过5000积分频次相对较高
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_month_range_batches
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockShareFloatTask(TushareTask):
    """获取限售股解禁数据 (share_float)

    实现要求:
    - 全量更新: 按月份范围分批获取全部历史数据（数据量大，季度分批会超出offset限制）
    - 增量模式: 根据时间跨度智能选择批处理策略
    """

    # 1. 核心属性
    name = "tushare_stock_sharefloat"
    description = "获取限售股解禁数据"
    table_name = "stock_sharefloat"
    primary_keys = ["ts_code", "ann_date", "float_date"]
    date_column = "ann_date"  # 公告日期
    default_start_date = "20050101"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 7

    # --- 默认配置 ---
    default_concurrent_limit = 3
    default_page_size = 6000

    # 2. TushareTask 特有属性
    api_name = "share_float"
    fields = [
        "ts_code",       # TS代码
        "ann_date",      # 公告日期
        "float_date",    # 解禁日期
        "float_share",   # 流通股份（股）
        "float_ratio",   # 流通股份占总股本比率
        "holder_name",   # 股东名称
        "share_type",    # 股份类型
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "float_share": float,
        "float_ratio": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "float_date": {"type": "DATE", "constraints": "NOT NULL"},
        "float_share": {"type": "NUMERIC(20,4)"},
        "float_ratio": {"type": "NUMERIC(10,6)"},
        "holder_name": {"type": "VARCHAR(200)"},
        "share_type": {"type": "VARCHAR(50)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_sharefloat_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_sharefloat_ann_date", "columns": "ann_date"},
        {"name": "idx_stock_sharefloat_float_date", "columns": "float_date"},
        {"name": "idx_stock_sharefloat_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["ann_date"].notna(), "公告日期不能为空"),
        (lambda df: df["float_date"].notna(), "解禁日期不能为空"),
        (lambda df: (df["float_share"] >= 0) | df["float_share"].isna(), "流通股份必须非负"),
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
            # 确定起止日期
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
                self.logger.info(f"时间跨度 {days_span} 天，使用单批次获取")
                return [{
                    "start_date": start_date,
                    "end_date": end_date,
                    "fields": ",".join(self.fields or [])
                }]
            else:
                # 数据量大，使用按月分批避免 offset 限制
                self.logger.info(f"按月份范围分批获取限售股解禁数据: {start_date} ~ {end_date}")
                return await generate_month_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])}
                )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []


__all__ = ["TushareStockShareFloatTask"]
