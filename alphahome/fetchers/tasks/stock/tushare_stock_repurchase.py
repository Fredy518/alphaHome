#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股票回购数据任务

接口文档: https://tushare.pro/document/2?doc_id=124
数据说明:
- 获取上市公司回购股票数据
- 包含回购进度、回购金额、回购数量等信息
- 任意填参数，如果都不填，单次默认返回2000条

权限要求: 需要至少600积分
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_quarter_range_batches
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockRepurchaseTask(TushareTask):
    """获取股票回购数据 (repurchase)

    实现要求:
    - 全量更新: 按季度范围分批获取全部历史数据
    - 增量模式: 根据时间跨度智能选择批处理策略
    """

    # 1. 核心属性
    name = "tushare_stock_repurchase"
    description = "获取股票回购数据"
    table_name = "stock_repurchase"
    primary_keys = ["ts_code", "ann_date", "proc"]
    date_column = "ann_date"  # 公告日期
    default_start_date = "20050101"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 7

    # --- 默认配置 ---
    default_concurrent_limit = 3
    default_page_size = 5000

    # 2. TushareTask 特有属性
    api_name = "repurchase"
    fields = [
        "ts_code",       # TS代码
        "ann_date",      # 公告日期
        "end_date",      # 截止日期
        "proc",          # 进度
        "exp_date",      # 过期日期
        "vol",           # 回购数量（股）
        "amount",        # 回购金额（元）
        "high_limit",    # 回购最高价
        "low_limit",     # 回购最低价
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {'vol': 'volume'}

    # 4. 数据类型转换
    transformations = {
        "vol": float,
        "amount": float,
        "high_limit": float,
        "low_limit": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE"},
        "proc": {"type": "VARCHAR(50)"},
        "exp_date": {"type": "DATE"},
        "volume": {"type": "NUMERIC(20,2)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "high_limit": {"type": "NUMERIC(15,4)"},
        "low_limit": {"type": "NUMERIC(15,4)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_repurchase_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_repurchase_ann_date", "columns": "ann_date"},
        {"name": "idx_stock_repurchase_proc", "columns": "proc"},
        {"name": "idx_stock_repurchase_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["ann_date"].notna(), "公告日期不能为空"),
        (lambda df: (df["vol"] >= 0) | df["vol"].isna(), "回购数量必须非负"),
        (lambda df: (df["amount"] >= 0) | df["amount"].isna(), "回购金额必须非负"),
        (lambda df: (df["low_limit"] <= df["high_limit"]) | df["low_limit"].isna() | df["high_limit"].isna(), 
         "回购最低价不能高于最高价"),
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
                self.logger.info("全量模式：按季度范围分批获取股票回购数据")
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


__all__ = ["TushareStockRepurchaseTask"]
