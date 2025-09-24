#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股东户数数据任务

接口文档: https://tushare.pro/document/2?doc_id=166
数据说明:
- 获取上市公司股东户数数据，数据不定期公布
- 支持两种批处理策略:
  1. 全量模式: 按股票代码分批获取，获取全部历史数据
  2. 增量模式: 按公告日期(ann_date)分批获取

权限要求: 需要至少600积分，单次最大3000条
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_quarter_range_batches
)
from ....common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class TushareStockHolderNumberTask(TushareTask):
    """获取股东户数数据 (stk_holdernumber)

    实现要求:
    - 全量更新: 按季度范围分批获取全部历史数据
    - 增量模式: 根据时间跨度智能选择批处理策略（短期单批次，长期季度分批）
    """

    # 1. 核心属性
    name = "tushare_stock_holdernumber"
    description = "获取上市公司股东户数数据"
    table_name = "stock_holdernumber"
    primary_keys = ["ts_code", "ann_date"]
    date_column = "ann_date"  # 公告日期
    default_start_date = "20050101"  # 默认开始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识
    smart_lookback_days = 7  # 智能增量模式下，回看7天（股东户数公告不频繁）

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3  # 降低并发，股东户数数据查询相对缓慢
    default_page_size = 3000  # API限制单次最大3000条

    # 2. TushareTask 特有属性
    api_name = "stk_holdernumber"
    # Tushare stk_holdernumber 接口返回的字段
    fields = [
        "ts_code",      # TS股票代码
        "ann_date",     # 公告日期
        "end_date",     # 截止日期
        "holder_num",   # 股东户数
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "holder_num": int,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE"},
        "holder_num": {"type": "INTEGER"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_holdernumber_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_holdernumber_ann_date", "columns": "ann_date"},
        {"name": "idx_stock_holdernumber_end_date", "columns": "end_date"},
        {"name": "idx_stock_holdernumber_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['ann_date'].notna(), "公告日期不能为空"),
        (lambda df: df['holder_num'].notna(), "股东户数不能为空"),  # 过滤NaN值
        (lambda df: df['holder_num'] >= 0, "股东户数必须非负"),     # 股东户数必须非负
        # 放宽日期逻辑检查: end_date通常应早于ann_date，但允许特殊情况
        # (lambda df: (df['end_date'] <= df['ann_date']) | df['end_date'].isnull(), "截止日期应早于或等于公告日期"),
    ]

    # 8. 验证模式配置 - 使用过滤模式自动移除不符合验证规则的数据
    validation_mode = "filter"  # 强制执行验证规则，过滤不合格数据

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 全量模式: 按季度范围分批获取历史数据
        2. 增量模式: 根据时间跨度智能选择批处理策略
           - 短期更新（< 30天）：单批次获取
           - 长期更新（≥ 30天）：扩展到季度边界后分批

        Args:
            **kwargs: 包含start_date, end_date等参数

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 判断是否为全量模式（基于日期范围是否覆盖默认起始日期到当前日期）
        current_date = datetime.now().strftime("%Y%m%d")
        is_full_mode = (start_date == self.default_start_date and end_date == current_date)

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 - is_full_mode: {is_full_mode}, start_date: {start_date}, end_date: {end_date}"
        )

        try:
            if is_full_mode:
                # 策略1: 全量模式 - 按季度范围分批获取数据
                self.logger.info(f"全量模式：按季度范围分批获取股东户数数据")
                return await generate_quarter_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])}
                )
            else:
                # 策略2: 增量模式 - 根据时间跨度选择批处理策略
                # 确定总体起止日期
                if not start_date:
                    latest_db_date = await self.get_latest_date()
                    if latest_db_date:
                        next_day_obj = latest_db_date + timedelta(days=1)
                        start_date = next_day_obj.strftime("%Y%m%d")
                    else:
                        start_date = self.default_start_date
                    self.logger.info(
                        f"任务 {self.name}: 未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date}"
                    )

                if not end_date:
                    end_date = datetime.now().strftime("%Y%m%d")
                    self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

                if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
                    self.logger.info(
                        f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
                    )
                    return []

                # 计算时间跨度并选择批处理策略
                start_dt = datetime.strptime(str(start_date), "%Y%m%d")
                end_dt = datetime.strptime(str(end_date), "%Y%m%d")
                days_span = (end_dt - start_dt).days

                if days_span < 30:  # 短期更新，直接获取
                    self.logger.info(f"增量模式：时间跨度 {days_span} 天 < 30天，使用单批次获取")
                    return [{
                        "start_date": start_date,
                        "end_date": end_date,
                        "fields": ",".join(self.fields or [])
                    }]
                else:  # 长期更新，使用季度分批（自动扩展到季度边界）
                    self.logger.info(
                        f"增量模式：时间跨度 {days_span} 天 >= 30天，使用季度分批（将自动扩展到季度边界）"
                    )
                    return await generate_quarter_range_batches(
                        start_date=start_date,
                        end_date=end_date,
                        logger=self.logger,
                        additional_params={"fields": ",".join(self.fields or [])}
                    )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: BatchPlanner 生成批次时出错: {e}", exc_info=True)
            return []


# 导出任务类
__all__ = ["TushareStockHolderNumberTask"]
