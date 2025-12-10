#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 龙虎榜每日明细数据任务

接口文档: https://tushare.pro/document/2?doc_id=106
数据说明:
- 获取龙虎榜每日交易明细数据
- 包含上榜理由、买入额、卖出额、净买入额等
- 数据历史: 2005年至今
- API 必填参数为 trade_date，需按交易日分批获取

权限要求: 需要至少2000积分
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_single_date_batches
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockTopListTask(TushareTask):
    """获取龙虎榜每日明细数据 (top_list)

    实现要求:
    - API 必填参数为 trade_date
    - 全量/增量更新: 按交易日分批获取数据
    """

    # 1. 核心属性
    name = "tushare_stock_toplist"
    description = "获取龙虎榜每日明细数据"
    table_name = "stock_toplist"
    primary_keys = ["ts_code", "trade_date", "reason"]
    date_column = "trade_date"
    default_start_date = "20050101"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 3

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 5000

    # 2. TushareTask 特有属性
    api_name = "top_list"
    fields = [
        "trade_date",    # 交易日期
        "ts_code",       # TS代码
        "name",          # 名称
        "close",         # 收盘价
        "pct_change",    # 涨跌幅
        "turnover_rate", # 换手率
        "amount",        # 总成交额
        "l_sell",        # 龙虎榜卖出额
        "l_buy",         # 龙虎榜买入额
        "l_amount",      # 龙虎榜成交额
        "net_amount",    # 龙虎榜净买入额
        "net_rate",      # 龙虎榜净买额占比
        "amount_rate",   # 龙虎榜成交额占比
        "float_values",  # 当日流通市值
        "reason",        # 上榜原因
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "close": float,
        "pct_change": float,
        "turnover_rate": float,
        "amount": float,
        "l_sell": float,
        "l_buy": float,
        "l_amount": float,
        "net_amount": float,
        "net_rate": float,
        "amount_rate": float,
        "float_values": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "close": {"type": "NUMERIC(15,4)"},
        "pct_change": {"type": "NUMERIC(10,4)"},
        "turnover_rate": {"type": "NUMERIC(10,4)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "l_sell": {"type": "NUMERIC(20,2)"},
        "l_buy": {"type": "NUMERIC(20,2)"},
        "l_amount": {"type": "NUMERIC(20,2)"},
        "net_amount": {"type": "NUMERIC(20,2)"},
        "net_rate": {"type": "NUMERIC(10,4)"},
        "amount_rate": {"type": "NUMERIC(10,4)"},
        "float_values": {"type": "NUMERIC(20,2)"},
        "reason": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_toplist_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_toplist_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_toplist_reason", "columns": "reason"},
        {"name": "idx_stock_toplist_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["reason"].notna(), "上榜原因不能为空"),
        (lambda df: (df["amount"] >= 0) | df["amount"].isna(), "成交额必须非负"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        API 必填参数为 trade_date，因此按交易日分批获取数据
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        current_date = datetime.now().strftime("%Y%m%d")

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 - start_date: {start_date}, end_date: {end_date}"
        )

        try:
            # 确定起止日期
            if not start_date:
                latest_db_date = await self.get_latest_date()
                if latest_db_date:
                    start_date = (latest_db_date + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    start_date = self.default_start_date
                self.logger.info(f"任务 {self.name}: 使用起始日期: {start_date}")

            if not end_date:
                end_date = current_date
                self.logger.info(f"任务 {self.name}: 使用结束日期: {end_date}")

            if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
                self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
                return []

            # 按交易日分批获取数据
            self.logger.info(f"按交易日分批获取龙虎榜数据: {start_date} ~ {end_date}")
            return await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field="trade_date",
                logger=self.logger,
                exchange=kwargs.get("exchange", "SSE"),
                additional_params={"fields": ",".join(self.fields or [])}
            )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []


__all__ = ["TushareStockTopListTask"]
