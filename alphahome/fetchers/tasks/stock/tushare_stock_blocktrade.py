#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 大宗交易数据任务

接口文档: https://tushare.pro/document/2?doc_id=161
数据说明:
- 获取大宗交易数据
- 包含成交价、成交量、买卖方营业部等信息
- 单次最大1000条，总量不限制

权限要求: 需要至少300积分，超过5000积分频次相对较高
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_single_date_batches
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockBlockTradeTask(TushareTask):
    """获取大宗交易数据 (block_trade)

    实现要求:
    - 股票代码和日期至少输入一个参数
    - 按交易日分批获取数据
    """

    # 1. 核心属性
    name = "tushare_stock_blocktrade"
    description = "获取大宗交易数据"
    table_name = "stock_blocktrade"
    primary_keys = ["ts_code", "trade_date", "buyer", "seller"]
    date_column = "trade_date"
    default_start_date = "20050101"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 3

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 1000

    # 2. TushareTask 特有属性
    api_name = "block_trade"
    fields = [
        "ts_code",       # TS代码
        "trade_date",    # 交易日期
        "price",         # 成交价
        "vol",           # 成交量（万股）
        "amount",        # 成交金额
        "buyer",         # 买方营业部
        "seller",        # 卖方营业部
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {
        "vol": "volume",  # 成交量映射
    }

    # 4. 数据类型转换
    transformations = {
        "price": float,
        "vol": float,
        "amount": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "price": {"type": "NUMERIC(15,4)"},
        "volume": {"type": "NUMERIC(20,4)"},  # 成交量（万股）
        "amount": {"type": "NUMERIC(20,2)"},
        "buyer": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "seller": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_blocktrade_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_blocktrade_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_blocktrade_buyer", "columns": "buyer"},
        {"name": "idx_stock_blocktrade_seller", "columns": "seller"},
        {"name": "idx_stock_blocktrade_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["buyer"].notna(), "买方营业部不能为空"),
        (lambda df: df["seller"].notna(), "卖方营业部不能为空"),
        (lambda df: (df["price"] > 0) | df["price"].isna(), "成交价必须为正"),
        (lambda df: (df["volume"] > 0) | df["volume"].isna(), "成交量必须为正"),
        (lambda df: (df["amount"] > 0) | df["amount"].isna(), "成交金额必须为正"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        按交易日分批获取数据
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
            self.logger.info(f"按交易日分批获取大宗交易数据: {start_date} ~ {end_date}")
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


__all__ = ["TushareStockBlockTradeTask"]
