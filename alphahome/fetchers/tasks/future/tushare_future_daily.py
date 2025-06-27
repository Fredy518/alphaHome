#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货及期权日线行情 (fut_daily) 更新任务
获取中金所（CFFEX）期货及期权的日线交易数据。
继承自 TushareTask。
使用自然日按月生成批次。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入自然日批次生成工具函数
from ...sources.tushare.batch_utils import generate_natural_day_batches

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareFutureDailyTask(TushareTask):
    """获取中金所（CFFEX）期货及期权日线行情数据"""

    # 1. 核心属性
    name = "tushare_future_daily"
    description = "获取中金所（CFFEX）期货及期权日线行情数据"
    table_name = "future_daily"
    primary_keys = ["ts_code", "trade_date"]  # 合约代码和交易日期组合是主键
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20060908"  # 中金所开始交易的日期（2006年9月8日正式成立并开始交易）

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 2000  # 单次最大2000行
    
    # 交易所约束：只获取中金所数据
    SUPPORTED_EXCHANGES = [
        "CFFEX"
    ]  # 暂时仅保存中金所数据; 其他交易所代码:'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX'

    # 2. TushareTask 特有属性
    api_name = "fut_daily"
    # Tushare fut_daily 接口实际返回的字段 (根据文档 [https://tushare.pro/document/2?doc_id=138])
    fields = [
        "ts_code",
        "trade_date",
        "pre_close",
        "pre_settle",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "change1",
        "change2",
        "vol",
        "amount",
        "oi",
        "oi_chg",
        "delv_settle",
    ]

    # 3. 列名映射 (API字段名与数据库列名不完全一致，进行映射)
    column_mapping = {"vol": "volume"}  # 将vol映射为volume

    # 4. 数据类型转换 (日期列在基类处理，数值列转换为 float)
    transformations = {
        "pre_close": float,
        "pre_settle": float,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "settle": float,
        "change1": float,
        "change2": float,
        "vol": float,  # 原始字段名
        "amount": float,
        "oi": float,
        "oi_chg": float,
        "delv_settle": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {
            "type": "VARCHAR(20)",
            "constraints": "NOT NULL",
        },  # 中金所期货代码
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pre_close": {"type": "NUMERIC(15,4)"},  # 昨收盘价
        "pre_settle": {"type": "NUMERIC(15,4)"},  # 昨结算价
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "settle": {"type": "NUMERIC(15,4)"},  # 结算价
        "change1": {"type": "NUMERIC(15,4)"},  # 涨跌1：收盘价-昨结算价
        "change2": {"type": "NUMERIC(15,4)"},  # 涨跌2：结算价-昨结算价
        "volume": {"type": "NUMERIC(20,4)"},  # 成交量 (映射后的名称)
        "amount": {"type": "NUMERIC(20,4)"},  # 成交额
        "oi": {"type": "NUMERIC(20,4)"},  # 持仓量
        "oi_chg": {"type": "NUMERIC(20,4)"},  # 持仓量变化
        "delv_settle": {"type": "NUMERIC(15,4)"},  # 交割结算价
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_future_daily_code_date", "columns": ["ts_code", "trade_date"]},
        {"name": "idx_future_daily_code", "columns": "ts_code"},
        {"name": "idx_future_daily_date", "columns": "trade_date"},
        {"name": "idx_future_daily_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['trade_date'].notna(),
        lambda df: df['close'] > 0,
        lambda df: df['high'] >= df['low'],
        lambda df: df['volume'] >= 0,
        lambda df: df['amount'] >= 0,
        lambda df: df['oi'] >= 0, # 持仓量不能为负
    ]

    # 8. 分批配置
    # 自然日按月分批，一个月大约30天
    batch_natural_days_month = 30

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表，使用自然日按月分批。
        只获取中金所（CFFEX）的期货数据。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            from datetime import datetime
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        all_batches: List[Dict] = []
        self.logger.info(
            f"任务 {self.name}: 为交易所列表 {self.SUPPORTED_EXCHANGES} 使用自然日按月生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        for ex_code in self.SUPPORTED_EXCHANGES:
            try:
                # 使用自然日批次工具函数，批次大小设置为一个月（约30天自然日）
                # 不传入 ts_code，获取指定交易所的日线数据
                batches_for_exchange = await generate_natural_day_batches(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=self.batch_natural_days_month,
                    ts_code=None,  # 不按代码分批
                    logger=self.logger,
                )
                
                # 为每个批次添加交易所过滤条件
                for batch in batches_for_exchange:
                    batch["exchange"] = ex_code
                
                if batches_for_exchange:
                    all_batches.extend(batches_for_exchange)
                    self.logger.info(
                        f"任务 {self.name}: 为交易所 {ex_code} 生成了 {len(batches_for_exchange)} 个自然日批次。"
                    )
                else:
                    self.logger.info(
                        f"任务 {self.name}: 交易所 {ex_code} 在指定日期范围 {start_date}-{end_date} 内无批次生成。"
                    )
            except Exception as e:
                self.logger.error(
                    f"任务 {self.name}: 为交易所 {ex_code} 生成批次时出错: {e}",
                    exc_info=True,
                )
                # 选择继续为其他交易所生成批次，或在此处抛出异常停止整个任务
                # 此处选择继续

        self.logger.info(f"任务 {self.name}: 总共生成 {len(all_batches)} 个批次。")
        return all_batches

