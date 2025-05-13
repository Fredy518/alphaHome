#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
港股每日复权行情及市值指标 (hk_daily_adj) 更新任务
获取港股的每日复权后行情、股本、市值等数据。
继承自 TushareTask。
数据来源：Tushare Pro hk_daily_adj 接口。
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register
from ...tools.batch_utils import generate_trade_day_batches

@task_register()
class TushareHkDailyadjTask(TushareTask):
    """获取港股每日复权行情、股本及市值指标"""

    # 1. 核心属性
    name = "tushare_hk_dailyadj"
    description = "获取港股每日复权行情、股本及市值数据"
    table_name = "tushare_hk_dailyadj"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date" 
    default_start_date = "19860402" # 港股市场较早的参考日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 6000 # hk_daily_adj 接口单次最大提取6000条

    # 2. TushareTask 特有属性
    api_name = "hk_daily_adj"
    # Tushare hk_daily_adj 接口返回字段 (据 https://tushare.pro/document/2?doc_id=339)
    fields = [
        'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close',
        'change', 'pct_change', 'vol', 'amount', 'vwap', 'adj_factor',
        'turnover_ratio', 'free_share', 'total_share', 'free_mv', 'total_mv'
    ]
    
    # 3. 列名映射 (vol -> volume)
    column_mapping = {
        "vol": "volume"
    }

    # 4. 数据类型转换
    transformations = {
        "open": float, "high": float, "low": float, "close": float,
        "pre_close": float, "change": float, "pct_change": float,
        "vol": float,  # API返回的是vol, 会被映射为volume
        "amount": float, "vwap": float, "adj_factor": float,
        "turnover_ratio": float, "free_share": float, "total_share": float,
        "free_mv": float, "total_mv": float
    }
    
    # 5. 数据库表结构
    schema = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(12,4)"},
        "high": {"type": "NUMERIC(12,4)"},
        "low": {"type": "NUMERIC(12,4)"},
        "close": {"type": "NUMERIC(12,4)"}, 
        "pre_close": {"type": "NUMERIC(12,4)"},
        "change": {"type": "NUMERIC(12,4)"},
        "pct_change": {"type": "NUMERIC(10,4)"}, # Tushare文档中的字段名
        "volume": {"type": "NUMERIC(22,2)"},  # 映射自 'vol'
        "amount": {"type": "NUMERIC(22,3)"}, 
        "vwap": {"type": "NUMERIC(12,4)"},
        "adj_factor": {"type": "NUMERIC(18,6)"}, 
        "turnover_ratio": {"type": "NUMERIC(10,4)"}, 
        "free_share": {"type": "NUMERIC(20,2)"},    
        "total_share": {"type": "NUMERIC(20,2)"},   
        "free_mv": {"type": "NUMERIC(22,3)"},       
        "total_mv": {"type": "NUMERIC(22,3)"}        
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_hk_dailyadj_cd_td", "columns": ["ts_code", "trade_date"], "unique": True},
        {"name": "idx_hk_dailyadj_td", "columns": "trade_date"}
    ]
    
    # 7. 分批配置 (与TushareHKDailyTask保持一致或根据接口特性调整)
    batch_trade_days_single_code = 240 * 2 
    batch_trade_days_all_codes = 3    

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """生成批处理参数列表 (使用专用交易日批次工具)"""
        start_date = kwargs.get('start_date', self.default_start_date)
        end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
        ts_code = kwargs.get('ts_code')
        exchange_code_for_calendar = kwargs.get('exchange', 'HKEX') 

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(f"任务 {self.name}: 使用交易日批次工具生成批处理列表，范围: {start_date} 到 {end_date} for exchange: {exchange_code_for_calendar}")

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes,
                ts_code=ts_code,
                exchange=exchange_code_for_calendar, 
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True)
            return [] 