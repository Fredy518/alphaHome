#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金日线行情 (fund_daily) 更新任务
获取场内基金（ETF、LOF等）的日线行情数据。
继承自 TushareTask，按 trade_date 增量更新。
"""

import pandas as pd
import logging
import asyncio
from typing import Dict, List, Any

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register
# 导入批处理工具
from ...tools.batch_utils import generate_trade_day_batches, generate_single_date_batches

@task_register()
class TushareFundDailyTask(TushareTask):
    """获取基金日线行情数据"""

    # 1. 核心属性
    name = "tushare_fund_daily"
    description = "获取场内基金日线行情"
    table_name = "tushare_fund_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20000101" # 调整为合理的起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 2
    default_page_size = 6000

    # 2. TushareTask 特有属性
    api_name = "fund_daily" # Tushare API 名称
    fields = [
        'ts_code', 'trade_date', 'open', 'high', 'low', 'close',
        'pre_close', 'change', 'pct_chg', 'vol', 'amount'
    ]
    # 2. TushareTask 特有属性
    api_name = "fund_daily"
    fields = [
        'ts_code', 'trade_date', 'open', 'high', 'low', 'close',
        'pre_close', 'change', 'pct_chg', 'vol', 'amount'
    ]

    # 3. 列名映射
    column_mapping = {
        "vol": "volume",
        "amount": "amount"
    }

    # 4. 数据类型转换
    transformations = {
        "open": lambda x: pd.to_numeric(x, errors='coerce'),
        "high": lambda x: pd.to_numeric(x, errors='coerce'),
        "low": lambda x: pd.to_numeric(x, errors='coerce'),
        "close": lambda x: pd.to_numeric(x, errors='coerce'),
        "pre_close": lambda x: pd.to_numeric(x, errors='coerce'),
        "change": lambda x: pd.to_numeric(x, errors='coerce'),
        "pct_chg": lambda x: pd.to_numeric(x, errors='coerce'),
        "vol": lambda x: pd.to_numeric(x, errors='coerce'), # 原始列
        "amount": lambda x: pd.to_numeric(x, errors='coerce') # 原始列
    }

    # 5. 数据库表结构 (使用映射后的列名)
    schema = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "FLOAT"},
        "high": {"type": "FLOAT"},
        "low": {"type": "FLOAT"},
        "close": {"type": "FLOAT"},
        "pre_close": {"type": "FLOAT"},
        "change": {"type": "FLOAT"},
        "pct_chg": {"type": "FLOAT"},
        "volume": {"type": "FLOAT"}, # 映射后的列名
        "amount": {"type": "FLOAT"} # 修正：保持amount列名不变
        # update_time 会自动添加
    }

    # 6. 自定义索引 (主键已包含)
    indexes = []
    
    # 7. 速率控制设置 - 添加这些属性
    # 降低并发数，避免过多并发请求导致触发API限制
    concurrent_limit = 2
    # 每次请求后的等待时间（秒），避免请求过于密集
    request_delay = 0.2
    
    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务，并设置特定的API调用限制"""
        super().__init__(db_connection, api_token, api)
        
        # 设置fund_daily API的速率限制（每分钟最多调用次数）
        # 为了避免触发Tushare的限制，将其设置为较低的值
        if hasattr(self.api, 'set_api_rate_limit'):
            self.api.set_api_rate_limit("fund_daily", 120)  # 每分钟最多120次调用
            self.logger.info(f"已设置 {self.api_name} API的速率限制为: 120次/分钟")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用单日期批次工具)。
        为每个交易日生成单独的批次，使用trade_date参数。
        """
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code')

        if not start_date:
            latest_db_date = await self.get_latest_date()
            start_date = (latest_db_date + pd.Timedelta(days=1)).strftime('%Y%m%d') if latest_db_date else self.default_start_date
            self.logger.info(f"未提供 start_date，使用: {start_date}")
        if not end_date:
            end_date = pd.Timestamp.now().strftime('%Y%m%d')
            self.logger.info(f"未提供 end_date，使用: {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。")
            return []

        self.logger.info(f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}")

        try:
            # 使用专用的单日期批次生成函数
            batch_list = await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field='trade_date',  # 使用 trade_date 作为日期字段
                ts_code=ts_code,
                logger=self.logger
            )
            
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    # validate_data 可以使用基类或自定义
    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证基金日线数据。
        """
        if df.empty:
            return df
        # 检查映射后的 volume 和 turnover 是否为非负数
        if 'volume' in df.columns:
            negative_vol = (df['volume'].dropna() < 0).sum()
            if negative_vol > 0:
                self.logger.warning(f"任务 {self.name}: 列 'volume' 发现 {negative_vol} 条负值记录。")
        if 'amount' in df.columns:
            negative_amount = (df['amount'].dropna() < 0).sum()
            if negative_amount > 0:
                self.logger.warning(f"任务 {self.name}: 列 'amount' 发现 {negative_amount} 条负值记录。")
        return df
        
    def prepare_params(self, batch_params: Dict) -> Dict:
        """
        准备 API 调用参数。
        将批次中的 trade_date 直接映射到 API 参数中。
        fund_daily API 需要 trade_date 或 ts_code 至少提供一个。
        """
        api_params = {}
        
        # 传递必要的参数：ts_code 和 trade_date
        if 'ts_code' in batch_params and batch_params['ts_code']:
            api_params['ts_code'] = batch_params['ts_code']
            
        if 'trade_date' in batch_params and batch_params['trade_date']:
            api_params['trade_date'] = batch_params['trade_date']
            
        return api_params
        
    async def fetch_batch(self, batch_params: Dict) -> pd.DataFrame:
        """重写fetch_batch方法，添加请求延迟"""
        # 调用父类方法获取数据
        df = await super().fetch_batch(batch_params)
        
        # 添加延迟，避免请求过于频繁
        if hasattr(self, 'request_delay') and self.request_delay > 0:
            await asyncio.sleep(self.request_delay)
            
        return df 