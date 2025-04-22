import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.calendar import get_trade_days_between
from ...tools.batch_utils import generate_trade_day_batches

@task_register()
class TushareStockAdjFactorTask(TushareTask):
    """股票复权因子任务
    
    获取股票的复权因子数据。复权因子是用来计算复权价格的基础数据，
    当股票由于分红、送股等原因发生除权除息时，会产生新的复权因子。
    """
    
    # 1.核心属性
    name = "tushare_stock_adjfactor"
    description = "获取股票复权因子"
    table_name = "tushare_stock_adjfactor"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19901219"  # Tushare最早的日期

    # 2.自定义索引
    indexes = [
        {"name": "idx_tushare_adjfactor_code", "columns": "ts_code"},
        {"name": "idx_tushare_adjfactor_date", "columns": "trade_date"}
    ]
    
    # 3.Tushare特有属性
    api_name = "adj_factor"
    fields = [
        "ts_code", "trade_date", "adj_factor"
    ]

    # 4.数据类型转换
    transformations = {
        "adj_factor": float
    }

    # 5.列名映射
    column_mapping = {}
    
    # 6.表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "adj_factor": {"type": "NUMERIC(18,6)"}  # 复权因子通常需要较高的精度
    }

    # 7.数据验证规则
    # validations = [
    #     # 验证复权因子是否为正数
    #     lambda df: df["adj_factor"].fillna(0) > 0,
    #     # 验证日期格式
    #     lambda df: pd.to_datetime(df["trade_date"], errors="coerce").notna()
    # ]

    # 8. 分批配置
    batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 15     # 全市场查询时，每个批次的交易日数量 (4周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用专用交易日批次工具)

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        start_date = kwargs.get('start_date', self.default_start_date)
        end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
        ts_code = kwargs.get('ts_code')  # 获取可能的ts_code
        exchange = kwargs.get('exchange', 'SSE')  # 获取可能的交易所参数

        self.logger.info(f"任务 {self.name}: 使用交易日批次工具生成批处理列表，范围: {start_date} 到 {end_date}")

        try:
            # 使用简化的专用函数
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes,
                ts_code=ts_code,
                exchange=exchange,
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True)
            return [] 