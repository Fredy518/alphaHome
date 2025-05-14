import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.calendar import get_trade_days_between # 导入交易日工具
from ...tools.batch_utils import generate_trade_day_batches # 导入交易日批次生成工具函数

@task_register()
class TushareStockDailyBasicTask(TushareTask):
    """股票每日基本面指标任务
    
    获取股票的每日基本面指标，包括市盈率、市净率、换手率、总市值等数据。
    该任务使用Tushare的daily_basic接口获取数据，并依赖于股票日线数据任务。
    """
    
    # 1.核心属性
    name = "tushare_stock_dailybasic"
    description = "获取股票每日基本面指标"
    table_name = "tushare_stock_dailybasic"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19910101" # Tushare 股票日基本指标大致起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 6000

    # 2.自定义索引
    indexes = [
        {"name": "idx_tushare_daily_basic_code", "columns": "ts_code"},
        {"name": "idx_tushare_daily_basic_date", "columns": "trade_date"},
        {"name": "idx_tushare_daily_basic_update_time", "columns": "update_time"}
    ]
    
    # 3.Tushare特有属性
    api_name = "daily_basic"
    fields = [
        "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", 
        "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", 
        "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"
    ]   

    # 4.数据类型转换
    transformations = {
        "close": float,
        "turnover_rate": float,
        "turnover_rate_f": float,
        "volume_ratio": float,
        "pe": float,
        "pe_ttm": float,
        "pb": float,
        "ps": float,
        "ps_ttm": float,
        "dv_ratio": float,
        "dv_ttm": float,
        "total_share": float,
        "float_share": float,
        "free_share": float,
        "total_mv": float,
        "circ_mv": float
    }

    # 5.列名映射
    column_mapping = {}
    
    # 6.表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "close": {"type": "NUMERIC(18,4)"},
        "turnover_rate": {"type": "NUMERIC(18,4)"},
        "turnover_rate_f": {"type": "NUMERIC(18,4)"},
        "volume_ratio": {"type": "NUMERIC(18,4)"},
        "pe": {"type": "NUMERIC(18,4)"},
        "pe_ttm": {"type": "NUMERIC(18,4)"},
        "pb": {"type": "NUMERIC(18,4)"},
        "ps": {"type": "NUMERIC(18,4)"},
        "ps_ttm": {"type": "NUMERIC(18,4)"},
        "dv_ratio": {"type": "NUMERIC(18,4)"},
        "dv_ttm": {"type": "NUMERIC(18,4)"},
        "total_share": {"type": "NUMERIC(20,4)"},
        "float_share": {"type": "NUMERIC(20,4)"},
        "free_share": {"type": "NUMERIC(20,4)"},
        "total_mv": {"type": "NUMERIC(20,4)"},
        "circ_mv": {"type": "NUMERIC(20,4)"},
        "free_mv": {"type": "NUMERIC(20,4)"},
        "float_ratio": {"type": "NUMERIC(18,4)"},
        "bp_ratio": {"type": "NUMERIC(18,4)"},
        "annual_div_yield": {"type": "NUMERIC(18,4)"}
    }

    # 7.数据验证规则
    # validations = [
    #     # 验证市值是否为正
    #     lambda df: df["total_mv"].fillna(0) >= 0,
    #     # 验证流通市值是否为正
    #     lambda df: df["circ_mv"].fillna(0) >= 0,
    #     # 验证换手率是否合理
    #     lambda df: df["turnover_rate"].fillna(0) >= 0,
    #     # 验证股本数据是否合理
    #     lambda df: df["total_share"].fillna(0) >= df["float_share"].fillna(0),
    #     # 验证日期格式
    #     lambda df: pd.to_datetime(df["trade_date"], errors="coerce").notna()
    # ]

    # 8. 分批配置 (与 TushareStockDailyTask 保持一致或根据需要调整)
    batch_trade_days_single_code = 240 # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 15    # 全市场查询时，每个批次的交易日数量 (3周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用专用交易日批次工具)

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        start_date = kwargs.get('start_date', '19910101')  # 保留原始默认值
        end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))  # 保留原始默认值
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

