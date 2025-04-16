import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.calendar import get_trade_days_between # 导入交易日工具

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

    # 2.自定义索引
    indexes = [
        {"name": "idx_tushare_daily_basic_code", "columns": "ts_code"},
        {"name": "idx_tushare_daily_basic_date", "columns": "trade_date"}
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
    validations = [
        # 验证市值是否为正
        lambda df: all(df["total_mv"].fillna(0) >= 0),
        # 验证流通市值是否为正
        lambda df: all(df["circ_mv"].fillna(0) >= 0),
        # 验证换手率是否合理
        lambda df: all((df["turnover_rate"].fillna(0) >= 0) & (df["turnover_rate"].fillna(0) <= 100)),
        # 验证股本数据是否合理
        lambda df: all(df["total_share"].fillna(0) >= df["float_share"].fillna(0)),
        # 验证日期格式
        lambda df: all(pd.to_datetime(df["trade_date"], errors="coerce").notna())
    ]

    # 8. 分批配置 (与 TushareStockDailyTask 保持一致或根据需要调整)
    batch_trade_days_single_code = 240 # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 15    # 全市场查询时，每个批次的交易日数量 (3周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (基于精确交易日数量)

        将查询参数转换为一系列批处理参数，每个批处理参数用于一次API调用。
        使用 get_trade_days_between 获取实际交易日，然后按指定数量分批。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        ts_code = kwargs.get('ts_code')
        start_date = kwargs.get('start_date', '19910101') # 股票市场最早的交易日
        end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
        exchange = kwargs.get('exchange', 'SSE') # 允许指定交易所

        self.logger.info(f"开始生成批处理列表 (DailyBasic)，范围: {start_date} 到 {end_date}, 代码: {ts_code or '全部'}")

        # 获取范围内的所有交易日
        try:
            trade_days = await get_trade_days_between(start_date, end_date, exchange=exchange)
        except Exception as e:
            self.logger.error(f"获取交易日历失败: {e}")
            return []

        if not trade_days:
            self.logger.warning(f"在 {start_date} 和 {end_date} 之间没有找到交易日")
            return []

        self.logger.info(f"找到 {len(trade_days)} 个交易日")

        # 确定批次大小 N
        if ts_code:
            batch_size_n = self.batch_trade_days_single_code
            self.logger.info(f"使用单代码批次大小: {batch_size_n} 个交易日")
        else:
            batch_size_n = self.batch_trade_days_all_codes
            self.logger.info(f"使用全市场批次大小: {batch_size_n} 个交易日")

        batch_list = []
        for i in range(0, len(trade_days), batch_size_n):
            batch_days = trade_days[i : i + batch_size_n]
            if not batch_days:
                continue

            batch_start = batch_days[0]
            batch_end = batch_days[-1]

            # 构建批次参数
            batch_params = {
                'start_date': batch_start,
                'end_date': batch_end
            }
            if ts_code:
                batch_params['ts_code'] = ts_code

            batch_list.append(batch_params)
            self.logger.debug(f"创建批次 {len(batch_list)}: {batch_start} - {batch_end}")

        # 检查: 单个批次
        if not batch_list and trade_days:
             self.logger.warning("交易日列表非空但未生成批次，将使用整个范围作为单个批次")
             batch_params = {
                'start_date': trade_days[0],
                'end_date': trade_days[-1]
             }
             if ts_code:
                batch_params['ts_code'] = ts_code
             return [batch_params]

        self.logger.info(f"成功生成 {len(batch_list)} 个批次")
        return batch_list

    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数

        将批处理参数转换为Tushare API调用所需的确切参数格式。
        对于daily_basic接口，参数可以直接使用。

        Args:
            batch_params: 批处理参数字典

        Returns:
            Dict: 准备好的API调用参数
        """
        # daily_basic接口参数无需特殊处理，直接返回
        return batch_params
