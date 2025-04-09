import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ...sources.tushare import TushareTask

class StockDailyBasicTask(TushareTask):
    """股票每日基本面指标任务
    
    获取股票的每日基本面指标，包括市盈率、市净率、换手率、总市值等数据。
    该任务使用Tushare的daily_basic接口获取数据，并依赖于股票日线数据任务。
    """
    
    # 核心属性
    name = "stock_daily_basic"
    description = "获取股票每日基本面指标"
    table_name = "stock_daily_basic"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # 依赖关系
    dependencies = ["stock_daily"]
    
    # Tushare特有属性
    api_name = "daily_basic"
    fields = [
        "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", 
        "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", 
        "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"
    ]
    
    # 表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "close": {"type": "NUMERIC(10,4)"},
        "turnover_rate": {"type": "NUMERIC(10,4)"},
        "turnover_rate_f": {"type": "NUMERIC(10,4)"},
        "volume_ratio": {"type": "NUMERIC(10,4)"},
        "pe": {"type": "NUMERIC(10,4)"},
        "pe_ttm": {"type": "NUMERIC(10,4)"},
        "pb": {"type": "NUMERIC(10,4)"},
        "ps": {"type": "NUMERIC(10,4)"},
        "ps_ttm": {"type": "NUMERIC(10,4)"},
        "dv_ratio": {"type": "NUMERIC(10,4)"},
        "dv_ttm": {"type": "NUMERIC(10,4)"},
        "total_share": {"type": "NUMERIC(20,4)"},
        "float_share": {"type": "NUMERIC(20,4)"},
        "free_share": {"type": "NUMERIC(20,4)"},
        "total_mv": {"type": "NUMERIC(20,4)"},
        "circ_mv": {"type": "NUMERIC(20,4)"},
        "free_mv": {"type": "NUMERIC(20,4)"},
        "float_ratio": {"type": "NUMERIC(10,4)"},
        "bp_ratio": {"type": "NUMERIC(10,4)"},
        "annual_div_yield": {"type": "NUMERIC(10,4)"}
    }
    
    # 数据处理规则
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
    
    # 数据验证规则
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
    
    # 自定义索引
    indexes = [
        {"name": "idx_daily_basic_code", "columns": "ts_code"},
        {"name": "idx_daily_basic_date", "columns": "trade_date"}
    ]
    
    def get_batch_list(self, start_date: str = None, end_date: str = None, **kwargs) -> List[Dict]:
        """获取批处理列表
        
        根据日期范围和其他参数生成批处理参数列表，用于分批获取数据。
        对于daily_basic接口，我们按照日期和股票代码进行批处理。
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            **kwargs: 其他参数，如ts_code（股票代码列表）
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        batch_list = []
        
        # 处理日期范围
        if start_date and end_date:
            # 将日期字符串转换为datetime对象
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            
            # 生成日期序列
            date_range = pd.date_range(start=start, end=end, freq='D')
            
            # 处理股票代码
            ts_codes = kwargs.get('ts_code', '')
            if ts_codes:
                # 如果提供了多个股票代码，按照代码分批
                codes = ts_codes.split(',')
                for date in date_range:
                    date_str = date.strftime('%Y%m%d')
                    for code in codes:
                        batch_list.append({
                            'ts_code': code.strip(),
                            'trade_date': date_str
                        })
            else:
                # 如果没有提供股票代码，只按日期分批
                for date in date_range:
                    batch_list.append({
                        'trade_date': date.strftime('%Y%m%d')
                    })
        else:
            # 如果没有提供日期范围，使用其他参数
            batch_list.append(kwargs)
        
        return batch_list
    
    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数
        
        将批处理参数转换为Tushare API调用所需的确切参数格式。
        
        Args:
            batch_params: 批处理参数字典
            
        Returns:
            Dict: 准备好的API调用参数
        """
        # 对于daily_basic接口，参数可以直接使用
        return batch_params
    
    def process_data(self, data):
        """处理股票每日基本面数据"""
        # 首先调用父类的数据处理
        data = super().process_data(data)
        
        # 处理特殊值
        if not data.empty:
            # 确保日期格式正确
            if "trade_date" in data.columns and data["trade_date"].dtype != 'datetime64[ns]':
                data["trade_date"] = pd.to_datetime(data["trade_date"])
            
            # 处理PE、PB等为负数的情况（表示业绩为负）
            for col in ["pe", "pe_ttm", "pb", "ps", "ps_ttm"]:
                if col in data.columns:
                    # 将过大的值（可能是异常值）设置为NA
                    data.loc[data[col] > 10000, col] = float('nan')
                    # 将负值标记为特殊值，表示业绩为负
                    data.loc[data[col] < 0, col] = -1
            
            # 计算额外的指标
            if "total_share" in data.columns and "close" in data.columns:
                # 计算市值（如果没有total_mv字段）
                if "total_mv" not in data.columns:
                    data["total_mv"] = data["total_share"] * data["close"]
            
            if "float_share" in data.columns and "close" in data.columns:
                # 计算流通市值（如果没有circ_mv字段）
                if "circ_mv" not in data.columns:
                    data["circ_mv"] = data["float_share"] * data["close"]
            
            # 计算自由流通市值
            if "free_share" in data.columns and "close" in data.columns:
                data["free_mv"] = data["free_share"] * data["close"]
            
            # 计算流通比例
            if "float_share" in data.columns and "total_share" in data.columns:
                data["float_ratio"] = data["float_share"] / data["total_share"]
                # 处理可能的除零错误
                data.loc[data["total_share"] == 0, "float_ratio"] = 0
            
            # 计算市净率倒数（净资产收益率）
            if "pb" in data.columns:
                data["bp_ratio"] = 1 / data["pb"]
                # 处理无穷大和NaN
                data.loc[data["pb"] == 0, "bp_ratio"] = float('nan')
                data.loc[data["pb"] < 0, "bp_ratio"] = float('nan')
            
            # 计算股息率年化（如果是季度数据）
            if "dv_ratio" in data.columns:
                data["annual_div_yield"] = data["dv_ratio"] * 4
            
            # 处理缺失值
            for col in data.columns:
                if col not in ["ts_code", "trade_date"] and data[col].dtype != 'object':
                    # 对于比率类指标，用0填充缺失值可能不合适，这里用中位数填充
                    if col in ["pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm"]:
                        median_value = data[col].median()
                        if pd.notna(median_value):
                            data[col] = data[col].fillna(median_value)
                    else:
                        # 其他数值型指标用0填充
                        data[col] = data[col].fillna(0)
        
        return data
    
    async def pre_execute(self):
        """任务执行前的准备工作"""
        await super().pre_execute()
        self.logger.info("准备执行股票每日基本面指标任务...")
        
        # 检查依赖任务是否已完成
        if self.db and "stock_daily" in self.dependencies:
            # 获取最新的股票日线数据日期
            latest_date_query = f"""SELECT MAX(trade_date) FROM stock_daily"""
            latest_date = await self.db.fetch_one(latest_date_query)
            
            if latest_date and latest_date[0]:
                self.logger.info(f"股票日线数据最新日期: {latest_date[0]}")
            else:
                self.logger.warning("未找到股票日线数据，依赖任务可能未完成")
    
    async def post_execute(self, result):
        """任务执行后的清理工作"""
        await super().post_execute(result)
        self.logger.info(f"股票每日基本面指标任务执行完成，结果: {result}")
        
        # 执行数据质量检查
        if self.db:
            await self._check_data_quality()
    
    async def _check_data_quality(self):
        """执行数据质量检查"""
        try:
            # 检查数据完整性
            today = datetime.now().date()
            last_trading_day = today - timedelta(days=1)  # 简化处理，实际应该查询交易日历
            
            # 检查最新数据日期
            latest_date_query = f"""SELECT MAX(trade_date) FROM {self.table_name}"""
            latest_date = await self.db.fetch_one(latest_date_query)
            
            if latest_date and latest_date[0]:
                days_diff = (today - latest_date[0]).days
                if days_diff > 5:  # 假设5天是合理的延迟
                    self.logger.warning(f"数据可能不是最新的，最新日期: {latest_date[0]}, 相差: {days_diff}天")
            
            # 检查主要股票是否都有数据
            major_stocks_query = """
            SELECT COUNT(DISTINCT ts_code) FROM stock_daily 
            WHERE trade_date = (SELECT MAX(trade_date) FROM stock_daily)
            """
            daily_count = await self.db.fetch_one(major_stocks_query)
            
            basic_stocks_query = f"""
            SELECT COUNT(DISTINCT ts_code) FROM {self.table_name} 
            WHERE trade_date = (SELECT MAX(trade_date) FROM {self.table_name})
            """
            basic_count = await self.db.fetch_one(basic_stocks_query)
            
            if daily_count and basic_count and daily_count[0] > 0 and basic_count[0] > 0:
                coverage = (basic_count[0] / daily_count[0]) * 100
                if coverage < 95:  # 假设95%是合理的覆盖率
                    self.logger.warning(f"基本面数据覆盖率较低: {coverage:.2f}%, 日线数据: {daily_count[0]}只股票, 基本面数据: {basic_count[0]}只股票")
            
            self.logger.info("数据质量检查完成")
            
        except Exception as e:
            self.logger.error(f"数据质量检查失败: {str(e)}")
