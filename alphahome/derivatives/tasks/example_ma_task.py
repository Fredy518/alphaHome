# alphahome/derivatives/tasks/example_ma_task.py
import logging
import pandas as pd
from typing import Dict, Any, List, Optional
import asyncio # 如果计算非常耗时，可以用于 loop.run_in_executor

from ..base_derivative_task import BaseDerivativeTask
from ..derivative_task_factory import derivative_task_register
# 实际的 DBManager 实例将由 DerivativeTaskFactory 在任务实例化时通过构造函数注入到 self.db_manager

# 移除模块级 logger，应使用由基类或工厂提供的 self.logger
# task_logger = logging.getLogger(__name__)


@derivative_task_register(name="example_stock_sma_5") # 将此任务注册到工厂，名称为 example_stock_sma_5
class ExampleMovingAverageTask(BaseDerivativeTask):
    """
    示例衍生品任务：计算股票5日简单移动平均线 (SMA)。
    """
    description: str = "计算指定股票日线收盘价的5日简单移动平均值。"
    
    input_tables: List[str] = ["tushare_stock_daily"] # 定义任务依赖的输入表
    output_table: str = "der_stock_daily_sma5"      # 定义计算结果输出的表名
    
    # 定义输出表的结构 (schema)
    output_schema: Dict[str, Any] = {
        "ts_code":    {"type": "VARCHAR(10)", "constraints": "NOT NULL"}, # 股票代码
        "trade_date": {"type": "DATE",        "constraints": "NOT NULL"}, # 交易日期
        "sma_5":      {"type": "NUMERIC(18,4)", "constraints": "NULL"},   # 5日均线值 (允许NULL，以处理数据不足的情况)
        # "close_price": {"type": "NUMERIC(18,4)"} # (可选) 同时存储原始收盘价用于参考
    }
    primary_keys: List[str] = ["ts_code", "trade_date"] # 定义输出表的主键
    date_column: Optional[str] = "trade_date"           # 用于增量更新的日期列
    default_start_date: Optional[str] = "19900101"     # 全量计算时的默认起始日期

    async def load_input_data(self, ts_code: str, start_date: str, end_date: str, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        加载计算所需的输入数据 (股票日线行情)。

        参数:
            ts_code (str): 股票代码。
            start_date (str): 数据加载的开始日期 (YYYYMMDD)。
            end_date (str): 数据加载的结束日期 (YYYYMMDD)。
            **kwargs: 其他可能的参数。
        返回:
            一个字典，键为输入表名，值为包含数据的 DataFrame。
        """
        self.logger.info(f"任务 '{self.name}': 正在为股票 {ts_code} 加载 {start_date} 至 {end_date} 的输入数据。")
        
        # ---- 真实场景下的数据加载示例 (当前被注释掉，使用的是模拟数据) ----
        # query = (
        #     f"SELECT trade_date, ts_code, close "
        #     f"FROM {self.input_tables[0]} " # self.input_tables[0] 即 'tushare_stock_daily'
        #     f"WHERE ts_code = '{ts_code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}' "
        #     f"ORDER BY trade_date ASC;"
        # )
        # self.logger.debug(f"执行查询: {query}")
        # # 假设 self.db_manager 有一个异步方法 fetch_dataframe 可以执行SQL并返回DataFrame
        # # df_daily = await self.db_manager.fetch_dataframe(query) 
        # # if df_daily is None:
        # #     df_daily = pd.DataFrame(columns=['trade_date', 'ts_code', 'close'])
        # ---- 真实场景数据加载结束 ----
        
        # ---- 模拟数据生成 (用于示例和测试) ----
        self.logger.warning(f"任务 '{self.name}': 当前正在使用模拟数据进行测试！")
        try:
            dates = pd.to_datetime(pd.date_range(start=start_date, end=end_date, freq='B')) # 'B' 表示工作日频率
        except ValueError as e:
            self.logger.error(f"任务 '{self.name}': 无效的日期范围 '{start_date}' - '{end_date}': {e}")
            return {self.input_tables[0]: pd.DataFrame(columns=['trade_date', 'ts_code', 'close'])}
            
        if dates.empty:
            self.logger.info(f"任务 '{self.name}': 日期范围 '{start_date}' - '{end_date}' 内没有工作日。")
            return {self.input_tables[0]: pd.DataFrame(columns=['trade_date', 'ts_code', 'close'])}

        simulated_data = {
            'trade_date': dates,
            'ts_code': ts_code,
            # 生成一些模拟的收盘价数据
            'close': [10 + i*0.1 + (i % 5 - 2) * 0.2 for i in range(len(dates))] 
        }
        df_daily = pd.DataFrame(simulated_data)
        # 确保 trade_date 列是 Python date 对象，而不是 Pandas Timestamp，如果数据库需要
        df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date']).dt.date 
        # ---- 模拟数据生成结束 ----
        
        self.logger.info(f"任务 '{self.name}': 为表 '{self.input_tables[0]}' 加载了 {len(df_daily)} 行数据。头部数据示例:\n{df_daily.head(2)}")
        return {self.input_tables[0]: df_daily}

    async def calculate(self, input_data: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """
        执行 SMA(5) 计算。

        参数:
            input_data: 包含 'tushare_stock_daily' 数据的字典。
            **kwargs: 可能包含 'ts_code' 等参数，用于日志或特定逻辑。
        返回:
            包含计算结果 (ts_code, trade_date, sma_5) 的 DataFrame。
        """
        ts_code = kwargs.get("ts_code", "UNKNOWN_TS_CODE") # 从kwargs获取ts_code用于日志
        self.logger.info(f"任务 '{self.name}': 正在为股票 {ts_code} 计算 SMA(5)。")
        
        df_daily = input_data.get(self.input_tables[0])
        if df_daily is None or df_daily.empty:
            self.logger.warning(f"任务 '{self.name}': 计算 SMA(5) 时输入数据为空 ({self.input_tables[0]}) for {ts_code}。")
            return pd.DataFrame() # 返回空DataFrame

        # 确保数据按日期排序
        df_daily = df_daily.sort_values(by='trade_date')
        
        # 定义内部函数进行均值计算，更清晰
        def _compute_sma(df: pd.DataFrame, window: int) -> pd.Series:
            return df['close'].rolling(window=window, min_periods=1).mean() # min_periods=1 确保在窗口不满时也计算

        # Pandas的rolling操作通常是C语言优化的，直接调用通常性能足够。
        # 如果这里的计算非常复杂且耗时，可以考虑使用 self.executor 在线程池中运行，如下所示：
        # if self.executor:
        #     loop = asyncio.get_running_loop()
        #     self.logger.debug(f"任务 '{self.name}': 使用线程池执行 SMA 计算。")
        #     # 注意: 传递 df_daily.copy() 以避免潜在的并发修改问题，尽管在这里可能不是必须的
        #     sma_series = await loop.run_in_executor(self.executor, _compute_sma, df_daily.copy(), 5)
        #     df_daily['sma_5'] = sma_series
        # else: 
        #     self.logger.debug(f"任务 '{self.name}': 直接执行 SMA 计算。")
        #     df_daily['sma_5'] = _compute_sma(df_daily, 5)
        df_daily['sma_5'] = _compute_sma(df_daily, 5) # 简化处理，直接计算
        
        # 构建输出 DataFrame，只包含需要的列
        output_df = df_daily[['ts_code', 'trade_date', 'sma_5']].copy()
        output_df['sma_5'] = output_df['sma_5'].round(4) # 保留4位小数
        
        self.logger.info(f"任务 '{self.name}': 股票 {ts_code} 的 SMA(5) 计算完成。共生成 {len(output_df)} 行输出数据。")
        return output_df 