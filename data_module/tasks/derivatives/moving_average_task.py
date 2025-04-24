import asyncio
import os
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from .base_derivative_task import BaseDerivativeTask
# 假设有一个工具函数用于构建过滤条件
# from ...tools.db_utils import build_filter_conditions

class MovingAverageTask(BaseDerivativeTask):
    """计算股票收盘价移动平均线的任务"""

    name = "moving_average"
    table_name = "derivative_stock_moving_average" # 存储结果的表名
    primary_keys = ["trade_date", "stock_code", "window"] # 结果表的主键
    date_column = "trade_date"
    description = "计算股票日收盘价的移动平均线"

    # 定义输入数据依赖
    input_spec = {
        'stock_daily': {
            'table': 'tushare_stock_daily', # 依赖的基础数据表 - 已修正
            'columns': ['trade_date', 'stock_code', 'close'], # 需要的列
            'date_col': 'trade_date' # 用于日期过滤的列
        }
    }

    async def fetch_data(self,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        stock_codes: Optional[List[str]] = None,
                        window: int = 20, # 需要窗口期来确定回溯多少数据
                         **kwargs) -> Dict[str, pd.DataFrame]:
        """获取计算移动平均线所需的股票日线数据

        需要根据 window 参数调整查询的起始日期，以确保有足够的数据计算第一个窗口。
        """
        input_data = {}
        spec = self.input_spec['stock_daily']
        table = spec['table']
        columns = spec['columns']
        date_col = spec['date_col']

        # 调整查询起始日期以包含计算窗口所需的数据
        # 注意：需要一个健壮的方式来获取实际的起始日期，可能需要查询交易日历
        # 这里用一个简化的方法：假设我们需要多回溯 window 天的数据
        # 更优的方法是查询数据库获取 start_date 前 window 个交易日
        query_start_date = start_date
        if start_date:
            try:
                # 粗略回溯 window 天
                # TODO: 替换为查询交易日历获取精确回溯日期
                start_dt_obj = datetime.strptime(start_date, '%Y%m%d')
                # 粗略回溯天数，增加冗余以覆盖非交易日
                query_start_dt_obj = start_dt_obj - timedelta(days=int(window * 1.5) + 5)
                query_start_date = query_start_dt_obj.strftime('%Y%m%d')
                self.logger.info(f"为计算窗口 {window}，将查询起始日期调整为 {query_start_date}")
            except ValueError as e:
                self.logger.warning(f"解析日期 {start_date} 失败: {e}，将使用原始 start_date")
                query_start_date = start_date
            except Exception as e:
                self.logger.warning(f"调整查询起始日期时发生未知错误: {e}，将使用原始 start_date: {start_date}")
                query_start_date = start_date

        # 构建 SQL 查询
        columns_str = ', '.join([f'"{c}"' for c in columns])
        query = f'SELECT {columns_str} FROM "{table}"' # Add quotes around table name

        # 添加过滤条件
        conditions = []
        params = []
        param_idx = 1

        if query_start_date:
            conditions.append(f'"{date_col}" >= ${param_idx}')
            params.append(query_start_date)
            param_idx += 1
        if end_date:
            conditions.append(f'"{date_col}" <= ${param_idx}')
            params.append(end_date)
            param_idx += 1
        if stock_codes:
            if len(stock_codes) == 1:
                conditions.append(f'"stock_code" = ${param_idx}')
                params.append(stock_codes[0])
                param_idx += 1
            else:
                placeholders = ', '.join([f'${param_idx + i}' for i in range(len(stock_codes))])
                conditions.append(f'"stock_code" IN ({placeholders})')
                params.extend(stock_codes)
                param_idx += len(stock_codes)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f' ORDER BY "stock_code", "{date_col}"' # 排序确保 rolling 计算正确

        self.logger.debug(f"Executing query: {query} with params: {params}")

        try:
            result = await self.db.fetch(query, *params)
            if not result:
                self.logger.warning("未查询到任何股票日线数据")
                return {}

            # 直接将 asyncpg Record 列表转为 DataFrame
            df = pd.DataFrame.from_records(result, columns=columns)

            # 转换数据类型，特别是日期和数值
            df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y%m%d') # 保持 YYYYMMDD 格式
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna(subset=['close'])

            self.logger.info(f"成功获取 {len(df)} 行股票日线数据")
            input_data['stock_daily'] = df

        except Exception as e:
            self.logger.error(f"获取股票日线数据失败: {str(e)}", exc_info=True)
            return {} # 返回空字典表示失败

        return input_data

    def _run_calculation(self, data_dict: Dict[str, pd.DataFrame], window: int = 20, **kwargs) -> Optional[pd.DataFrame]:
        """计算移动平均线的同步函数"""
        # 注意：此方法在子进程中运行，self.logger 不可用。使用 print 或配置进程安全日志。
        pid = os.getpid()
        print(f"[PID:{pid}] 开始计算窗口为 {window} 的移动平均线...")

        if 'stock_daily' not in data_dict or data_dict['stock_daily'].empty:
            print(f"[PID:{pid}] 输入数据为空，无法计算移动平均线")
            return None

        df = data_dict['stock_daily'].copy() # 使用副本以避免 SettingWithCopyWarning
        print(f"[PID:{pid}] 输入数据 {len(df)} 行.")

        try:
            ma_col_name = f'ma_{window}'
            # 按股票代码分组计算移动平均线
            # 使用 reset_index() 后再 rolling 可以避免潜在的 MultiIndex 问题
            df[ma_col_name] = df.groupby('stock_code', group_keys=False)['close']\
                                    .rolling(window=window, min_periods=window)\
                                    .mean()
            # .reset_index(level=0, drop=True) # reset_index on rolling is tricky, apply after

            # 删除因窗口不足而产生的 NaN 值
            result_df = df.dropna(subset=[ma_col_name]).copy() # 使用副本

            if result_df.empty:
                 print(f"[PID:{pid}] 计算MA({window})后没有有效数据（可能窗口期过长或数据不足）。")
                 return None

            # 添加 window 列，用于主键
            result_df['window'] = window

            # 选择并重排最终需要的列，确保顺序与 primary_keys + 结果列 一致
            # 使用 list comprehension 动态构建结果列名
            result_columns = [col for col in self.primary_keys if col in result_df.columns] + [ma_col_name]
            # 确保所有需要的列都存在
            missing_cols = [col for col in result_columns if col not in result_df.columns]
            if missing_cols:
                 print(f"[PID:{pid}] 错误：计算结果缺少列: {missing_cols}")
                 return None

            result_df = result_df[result_columns]

            print(f"[PID:{pid}] 移动平均线计算完成，生成 {len(result_df)} 行结果。")
            return result_df

        except Exception as e:
            # 在子进程中打印异常
            print(f"[PID:{pid}] 计算MA({window})时出错: {type(e).__name__} - {e}")
            import traceback
            traceback.print_exc() # 打印详细的回溯信息
            return None

    # process_data 方法由 BaseDerivativeTask 提供
    # save_data 方法默认使用基类 Task 的 upsert 实现

# 如果需要，可以在这里添加任务注册逻辑，但通常在 __init__.py 或使用装饰器完成
# from ..task_factory import TaskFactory
# TaskFactory.register_task(MovingAverageTask.name, MovingAverageTask) 