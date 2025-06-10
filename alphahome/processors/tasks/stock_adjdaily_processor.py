#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票日线后复权调整与交易日补全处理器

数据源:
- tushare_stock_factor_pro: 提供股票复权因子、后复权行情(OHLCVA_hfq)、换手率、股本等数据。
- tushare_others_calendar: 提供交易所交易日历。

主要处理逻辑:
1. 获取指定股票列表的后复权行情及其他因子数据。
2. 获取指定交易所的交易日历 (范围足够大)。
3. 对每只股票，将其行情数据与交易日历对齐。
4. 补全停牌日的数据：所有字段均使用前一个交易日数据填充 (ffill)。
   - 注意: 成交量、成交额、换手率等通常在停牌日为0，但按当前指示也进行ffill。
     如果需要将这些指标在停牌日设为0，请在 process_block 方法中进行调整。
5. 添加 'is_trade' 字段：实际交易日为1，补全的停牌日为0。

输出:
- stock_daily_adjusted_hfq: 包含所有交易日（含停牌日）的、后复权日线数据，
  并带有 'is_trade' 标记。
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# 实际项目中，你需要确保这些导入路径是正确的
from ..base.block_processor import BlockProcessor
from ..utils.query_builder import QueryBuilder
from ...common.db_manager import DBManager
from ...common.logging_utils import get_logger


class StockAdjDailyProcessor(BlockProcessor):
    """
    股票日线后复权调整与交易日补全处理器
    """

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        """
        初始化处理器
        Args:
            db_connection: 数据库连接实例
            config: 配置字典, 可能包含:
                - source_table_stock_factor: 股票因子源表名 (默认: 'tushare_stock_factor_pro')
                - source_table_calendar: 交易日历源表名 (默认: 'tushare_others_calendar')
                - result_table: 结果表名 (默认: 'stock_daily_adjusted_hfq')
                - calendar_exchange: 用于获取交易日历的交易所代码 (默认: 'SSE')
                - block_size_codes: 每个块处理的股票代码数量 (默认: 20)
                - default_calendar_start_date: 日历数据获取的默认起始日期 (默认: '19900101')
                - default_calendar_end_date: 日历数据获取的默认结束日期 (默认: '20991231')
        """
        resolved_config = config or {}
        super().__init__(
            db_connection=db_connection,
            date_column='trade_date',
            code_column='ts_code',
            block_type='code', 
            block_size=resolved_config.get('block_size_codes', 20),
            config=resolved_config
        )

        self.source_table_stock_factor = resolved_config.get('source_table_stock_factor', 'tushare_stock_factor_pro')
        self.source_table_calendar = resolved_config.get('source_table_calendar', 'tushare_others_calendar')
        self.result_table = resolved_config.get('result_table', 'stock_daily_adjusted_hfq')
        self.calendar_exchange = resolved_config.get('calendar_exchange', 'SSE')
        
        self.default_calendar_start_date_str = resolved_config.get('default_calendar_start_date', '19900101')
        self.default_calendar_end_date_str = resolved_config.get('default_calendar_end_date', '20991231')

        self._calendar_data: Optional[pd.DataFrame] = None 

        self.selected_fields = [
            'ts_code', 'trade_date',
            'open_hfq', 'high_hfq', 'low_hfq', 'close_hfq', 'volume_hfq', 'amount_hfq',
            'adj_factor', 
            'turnover_rate', 'turnover_rate_f', 'volume_ratio',
            'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
        ]

    async def _load_calendar_data(self) -> None:
        if self._calendar_data is not None:
            self.logger.info("交易日历数据已缓存。")
            return

        start_date = pd.to_datetime(self.default_calendar_start_date_str, format='%Y%m%d').date()
        end_date = pd.to_datetime(self.default_calendar_end_date_str, format='%Y%m%d').date()

        self.logger.info(f"首次从数据库加载交易所 '{self.calendar_exchange}' 的交易日历，范围: {start_date} 到 {end_date}")
        
        qb_calendar = QueryBuilder(self.source_table_calendar)
        qb_calendar.select(['cal_date', 'is_open']) 
        qb_calendar.add_condition("exchange = $exchange")
        qb_calendar.add_condition("cal_date >= $start_date")
        qb_calendar.add_condition("cal_date <= $end_date")
        
        query, params = qb_calendar.build({
            'exchange': self.calendar_exchange,
            'start_date': start_date.strftime('%Y%m%d'),
            'end_date': end_date.strftime('%Y%m%d')
        })

        try:
            calendar_rows = await self.db.fetch(query, **params)
            if not calendar_rows:
                self.logger.error(f"未能从表 {self.source_table_calendar} 加载到任何交易日历数据。")
                self._calendar_data = pd.DataFrame(columns=['cal_date'])
                return
            
            calendar_df = pd.DataFrame([dict(row) for row in calendar_rows])
            calendar_df['cal_date'] = pd.to_datetime(calendar_df['cal_date']).dt.date
            self._calendar_data = calendar_df[calendar_df['is_open'] == 1][['cal_date']].drop_duplicates().sort_values('cal_date').reset_index(drop=True)
            self.logger.info(f"加载并缓存了 {len(self._calendar_data)} 个开市交易日。")
        except Exception as e:
            self.logger.error(f"加载交易日历失败: {e}", exc_info=True)
            self._calendar_data = pd.DataFrame(columns=['cal_date'])
            # raise # 考虑此错误是否应中止操作

    async def _get_calendar_for_range(self, stock_start_date: date, stock_end_date: date) -> pd.DataFrame:
        if self._calendar_data is None:
            await self._load_calendar_data()
        
        if self._calendar_data.empty:
            return pd.DataFrame(columns=['cal_date'])

        return self._calendar_data[
            (self._calendar_data['cal_date'] >= stock_start_date) &
            (self._calendar_data['cal_date'] <= stock_end_date)
        ].copy()

    async def _fetch_block_data(self, block_params: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        codes_to_fetch = block_params.get('codes')
        if not codes_to_fetch:
            return {'stock_data': pd.DataFrame(), 'calendar_data': pd.DataFrame()}

        start_date_str = block_params.get('start_date') 
        end_date_str = block_params.get('end_date')     
        
        qb_stock = QueryBuilder(self.source_table_stock_factor)
        qb_stock.select(self.selected_fields)
        qb_stock.add_in_condition(self.code_column, '$codes') 

        query_params_stock = {'codes': codes_to_fetch}
        log_date_range = "所有日期"

        if start_date_str and end_date_str:
            qb_stock.add_condition(f"{self.date_column} >= $start_date")
            qb_stock.add_condition(f"{self.date_column} <= $end_date")
            query_params_stock['start_date'] = start_date_str
            query_params_stock['end_date'] = end_date_str
            log_date_range = f"{start_date_str} to {end_date_str}"
        
        self.logger.info(f"获取股票数据 for codes: {codes_to_fetch}, dates: {log_date_range}")
            
        qb_stock.add_order_by(self.code_column).add_order_by(self.date_column)
        query_stock, params_stock = qb_stock.build(query_params_stock)
        
        stock_df = pd.DataFrame(columns=self.selected_fields) # 默认为带有列的空DataFrame
        try:
            stock_rows = await self.db.fetch(query_stock, **params_stock)
            if stock_rows:
                stock_df = pd.DataFrame([dict(row) for row in stock_rows])
                if not stock_df.empty : # 转换前确保 'trade_date' 列存在
                     stock_df[self.date_column] = pd.to_datetime(stock_df[self.date_column]).dt.date
                self.logger.info(f"为代码 {codes_to_fetch} 加载了 {len(stock_df)} 行股票因子数据")
            else:
                self.logger.warning(f"表 {self.source_table_stock_factor} 未找到代码 {codes_to_fetch} 的数据 {log_date_range}")
        except Exception as e:
            self.logger.error(f"加载股票因子数据失败 for codes {codes_to_fetch}: {e}", exc_info=True)
        
        calendar_df = pd.DataFrame(columns=['cal_date'])
        min_date_for_cal: Optional[date] = None
        max_date_for_cal: Optional[date] = None

        if not stock_df.empty:
            min_date_for_cal = stock_df[self.date_column].min()
            max_date_for_cal = stock_df[self.date_column].max()
        elif start_date_str and end_date_str:
             min_date_for_cal = pd.to_datetime(start_date_str, format='%Y%m%d').date()
             max_date_for_cal = pd.to_datetime(end_date_str, format='%Y%m%d').date()
        
        if min_date_for_cal and max_date_for_cal:
            calendar_df = await self._get_calendar_for_range(min_date_for_cal, max_date_for_cal)
        else: # 如果无法确定日期范围，则使用回退方案
            await self._load_calendar_data() # 确保默认日历已加载
            if self._calendar_data is not None:
                calendar_df = self._calendar_data.copy()
        
        return {'stock_data': stock_df, 'calendar_data': calendar_df}

    async def process_block(self, data_dict: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        stock_df_original = data_dict.get('stock_data')
        calendar_df = data_dict.get('calendar_data')

        if stock_df_original is None:
            return pd.DataFrame()
        
        if calendar_df is None or calendar_df.empty:
            self.logger.warning("日历数据为空，不进行日期补全。is_trade 将全设为1 (假设)。")
            if stock_df_original.empty: return pd.DataFrame()
            processed_df = stock_df_original.copy()
            processed_df['is_trade'] = 1
            # 确保所有选定字段以及 'is_trade' 都存在
            final_cols = self.selected_fields[:] + ['is_trade']
            for col in final_cols:
                if col not in processed_df.columns: processed_df[col] = np.nan
            return processed_df[final_cols]

        all_processed_dfs = []
        trade_calendar_dates_series = calendar_df['cal_date'].sort_values().unique()

        for ts_code, group_df_orig in stock_df_original.groupby(self.code_column):
            if group_df_orig.empty: continue

            group_df = group_df_orig.sort_values(self.date_column).set_index(self.date_column)
            min_stock_date, max_stock_date = group_df.index.min(), group_df.index.max()
            
            current_code_calendar_dates = trade_calendar_dates_series[
                (trade_calendar_dates_series >= min_stock_date) & (trade_calendar_dates_series <= max_stock_date)
            ]

            if len(current_code_calendar_dates) == 0:
                self.logger.warning(f"股票 {ts_code} ({min_stock_date}-{max_stock_date}) 无对应交易日历，仅处理原始数据。")
                processed_group = group_df.reset_index()
                processed_group['is_trade'] = 1
                all_processed_dfs.append(processed_group)
                continue

            full_date_index = pd.Index(current_code_calendar_dates, name=self.date_column)
            merged_df = pd.DataFrame(index=full_date_index).join(group_df, how='left')
            merged_df[self.code_column] = ts_code

            merged_df['is_trade'] = 0
            actual_trade_dates_for_stock = group_df.index[group_df.index.isin(full_date_index)]
            merged_df.loc[actual_trade_dates_for_stock, 'is_trade'] = 1
            
            cols_to_ffill = [col for col in self.selected_fields if col not in [self.code_column, self.date_column]]
            for col in cols_to_ffill:
                if col in merged_df.columns: merged_df[col] = merged_df[col].ffill()
                else: merged_df[col] = np.nan
            
            all_processed_dfs.append(merged_df.reset_index())

        if not all_processed_dfs: return pd.DataFrame()
            
        final_df = pd.concat(all_processed_dfs, ignore_index=True)
        output_columns = self.selected_fields[:]
        if 'is_trade' not in output_columns: output_columns.append('is_trade')
        
        for col in output_columns: # 确保所有列都存在
            if col not in final_df.columns: final_df[col] = np.nan
        
        return final_df[output_columns].copy()

    async def _clear_existing_results(self, data: pd.DataFrame, block_params: Dict[str, Any]):
        if not hasattr(self, 'result_table') or not self.result_table or data.empty:
            return

        codes_in_block = data[self.code_column].unique().tolist()
        min_date_in_data = data[self.date_column].min()
        max_date_in_data = data[self.date_column].max()

        if not codes_in_block or pd.isna(min_date_in_data) or pd.isna(max_date_in_data):
            self.logger.warning(f"无法确定清除范围，跳过清除。Codes: {codes_in_block}, MinDate: {min_date_in_data}, MaxDate: {max_date_in_data}")
            return

        start_date_str = min_date_in_data.strftime('%Y%m%d')
        end_date_str = max_date_in_data.strftime('%Y%m%d')

        delete_conditions = [
            f"{self.code_column} = ANY($codes)",
            f"{self.date_column} >= $start_date",
            f"{self.date_column} <= $end_date"
        ]
        delete_params = {
            'codes': codes_in_block,
            'start_date': start_date_str,
            'end_date': end_date_str
        }
            
        query = f"DELETE FROM {self.result_table} WHERE " + " AND ".join(delete_conditions)
        self.logger.info(f"清除旧结果 (表: {self.result_table}): codes {codes_in_block}, dates {start_date_str}-{end_date_str}")
        try:
            await self.db.execute(query, delete_params)
            self.logger.info(f"成功清除旧结果。")
        except Exception as e:
            self.logger.error(f"清除旧结果时发生错误: {e}", exc_info=True) 