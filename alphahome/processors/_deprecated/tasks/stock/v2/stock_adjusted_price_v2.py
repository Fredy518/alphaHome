#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票后复权价格计算任务 (V2)

演示如何使用新的三层架构（Engine -> Task -> Operation）实现一个完整的处理任务。
计算公式：后复权价格 = 原始价格 * 复权因子
"""

from typing import Any, Dict, Optional
import pandas as pd

from ...common.task_system import task_register
from ...processors.tasks.base_task import ProcessorTaskBase
from ...processors.operations.base_operation import Operation, OperationPipeline
from ...processors.utils.query_builder import QueryBuilder


class AdjustmentFactorOperation(Operation):
    """
    核心计算操作：应用复权因子。
    """
    def __init__(self, config=None):
        super().__init__(name="AdjustmentFactorOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用复权因子计算"""
        if data.empty:
            return data
        
        result = data.copy()
        
        required_cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'adj_factor']
        if not set(required_cols).issubset(result.columns):
            missing = set(required_cols) - set(result.columns)
            raise ValueError(f"缺少必要的列进行复权计算: {missing}")
        
        # 为了安全，填充可能由外连接产生的NaN复权因子
        result['adj_factor'] = result['adj_factor'].fillna(1.0)
        
        result['adj_open'] = result['open'] * result['adj_factor']
        result['adj_high'] = result['high'] * result['adj_factor']
        result['adj_low'] = result['low'] * result['adj_factor']
        result['adj_close'] = result['close'] * result['adj_factor']
        result['adj_vol'] = result['vol']  # 成交量通常不做复权调整

        output_cols = ['ts_code', 'trade_date', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_vol']
        final_result = result[output_cols].copy()
        
        self.logger.info(f"复权价格计算完成，处理了 {len(final_result)} 行数据。")
        return final_result


class DataValidationOperation(Operation):
    """
    数据验证操作：确保处理后的数据质量。
    """
    def __init__(self, config=None):
        super().__init__(name="DataValidationOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """验证和清理数据"""
        if data.empty:
            return data
        
        result = data.copy()
        original_len = len(result)
        
        price_cols = ['adj_open', 'adj_high', 'adj_low', 'adj_close']
        result = result.dropna(subset=price_cols, how='any')
        result = result[(result[price_cols] > 0).all(axis=1)]
        
        if 'adj_high' in result.columns and 'adj_low' in result.columns:
            result = result[result['adj_high'] >= result['adj_low']]
        
        if 'adj_vol' in result.columns:
            result = result[result['adj_vol'] >= 0]
        
        removed_count = original_len - len(result)
        if removed_count > 0:
            self.logger.info(f"数据验证完成，移除了 {removed_count} 行无效数据。")
        
        return result


# 移除独立的Pipeline类，将其功能集成到Task中


@task_register()
class StockAdjustedPriceV2Task(ProcessorTaskBase):
    """
    股票后复权价格计算任务，负责定义整个业务流程（I/O + 处理）。
    """
    name = "stock_adjusted_price_v2"
    table_name = "stock_adjusted_daily_v2"
    description = "计算股票后复权日线价格 (V2 架构)"

    source_tables = ["tushare_stock_daily", "tushare_stock_adj_factor"]
    primary_keys = ["ts_code", "trade_date"]

    async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        在任务内部编排操作来处理数据
        """
        if data is None or data.empty:
            self.logger.warning("输入数据为空，跳过处理。")
            return pd.DataFrame()

        self.logger.info(f"开始处理股票后复权价格数据，输入行数: {len(data)}")

        # 使用OperationPipeline作为内部辅助工具
        pipeline = OperationPipeline("StockAdjustedPriceV2InternalPipeline")
        pipeline.add_operation(AdjustmentFactorOperation())
        pipeline.add_operation(DataValidationOperation())

        processed_data = await pipeline.apply(data)

        self.logger.info(f"股票后复权价格处理完成，输出行数: {len(processed_data)}")
        return processed_data
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        通过左连接从数据库获取日线行情和复权因子数据。
        """
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        codes = kwargs.get('codes')

        qb = QueryBuilder("tushare_stock_daily as d")
        qb.select([
            'd.ts_code', 'd.trade_date', 
            'd.open', 'd.high', 'd.low', 'd.close', 'd.vol',
            'a.adj_factor'
        ])
        qb.add_condition(
            "LEFT JOIN tushare_stock_adj_factor as a ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date"
        )
        
        query_params = {}
        if start_date and end_date:
            qb.add_condition("d.trade_date BETWEEN $start_date AND $end_date")
            query_params['start_date'] = start_date
            query_params['end_date'] = end_date
        
        if codes:
            qb.add_in_condition('d.ts_code', '$codes')
            query_params['codes'] = codes
            
        qb.add_order_by('d.ts_code').add_order_by('d.trade_date')

        # 这里的 build 方法需要能处理 JOIN 语句，我们简化一下，直接拼接
        # 注意：一个更健壮的QueryBuilder需要能优雅地处理JOIN
        join_str = "LEFT JOIN tushare_stock_adj_factor as a ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date"
        base_query, params = qb.build(query_params)
        
        # 这是一个临时的hack，来将JOIN注入查询
        final_query = base_query.replace(f"FROM {qb.table_name}", f"FROM {qb.table_name} {join_str}")
        
        self.logger.info(f"执行查询获取行情和复权因子数据...")
        self.logger.debug(f"Query: {final_query}, Params: {params}")

        try:
            data = await self.db.fetch_dataframe(final_query, **params)
            if data is not None and not data.empty:
                self.logger.info(f"成功获取 {len(data)} 条数据进行处理。")
                return data
            else:
                self.logger.warning("未获取到任何需要处理的数据。")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"获取数据失败: {e}", exc_info=True)
            return pd.DataFrame()

    async def save_result(self, data: pd.DataFrame, **kwargs):
        """
        将处理后的复权价格数据保存到数据库。
        """
        if data.empty:
            self.logger.warning("没有数据需要保存。")
            return
        
        self.logger.info(f"准备保存 {len(data)} 条后复权价格数据到 {self.table_name}...")
        
        # 在这里可以添加数据类型转换和格式化逻辑
        price_cols = ['adj_open', 'adj_high', 'adj_low', 'adj_close']
        for col in price_cols:
            if col in data.columns:
                data[col] = data[col].round(4)
        
        if 'adj_vol' in data.columns:
            data['adj_vol'] = data['adj_vol'].astype('int64')

        # 使用DBManager的upsert功能保存数据
        try:
            await self.db.save_dataframe(
                data, 
                self.table_name, 
                primary_keys=self.primary_keys,
                use_insert_mode=self.use_insert_mode
            )
            self.logger.info(f"成功保存 {len(data)} 条数据到 {self.table_name}。")
        except Exception as e:
            self.logger.error(f"保存数据到 {self.table_name} 失败: {e}", exc_info=True)
            raise

    # _calculate_from_multiple_sources 方法不再需要，其逻辑被分解到
    # fetch_data 和 Pipeline 中。
