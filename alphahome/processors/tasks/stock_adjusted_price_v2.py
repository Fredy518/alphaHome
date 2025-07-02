#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票后复权价格计算任务 (重构版本)

使用新的处理器架构，通过流水线组合操作来计算股票后复权价格。
计算公式：后复权价格 = 原始价格 * 累积复权因子
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ...common.task_system import task_register
from .base_task import ProcessorTaskBase
from ..pipelines.base_pipeline import ProcessingPipeline
from ..operations.base_operation import Operation


class AdjustmentFactorOperation(Operation):
    """复权因子处理操作"""
    
    def __init__(self, config=None):
        super().__init__(name="AdjustmentFactorOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用复权因子计算"""
        if data.empty:
            return data
        
        result = data.copy()
        
        # 确保有必要的列
        required_cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'adj_factor']
        missing_cols = set(required_cols) - set(result.columns)
        if missing_cols:
            raise ValueError(f"缺少必要的列: {missing_cols}")
        
        # 计算后复权价格
        result['adj_open'] = result['open'] * result['adj_factor']
        result['adj_high'] = result['high'] * result['adj_factor']
        result['adj_low'] = result['low'] * result['adj_factor']
        result['adj_close'] = result['close'] * result['adj_factor']
        result['adj_vol'] = result['vol']  # 成交量不需要复权
        
        # 保留必要的列
        output_cols = ['ts_code', 'trade_date', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_vol']
        result = result[output_cols]
        
        self.logger.info(f"完成复权价格计算，处理 {len(result)} 行数据")
        return result


class DataValidationOperation(Operation):
    """数据验证操作"""
    
    def __init__(self, config=None):
        super().__init__(name="DataValidationOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """验证和清理数据"""
        if data.empty:
            return data
        
        result = data.copy()
        original_len = len(result)
        
        # 移除价格为负数或零的记录
        price_cols = ['adj_open', 'adj_high', 'adj_low', 'adj_close']
        for col in price_cols:
            if col in result.columns:
                result = result[result[col] > 0]
        
        # 移除异常的价格关系（如最高价小于最低价）
        if all(col in result.columns for col in ['adj_high', 'adj_low']):
            result = result[result['adj_high'] >= result['adj_low']]
        
        # 移除成交量为负数的记录
        if 'adj_vol' in result.columns:
            result = result[result['adj_vol'] >= 0]
        
        removed_count = original_len - len(result)
        if removed_count > 0:
            self.logger.info(f"数据验证完成，移除异常数据 {removed_count} 行")
        
        return result


class StockAdjustedPricePipeline(ProcessingPipeline):
    """股票后复权价格计算流水线"""
    
    def build_pipeline(self):
        """构建处理流水线"""
        # 阶段1: 复权因子计算
        self.add_stage(
            name="复权价格计算",
            operations=AdjustmentFactorOperation(config=self.config)
        )
        
        # 阶段2: 数据验证和清理
        self.add_stage(
            name="数据验证",
            operations=DataValidationOperation(config=self.config)
        )


@task_register()
class StockAdjustedPriceTaskV2(ProcessorTaskBase):
    """
    股票后复权价格计算任务 (重构版本)
    
    使用新的处理器架构实现股票后复权价格计算。
    """
    
    name = "stock_adjusted_price_v2"
    table_name = "stock_adjusted_daily_v2"
    description = "计算股票后复权价格 (重构版本)"
    
    # 源数据表
    source_tables = ["tushare_stock_daily", "tushare_stock_adj_factor"]
    
    # 主键和日期列
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # 计算方法标识
    calculation_method = "backward_adjustment"
    
    # 必需的输出列
    required_columns = ["ts_code", "trade_date", "adj_open", "adj_high", "adj_low", "adj_close", "adj_vol"]
    
    def create_pipeline(self) -> ProcessingPipeline:
        """创建处理流水线"""
        return StockAdjustedPricePipeline(
            name="StockAdjustedPricePipeline",
            config=self.get_pipeline_config()
        )
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """获取流水线配置"""
        config = super().get_pipeline_config()
        config.update({
            "continue_on_stage_error": False,  # 遇到错误时停止
            "collect_stats": True,             # 收集统计信息
        })
        return config
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取股票数据和复权因子数据
        
        这里应该实现具体的数据获取逻辑，从数据库中获取：
        1. 股票日线数据 (tushare_stock_daily)
        2. 复权因子数据 (tushare_stock_adj_factor)
        
        然后将两个数据集合并。
        """
        self.logger.info("获取股票日线数据和复权因子数据")
        
        # TODO: 实现具体的数据获取逻辑
        # 这里应该：
        # 1. 从 tushare_stock_daily 获取股票日线数据
        # 2. 从 tushare_stock_adj_factor 获取复权因子数据
        # 3. 按 ts_code 和 trade_date 合并数据
        
        # 示例数据结构（实际应该从数据库获取）
        sample_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20240101', '20240101'],
            'open': [10.0, 20.0],
            'high': [11.0, 21.0],
            'low': [9.5, 19.5],
            'close': [10.5, 20.5],
            'vol': [1000000, 2000000],
            'adj_factor': [1.0, 1.0]
        })
        
        return sample_data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存处理结果到数据库"""
        if data.empty:
            self.logger.warning("没有数据需要保存")
            return
        
        self.logger.info(f"保存后复权价格数据到 {self.table_name}，行数: {len(data)}")
        
        # TODO: 实现具体的数据保存逻辑
        # 这里应该将数据保存到 stock_adjusted_daily_v2 表
        
        # 验证数据完整性
        missing_cols = set(self.required_columns) - set(data.columns)
        if missing_cols:
            raise ValueError(f"保存数据缺少必要的列: {missing_cols}")
        
        # 数据类型转换和格式化
        result = data.copy()
        
        # 确保价格列的精度
        price_cols = ['adj_open', 'adj_high', 'adj_low', 'adj_close']
        for col in price_cols:
            if col in result.columns:
                result[col] = result[col].round(2)
        
        # 确保成交量为整数
        if 'adj_vol' in result.columns:
            result['adj_vol'] = result['adj_vol'].astype(int)
        
        self.logger.info(f"数据格式化完成，准备保存 {len(result)} 行数据")
        
        # 这里应该调用数据库管理器保存数据
        # await self.db.save_dataframe(result, self.table_name)
    
    def _calculate_from_multiple_sources(self, data: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """
        从多个数据源计算（兼容旧接口）
        
        这个方法保持与旧版本的兼容性。
        """
        # 如果传入的是字典格式的多表数据，需要先合并
        if isinstance(data, dict):
            # 合并股票数据和复权因子数据
            stock_data = data.get('tushare_stock_daily', pd.DataFrame())
            adj_factor_data = data.get('tushare_stock_adj_factor', pd.DataFrame())
            
            if stock_data.empty or adj_factor_data.empty:
                self.logger.warning("股票数据或复权因子数据为空")
                return pd.DataFrame()
            
            # 合并数据
            merged_data = pd.merge(
                stock_data, adj_factor_data,
                on=['ts_code', 'trade_date'],
                how='inner'
            )
            
            return merged_data
        
        return data
