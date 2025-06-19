#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票后复权价格计算任务

从不复权行情数据和复权因子计算后复权的开高低收价格。
这是processors模块的第一个具体实现示例。
"""

import pandas as pd
import numpy as np
from ..processor_task import ProcessorTask
from ...common.task_system import task_register
from typing import Optional


@task_register()
class StockAdjustedPriceTask(ProcessorTask):
    """
    股票后复权价格计算任务 (非分块处理器示例)

    计算公式：后复权价格 = 原始价格 * 累积复权因子
    """
    
    name = "stock_adjusted_price"
    table_name = "stock_adjusted_daily"
    description = "计算股票后复权价格"
    
    # 源数据表
    source_tables = ["tushare_stock_daily", "tushare_stock_adj_factor"]
    
    # 主键和日期列
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # 计算方法标识
    calculation_method = "backward_adjustment"
    
    # 必需的输出列
    required_columns = ["ts_code", "trade_date", "adj_open", "adj_high", "adj_low", "adj_close", "adj_vol"]
    
    # 数据库表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "VARCHAR(8)", "constraints": "NOT NULL"},
        "adj_open": {"type": "DECIMAL(10,2)"},
        "adj_high": {"type": "DECIMAL(10,2)"},
        "adj_low": {"type": "DECIMAL(10,2)"},
        "adj_close": {"type": "DECIMAL(10,2)", "constraints": "NOT NULL"},
        "adj_vol": {"type": "DECIMAL(20,2)"},
        "adj_amount": {"type": "DECIMAL(20,2)"},
        "original_close": {"type": "DECIMAL(10,2)"},  # 保留原始收盘价以便对比
        "adj_factor": {"type": "DECIMAL(15,8)"},      # 保留复权因子
    }

    # 标记为非分块任务
    is_block_processor = False

    async def process(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        非分块任务的核心处理逻辑。
        它将一次性获取所有需要的数据，并进行计算。
        """
        self.logger.info(f"任务 '{self.name}' 开始执行 (非分块模式)...")
        
        # 1. 获取数据
        # _fetch_multiple_tables 方法现在是 process 逻辑的一部分
        data = await self._fetch_multiple_tables(**kwargs)
        if data.empty:
            self.logger.warning("未能获取到任何数据，任务终止。")
            return None

        # 2. 计算数据
        # _calculate_from_multiple_sources 方法现在也是 process 逻辑的一部分
        processed_data = self._calculate_from_multiple_sources(data, **kwargs)

        # 3. (可选) 保存数据
        # 在实际应用中，这里可以调用 self.db.save_dataframe(processed_data, self.table_name)
        if not processed_data.empty:
            self.logger.info(f"计算完成，生成 {len(processed_data)} 条后复权数据。")
            # 示例：此处可以添加保存逻辑
        else:
            self.logger.info("计算完成，但没有生成任何数据。")

        self.logger.info(f"任务 '{self.name}' 执行完毕。")
        return processed_data

    async def _fetch_multiple_tables(self, **kwargs):
        """辅助方法：实现行情数据和复权因子的联合查询"""
        
        # 构建联合查询SQL，一次性获取所需数据
        query = """
        SELECT 
            d.ts_code,
            d.trade_date,
            d.open,
            d.high,
            d.low,
            d.close,
            d.vol,
            d.amount,
            COALESCE(a.adj_factor, 1.0) as adj_factor
        FROM tushare_stock_daily d
        LEFT JOIN tushare_stock_adj_factor a 
        ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date
        """
        
        conditions = []
        params = []
        
        # 添加日期范围条件
        if 'start_date' in kwargs and 'end_date' in kwargs:
            conditions.append("d.trade_date >= %s AND d.trade_date <= %s")
            params.extend([kwargs['start_date'], kwargs['end_date']])
        
        # 添加股票代码条件
        if 'ts_code' in kwargs:
            conditions.append("d.ts_code = %s")
            params.append(kwargs['ts_code'])
        
        # 组装完整查询
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY d.ts_code, d.trade_date"
        
        self.logger.info(f"执行联合查询获取行情和复权因子数据")
        self.logger.debug(f"查询SQL: {query}")
        
        try:
            result = await self.db.fetch_dataframe(query, params)
            self.logger.info(f"联合查询获取 {len(result)} 行数据")
            return result
        except Exception as e:
            self.logger.error(f"联合查询失败: {str(e)}")
            raise

    def _calculate_from_multiple_sources(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """辅助方法：计算后复权价格"""
        if data.empty:
            self.logger.warning("输入数据为空")
            return pd.DataFrame()
        
        self.logger.info(f"开始计算后复权价格，共 {len(data)} 行数据")
        
        # 复制数据以避免修改原始数据
        result = data.copy()
        
        # 填充缺失的复权因子为1.0（表示不需要复权）
        result['adj_factor'] = result['adj_factor'].fillna(1.0)
        
        # 按股票分组计算累积复权因子
        self.logger.info("按股票计算累积复权因子")
        result = result.sort_values(['ts_code', 'trade_date'])
        
        # 计算累积复权因子（从最早日期到当前日期的累积）
        result['cumulative_adj_factor'] = result.groupby('ts_code')['adj_factor'].cumprod()
        
        # 计算后复权价格
        self.logger.info("计算后复权价格")
        
        # 处理价格数据
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            # 检查列是否存在且不全为空
            if col in result.columns and not result[col].isna().all():
                result[f'adj_{col}'] = result[col] * result['cumulative_adj_factor']
            else:
                self.logger.warning(f"列 {col} 不存在或全为空值")
                result[f'adj_{col}'] = np.nan
        
        # 处理成交量（成交量需要除以复权因子）
        if 'vol' in result.columns and not result['vol'].isna().all():
            # 成交量的复权计算：原成交量 / 累积复权因子
            result['adj_vol'] = result['vol'] / result['cumulative_adj_factor']
        else:
            result['adj_vol'] = np.nan
        
        # 处理成交额（通常不需要复权，但这里提供选项）
        if 'amount' in result.columns:
            result['adj_amount'] = result['amount']  # 成交额通常不需要复权
        else:
            result['adj_amount'] = np.nan
        
        # 保留原始收盘价和复权因子用于验证
        result['original_close'] = result['close']
        result['adj_factor'] = result['cumulative_adj_factor']
        
        # 选择最终输出的列
        output_columns = [
            'ts_code', 'trade_date',
            'adj_open', 'adj_high', 'adj_low', 'adj_close',
            'adj_vol', 'adj_amount',
            'original_close', 'adj_factor'
        ]
        
        # 确保所有需要的列都存在
        for col in output_columns:
            if col not in result.columns:
                result[col] = np.nan
        
        result = result[output_columns]
        
        # 数据质量检查
        valid_records = result.dropna(subset=['adj_close']).shape[0]
        self.logger.info(f"后复权价格计算完成，有效记录 {valid_records}/{len(result)} 条")
        
        # 记录一些统计信息
        if valid_records > 0:
            avg_adj_factor = result['adj_factor'].mean()
            self.logger.info(f"平均复权因子: {avg_adj_factor:.4f}")
            
            if 'original_close' in result.columns and not result['original_close'].isna().all():
                price_change_ratio = (result['adj_close'] / result['original_close']).mean()
                self.logger.info(f"平均价格调整比例: {price_change_ratio:.4f}")
        
        return result

    def validate_data(self, data):
        """验证后复权价格计算结果"""
        # 调用基类验证
        base_valid = super().validate_data(data)
        
        if not isinstance(data, pd.DataFrame) or data.empty:
            return base_valid
        
        # 检查关键价格列
        price_columns = ['adj_open', 'adj_high', 'adj_low', 'adj_close']
        for col in price_columns:
            if col in data.columns:
                # 检查价格是否为正值
                negative_prices = (data[col] < 0).sum()
                if negative_prices > 0:
                    self.logger.error(f"发现 {negative_prices} 条负价格记录 (列: {col})")
                    return False
                
                # 检查价格是否合理（不超过极值）
                extreme_prices = (data[col] > 10000).sum()  # 假设股价不会超过1万元
                if extreme_prices > 0:
                    self.logger.warning(f"发现 {extreme_prices} 条异常高价格记录 (列: {col})")
        
        # 检查价格关系的合理性：最高价 >= 最低价
        if 'adj_high' in data.columns and 'adj_low' in data.columns:
            invalid_range = (data['adj_high'] < data['adj_low']).sum()
            if invalid_range > 0:
                self.logger.error(f"发现 {invalid_range} 条高低价关系异常的记录")
                return False
        
        # 检查复权因子的合理性
        if 'adj_factor' in data.columns:
            invalid_factors = ((data['adj_factor'] <= 0) | (data['adj_factor'] > 100)).sum()
            if invalid_factors > 0:
                self.logger.warning(f"发现 {invalid_factors} 条异常复权因子记录")
        
        self.logger.info("后复权价格数据验证通过")
        return base_valid 