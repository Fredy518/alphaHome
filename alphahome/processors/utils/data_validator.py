#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据验证工具

用于验证和清洁数据的工具类，包括类型检查、空值检查、异常值检查等。
"""

from typing import Dict, List, Any, Optional, Union, Callable
import pandas as pd
import numpy as np

from ...common.logging_utils import get_logger

class DataValidator:
    """数据验证工具
    
    用于验证和清洁数据框的工具类，提供多种验证和转换方法。
    """
    
    def __init__(self):
        """初始化数据验证器
        """
        self.logger = get_logger("DataValidator")
        
    def validate(self, df: pd.DataFrame, schema: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """根据schema验证数据框
        
        schema格式:
        {
            'column_name': {
                'type': str/int/float/bool/date,
                'required': True/False,
                'min': 最小值,
                'max': 最大值,
                'choices': [可选值列表],
                'default': 默认值,
                'transform': 转换函数
            }
        }
        
        Args:
            df: 要验证的数据框
            schema: 列定义字典
            
        Returns:
            pd.DataFrame: 验证并转换后的数据框
        """
        if df is None or df.empty:
            self.logger.warning("输入数据为空")
            return pd.DataFrame()
            
        # 创建结果DataFrame的副本
        result = df.copy()
        
        for column, rules in schema.items():
            # 检查列是否存在
            if column not in result.columns:
                if rules.get('required', False):
                    self.logger.error(f"必需的列 '{column}' 不存在")
                    raise ValueError(f"必需的列 '{column}' 不存在")
                elif 'default' in rules:
                    # 添加默认值列
                    result[column] = rules['default']
                else:
                    # 非必需且无默认值，跳过
                    continue
                    
            # 处理缺失值
            if 'default' in rules:
                result[column] = result[column].fillna(rules['default'])
                
            # 应用转换函数
            if 'transform' in rules and callable(rules['transform']):
                try:
                    result[column] = result[column].apply(rules['transform'])
                except Exception as e:
                    self.logger.error(f"应用转换函数到列 '{column}' 时出错: {str(e)}")
                    
            # 类型转换
            if 'type' in rules:
                try:
                    if rules['type'] == str:
                        result[column] = result[column].astype(str)
                    elif rules['type'] == int:
                        result[column] = pd.to_numeric(result[column], errors='coerce').astype('Int64')
                    elif rules['type'] == float:
                        result[column] = pd.to_numeric(result[column], errors='coerce')
                    elif rules['type'] == bool:
                        result[column] = result[column].astype(bool)
                    elif rules['type'] == 'date':
                        result[column] = pd.to_datetime(result[column], errors='coerce')
                except Exception as e:
                    self.logger.error(f"转换列 '{column}' 到类型 {rules['type']} 时出错: {str(e)}")
                    
            # 验证值范围
            if 'min' in rules:
                mask = result[column] < rules['min']
                if mask.any():
                    count = mask.sum()
                    self.logger.warning(f"列 '{column}' 有 {count} 个值小于最小值 {rules['min']}")
                    if rules.get('clip', False):
                        result.loc[mask, column] = rules['min']
                        
            if 'max' in rules:
                mask = result[column] > rules['max']
                if mask.any():
                    count = mask.sum()
                    self.logger.warning(f"列 '{column}' 有 {count} 个值大于最大值 {rules['max']}")
                    if rules.get('clip', False):
                        result.loc[mask, column] = rules['max']
                        
            # 验证可选值
            if 'choices' in rules:
                mask = ~result[column].isin(rules['choices'])
                if mask.any():
                    count = mask.sum()
                    self.logger.warning(f"列 '{column}' 有 {count} 个值不在可选值列表中")
                    if 'default' in rules:
                        result.loc[mask, column] = rules['default']
                        
        return result
        
    def validate_numeric(self, df: pd.DataFrame, columns: List[str], 
                         allow_negative: bool = True, 
                         handle_missing: str = 'ignore') -> pd.DataFrame:
        """验证数值列
        
        Args:
            df: 数据框
            columns: 要验证的列名列表
            allow_negative: 是否允许负值
            handle_missing: 处理缺失值的方式: 'ignore', 'drop', 'zero'
            
        Returns:
            pd.DataFrame: 验证后的数据框
        """
        if df is None or df.empty:
            return pd.DataFrame()
            
        result = df.copy()
        
        for column in columns:
            if column not in result.columns:
                self.logger.warning(f"列 '{column}' 不存在")
                continue
                
            # 转换为数值类型
            result[column] = pd.to_numeric(result[column], errors='coerce')
            
            # 处理负值
            if not allow_negative:
                mask = result[column] < 0
                if mask.any():
                    count = mask.sum()
                    self.logger.warning(f"列 '{column}' 有 {count} 个负值")
                    result.loc[mask, column] = np.nan
                    
            # 处理缺失值
            if handle_missing == 'drop':
                result = result.dropna(subset=[column])
            elif handle_missing == 'zero':
                result[column] = result[column].fillna(0)
                
        return result
        
    def detect_outliers(self, df: pd.DataFrame, columns: List[str], 
                        method: str = 'zscore', 
                        threshold: float = 3.0,
                        handle_outliers: str = 'flag') -> pd.DataFrame:
        """检测异常值
        
        Args:
            df: 数据框
            columns: 要检测的列名列表
            method: 检测方法: 'zscore', 'iqr'
            threshold: 阈值 (zscore方法下的标准差倍数，或iqr方法下的IQR倍数)
            handle_outliers: 处理方式: 'flag', 'remove', 'clip'
            
        Returns:
            pd.DataFrame: 处理后的数据框
        """
        if df is None or df.empty:
            return pd.DataFrame()
            
        result = df.copy()
        
        # 添加异常值标记列
        if handle_outliers == 'flag':
            result['outlier_flags'] = pd.Series([{} for _ in range(len(result))], index=result.index)
            
        for column in columns:
            if column not in result.columns:
                self.logger.warning(f"列 '{column}' 不存在")
                continue
                
            # 转换为数值类型
            series = pd.to_numeric(result[column], errors='coerce')
            
            # 检测异常值
            outlier_mask = None
            
            if method == 'zscore':
                z_scores = (series - series.mean()) / series.std()
                outlier_mask = abs(z_scores) > threshold
            elif method == 'iqr':
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - threshold * iqr
                upper_bound = q3 + threshold * iqr
                outlier_mask = (series < lower_bound) | (series > upper_bound)
                
            if outlier_mask is not None and outlier_mask.any():
                count = outlier_mask.sum()
                self.logger.info(f"列 '{column}' 使用 {method} 方法检测到 {count} 个异常值")
                
                # 处理异常值
                if handle_outliers == 'flag':
                    for idx in result.index[outlier_mask]:
                        result.at[idx, 'outlier_flags'][column] = True
                elif handle_outliers == 'remove':
                    result = result[~outlier_mask]
                elif handle_outliers == 'clip':
                    if method == 'zscore':
                        mean = series.mean()
                        std = series.std()
                        result.loc[series > mean + threshold * std, column] = mean + threshold * std
                        result.loc[series < mean - threshold * std, column] = mean - threshold * std
                    elif method == 'iqr':
                        result.loc[series > upper_bound, column] = upper_bound
                        result.loc[series < lower_bound, column] = lower_bound
                        
        return result
        
    def validate_date_columns(self, df: pd.DataFrame, 
                             date_columns: List[str], 
                             min_date: Optional[str] = None,
                             max_date: Optional[str] = None) -> pd.DataFrame:
        """验证日期列
        
        Args:
            df: 数据框
            date_columns: 日期列名列表
            min_date: 最小日期字符串 (YYYY-MM-DD)
            max_date: 最大日期字符串 (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: 验证后的数据框
        """
        if df is None or df.empty:
            return pd.DataFrame()
            
        result = df.copy()
        
        min_date_pd = pd.to_datetime(min_date) if min_date else None
        max_date_pd = pd.to_datetime(max_date) if max_date else None
        
        for column in date_columns:
            if column not in result.columns:
                self.logger.warning(f"列 '{column}' 不存在")
                continue
                
            # 转换为日期类型
            result[column] = pd.to_datetime(result[column], errors='coerce')
            
            # 检查空值
            null_mask = result[column].isna()
            if null_mask.any():
                count = null_mask.sum()
                self.logger.warning(f"日期列 '{column}' 有 {count} 个空值")
                
            # 检查日期范围
            if min_date_pd is not None:
                before_min_mask = result[column] < min_date_pd
                if before_min_mask.any():
                    count = before_min_mask.sum()
                    self.logger.warning(f"日期列 '{column}' 有 {count} 个值早于 {min_date}")
                    
            if max_date_pd is not None:
                after_max_mask = result[column] > max_date_pd
                if after_max_mask.any():
                    count = after_max_mask.sum()
                    self.logger.warning(f"日期列 '{column}' 有 {count} 个值晚于 {max_date}")
                    
        return result 