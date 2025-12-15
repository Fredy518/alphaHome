"""
物化视图数据质量检查器

实现最小的数据质量检查机制，包括：
- 缺失值检查
- 异常值检查
- 行数变化检查
- 重复值检查
- 类型检查

数据质量问题被暴露而不是掩盖，用户需要手动复查。
"""

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


class MaterializedViewValidator:
    """
    物化视图数据质量检查器
    
    职责：
    1. 检查缺失值
    2. 检查异常值
    3. 检查行数变化
    4. 检查重复值
    5. 检查列类型
    
    所有检查都是非破坏性的，不会修改或掩盖数据。
    """
    
    def __init__(self, logger=None):
        """初始化检查器"""
        self.logger = logger or logging.getLogger(__name__)
        self.previous_row_count: Optional[int] = None
    
    async def validate_null_values(
        self,
        data: pd.DataFrame,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        检查缺失值
        
        参数：
        - data: 要检查的数据
        - config: 检查配置，包含：
            - columns: 要检查的列列表
            - threshold: 允许的缺失值比例（0-1）
        
        返回：
        {
            'check_name': 'null_check',
            'status': 'pass' | 'warning' | 'error',
            'message': str,
            'details': {
                'column': str,
                'null_count': int,
                'null_percentage': float,
                'threshold': float
            }
        }
        """
        check_name = 'null_check'
        columns = config.get('columns', [])
        threshold = config.get('threshold', 0.0)
        
        if not columns:
            return {
                'check_name': check_name,
                'status': 'pass',
                'message': 'No columns specified for null check',
                'details': {}
            }
        
        # 检查每一列
        issues = []
        for col in columns:
            if col not in data.columns:
                self.logger.warning(f"Column {col} not found in data")
                continue
            
            null_count = data[col].isna().sum()
            null_percentage = null_count / len(data) if len(data) > 0 else 0
            
            if null_percentage > threshold:
                issues.append({
                    'column': col,
                    'null_count': int(null_count),
                    'null_percentage': float(null_percentage),
                    'threshold': threshold
                })
        
        if issues:
            status = 'warning'
            message = f"Null values detected in {len(issues)} column(s) exceeding threshold"
            details = {'columns_with_issues': issues}
        else:
            status = 'pass'
            message = f"All {len(columns)} columns passed null check"
            details = {}
        
        return {
            'check_name': check_name,
            'status': status,
            'message': message,
            'details': details
        }
    
    async def validate_outliers(
        self,
        data: pd.DataFrame,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        检查异常值
        
        参数：
        - data: 要检查的数据
        - config: 检查配置，包含：
            - columns: 要检查的列列表
            - method: 检查方法（'iqr' | 'zscore' | 'percentile'）
            - threshold: 阈值（IQR 倍数、Z-score 标准差、百分位数）
        
        返回：
        {
            'check_name': 'outlier_check',
            'status': 'pass' | 'warning' | 'error',
            'message': str,
            'details': {
                'column': str,
                'method': str,
                'outlier_count': int,
                'outlier_percentage': float
            }
        }
        """
        check_name = 'outlier_check'
        columns = config.get('columns', [])
        method = config.get('method', 'iqr')
        threshold = config.get('threshold', 3.0)
        
        if not columns:
            return {
                'check_name': check_name,
                'status': 'pass',
                'message': 'No columns specified for outlier check',
                'details': {}
            }
        
        # 检查每一列
        issues = []
        for col in columns:
            if col not in data.columns:
                self.logger.warning(f"Column {col} not found in data")
                continue
            
            # 只检查数值列
            if not pd.api.types.is_numeric_dtype(data[col]):
                self.logger.warning(f"Column {col} is not numeric, skipping outlier check")
                continue
            
            # 移除 NaN 值
            col_data = data[col].dropna()
            if len(col_data) == 0:
                continue
            
            # 根据方法检查异常值
            if method == 'iqr':
                outlier_mask = self._detect_outliers_iqr(col_data, threshold)
            elif method == 'zscore':
                outlier_mask = self._detect_outliers_zscore(col_data, threshold)
            elif method == 'percentile':
                outlier_mask = self._detect_outliers_percentile(col_data, threshold)
            else:
                self.logger.warning(f"Unknown outlier detection method: {method}")
                continue
            
            outlier_count = outlier_mask.sum()
            outlier_percentage = outlier_count / len(col_data) if len(col_data) > 0 else 0
            
            if outlier_count > 0:
                issues.append({
                    'column': col,
                    'method': method,
                    'outlier_count': int(outlier_count),
                    'outlier_percentage': float(outlier_percentage),
                    'threshold': threshold
                })
        
        if issues:
            status = 'warning'
            message = f"Outliers detected in {len(issues)} column(s)"
            details = {'columns_with_issues': issues}
        else:
            status = 'pass'
            message = f"All {len(columns)} columns passed outlier check"
            details = {}
        
        return {
            'check_name': check_name,
            'status': status,
            'message': message,
            'details': details
        }
    
    async def validate_row_count_change(
        self,
        data: pd.DataFrame,
        config: Dict[str, Any],
        previous_row_count: Optional[int] = None,
        current_row_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        检查行数变化
        
        参数：
        - data: 要检查的数据
        - config: 检查配置，包含：
            - threshold: 允许的行数变化比例（0-1）
        - previous_row_count: 上次刷新的行数（可选）
        
        返回：
        {
            'check_name': 'row_count_change',
            'status': 'pass' | 'warning' | 'error',
            'message': str,
            'details': {
                'current_row_count': int,
                'previous_row_count': int,
                'change_percentage': float,
                'threshold': float
            }
        }
        """
        check_name = 'row_count_change'
        threshold = config.get('threshold', 0.5)
        current_row_count = int(current_row_count) if current_row_count is not None else len(data)
        
        # 如果没有提供上次行数，使用实例变量
        if previous_row_count is None:
            previous_row_count = self.previous_row_count
        
        # 如果没有上次行数，无法检查
        if previous_row_count is None:
            self.previous_row_count = current_row_count
            return {
                'check_name': check_name,
                'status': 'pass',
                'message': 'No previous row count available for comparison',
                'details': {
                    'current_row_count': current_row_count,
                    'previous_row_count': None,
                    'change_percentage': None,
                    'threshold': threshold
                }
            }
        
        # 计算行数变化比例
        if previous_row_count == 0:
            change_percentage = 1.0 if current_row_count > 0 else 0.0
        else:
            change_percentage = abs(current_row_count - previous_row_count) / previous_row_count
        
        # 更新实例变量
        self.previous_row_count = current_row_count
        
        if change_percentage > threshold:
            status = 'warning'
            message = f"Row count changed by {change_percentage:.1%}, exceeding threshold {threshold:.1%}"
            details = {
                'current_row_count': current_row_count,
                'previous_row_count': previous_row_count,
                'change_percentage': float(change_percentage),
                'threshold': threshold
            }
        else:
            status = 'pass'
            message = f"Row count change {change_percentage:.1%} within threshold"
            details = {
                'current_row_count': current_row_count,
                'previous_row_count': previous_row_count,
                'change_percentage': float(change_percentage),
                'threshold': threshold
            }
        
        return {
            'check_name': check_name,
            'status': status,
            'message': message,
            'details': details
        }
    
    async def validate_duplicates(
        self,
        data: pd.DataFrame,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        检查重复值
        
        参数：
        - data: 要检查的数据
        - config: 检查配置，包含：
            - columns: 主键列列表
        
        返回：
        {
            'check_name': 'duplicate_check',
            'status': 'pass' | 'warning' | 'error',
            'message': str,
            'details': {
                'duplicate_count': int,
                'duplicate_percentage': float
            }
        }
        """
        check_name = 'duplicate_check'
        columns = config.get('columns', [])
        
        if not columns:
            return {
                'check_name': check_name,
                'status': 'pass',
                'message': 'No columns specified for duplicate check',
                'details': {}
            }
        
        # 检查指定列中是否存在重复值
        missing_columns = [col for col in columns if col not in data.columns]
        if missing_columns:
            self.logger.warning(f"Columns not found in data: {missing_columns}")
            return {
                'check_name': check_name,
                'status': 'error',
                'message': f"Columns not found: {missing_columns}",
                'details': {}
            }
        
        # 检查重复行
        duplicate_mask = data.duplicated(subset=columns, keep=False)
        duplicate_count = duplicate_mask.sum()
        duplicate_percentage = duplicate_count / len(data) if len(data) > 0 else 0
        
        if duplicate_count > 0:
            status = 'error'
            message = f"Found {duplicate_count} duplicate rows based on {columns}"
            details = {
                'duplicate_count': int(duplicate_count),
                'duplicate_percentage': float(duplicate_percentage),
                'key_columns': columns
            }
        else:
            status = 'pass'
            message = f"No duplicates found based on {columns}"
            details = {
                'duplicate_count': 0,
                'duplicate_percentage': 0.0,
                'key_columns': columns
            }
        
        return {
            'check_name': check_name,
            'status': status,
            'message': message,
            'details': details
        }
    
    async def validate_types(
        self,
        data: pd.DataFrame,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        检查列类型
        
        参数：
        - data: 要检查的数据
        - config: 检查配置，包含：
            - columns: 列名到期望类型的映射
              例如：{'col1': 'int64', 'col2': 'float64', 'col3': 'object'}
        
        返回：
        {
            'check_name': 'type_check',
            'status': 'pass' | 'warning' | 'error',
            'message': str,
            'details': {
                'column': str,
                'expected_type': str,
                'actual_type': str
            }
        }
        """
        check_name = 'type_check'
        columns = config.get('columns', {})
        
        if not columns:
            return {
                'check_name': check_name,
                'status': 'pass',
                'message': 'No columns specified for type check',
                'details': {}
            }
        
        # 检查每一列的类型
        issues = []
        for col, expected_type in columns.items():
            if col not in data.columns:
                issues.append({
                    'column': col,
                    'expected_type': expected_type,
                    'actual_type': 'MISSING',
                    'status': 'error'
                })
                continue
            
            actual_type = str(data[col].dtype)
            
            # 检查类型是否匹配
            if actual_type != expected_type:
                issues.append({
                    'column': col,
                    'expected_type': expected_type,
                    'actual_type': actual_type,
                    'status': 'error'
                })
        
        if issues:
            status = 'error'
            message = f"Type mismatch in {len(issues)} column(s)"
            details = {'columns_with_issues': issues}
        else:
            status = 'pass'
            message = f"All {len(columns)} columns have correct types"
            details = {}
        
        return {
            'check_name': check_name,
            'status': status,
            'message': message,
            'details': details
        }
    
    # =========================================================================
    # 辅助方法
    # =========================================================================
    
    @staticmethod
    def _detect_outliers_iqr(data: pd.Series, threshold: float = 3.0) -> pd.Series:
        """
        使用四分位数法检测异常值
        
        参数：
        - data: 数据序列
        - threshold: IQR 倍数（默认 3.0）
        
        返回：
        布尔序列，True 表示异常值
        """
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        
        return (data < lower_bound) | (data > upper_bound)
    
    @staticmethod
    def _detect_outliers_zscore(data: pd.Series, threshold: float = 3.0) -> pd.Series:
        """
        使用 Z-score 法检测异常值
        
        参数：
        - data: 数据序列
        - threshold: 标准差倍数（默认 3.0）
        
        返回：
        布尔序列，True 表示异常值
        """
        mean = data.mean()
        std = data.std()
        
        if std == 0:
            return pd.Series([False] * len(data), index=data.index)
        
        z_scores = np.abs((data - mean) / std)
        return z_scores > threshold
    
    @staticmethod
    def _detect_outliers_percentile(data: pd.Series, threshold: float = 0.05) -> pd.Series:
        """
        使用百分位数法检测异常值
        
        参数：
        - data: 数据序列
        - threshold: 百分位数（0-1，默认 0.05 表示 5% 和 95%）
        
        返回：
        布尔序列，True 表示异常值
        """
        lower_percentile = data.quantile(threshold)
        upper_percentile = data.quantile(1 - threshold)
        
        return (data < lower_percentile) | (data > upper_percentile)
