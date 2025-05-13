import pandas as pd
import numpy as np
import logging
from typing import TYPE_CHECKING

# 避免循环导入，仅用于类型提示
if TYPE_CHECKING:
    from .tushare_task import TushareTask # type: ignore

class TushareDataTransformer:
    """负责 Tushare 数据的转换、处理和验证逻辑。"""

    def __init__(self, task_instance: 'TushareTask'):
        """初始化 Transformer。

        Args:
            task_instance: 关联的 TushareTask 实例，用于访问配置和日志记录器。
        """
        self.task = task_instance
        # 直接从 task_instance 获取 logger，避免重复创建
        self.logger = self.task.logger if hasattr(self.task, 'logger') else logging.getLogger(__name__)

    def _apply_column_mapping(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用列名映射
        
        将原始列名映射为目标列名，只处理数据中存在的列。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 应用列名映射后的数据
        """
        # 检查 self.task 是否具有 column_mapping 属性
        if not hasattr(self.task, 'column_mapping') or not self.task.column_mapping:
            return data
            
        # 检查映射前的列是否存在
        missing_original_cols = [orig_col for orig_col in self.task.column_mapping.keys() 
                                if orig_col not in data.columns]
        if missing_original_cols:
            self.logger.warning(f"列名映射失败：原始数据中缺少以下列: {missing_original_cols}")
        
        # 执行重命名，只重命名数据中存在的列
        rename_map = {k: v for k, v in self.task.column_mapping.items() if k in data.columns}
        if rename_map:
            data.rename(columns=rename_map, inplace=True)
            self.logger.info(f"已应用列名映射: {rename_map}")
            
        return data

    def _process_date_column(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理日期列
        
        将日期列转换为标准的日期时间格式，并移除无效日期的行。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 处理日期后的数据，如果日期列不存在则返回原始数据
        """
        # 检查 self.task 是否具有 date_column 属性
        if not hasattr(self.task, 'date_column') or not self.task.date_column or self.task.date_column not in data.columns:
            if hasattr(self.task, 'date_column') and self.task.date_column:
                self.logger.warning(f"指定的日期列 '{self.task.date_column}' 不在数据中，无法进行日期格式转换。")
            return data
            
        try:
            # 如果是字符串格式（如'20210101'），转换为日期对象
            data[self.task.date_column] = pd.to_datetime(data[self.task.date_column], format='%Y%m%d', errors='coerce')
            # 删除转换失败的行 (NaT)
            original_count = len(data)
            data.dropna(subset=[self.task.date_column], inplace=True)
            if len(data) < original_count:
                self.logger.warning(f"移除了 {original_count - len(data)} 行，因为日期列 '{self.task.date_column}' 格式无效。")
        except Exception as e:
            self.logger.warning(f"日期列 {self.task.date_column} 格式转换时发生错误: {str(e)}")
            
        return data

    def _sort_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """对数据进行排序
        
        根据日期列和主键列对数据进行排序。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 排序后的数据
        """
        # 构建排序键列表
        sort_keys = []
        if hasattr(self.task, 'date_column') and self.task.date_column and self.task.date_column in data.columns:
            sort_keys.append(self.task.date_column)

        if hasattr(self.task, 'primary_keys') and self.task.primary_keys:
            other_keys = [pk for pk in self.task.primary_keys if pk != getattr(self.task, 'date_column', None) and pk in data.columns]
            sort_keys.extend(other_keys)
            
        # 如果没有有效的排序键，则返回原始数据
        if not sort_keys:
            return data
            
        # 检查所有排序键是否都在数据中
        missing_keys = [key for key in sort_keys if key not in data.columns]
        if missing_keys:
            self.logger.warning(f"排序失败：数据中缺少以下排序键: {missing_keys}")
            # 从排序键列表中移除缺失的键
            sort_keys = [key for key in sort_keys if key not in missing_keys]
            if not sort_keys:
                return data
                
        try:
            # 执行排序
            data = data.sort_values(by=sort_keys)
            self.logger.info(f"数据已按以下键排序: {sort_keys}")
        except Exception as e:
            self.logger.warning(f"排序时发生错误: {str(e)}")
            
        return data

    def _apply_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用数据转换
        
        根据转换规则对指定列应用转换函数。
        增加了对None/NaN值的安全处理。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 应用转换后的数据
        """
        # 检查 self.task 是否具有 transformations 属性
        if not hasattr(self.task, 'transformations') or not self.task.transformations:
            return data
            
        for column, transform_func in self.task.transformations.items():
            if column in data.columns:
                try:
                    # 确保处理前列中没有Python原生的None，统一使用np.nan
                    if data[column].dtype == 'object':
                        data[column] = data[column].fillna(np.nan)
                    
                    # 定义一个安全的转换函数，处理np.nan值
                    def safe_transform(x):
                        if pd.isna(x):
                            return np.nan  # 保持np.nan
                        try:
                            return transform_func(x) # 应用原始转换
                        except Exception as e:
                            self.logger.warning(f"转换值 '{x}' (类型: {type(x)}) 到列 '{column}' 时失败: {str(e)}")
                            return np.nan # 转换失败时返回np.nan

                    # 应用安全转换
                    original_dtype = data[column].dtype
                    data[column] = data[column].apply(safe_transform)
                    
                    # 尝试恢复原始数据类型
                    try:
                        if data[column].dtype == 'object' and original_dtype != 'object':
                            data[column] = pd.to_numeric(data[column], errors='coerce')
                    except Exception as type_e:
                        self.logger.debug(f"尝试恢复列 '{column}' 类型失败: {str(type_e)}")
                        
                except Exception as e:
                    self.logger.error(f"处理列 '{column}' 的转换时发生意外错误: {str(e)}", exc_info=True)
                    
        return data
        
    async def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理从Tushare获取的数据
        
        包括列名映射、日期处理、数据排序和数据转换。
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要处理")
            return pd.DataFrame()
            
        # 1. 应用列名映射
        data = self._apply_column_mapping(data)
        
        # 2. 处理主要的 date_column (如果定义)
        data = self._process_date_column(data) 
        
        # 3. 应用通用数据类型转换 (from transformations dict)
        data = self._apply_transformations(data)

        # 4. 显式处理 schema 中定义的其他 DATE/TIMESTAMP 列
        # 检查 self.task 是否具有 schema 属性
        if hasattr(self.task, 'schema') and self.task.schema:
            date_columns_to_process = []
            # 识别需要处理的日期列
            for col_name, col_def in self.task.schema.items():
                col_type = col_def.get('type', '').upper() if isinstance(col_def, dict) else str(col_def).upper()
                if ('DATE' in col_type or 'TIMESTAMP' in col_type) and col_name in data.columns and col_name != getattr(self.task, 'date_column', None):
                    # 仅处理尚未是日期时间类型的列
                    if data[col_name].dtype == 'object' or pd.api.types.is_string_dtype(data[col_name]):
                        date_columns_to_process.append(col_name)
            
            # 批量处理识别出的日期列
            if date_columns_to_process:
                self.logger.info(f"转换以下列为日期时间格式 (YYYYMMDD): {', '.join(date_columns_to_process)}")
                original_count = len(data)
                for col_name in date_columns_to_process:
                    # 尝试使用 YYYYMMDD 格式转换
                    converted_col = pd.to_datetime(data[col_name], format='%Y%m%d', errors='coerce')
                    data[col_name] = converted_col
                
                if len(data) < original_count:
                    self.logger.warning(f"处理日期列: 移除了 {original_count - len(data)} 行 (注意：移除逻辑已修改)。")

        # 5. 对数据进行排序 (应该在所有转换后进行)
        data = self._sort_data(data)
        
        return data
        
    async def validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """验证从Tushare获取的数据
        
        不符合验证规则的数据会被过滤掉。
        
        Args:
            data (pd.DataFrame): 待验证的数据
            
        Returns:
            pd.DataFrame: 验证后的数据（已过滤掉不符合规则的数据）
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要验证")
            return data
            
        # 记录原始数据行数
        original_count = len(data)
        valid_mask = pd.Series(True, index=data.index)
        
        # 应用自定义验证规则
        # 检查 self.task 是否具有 validations 属性
        if hasattr(self.task, 'validations') and self.task.validations:
            for validation_func in self.task.validations:
                try:
                    # 获取每行数据的验证结果
                    validation_result = validation_func(data[valid_mask])
                    if isinstance(validation_result, pd.Series):
                        valid_mask &= validation_result
                    else:
                        if not validation_result:
                            self.logger.warning(f"整批数据未通过验证: {validation_func.__name__ if hasattr(validation_func, '__name__') else '未命名验证'}")
                            valid_mask &= False
                except Exception as e:
                    self.logger.warning(f"执行验证时发生错误: {str(e)}")
                    valid_mask &= False
        
        # 应用验证结果
        filtered_data = data[valid_mask].copy()
        filtered_count = len(filtered_data)
        
        if filtered_count < original_count:
            self.logger.warning(f"数据验证: 过滤掉 {original_count - filtered_count} 行不符合规则的数据")
            
        return filtered_data 