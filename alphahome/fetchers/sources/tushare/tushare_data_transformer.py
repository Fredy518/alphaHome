import logging
import inspect
from typing import TYPE_CHECKING, cast, Any

import numpy as np
import pandas as pd

# 避免循环导入，仅用于类型提示
if TYPE_CHECKING:
    from .tushare_task import TushareTask  # type: ignore


class TushareDataTransformer:
    """负责 Tushare 数据的转换、处理和验证逻辑。

    该类提供了基础的数据处理功能，包括列名映射、数据转换和验证等。
    它同时支持 Task 子类（如 TushareFinaIndicatorTask）重写的 process_data 方法，
    确保子类可以添加特定的数据处理逻辑，而不会被基础处理流程忽略。

    处理流程：
    1. 执行基础的数据转换操作
    2. 检测并调用 Task 子类可能重写的 process_data 方法
    3. 返回最终处理的数据

    注意：子类重写的 process_data 方法可以是同步方法或异步方法，
    系统会自动检测方法类型并适当调用。推荐使用同步方法，符合基类实现。
    """

    def __init__(self, task_instance: Any) -> None:
        """初始化 Transformer。

        Args:
            task_instance: 关联的 TushareTask 实例，用于访问配置和日志记录器。
        """
        self.task = task_instance
        # 直接从 task_instance 获取 logger，避免重复创建
        self.logger = (
            self.task.logger
            if hasattr(self.task, "logger")
            else logging.getLogger(__name__)
        )

    def _apply_column_mapping(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用列名映射

        将原始列名映射为目标列名，只处理数据中存在的列。

        Args:
            data (DataFrame): 原始数据

        Returns:
            DataFrame: 应用列名映射后的数据
        """
        # 检查 self.task 是否具有 column_mapping 属性
        if not hasattr(self.task, "column_mapping") or not self.task.column_mapping:
            return data

        # 检查映射前的列是否存在
        missing_original_cols = [
            orig_col
            for orig_col in self.task.column_mapping.keys()
            if orig_col not in data.columns
        ]
        if missing_original_cols:
            self.logger.warning(
                f"列名映射失败：原始数据中缺少以下列: {missing_original_cols}"
            )

        # 执行重命名，只重命名数据中存在的列
        rename_map = {
            k: v for k, v in self.task.column_mapping.items() if k in data.columns
        }
        if rename_map:
            data.rename(columns=rename_map, inplace=True)
            self.logger.info(f"已应用列名映射: {rename_map}")

        return data

    def _convert_date_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        根据 schema_def 自动转换日期列。
        """
        if not hasattr(self.task, "schema_def") or not self.task.schema_def:
            return data

        for col, definition in self.task.schema_def.items():
            col_type = definition.get("type", "").upper()
            if col in data.columns and (col_type.startswith("DATE") or col_type.startswith("TIMESTAMP")):
                if pd.api.types.is_datetime64_any_dtype(data[col]):
                    continue
                
                self.logger.debug(f"自动转换日期列: {col}")
                
                original_nan_count = data[col].isna().sum()
                
                # 替换空字符串为 NaT
                if data[col].dtype == 'object':
                    data[col] = data[col].replace('', None)

                converted_col = pd.to_datetime(data[col], errors='coerce')
                
                new_nan_count = converted_col.isna().sum()
                
                if new_nan_count > original_nan_count:
                    self.logger.warning(
                        f"列 '{col}' 在日期转换中有 {new_nan_count - original_nan_count} 个值无法解析，已转换为 NaT。"
                    )
                data[col] = converted_col
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
        if not hasattr(self.task, "transformations") or not self.task.transformations:
            return data

        for column, transform_func in self.task.transformations.items():
            if column in data.columns:
                try:
                    # 确保处理前列中没有Python原生的None，统一使用np.nan
                    if data[column].dtype == "object":
                        data[column] = data[column].fillna(np.nan)

                    # 定义一个安全的转换函数，处理np.nan值
                    def safe_transform(x):
                        if pd.isna(x):
                            return np.nan  # 保持np.nan
                        try:
                            return transform_func(x)  # 应用原始转换
                        except Exception as e:
                            self.logger.warning(
                                f"转换值 '{x}' (类型: {type(x)}) 到列 '{column}' 时失败: {str(e)}"
                            )
                            return np.nan  # 转换失败时返回np.nan

                    # 应用安全转换
                    original_dtype = data[column].dtype
                    data[column] = data[column].apply(safe_transform)

                    # 尝试恢复原始数据类型
                    try:
                        if (
                            data[column].dtype == "object"
                            and original_dtype != "object"
                        ):
                            data[column] = pd.to_numeric(data[column], errors="coerce")
                    except Exception as type_e:
                        self.logger.debug(
                            f"尝试恢复列 '{column}' 类型失败: {str(type_e)}"
                        )

                except Exception as e:
                    self.logger.error(
                        f"处理列 '{column}' 的转换时发生意外错误: {str(e)}",
                        exc_info=True,
                    )

        return data

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从Tushare获取的数据（在fetch阶段的初步处理）

        注意：在新的重构架构下，这个方法主要负责 fetch 阶段的初步数据处理，
        包括列名映射等。主要的数据转换和业务逻辑处理现在由 BaseTask.process_data
        和 TushareTask.process_data 负责，避免重复处理。

        Args:
            data: 从API获取的原始数据
            **kwargs: 额外参数

        Returns:
            pd.DataFrame: 初步处理后的数据
        """
        if data is None or data.empty:
            self.logger.info("process_data: 输入数据为空，跳过处理。")
            return pd.DataFrame()

        # 1. 应用列名映射
        data = self._apply_column_mapping(data)

        # 2. 自动转换日期列
        data = self._convert_date_columns(data)

        # 3. 应用在任务中定义的其他转换
        data = self._apply_transformations(data)

        self.logger.debug(f"Transformer初步处理完成，数据行数: {len(data)}")
        return data

    # 注意：验证功能已统一到 BaseTask._validate_data 方法中
    # 不再在 Transformer 阶段进行验证，避免重复验证和架构混乱
    # 如果需要过滤式验证，可以在任务中设置 validation_mode = "filter"
