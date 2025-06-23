import logging
from typing import TYPE_CHECKING, cast

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

    def __init__(self, task_instance: "TushareTask") -> None:
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

    async def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理从Tushare获取的数据

        包括列名映射和数据转换。
        """
        if data is None or data.empty:
            self.logger.info("process_data: 输入数据为空，跳过处理。")
            return pd.DataFrame()

        # 1. 应用列名映射
        data = self._apply_column_mapping(data)

        # 2. 应用通用数据类型转换 (from transformations dict)
        data = self._apply_transformations(data)

        # 3. 调用子类可能重写的 process_data 方法进行额外处理
        # if hasattr(self.task, "_task_specific_process_data") and callable(
        #     self.task._task_specific_process_data
        # ):
        #     self.logger.debug("调用Task子类特定的数据处理方法...")
        #     # 检查是否为异步方法
        #     import inspect
        #     if inspect.iscoroutinefunction(self.task._task_specific_process_data):
        #         data = await self.task._task_specific_process_data(data)
        #     else:
        #         data = self.task._task_specific_process_data(data)

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
        if hasattr(self.task, "validations") and self.task.validations:
            for validation_func in self.task.validations:
                try:
                    # 获取每行数据的验证结果
                    validation_result = validation_func(data[valid_mask])
                    if isinstance(validation_result, pd.Series):
                        valid_mask &= validation_result
                    else:
                        if not validation_result:
                            self.logger.warning(
                                f"整批数据未通过验证: {validation_func.__name__ if hasattr(validation_func, '__name__') else '未命名验证'}"
                            )
                            valid_mask &= False
                except Exception as e:
                    self.logger.warning(f"执行验证时发生错误: {str(e)}")
                    valid_mask &= False

        # 应用验证结果
        filtered_data = data[valid_mask].copy()
        filtered_count = len(filtered_data)

        if filtered_count < original_count:
            self.logger.warning(
                f"数据验证: 过滤掉 {original_count - filtered_count} 行不符合规则的数据"
            )

        return cast(pd.DataFrame, filtered_data)
