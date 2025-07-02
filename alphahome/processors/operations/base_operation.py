#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理操作基类

定义了数据处理操作的基础接口，所有具体操作都应继承此类。
"""

import abc
from typing import Any, Dict, Optional, List, Callable
from datetime import datetime

import pandas as pd

from ...common.logging_utils import get_logger


class Operation(abc.ABC):
    """数据处理操作基类

    所有数据处理操作的抽象基类，定义了统一的接口。

    示例:
    ```python
    class MyOperation(Operation):
        def __init__(self, param1=1, param2="value"):
            super().__init__(name="MyOperation")
            self.param1 = param1
            self.param2 = param2

        async def apply(self, data):
            # 实现具体的数据处理逻辑
            result = data.copy()
            # ... 数据处理操作
            return result
    ```
    """

    def __init__(
        self, name: Optional[str] = None, config: Optional[Dict[str, Any]] = None
    ):
        """初始化操作

        Args:
            name: 操作名称，默认为类名
            config: 配置参数
        """
        self.name = name or self.__class__.__name__
        self.config = config or {}
        self.logger = get_logger(f"operation.{self.name}")

        # 操作统计信息
        self._execution_count = 0
        self._last_execution_time = None
        self._total_processing_time = 0.0

    @abc.abstractmethod
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用操作到数据

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 处理后的数据框
        """
        raise NotImplementedError("子类必须实现apply方法")

    async def execute(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        执行操作的完整流程，包含统计和错误处理

        Args:
            data: 输入数据

        Returns:
            包含结果和元数据的字典
        """
        start_time = datetime.now()

        try:
            # 验证输入
            self._validate_input(data)

            # 执行操作
            result = await self.apply(data)

            # 验证输出
            self._validate_output(result)

            # 更新统计信息
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            self._execution_count += 1
            self._last_execution_time = end_time
            self._total_processing_time += processing_time

            self.logger.debug(
                f"操作 {self.name} 执行完成，"
                f"处理时间: {processing_time:.3f}秒，"
                f"输入行数: {len(data)}，"
                f"输出行数: {len(result)}"
            )

            return {
                "status": "success",
                "data": result,
                "metadata": {
                    "operation_name": self.name,
                    "processing_time": processing_time,
                    "input_rows": len(data),
                    "output_rows": len(result),
                    "execution_count": self._execution_count,
                    "timestamp": end_time
                }
            }

        except Exception as e:
            self.logger.error(f"操作 {self.name} 执行失败: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "metadata": {
                    "operation_name": self.name,
                    "timestamp": datetime.now()
                }
            }

    def _validate_input(self, data: pd.DataFrame):
        """验证输入数据"""
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"输入数据必须是pandas DataFrame，实际类型: {type(data)}")

        if data.empty:
            self.logger.warning(f"操作 {self.name} 接收到空数据")

    def _validate_output(self, data: pd.DataFrame):
        """验证输出数据"""
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"输出数据必须是pandas DataFrame，实际类型: {type(data)}")

    def get_stats(self) -> Dict[str, Any]:
        """获取操作统计信息"""
        return {
            "name": self.name,
            "execution_count": self._execution_count,
            "last_execution_time": self._last_execution_time,
            "total_processing_time": self._total_processing_time,
            "average_processing_time": (
                self._total_processing_time / self._execution_count
                if self._execution_count > 0 else 0
            )
        }

    def __str__(self) -> str:
        """返回操作的字符串表示"""
        return f"{self.name}({self.config})"

    def __repr__(self) -> str:
        """返回操作的开发者字符串表示"""
        return f"{self.__class__.__name__}(name='{self.name}', config={self.config})"


class OperationPipeline:
    """操作流水线

    将多个操作组合成一个流水线，按顺序应用到数据上。

    示例:
    ```python
    # 创建操作
    fill_na = FillNAOperation(method='mean', columns=['close', 'volume'])
    ma5 = MovingAverageOperation(window=5, column='close', result_column='ma5')

    # 创建流水线
    pipeline = OperationPipeline("日线处理")
    pipeline.add_operation(fill_na)
    pipeline.add_operation(ma5)

    # 应用流水线
    result = await pipeline.apply(data)
    ```
    """

    def __init__(self, name: str = "Pipeline", config: Optional[Dict[str, Any]] = None):
        """初始化操作流水线

        Args:
            name: 流水线名称
            config: 配置参数
        """
        self.name = name
        self.config = config or {}
        self.operations = []
        self.logger = get_logger(f"pipeline.{name}")

        # 流水线配置
        self.stop_on_error = self.config.get("stop_on_error", True)
        self.collect_stats = self.config.get("collect_stats", True)

        # 统计信息
        self._execution_count = 0
        self._last_execution_time = None
        self._operation_stats = []

    def add_operation(
        self, operation: Operation, condition: Optional[Callable[[pd.DataFrame], bool]] = None
    ) -> "OperationPipeline":
        """添加操作到流水线

        Args:
            operation: 要添加的操作
            condition: 可选的条件函数，决定是否执行该操作

        Returns:
            OperationPipeline: 返回自身，支持链式调用
        """
        self.operations.append((operation, condition))
        return self

    def add_operations(self, operations: List[Operation]) -> "OperationPipeline":
        """批量添加操作到流水线

        Args:
            operations: 操作列表

        Returns:
            OperationPipeline: 返回自身，支持链式调用
        """
        for operation in operations:
            self.add_operation(operation)
        return self

    def remove_operation(self, operation_name: str) -> bool:
        """从流水线中移除操作

        Args:
            operation_name: 要移除的操作名称

        Returns:
            bool: 是否成功移除
        """
        for i, (operation, condition) in enumerate(self.operations):
            if operation.name == operation_name:
                self.operations.pop(i)
                self.logger.info(f"从流水线中移除操作: {operation_name}")
                return True
        return False

    def get_operation_names(self) -> List[str]:
        """获取流水线中所有操作的名称"""
        return [operation.name for operation, _ in self.operations]

    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用流水线到数据

        按顺序应用流水线中的所有操作。

        Args:
            data: 输入数据框

        Returns:
            pd.DataFrame: 处理后的数据框
        """
        if data is None or data.empty:
            self.logger.warning("输入数据为空")
            return pd.DataFrame()

        self.logger.info(
            f"开始执行流水线 '{self.name}'，操作数量: {len(self.operations)}"
        )

        # 创建输入数据的副本
        result = data.copy()

        # 依次应用各个操作
        for i, (operation, condition) in enumerate(self.operations):
            # 检查条件
            if condition is not None:
                try:
                    should_apply = condition(result)
                    if not should_apply:
                        self.logger.info(
                            f"跳过操作 {i+1}/{len(self.operations)}: {operation.name} (条件不满足)"
                        )
                        continue
                except Exception as e:
                    self.logger.error(f"执行条件函数时出错: {str(e)}")
                    continue

            # 应用操作
            try:
                self.logger.debug(
                    f"执行操作 {i+1}/{len(self.operations)}: {operation.name}"
                )
                result = await operation.apply(result)
                self.logger.debug(
                    f"操作 {operation.name} 完成，结果行数: {len(result)}"
                )
            except Exception as e:
                self.logger.error(
                    f"执行操作 {operation.name} 时出错: {str(e)}", exc_info=True
                )
                raise

        self.logger.info(f"流水线 '{self.name}' 执行完成，结果行数: {len(result)}")
        return result

    async def execute(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        执行流水线的完整流程，包含统计和错误处理

        Args:
            data: 输入数据

        Returns:
            包含结果和元数据的字典
        """
        start_time = datetime.now()

        try:
            result = await self.apply(data)

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            self._execution_count += 1
            self._last_execution_time = end_time

            self.logger.info(
                f"流水线 {self.name} 执行完成，"
                f"处理时间: {processing_time:.2f}秒，"
                f"输入行数: {len(data)}，"
                f"输出行数: {len(result)}"
            )

            return {
                "status": "success",
                "data": result,
                "metadata": {
                    "pipeline_name": self.name,
                    "processing_time": processing_time,
                    "input_rows": len(data),
                    "output_rows": len(result),
                    "execution_count": self._execution_count,
                    "operation_count": len(self.operations),
                    "operation_stats": self._operation_stats if self.collect_stats else None,
                    "timestamp": end_time
                }
            }

        except Exception as e:
            self.logger.error(f"流水线 {self.name} 执行失败: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "metadata": {
                    "pipeline_name": self.name,
                    "timestamp": datetime.now()
                }
            }

    def get_stats(self) -> Dict[str, Any]:
        """获取流水线统计信息"""
        return {
            "name": self.name,
            "execution_count": self._execution_count,
            "last_execution_time": self._last_execution_time,
            "operation_count": len(self.operations),
            "operation_names": self.get_operation_names(),
            "operation_stats": self._operation_stats
        }

    def clear_stats(self):
        """清除统计信息"""
        self._execution_count = 0
        self._last_execution_time = None
        self._operation_stats = []
        self.logger.info(f"清除流水线 {self.name} 的统计信息")

    def __len__(self):
        """返回流水线中操作的数量"""
        return len(self.operations)

    def __str__(self):
        return f"OperationPipeline(name='{self.name}', operations={len(self.operations)})"

    def __repr__(self):
        return self.__str__()
