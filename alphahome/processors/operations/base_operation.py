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
    一个操作是纯粹的、无状态的数据转换函数。
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

    @abc.abstractmethod
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用操作到数据

        Args:
            data: 输入数据框
            **kwargs: 其他处理参数

        Returns:
            pd.DataFrame: 处理后的数据框
        """
        raise NotImplementedError("子类必须实现apply方法")

    def __str__(self) -> str:
        """返回操作的字符串表示"""
        return f"{self.name}({self.config})"

    def __repr__(self) -> str:
        """返回操作的开发者字符串表示"""
        return f"{self.__class__.__name__}(name='{self.name}', config={self.config})"


class OperationPipeline:
    """操作流水线

    将多个操作组合成一个流水线，按顺序应用到数据上。
    这是一个可选的辅助工具，用于组织多个操作。
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

    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用流水线到数据

        按顺序应用流水线中的所有操作。

        Args:
            data: 输入数据框
            **kwargs: 传递给每个操作的额外参数

        Returns:
            pd.DataFrame: 处理后的数据框
        """
        if data is None or data.empty:
            self.logger.warning("流水线接收到空数据，直接返回。")
            return pd.DataFrame() if data is None else data.copy()

        self.logger.debug(
            f"开始执行流水线 '{self.name}'，操作数量: {len(self.operations)}"
        )

        result = data

        # 依次应用各个操作
        for i, (operation, condition) in enumerate(self.operations):
            # 检查条件
            if condition is not None:
                try:
                    should_apply = condition(result)
                    if not should_apply:
                        self.logger.debug(
                            f"跳过操作 {i+1}/{len(self.operations)}: {operation.name} (条件不满足)"
                        )
                        continue
                except Exception as e:
                    self.logger.error(f"执行条件函数时出错 for {operation.name}: {str(e)}")
                    if self.stop_on_error:
                        raise
                    continue

            # 应用操作
            try:
                self.logger.debug(
                    f"执行操作 {i+1}/{len(self.operations)}: {operation.name}"
                )
                result = await operation.apply(result, **kwargs)
            except Exception as e:
                self.logger.error(
                    f"执行操作 {operation.name} 时出错: {str(e)}", exc_info=True
                )
                if self.stop_on_error:
                    raise

        self.logger.debug(f"流水线 '{self.name}' 执行完成，结果行数: {len(result)}")
        return result

    def __len__(self):
        """返回流水线中操作的数量"""
        return len(self.operations)

    def __str__(self):
        return f"OperationPipeline(name='{self.name}', operations={len(self.operations)})"

    def __repr__(self):
        return self.__str__()
