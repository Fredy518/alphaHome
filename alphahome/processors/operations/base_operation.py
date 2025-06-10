#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理操作基类

定义了数据处理操作的基础接口，所有具体操作都应继承此类。
"""

from typing import Dict, Any, Optional
import pandas as pd
import abc

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
    
    def __init__(self, name: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        """初始化操作
        
        Args:
            name: 操作名称，默认为类名
            config: 配置参数
        """
        self.name = name or self.__class__.__name__
        self.config = config or {}
        self.logger = get_logger(f"operation.{self.name}")
        
    @abc.abstractmethod
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用操作到数据
        
        Args:
            data: 输入数据框
            
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
    
    def __init__(self, name: str = "Pipeline"):
        """初始化操作流水线
        
        Args:
            name: 流水线名称
        """
        self.name = name
        self.operations = []
        self.logger = get_logger(f"pipeline.{name}")
        
    def add_operation(self, operation: Operation, condition=None) -> 'OperationPipeline':
        """添加操作到流水线
        
        Args:
            operation: 要添加的操作
            condition: 可选的条件函数，决定是否执行该操作
            
        Returns:
            OperationPipeline: 返回自身，支持链式调用
        """
        self.operations.append((operation, condition))
        return self
        
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
            
        self.logger.info(f"开始执行流水线 '{self.name}'，操作数量: {len(self.operations)}")
        
        # 创建输入数据的副本
        result = data.copy()
        
        # 依次应用各个操作
        for i, (operation, condition) in enumerate(self.operations):
            # 检查条件
            if condition is not None:
                try:
                    should_apply = condition(result)
                    if not should_apply:
                        self.logger.info(f"跳过操作 {i+1}/{len(self.operations)}: {operation.name} (条件不满足)")
                        continue
                except Exception as e:
                    self.logger.error(f"执行条件函数时出错: {str(e)}")
                    continue
                    
            # 应用操作
            try:
                self.logger.debug(f"执行操作 {i+1}/{len(self.operations)}: {operation.name}")
                result = await operation.apply(result)
                self.logger.debug(f"操作 {operation.name} 完成，结果行数: {len(result)}")
            except Exception as e:
                self.logger.error(f"执行操作 {operation.name} 时出错: {str(e)}", exc_info=True)
                raise
                
        self.logger.info(f"流水线 '{self.name}' 执行完成，结果行数: {len(result)}")
        return result 