#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理器基类

定义了所有数据处理器的基础接口和通用功能。
这是processors模块的核心基类，提供了数据处理的标准化框架。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import logging
from datetime import datetime

from ...common.logging_utils import get_logger


class BaseProcessor(ABC):
    """
    数据处理器基类
    
    所有数据处理器的抽象基类，定义了统一的处理接口。
    提供了数据处理的标准化流程和通用功能。
    
    主要功能：
    1. 统一的数据处理接口
    2. 日志记录和错误处理
    3. 数据验证和质量检查
    4. 处理状态管理
    5. 配置管理
    
    示例:
    ```python
    class MyProcessor(BaseProcessor):
        def __init__(self, config=None):
            super().__init__(name="MyProcessor", config=config)
        
        def process(self, data, **kwargs):
            # 实现具体的数据处理逻辑
            result = self._transform_data(data)
            return result
        
        def _transform_data(self, data):
            # 具体的数据转换逻辑
            return data.copy()
    ```
    """
    
    def __init__(
        self, 
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """初始化处理器
        
        Args:
            name: 处理器名称，默认为类名
            config: 配置参数字典
            logger: 日志记录器，如果不提供则自动创建
        """
        self.name = name or self.__class__.__name__
        self.config = config or {}
        self.logger = logger or get_logger(f"processor.{self.name}")
        
        # 处理状态
        self._is_initialized = False
        self._last_process_time = None
        self._process_count = 0
        self._total_time = 0.0
        
        # 初始化处理器
        self._initialize()
    
    def _initialize(self):
        """初始化处理器内部状态"""
        self.logger.debug(f"初始化处理器: {self.name}")
        self._is_initialized = True
    
    @abstractmethod
    def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理数据的核心方法
        
        子类必须实现此方法来定义具体的数据处理逻辑。
        
        Args:
            data: 输入数据DataFrame
            **kwargs: 其他处理参数
            
        Returns:
            处理后的数据DataFrame
            
        Raises:
            NotImplementedError: 子类未实现此方法
        """
        raise NotImplementedError("子类必须实现 process 方法")
    
    async def execute(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """
        执行数据处理的完整流程
        
        这是处理器的主要入口点，包含了完整的处理流程：
        1. 预处理检查
        2. 数据验证
        3. 核心处理
        4. 后处理
        5. 结果验证
        
        Args:
            data: 输入数据
            **kwargs: 处理参数
            
        Returns:
            包含处理结果和元数据的字典
        """
        if not self._is_initialized:
            raise RuntimeError(f"处理器 {self.name} 未正确初始化")
        
        start_time = datetime.now()
        self.logger.info(f"开始执行处理器: {self.name}")
        
        try:
            # 1. 预处理检查
            self._pre_process_check(data, **kwargs)
            
            # 2. 数据验证
            self._validate_input_data(data)
            
            # 3. 核心处理
            result_data = await self.process(data, **kwargs)

            # 4. 后处理
            result_data = await self._post_process(result_data, **kwargs)
            
            # 5. 结果验证
            self._validate_output_data(result_data)
            
            # 更新状态
            self._last_process_time = datetime.now()
            self._process_count += 1
            
            processing_time = (self._last_process_time - start_time).total_seconds()
            
            self.logger.info(
                f"处理器 {self.name} 执行完成，"
                f"处理时间: {processing_time:.2f}秒，"
                f"输入行数: {len(data)}，"
                f"输出行数: {len(result_data)}"
            )
            
            return {
                "status": "success",
                "data": result_data,
                "metadata": {
                    "processor_name": self.name,
                    "processing_time": processing_time,
                    "input_rows": len(data),
                    "output_rows": len(result_data),
                    "process_count": self._process_count,
                    "timestamp": self._last_process_time
                }
            }
            
        except Exception as e:
            self.logger.error(f"处理器 {self.name} 执行失败: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "metadata": {
                    "processor_name": self.name,
                    "timestamp": datetime.now()
                }
            }
    
    def _pre_process_check(self, data: pd.DataFrame, **kwargs):
        """预处理检查"""
        if data is None:
            raise ValueError("输入数据不能为None")
        
        if data.empty:
            self.logger.warning("输入数据为空")
    
    def _validate_input_data(self, data: pd.DataFrame):
        """验证输入数据"""
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"输入数据必须是pandas DataFrame，实际类型: {type(data)}")
        
        # 子类可以重写此方法添加特定的验证逻辑
        self._custom_input_validation(data)
    
    def _custom_input_validation(self, data: pd.DataFrame):
        """自定义输入验证，子类可以重写"""
        pass
    
    async def _post_process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """后处理，子类可以重写"""
        return data
    
    def _validate_output_data(self, data: pd.DataFrame):
        """验证输出数据"""
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"输出数据必须是pandas DataFrame，实际类型: {type(data)}")
        
        # 子类可以重写此方法添加特定的验证逻辑
        self._custom_output_validation(data)
    
    def _custom_output_validation(self, data: pd.DataFrame):
        """自定义输出验证，子类可以重写"""
        pass
    
    def get_info(self) -> Dict[str, Any]:
        """获取处理器信息"""
        return {
            "name": self.name,
            "class": self.__class__.__name__,
            "config": self.config,
            "is_initialized": self._is_initialized,
            "process_count": self._process_count,
            "last_process_time": self._last_process_time
        }
    
    def reset(self):
        """重置处理器状态"""
        self.logger.info(f"重置处理器状态: {self.name}")
        self._process_count = 0
        self._total_time = 0.0
        self._last_process_time = None
    
    def __str__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"
    
    def __repr__(self):
        return self.__str__()


class DataProcessor(BaseProcessor):
    """
    数据处理器的具体实现基类
    
    提供了更多数据处理相关的通用功能，如数据类型转换、
    缺失值处理、数据清洗等。
    """
    
    def __init__(
        self,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ):
        super().__init__(name=name, config=config, logger=logger)
        
        # 数据处理相关配置
        self.drop_duplicates = self.config.get("drop_duplicates", False)
        self.fill_na_method = self.config.get("fill_na_method", None)
        self.required_columns = self.config.get("required_columns", [])
    
    def _custom_input_validation(self, data: pd.DataFrame):
        """数据处理器的输入验证"""
        super()._custom_input_validation(data)
        
        # 检查必需列
        if self.required_columns:
            missing_columns = set(self.required_columns) - set(data.columns)
            if missing_columns:
                raise ValueError(f"缺少必需的列: {missing_columns}")
    
    async def _post_process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """数据处理器的后处理"""
        result = data.copy()

        # 去重
        if self.drop_duplicates:
            original_len = len(result)
            result = result.drop_duplicates()
            if len(result) < original_len:
                self.logger.info(f"去除重复数据: {original_len - len(result)} 行")

        # 填充缺失值
        if self.fill_na_method:
            if self.fill_na_method == "forward":
                result = result.fillna(method="ffill")
            elif self.fill_na_method == "backward":
                result = result.fillna(method="bfill")
            elif isinstance(self.fill_na_method, (int, float, str)):
                result = result.fillna(self.fill_na_method)

        return await super()._post_process(result, **kwargs)
