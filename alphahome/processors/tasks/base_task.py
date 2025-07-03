#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务基类

定义了具体数据处理任务的基础接口。
任务层负责编排一个或多个原子操作来完成复杂的处理逻辑。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import asyncio

from ...common.task_system.base_task import BaseTask
from ...common.logging_utils import get_logger


class ProcessorTaskBase(BaseTask, ABC):
    """
    数据处理任务基类
    
    这是所有处理器任务的统一基类，负责定义一个完整的业务处理流程。
    它编排了数据获取、处理和保存的整个过程。
    
    子类需要实现的核心方法：
    1. `fetch_data`: 定义从哪里获取源数据。
    2. `process_data`: 定义如何通过编排一个或多个`Operation`来处理数据。
    3. `save_result`: 定义如何保存处理后的结果。
    
    示例:
    ```python
    from .operations import FillNAOperation, MovingAverageOperation
    from .operations.base_operation import OperationPipeline
    
    @task_register()
    class MyProcessorTask(ProcessorTaskBase):
        name = "my_processor"
        table_name = "my_result_table"
        
        async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
            # Task内部负责编排Operations
            pipeline = OperationPipeline(name="MyInternalPipeline")
            pipeline.add_operation(FillNAOperation(method='ffill'))
            pipeline.add_operation(MovingAverageOperation(window=5))
            
            processed_data = await pipeline.apply(data)
            return processed_data
            
        async def fetch_data(self, **kwargs) -> pd.DataFrame:
            ... # 实现数据获取逻辑
            
        async def save_result(self, data: pd.DataFrame, **kwargs):
            ... # 实现结果保存逻辑
    ```
    """
    
    # 任务类型标识
    task_type: str = "processor"
    
    # 处理器任务特有属性
    # source_tables, dependencies 等属性已由 BaseTask 提供
    calculation_method: Optional[str] = None  # 计算方法标识
    
    def __init__(self, db_connection=None, **kwargs):
        """初始化处理任务"""
        super().__init__(db_connection=db_connection, **kwargs)
        
        if not self.source_tables:
            self.logger.warning(f"处理任务 {self.name} 未定义 source_tables")

    async def _fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取数据（内部实现）。
        
        这是 BaseTask._fetch_data 的具体实现，它会调用子类定义的 fetch_data。
        """
        self.logger.info(f"从源表获取数据: {self.source_tables}")
        return await self.fetch_data(**kwargs)

    @abstractmethod
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取数据的抽象方法。
        
        子类必须实现此方法来定义具体的数据获取逻辑，例如从多个数据源查询并合并数据。
        
        Args:
            **kwargs: 数据获取参数
            
        Returns:
            Optional[pd.DataFrame]: 获取并准备好的数据。
        """
        raise NotImplementedError("子类必须实现 fetch_data 方法")

    async def process_data(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        处理数据（重写 BaseTask 的 process_data）。
        
        默认实现是直接返回输入数据。
        子类应该重写此方法，通过编排一个或多个`Operation`来定义核心处理逻辑。
        """
        self.logger.warning(f"任务 {self.name} 未实现 process_data 方法，将直接返回原始数据。")
        return data

    async def _save_data(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """
        保存处理结果（内部实现）。
        
        这是 BaseTask._save_data 的具体实现，它会调用子类定义的 save_result。
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return
            
        if not hasattr(self, 'table_name') or not self.table_name:
            self.logger.warning("未定义 table_name，无法保存结果")
            return
        
        self.logger.info(f"保存结果到表: {self.table_name}，行数: {len(data)}")
        await self.save_result(data, **kwargs)


    @abstractmethod
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """
        保存处理结果的抽象方法。
        
        子类必须实现此方法来定义具体的结果保存逻辑。
        
        Args:
            data: 要保存的数据。
            **kwargs: 保存参数。
        """
        raise NotImplementedError("子类必须实现 save_result 方法")

    async def run(self, **kwargs) -> Dict[str, Any]:
        """
        任务执行入口点。
        
        重写BaseTask的run方法，确保调用正确的执行流程。
        """
        return await self.execute(**kwargs)

    def get_task_info(self) -> Dict[str, Any]:
        """获取处理任务的详细信息"""
        info = {
            "name": self.name,
            "type": self.task_type,
            "source_tables": self.source_tables,
            "target_table": self.table_name,
            "dependencies": self.dependencies,
            "description": self.description,
        }
        return info
