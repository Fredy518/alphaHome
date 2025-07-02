#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务基类

定义了具体数据处理任务的基础接口。
任务层使用流水线层和操作层来组合复杂的处理逻辑。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from ..pipelines.base_pipeline import ProcessingPipeline
from ..base.block_processor import BlockProcessor
from ...common.task_system.base_task import BaseTask
from ...common.logging_utils import get_logger


class ProcessorTaskBase(BaseTask, ABC):
    """
    数据处理任务基类
    
    结合了统一任务系统的BaseTask和新的处理器架构。
    任务层负责：
    1. 定义具体的业务处理逻辑
    2. 组合使用流水线和操作
    3. 处理数据获取和保存
    4. 与统一任务系统集成
    
    示例:
    ```python
    @task_register()
    class MyProcessorTask(ProcessorTaskBase):
        name = "my_processor"
        table_name = "my_result_table"
        description = "我的数据处理任务"
        
        def create_pipeline(self):
            return MyDataPipeline(config=self.get_pipeline_config())
        
        async def execute_task(self, **kwargs):
            # 获取数据
            data = await self.fetch_data(**kwargs)
            
            # 创建并执行流水线
            pipeline = self.create_pipeline()
            result = await pipeline.execute(data)
            
            # 保存结果
            await self.save_result(result["data"])
            
            return result
    ```
    """
    
    # 任务类型标识
    task_type: str = "processor"
    
    # 处理器任务特有属性
    source_tables: List[str] = []        # 源数据表列表
    dependencies: List[str] = []         # 依赖的其他任务
    calculation_method: Optional[str] = None  # 计算方法标识
    
    def __init__(self, db_connection=None):
        """初始化处理任务
        
        Args:
            db_connection: 数据库连接
        """
        super().__init__(db_connection)
        
        # 设置处理任务专用的日志记录器
        self.logger = get_logger(f"processor_task.{self.name}")
        
        # 处理器实例
        self._pipeline = None
        
        # 验证必要的配置
        if not self.source_tables:
            self.logger.warning(f"处理任务 {self.name} 未定义source_tables")
    
    @abstractmethod
    def create_pipeline(self) -> ProcessingPipeline:
        """
        创建处理流水线
        
        子类必须实现此方法来创建具体的处理流水线。
        
        Returns:
            ProcessingPipeline: 处理流水线实例
        """
        raise NotImplementedError("子类必须实现 create_pipeline 方法")
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """
        获取流水线配置
        
        子类可以重写此方法来提供特定的流水线配置。
        
        Returns:
            Dict[str, Any]: 流水线配置字典
        """
        return {
            "continue_on_stage_error": False,
            "collect_stats": True
        }
    
    async def execute_task(self, **kwargs) -> Dict[str, Any]:
        """
        执行处理任务的核心逻辑
        
        子类可以重写此方法来实现具体的任务执行逻辑。
        默认实现提供了标准的处理流程。
        
        Args:
            **kwargs: 任务执行参数
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"开始执行处理任务: {self.name}")
        
        try:
            # 1. 获取数据
            data = await self.fetch_data(**kwargs)
            if data is None or data.empty:
                self.logger.warning("未获取到数据，任务结束")
                return {
                    "status": "success",
                    "message": "No data to process",
                    "rows": 0
                }
            
            # 2. 创建并执行流水线
            if self._pipeline is None:
                self._pipeline = self.create_pipeline()
            
            result = await self._pipeline.execute(data, **kwargs)
            
            if result["status"] != "success":
                raise Exception(f"流水线执行失败: {result.get('error', 'Unknown error')}")
            
            processed_data = result["data"]
            
            # 3. 保存结果
            if processed_data is not None and not processed_data.empty:
                await self.save_result(processed_data, **kwargs)
                rows_processed = len(processed_data)
            else:
                rows_processed = 0
            
            self.logger.info(f"处理任务 {self.name} 完成，处理行数: {rows_processed}")
            
            return {
                "status": "success",
                "rows": rows_processed,
                "metadata": result.get("metadata", {}),
                "message": f"Successfully processed {rows_processed} rows"
            }
            
        except Exception as e:
            self.logger.error(f"处理任务 {self.name} 执行失败: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "message": f"Task execution failed: {str(e)}"
            }
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取数据
        
        子类可以重写此方法来实现具体的数据获取逻辑。
        默认实现尝试从source_tables获取数据。
        
        Args:
            **kwargs: 数据获取参数
            
        Returns:
            Optional[pd.DataFrame]: 获取的数据
        """
        if not self.source_tables:
            self.logger.warning("未定义source_tables，无法获取数据")
            return None
        
        # 这里应该实现具体的数据获取逻辑
        # 由于涉及到数据库操作，这里提供一个框架
        self.logger.info(f"从源表获取数据: {self.source_tables}")
        
        # TODO: 实现具体的数据获取逻辑
        # 可能需要调用数据库管理器或其他数据源
        
        return pd.DataFrame()  # 占位符
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """
        保存处理结果
        
        子类可以重写此方法来实现具体的结果保存逻辑。
        
        Args:
            data: 要保存的数据
            **kwargs: 保存参数
        """
        if not hasattr(self, 'table_name') or not self.table_name:
            self.logger.warning("未定义table_name，无法保存结果")
            return
        
        self.logger.info(f"保存结果到表: {self.table_name}，行数: {len(data)}")
        
        # TODO: 实现具体的数据保存逻辑
        # 可能需要调用数据库管理器
    
    async def run(self, **kwargs) -> Dict[str, Any]:
        """
        任务执行入口点
        
        重写BaseTask的run方法，使用新的执行逻辑。
        
        Args:
            **kwargs: 执行参数
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        return await self.execute_task(**kwargs)
    
    def get_processing_info(self) -> Dict[str, Any]:
        """获取处理任务的详细信息"""
        info = {
            "name": self.name,
            "type": self.task_type,
            "source_tables": self.source_tables,
            "target_table": getattr(self, 'table_name', None),
            "dependencies": self.dependencies,
            "calculation_method": self.calculation_method,
            "description": self.description,
        }
        
        # 添加流水线信息
        if self._pipeline is not None:
            info["pipeline"] = self._pipeline.get_pipeline_info()
        
        return info
