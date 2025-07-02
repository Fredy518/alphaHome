#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理流水线基类

定义了高级数据处理流水线的基础接口和通用功能。
流水线层组合多个操作来完成复杂的数据处理任务。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import pandas as pd

from ..base.processor import BaseProcessor
from ..operations.base_operation import Operation, OperationPipeline
from ...common.logging_utils import get_logger


class ProcessingPipeline(BaseProcessor):
    """
    数据处理流水线基类
    
    流水线层组合多个操作来完成复杂的数据处理任务。
    与操作层的区别在于，流水线层关注业务逻辑的组合，
    而操作层关注原子级的数据处理。
    
    主要功能：
    1. 组合多个操作形成业务流水线
    2. 提供流水线级别的配置和控制
    3. 支持条件执行和分支逻辑
    4. 提供流水线级别的统计和监控
    5. 支持流水线的序列化和反序列化
    
    示例:
    ```python
    class StockDataPipeline(ProcessingPipeline):
        def __init__(self, config=None):
            super().__init__(name="StockDataPipeline", config=config)
        
        def build_pipeline(self):
            # 构建具体的处理流水线
            self.add_stage("数据清洗", self._create_cleaning_operations())
            self.add_stage("特征工程", self._create_feature_operations())
            self.add_stage("技术指标", self._create_indicator_operations())
        
        def _create_cleaning_operations(self):
            return [
                FillNAOperation(method='forward'),
                RemoveOutliersOperation(method='iqr')
            ]
    ```
    """
    
    def __init__(
        self,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[Any] = None
    ):
        """初始化处理流水线
        
        Args:
            name: 流水线名称
            config: 配置参数
            logger: 日志记录器
        """
        super().__init__(name=name, config=config, logger=logger)
        
        # 流水线阶段
        self.stages = []  # List[Dict[str, Any]]
        
        # 流水线配置
        self.parallel_stages = self.config.get("parallel_stages", False)
        self.stage_timeout = self.config.get("stage_timeout", None)
        self.continue_on_stage_error = self.config.get("continue_on_stage_error", False)
        
        # 构建流水线
        self.build_pipeline()
    
    @abstractmethod
    def build_pipeline(self):
        """
        构建流水线的抽象方法
        
        子类必须实现此方法来定义具体的流水线结构。
        通常在此方法中调用add_stage来添加处理阶段。
        """
        raise NotImplementedError("子类必须实现 build_pipeline 方法")
    
    def add_stage(
        self, 
        name: str, 
        operations: Union[List[Operation], OperationPipeline, Operation],
        condition: Optional[callable] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        添加处理阶段
        
        Args:
            name: 阶段名称
            operations: 操作列表、操作流水线或单个操作
            condition: 可选的条件函数，决定是否执行该阶段
            config: 阶段特定的配置
        """
        # 标准化操作为OperationPipeline
        if isinstance(operations, Operation):
            pipeline = OperationPipeline(f"{name}_pipeline")
            pipeline.add_operation(operations)
        elif isinstance(operations, list):
            pipeline = OperationPipeline(f"{name}_pipeline")
            pipeline.add_operations(operations)
        elif isinstance(operations, OperationPipeline):
            pipeline = operations
        else:
            raise TypeError(f"不支持的操作类型: {type(operations)}")
        
        stage = {
            "name": name,
            "pipeline": pipeline,
            "condition": condition,
            "config": config or {},
            "stats": {
                "execution_count": 0,
                "total_time": 0.0,
                "last_execution": None
            }
        }
        
        self.stages.append(stage)
        self.logger.info(f"添加处理阶段: {name}，操作数量: {len(pipeline)}")
    
    def remove_stage(self, name: str) -> bool:
        """
        移除处理阶段
        
        Args:
            name: 要移除的阶段名称
            
        Returns:
            bool: 是否成功移除
        """
        for i, stage in enumerate(self.stages):
            if stage["name"] == name:
                self.stages.pop(i)
                self.logger.info(f"移除处理阶段: {name}")
                return True
        return False
    
    def get_stage_names(self) -> List[str]:
        """获取所有阶段的名称"""
        return [stage["name"] for stage in self.stages]
    
    async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        执行流水线处理
        
        Args:
            data: 输入数据
            **kwargs: 处理参数
            
        Returns:
            处理后的数据
        """
        if not self.stages:
            self.logger.warning("流水线没有定义任何阶段")
            return data
        
        result = data.copy()
        
        self.logger.info(f"开始执行流水线 {self.name}，阶段数量: {len(self.stages)}")
        
        for i, stage in enumerate(self.stages):
            stage_name = stage["name"]
            pipeline = stage["pipeline"]
            condition = stage["condition"]
            
            # 检查条件
            if condition is not None:
                try:
                    should_execute = condition(result)
                    if not should_execute:
                        self.logger.info(f"跳过阶段 {i+1}/{len(self.stages)}: {stage_name} (条件不满足)")
                        continue
                except Exception as e:
                    self.logger.error(f"执行阶段条件函数时出错: {str(e)}")
                    if not self.continue_on_stage_error:
                        raise
                    continue
            
            # 执行阶段
            try:
                stage_start_time = datetime.now()
                self.logger.info(f"执行阶段 {i+1}/{len(self.stages)}: {stage_name}")
                
                stage_result = await pipeline.execute(result)
                
                if stage_result["status"] == "success":
                    result = stage_result["data"]
                    
                    # 更新阶段统计
                    stage_end_time = datetime.now()
                    stage_time = (stage_end_time - stage_start_time).total_seconds()
                    stage["stats"]["execution_count"] += 1
                    stage["stats"]["total_time"] += stage_time
                    stage["stats"]["last_execution"] = stage_end_time
                    
                    self.logger.info(
                        f"阶段 {stage_name} 完成，"
                        f"处理时间: {stage_time:.2f}秒，"
                        f"结果行数: {len(result)}"
                    )
                else:
                    raise Exception(f"阶段执行失败: {stage_result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                self.logger.error(f"执行阶段 {stage_name} 时出错: {str(e)}", exc_info=True)
                if not self.continue_on_stage_error:
                    raise
        
        self.logger.info(f"流水线 {self.name} 执行完成，最终结果行数: {len(result)}")
        return result
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """获取流水线详细信息"""
        return {
            "name": self.name,
            "class": self.__class__.__name__,
            "config": self.config,
            "stage_count": len(self.stages),
            "stages": [
                {
                    "name": stage["name"],
                    "operation_count": len(stage["pipeline"]),
                    "stats": stage["stats"]
                }
                for stage in self.stages
            ]
        }
    
    def reset_stats(self):
        """重置所有统计信息"""
        super().reset()
        for stage in self.stages:
            stage["stats"] = {
                "execution_count": 0,
                "total_time": 0.0,
                "last_execution": None
            }
        self.logger.info(f"重置流水线 {self.name} 的统计信息")
    
    def __len__(self):
        """返回流水线中阶段的数量"""
        return len(self.stages)
    
    def __str__(self):
        return f"ProcessingPipeline(name='{self.name}', stages={len(self.stages)})"
    
    def __repr__(self):
        return self.__str__()
