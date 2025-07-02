#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理引擎

协调和执行数据处理任务的核心引擎。
负责任务调度、执行监控、资源管理等功能。
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from ..tasks.base_task import ProcessorTaskBase
from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory, get_task


class ProcessorEngine:
    """
    数据处理引擎
    
    负责协调和执行数据处理任务的核心引擎。
    
    主要功能：
    1. 任务调度和执行
    2. 依赖关系管理
    3. 并发控制
    4. 执行监控和统计
    5. 错误处理和恢复
    6. 资源管理
    
    示例:
    ```python
    # 创建引擎
    engine = ProcessorEngine(max_workers=4)
    
    # 执行单个任务
    result = await engine.execute_task("stock_adjusted_price_v2")
    
    # 执行多个任务
    results = await engine.execute_tasks([
        "stock_adjusted_price_v2",
        "stock_technical_indicators"
    ])
    
    # 批量执行
    results = await engine.execute_batch({
        "task1": {"param1": "value1"},
        "task2": {"param2": "value2"}
    })
    ```
    """
    
    def __init__(
        self,
        max_workers: int = 4,
        timeout: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """初始化处理引擎
        
        Args:
            max_workers: 最大并发工作线程数
            timeout: 任务执行超时时间（秒）
            config: 引擎配置
        """
        self.max_workers = max_workers
        self.timeout = timeout
        self.config = config or {}
        
        self.logger = get_logger("processor_engine")
        
        # 执行统计
        self._execution_stats = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "total_execution_time": 0.0,
            "start_time": None,
            "last_execution": None
        }
        
        # 任务缓存
        self._task_cache = {}
        
        # 线程池
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        self.logger.info(f"处理引擎初始化完成，最大工作线程数: {max_workers}")
    
    async def execute_task(
        self,
        task_name: str,
        db_connection=None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行单个处理任务
        
        Args:
            task_name: 任务名称
            db_connection: 数据库连接
            **kwargs: 任务执行参数
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        start_time = datetime.now()
        self._execution_stats["total_tasks"] += 1
        
        if self._execution_stats["start_time"] is None:
            self._execution_stats["start_time"] = start_time
        
        try:
            self.logger.info(f"开始执行任务: {task_name}")
            
            # 获取任务实例
            task = await self._get_task_instance(task_name, db_connection)
            
            # 检查依赖
            await self._check_dependencies(task)
            
            # 执行任务
            result = await self._execute_single_task(task, **kwargs)
            
            # 更新统计
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            if result.get("status") == "success":
                self._execution_stats["successful_tasks"] += 1
            else:
                self._execution_stats["failed_tasks"] += 1
            
            self._execution_stats["total_execution_time"] += execution_time
            self._execution_stats["last_execution"] = end_time
            
            self.logger.info(
                f"任务 {task_name} 执行完成，"
                f"状态: {result.get('status')}，"
                f"执行时间: {execution_time:.2f}秒"
            )
            
            # 添加引擎元数据
            result["engine_metadata"] = {
                "task_name": task_name,
                "execution_time": execution_time,
                "start_time": start_time,
                "end_time": end_time,
                "engine_stats": self._execution_stats.copy()
            }
            
            return result
            
        except Exception as e:
            self._execution_stats["failed_tasks"] += 1
            self.logger.error(f"任务 {task_name} 执行失败: {str(e)}", exc_info=True)
            
            return {
                "status": "error",
                "error": str(e),
                "task_name": task_name,
                "engine_metadata": {
                    "execution_time": (datetime.now() - start_time).total_seconds(),
                    "start_time": start_time,
                    "end_time": datetime.now()
                }
            }
    
    async def execute_tasks(
        self,
        task_names: List[str],
        db_connection=None,
        parallel: bool = True,
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """
        执行多个处理任务
        
        Args:
            task_names: 任务名称列表
            db_connection: 数据库连接
            parallel: 是否并行执行
            **kwargs: 任务执行参数
            
        Returns:
            Dict[str, Dict[str, Any]]: 任务名称到执行结果的映射
        """
        self.logger.info(f"开始执行 {len(task_names)} 个任务，并行模式: {parallel}")
        
        if parallel and len(task_names) > 1:
            return await self._execute_tasks_parallel(task_names, db_connection, **kwargs)
        else:
            return await self._execute_tasks_sequential(task_names, db_connection, **kwargs)
    
    async def execute_batch(
        self,
        task_configs: Dict[str, Dict[str, Any]],
        db_connection=None,
        parallel: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量执行任务，每个任务可以有不同的参数
        
        Args:
            task_configs: 任务配置字典，格式为 {task_name: {param1: value1, ...}}
            db_connection: 数据库连接
            parallel: 是否并行执行
            
        Returns:
            Dict[str, Dict[str, Any]]: 任务名称到执行结果的映射
        """
        self.logger.info(f"开始批量执行 {len(task_configs)} 个任务")
        
        results = {}
        
        if parallel and len(task_configs) > 1:
            # 并行执行
            tasks = []
            for task_name, config in task_configs.items():
                task_coro = self.execute_task(task_name, db_connection, **config)
                tasks.append((task_name, task_coro))
            
            # 等待所有任务完成
            for task_name, task_coro in tasks:
                try:
                    result = await task_coro
                    results[task_name] = result
                except Exception as e:
                    results[task_name] = {
                        "status": "error",
                        "error": str(e),
                        "task_name": task_name
                    }
        else:
            # 顺序执行
            for task_name, config in task_configs.items():
                result = await self.execute_task(task_name, db_connection, **config)
                results[task_name] = result
        
        return results
    
    async def _get_task_instance(self, task_name: str, db_connection=None) -> ProcessorTaskBase:
        """获取任务实例"""
        # 检查缓存
        if task_name in self._task_cache:
            return self._task_cache[task_name]
        
        # 从任务工厂获取任务
        task_class = get_task(task_name)
        if task_class is None:
            raise ValueError(f"未找到任务: {task_name}")
        
        # 创建任务实例
        task = task_class(db_connection=db_connection)
        
        # 验证任务类型
        if not isinstance(task, ProcessorTaskBase):
            raise TypeError(f"任务 {task_name} 不是 ProcessorTaskBase 的实例")
        
        # 缓存任务实例
        self._task_cache[task_name] = task
        
        return task
    
    async def _check_dependencies(self, task: ProcessorTaskBase):
        """检查任务依赖"""
        if not hasattr(task, 'dependencies') or not task.dependencies:
            return
        
        self.logger.info(f"检查任务 {task.name} 的依赖: {task.dependencies}")
        
        # TODO: 实现依赖检查逻辑
        # 这里应该检查依赖任务是否已经完成
        # 可能需要查询数据库或任务状态管理系统
    
    async def _execute_single_task(self, task: ProcessorTaskBase, **kwargs) -> Dict[str, Any]:
        """执行单个任务"""
        try:
            if self.timeout:
                result = await asyncio.wait_for(task.run(**kwargs), timeout=self.timeout)
            else:
                result = await task.run(**kwargs)
            
            return result
            
        except asyncio.TimeoutError:
            raise Exception(f"任务 {task.name} 执行超时 ({self.timeout}秒)")
        except Exception as e:
            raise Exception(f"任务 {task.name} 执行失败: {str(e)}")
    
    async def _execute_tasks_parallel(
        self,
        task_names: List[str],
        db_connection=None,
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """并行执行任务"""
        tasks = []
        for task_name in task_names:
            task_coro = self.execute_task(task_name, db_connection, **kwargs)
            tasks.append((task_name, task_coro))
        
        results = {}
        
        # 使用 asyncio.gather 并行执行
        try:
            task_results = await asyncio.gather(
                *[task_coro for _, task_coro in tasks],
                return_exceptions=True
            )
            
            for (task_name, _), result in zip(tasks, task_results):
                if isinstance(result, Exception):
                    results[task_name] = {
                        "status": "error",
                        "error": str(result),
                        "task_name": task_name
                    }
                else:
                    results[task_name] = result
                    
        except Exception as e:
            self.logger.error(f"并行执行任务时发生错误: {str(e)}")
            raise
        
        return results
    
    async def _execute_tasks_sequential(
        self,
        task_names: List[str],
        db_connection=None,
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """顺序执行任务"""
        results = {}
        
        for task_name in task_names:
            result = await self.execute_task(task_name, db_connection, **kwargs)
            results[task_name] = result
            
            # 如果任务失败且配置为遇错停止，则中断执行
            if (result.get("status") != "success" and 
                not self.config.get("continue_on_error", True)):
                self.logger.warning(f"任务 {task_name} 失败，中断后续任务执行")
                break
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """获取引擎统计信息"""
        stats = self._execution_stats.copy()
        
        if stats["total_tasks"] > 0:
            stats["success_rate"] = stats["successful_tasks"] / stats["total_tasks"]
            stats["average_execution_time"] = stats["total_execution_time"] / stats["total_tasks"]
        else:
            stats["success_rate"] = 0.0
            stats["average_execution_time"] = 0.0
        
        return stats
    
    def reset_stats(self):
        """重置统计信息"""
        self._execution_stats = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "total_execution_time": 0.0,
            "start_time": None,
            "last_execution": None
        }
        self.logger.info("引擎统计信息已重置")
    
    def clear_cache(self):
        """清除任务缓存"""
        self._task_cache.clear()
        self.logger.info("任务缓存已清除")
    
    def shutdown(self):
        """关闭引擎"""
        self._executor.shutdown(wait=True)
        self.logger.info("处理引擎已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
