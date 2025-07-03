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
from ...common.task_system import UnifiedTaskFactory, get_task, BaseTask


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
        db_manager: "DBManager",
        max_workers: int = 4,
        timeout: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """初始化处理引擎
        
        Args:
            db_manager: 已初始化的数据库管理器实例。
            max_workers: 最大并发工作线程数
            timeout: 任务执行超时时间（秒）
            config: 引擎配置
        """
        if db_manager is None:
            raise ValueError("必须为ProcessorEngine提供一个DBManager实例。")
            
        self.db_manager = db_manager
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
        
        # 线程池
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        self.logger.info(f"处理引擎初始化完成，最大工作线程数: {max_workers}")
    
    async def execute_task(
        self,
        task_name: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行单个处理任务
        
        Args:
            task_name: 任务名称
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
            task = await self._get_task_instance(task_name)
            
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
        parallel: bool = True,
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """
        执行多个处理任务
        
        Args:
            task_names: 任务名称列表
            parallel: 是否并行执行
            **kwargs: 任务执行参数
            
        Returns:
            Dict[str, Dict[str, Any]]: 任务名称到执行结果的映射
        """
        self.logger.info(f"开始执行 {len(task_names)} 个任务，并行模式: {parallel}")
        
        if parallel and len(task_names) > 1:
            return await self._execute_tasks_parallel(task_names, **kwargs)
        else:
            return await self._execute_tasks_sequential(task_names, **kwargs)
    
    async def execute_batch(
        self,
        task_configs: Dict[str, Dict[str, Any]],
        parallel: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量执行任务，每个任务可以有不同的参数
        
        Args:
            task_configs: 任务配置字典，格式为 {task_name: {param1: value1, ...}}
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
                task_coro = self.execute_task(task_name, **config)
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
                result = await self.execute_task(task_name, **config)
                results[task_name] = result
        
        return results
    
    async def _get_task_instance(self, task_name: str) -> BaseTask:
        """
        获取一个新创建的任务实例。
        
        使用工厂的 `create_task_instance` 方法来确保每次执行
        都能获得一个干净的任务实例，并传入引擎级别的配置。
        """
        task = await UnifiedTaskFactory.create_task_instance(
            task_name,
            **self.config.get(task_name, {})
        )
        return task
    
    async def _check_dependencies(self, task: BaseTask):
        """检查任务依赖"""
        if not hasattr(task, 'dependencies') or not task.dependencies:
            self.logger.info("所有依赖项检查通过")
            return
        
        self.logger.info(f"检查任务 {task.name} 的依赖: {task.dependencies}")
        
        # TODO: 实现依赖检查逻辑
        # 1. 查询一个中央任务状态表(task_registry)
        # 2. 对每个依赖任务，检查其最新状态是否为 'success'
        # 3. 如果任何依赖项未成功完成，可以抛出异常或等待
    
    async def _execute_single_task(self, task: BaseTask, **kwargs) -> Dict[str, Any]:
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
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """并行执行多个任务"""
        loop = asyncio.get_running_loop()
        futures = {
            loop.run_in_executor(
                self._executor, self._run_async_in_thread, self.execute_task, name, **kwargs
            ): name
            for name in task_names
        }

        results = {}
        for future in as_completed(futures):
            task_name = futures[future]
            try:
                result = future.result()
                results[task_name] = result
            except Exception as e:
                self.logger.error(f"并行任务 {task_name} 执行失败: {e}", exc_info=True)
                results[task_name] = {"status": "error", "error": str(e)}
        return results
    
    async def _execute_tasks_sequential(
        self,
        task_names: List[str],
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """顺序执行多个任务"""
        results = {}
        for name in task_names:
            results[name] = await self.execute_task(name, **kwargs)
        return results
    
    def _run_async_in_thread(self, coro, *args, **kwargs):
        """在线程池中运行异步函数的辅助方法"""
        return asyncio.run(coro(*args, **kwargs))
    
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
        """重置执行统计"""
        self._execution_stats = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "total_execution_time": 0.0,
            "start_time": None,
            "last_execution": None
        }
        self.logger.info("处理器引擎统计信息已重置")
    
    def clear_cache(self):
        """清理引擎内部缓存（此版本中无缓存）"""
        self.logger.info("处理器引擎没有内部任务缓存，无需清理。")
    
    def shutdown(self):
        """关闭引擎的线程池"""
        self._executor.shutdown(wait=True)
        self.logger.info("处理引擎已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
