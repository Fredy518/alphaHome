#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理引擎

协调和执行数据处理任务的核心引擎。
负责任务调度、执行监控、资源管理等功能。
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from ..tasks.base_task import ProcessorTaskBase
from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory, get_task, BaseTask


class TaskStatus(Enum):
    """任务执行状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskExecutionResult:
    """任务执行结果数据类"""
    task_name: str
    status: str
    rows_processed: int = 0
    execution_time: float = 0.0
    error_message: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "task_name": self.task_name,
            "status": self.status,
            "rows_processed": self.rows_processed,
            "execution_time": self.execution_time,
            "error_message": self.error_message,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "metadata": self.metadata
        }


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
            max_workers: 最大并发工作线程数（asyncio 信号量限制）
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
        
        # 任务状态追踪 (Requirements 5.2)
        self._task_status: Dict[str, TaskExecutionResult] = {}
        
        # asyncio 信号量用于并发控制 (Requirements 5.1)
        self._semaphore: Optional[asyncio.Semaphore] = None
        
        # 线程池（保留用于兼容性）
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        self.logger.info(f"处理引擎初始化完成，最大并发数: {max_workers}")
    
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
        
        # 初始化任务状态追踪 (Requirements 5.2)
        task_result = TaskExecutionResult(
            task_name=task_name,
            status=TaskStatus.RUNNING.value,
            start_time=start_time
        )
        self._task_status[task_name] = task_result
        
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
                task_result.status = TaskStatus.SUCCESS.value
            else:
                self._execution_stats["failed_tasks"] += 1
                task_result.status = TaskStatus.FAILED.value
            
            # 更新任务结果
            task_result.end_time = end_time
            task_result.execution_time = execution_time
            task_result.rows_processed = result.get("rows_processed", 0)
            
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
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            self._execution_stats["failed_tasks"] += 1
            
            # 更新任务状态为失败 (Requirements 5.2)
            task_result.status = TaskStatus.FAILED.value
            task_result.end_time = end_time
            task_result.execution_time = execution_time
            task_result.error_message = str(e)
            
            self.logger.error(f"任务 {task_name} 执行失败: {str(e)}", exc_info=True)
            
            return {
                "status": "error",
                "error": str(e),
                "task_name": task_name,
                "engine_metadata": {
                    "execution_time": execution_time,
                    "start_time": start_time,
                    "end_time": end_time
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
        """
        检查任务依赖
        
        **当前状态**：
        依赖检查功能尚未实现，仅保留扩展点。
        当前仅记录日志，不执行实际的依赖验证。
        
        **中长期实现方向**：
        1. 查询中央任务状态表（task_registry）
        2. 对每个依赖任务，检查其最新状态是否为 'success'
        3. 如果任何依赖项未成功完成，可以抛出异常或等待
        4. 支持依赖超时和重试策略
        
        **集成建议**：
        可以挂接到统一任务状态表，与监控系统集成。
        
        Args:
            task: 待检查的任务实例
        """
        if not hasattr(task, 'dependencies') or not task.dependencies:
            self.logger.debug("任务无依赖项")
            return
        
        self.logger.info(f"检查任务 {task.name} 的依赖: {task.dependencies}")
        self.logger.warning(
            f"依赖检查功能尚未实现，任务 {task.name} 的依赖 {task.dependencies} 未被验证"
        )
        
        # TODO: 实现依赖检查逻辑
        # 示例实现框架：
        # for dep_task_name in task.dependencies:
        #     dep_status = await self._query_task_status(dep_task_name)
        #     if dep_status != 'success':
        #         raise DependencyError(f"依赖任务 {dep_task_name} 未成功完成")
        
        pass
    
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
        """
        并行执行多个任务 (Requirements 5.1)
        
        使用 asyncio.Semaphore 控制并发数量，确保不超过 max_workers。
        失败的任务不会影响其他任务的执行 (Requirements 5.3)。
        """
        # 创建信号量控制并发数
        semaphore = asyncio.Semaphore(self.max_workers)
        
        async def execute_with_semaphore(task_name: str) -> tuple:
            """使用信号量包装的任务执行"""
            async with semaphore:
                try:
                    result = await self.execute_task(task_name, **kwargs)
                    return task_name, result
                except Exception as e:
                    self.logger.error(f"并行任务 {task_name} 执行失败: {e}", exc_info=True)
                    return task_name, {"status": "error", "error": str(e)}
        
        # 创建所有任务的协程
        tasks = [execute_with_semaphore(name) for name in task_names]
        
        # 使用 asyncio.gather 并发执行，return_exceptions=True 确保失败不影响其他任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        results = {}
        for i, result in enumerate(task_results):
            task_name = task_names[i]
            if isinstance(result, Exception):
                # 处理异常情况
                self.logger.error(f"任务 {task_name} 执行异常: {result}")
                results[task_name] = {"status": "error", "error": str(result)}
            elif isinstance(result, tuple):
                # 正常返回 (task_name, result)
                results[result[0]] = result[1]
            else:
                # 其他情况
                results[task_name] = result
        
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
    
    def get_task_status(self, task_name: str) -> Optional[TaskExecutionResult]:
        """
        获取指定任务的执行状态 (Requirements 5.2)
        
        Args:
            task_name: 任务名称
            
        Returns:
            TaskExecutionResult 或 None（如果任务未执行过）
        """
        return self._task_status.get(task_name)
    
    def get_all_task_status(self) -> Dict[str, TaskExecutionResult]:
        """
        获取所有任务的执行状态 (Requirements 5.2)
        
        Returns:
            任务名称到执行结果的映射
        """
        return self._task_status.copy()
    
    def get_failed_tasks(self) -> List[TaskExecutionResult]:
        """
        获取所有失败的任务 (Requirements 5.3)
        
        Returns:
            失败任务的执行结果列表
        """
        return [
            result for result in self._task_status.values()
            if result.status == TaskStatus.FAILED.value
        ]
    
    def get_successful_tasks(self) -> List[TaskExecutionResult]:
        """
        获取所有成功的任务
        
        Returns:
            成功任务的执行结果列表
        """
        return [
            result for result in self._task_status.values()
            if result.status == TaskStatus.SUCCESS.value
        ]
    
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
        self._task_status.clear()
        self.logger.info("处理器引擎统计信息已重置")
    
    def get_available_tasks(self, task_type: Optional[str] = "processor") -> List[str]:
        """
        通过任务注册表发现可用任务 (Requirements 5.4, 5.5)
        
        Args:
            task_type: 任务类型过滤，默认为 "processor"
            
        Returns:
            可用任务名称列表
        """
        try:
            return UnifiedTaskFactory.get_task_names_by_type(task_type)
        except RuntimeError:
            self.logger.warning("UnifiedTaskFactory 未初始化，无法获取任务列表")
            return []
    
    def get_task_info(self, task_name: str) -> Optional[Dict[str, Any]]:
        """
        获取任务详细信息 (Requirements 5.5)
        
        Args:
            task_name: 任务名称
            
        Returns:
            任务信息字典或 None
        """
        try:
            return UnifiedTaskFactory.get_task_info(task_name)
        except (RuntimeError, ValueError) as e:
            self.logger.warning(f"获取任务信息失败: {e}")
            return None
    
    def clear_cache(self):
        """清理引擎内部缓存（此版本中无缓存）"""
        self.logger.info("处理器引擎没有内部任务缓存，无需清理。")
    
    def generate_execution_report(self) -> Dict[str, Any]:
        """
        生成任务执行报告 (Requirements 5.2, 5.3)
        
        Returns:
            包含执行摘要和详细结果的报告
        """
        stats = self.get_stats()
        failed_tasks = self.get_failed_tasks()
        successful_tasks = self.get_successful_tasks()
        
        report = {
            "summary": {
                "total_tasks": stats["total_tasks"],
                "successful": stats["successful_tasks"],
                "failed": stats["failed_tasks"],
                "success_rate": stats["success_rate"],
                "total_execution_time": stats["total_execution_time"],
                "average_execution_time": stats["average_execution_time"],
            },
            "failed_tasks": [
                {
                    "task_name": t.task_name,
                    "error": t.error_message,
                    "execution_time": t.execution_time
                }
                for t in failed_tasks
            ],
            "successful_tasks": [
                {
                    "task_name": t.task_name,
                    "rows_processed": t.rows_processed,
                    "execution_time": t.execution_time
                }
                for t in successful_tasks
            ]
        }
        
        return report
    
    def shutdown(self):
        """关闭引擎的线程池"""
        self._executor.shutdown(wait=True)
        self.logger.info("处理引擎已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
