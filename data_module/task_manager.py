import asyncio
import logging
import datetime
from typing import Dict, List, Any, Type, Optional, Union, Set
from collections import defaultdict

from .task import Task


class TaskManager:
    """
    任务管理器，负责管理和执行数据任务
    
    主要功能：
    1. 注册和管理任务
    2. 处理任务依赖关系
    3. 执行任务（包括全量更新和增量更新）
    4. 任务状态跟踪
    """
    
    def __init__(self, db_manager=None):
        """
        初始化任务管理器
        
        Args:
            db_manager: 数据库管理器实例，如果为None则任务需要自行提供数据库连接
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_manager = db_manager
        
        # 注册的任务类字典，键为任务名称，值为任务类
        self.registered_tasks: Dict[str, Type[Task]] = {}
        
        # 任务实例缓存，键为任务名称，值为任务实例
        self.task_instances: Dict[str, Task] = {}
        
        # 任务依赖图，键为任务名称，值为依赖该任务的任务集合
        self.dependency_graph: Dict[str, Set[str]] = defaultdict(set)
        
        # 任务执行历史，键为任务ID，值为任务执行记录
        self.task_history: Dict[str, Dict[str, Any]] = {}
        
        # 任务执行计数器，用于生成唯一的任务ID
        self.task_counter = 0
    
    def register_task(self, task_class: Type[Task]) -> None:
        """
        注册任务类
        
        Args:
            task_class: 任务类，必须是Task的子类
        """
        if not issubclass(task_class, Task):
            raise TypeError(f"任务类必须是Task的子类，而不是{task_class.__name__}")
        
        task_name = task_class.name
        if task_name in self.registered_tasks:
            self.logger.warning(f"任务'{task_name}'已被注册，将被覆盖")
        
        self.registered_tasks[task_name] = task_class
        self.logger.info(f"任务'{task_name}'注册成功")
        
        # 更新依赖图
        self._update_dependency_graph(task_class)
    
    def _update_dependency_graph(self, task_class: Type[Task]) -> None:
        """
        更新任务依赖图
        
        Args:
            task_class: 任务类
        """
        task_name = task_class.name
        dependencies = getattr(task_class, 'dependencies', [])
        
        for dep in dependencies:
            # 将当前任务添加到其依赖的任务的依赖者列表中
            self.dependency_graph[dep].add(task_name)
    
    def get_task_instance(self, task_name: str) -> Task:
        """
        获取任务实例，如果不存在则创建
        
        Args:
            task_name: 任务名称
            
        Returns:
            任务实例
        """
        if task_name not in self.registered_tasks:
            raise ValueError(f"任务'{task_name}'未注册")
        
        if task_name not in self.task_instances:
            task_class = self.registered_tasks[task_name]
            self.task_instances[task_name] = task_class(db_manager=self.db_manager)
        
        return self.task_instances[task_name]
    
    def get_dependent_tasks(self, task_name: str) -> List[str]:
        """
        获取依赖指定任务的所有任务
        
        Args:
            task_name: 任务名称
            
        Returns:
            依赖该任务的任务名称列表
        """
        return list(self.dependency_graph.get(task_name, set()))
    
    def get_dependencies(self, task_name: str) -> List[str]:
        """
        获取指定任务的所有依赖
        
        Args:
            task_name: 任务名称
            
        Returns:
            该任务依赖的任务名称列表
        """
        if task_name not in self.registered_tasks:
            raise ValueError(f"任务'{task_name}'未注册")
        
        task_class = self.registered_tasks[task_name]
        return getattr(task_class, 'dependencies', [])
    
    def _generate_task_id(self) -> str:
        """
        生成唯一的任务ID
        
        Returns:
            任务ID
        """
        self.task_counter += 1
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return f"task_{timestamp}_{self.task_counter}"
    
    def _record_task_execution(self, task_name: str, start_date: str, end_date: str, 
                              params: Dict[str, Any], status: str, result: Any = None, 
                              error: Exception = None) -> str:
        """
        记录任务执行
        
        Args:
            task_name: 任务名称
            start_date: 开始日期
            end_date: 结束日期
            params: 执行参数
            status: 执行状态（'started', 'completed', 'failed'）
            result: 执行结果
            error: 执行错误
            
        Returns:
            任务ID
        """
        task_id = self._generate_task_id()
        
        self.task_history[task_id] = {
            'task_name': task_name,
            'start_date': start_date,
            'end_date': end_date,
            'params': params,
            'status': status,
            'start_time': datetime.datetime.now(),
            'end_time': None,
            'result': result,
            'error': str(error) if error else None
        }
        
        return task_id
    
    def _update_task_execution(self, task_id: str, status: str, result: Any = None, 
                              error: Exception = None) -> None:
        """
        更新任务执行记录
        
        Args:
            task_id: 任务ID
            status: 执行状态
            result: 执行结果
            error: 执行错误
        """
        if task_id not in self.task_history:
            raise ValueError(f"任务ID'{task_id}'不存在")
        
        self.task_history[task_id].update({
            'status': status,
            'end_time': datetime.datetime.now(),
            'result': result,
            'error': str(error) if error else None
        })
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        获取任务执行状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务执行记录
        """
        if task_id not in self.task_history:
            raise ValueError(f"任务ID'{task_id}'不存在")
        
        return self.task_history[task_id]
    
    async def execute_task(self, task_name: str, start_date: str = None, end_date: str = None, 
                         execute_dependencies: bool = True, **kwargs) -> Dict[str, Any]:
        """
        执行任务
        
        Args:
            task_name: 任务名称
            start_date: 开始日期，如果为None则使用任务默认值
            end_date: 结束日期，如果为None则使用任务默认值
            execute_dependencies: 是否执行依赖任务
            **kwargs: 其他执行参数
            
        Returns:
            任务执行结果
        """
        if task_name not in self.registered_tasks:
            raise ValueError(f"任务'{task_name}'未注册")
        
        # 记录任务开始执行
        params = {'start_date': start_date, 'end_date': end_date, **kwargs}
        task_id = self._record_task_execution(task_name, start_date, end_date, params, 'started')
        
        try:
            # 如果需要执行依赖任务
            if execute_dependencies:
                await self._execute_dependencies(task_name, start_date, end_date, **kwargs)
            
            # 获取任务实例并执行
            task = self.get_task_instance(task_name)
            result = await task.execute(start_date, end_date, **kwargs)
            
            # 更新任务执行记录
            self._update_task_execution(task_id, 'completed', result)
            
            return {
                'task_id': task_id,
                'status': 'completed',
                'result': result
            }
            
        except Exception as e:
            self.logger.error(f"任务'{task_name}'执行失败: {str(e)}", exc_info=True)
            
            # 更新任务执行记录
            self._update_task_execution(task_id, 'failed', error=e)
            
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': str(e)
            }
    
    async def _execute_dependencies(self, task_name: str, start_date: str = None, 
                                 end_date: str = None, **kwargs) -> List[Dict[str, Any]]:
        """
        执行任务的依赖任务
        
        Args:
            task_name: 任务名称
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他执行参数
            
        Returns:
            依赖任务执行结果列表
        """
        dependencies = self.get_dependencies(task_name)
        if not dependencies:
            return []
        
        self.logger.info(f"执行任务'{task_name}'的依赖任务: {dependencies}")
        
        # 并行执行所有依赖任务
        tasks = [self.execute_task(dep, start_date, end_date, True, **kwargs) 
                for dep in dependencies]
        
        return await asyncio.gather(*tasks)
    
    async def full_update(self, task_names: Union[str, List[str]], start_date: str = None, 
                        end_date: str = None, **kwargs) -> Dict[str, Any]:
        """
        执行全量更新
        
        Args:
            task_names: 任务名称或名称列表
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他执行参数
            
        Returns:
            任务执行结果
        """
        if isinstance(task_names, str):
            task_names = [task_names]
        
        self.logger.info(f"开始全量更新任务: {task_names}")
        
        results = {}
        for task_name in task_names:
            result = await self.execute_task(task_name, start_date, end_date, **kwargs)
            results[task_name] = result
        
        return results
    
    async def incremental_update(self, task_names: Union[str, List[str]], 
                               days_lookback: int = 7, **kwargs) -> Dict[str, Any]:
        """
        执行增量更新
        
        Args:
            task_names: 任务名称或名称列表
            days_lookback: 回溯天数
            **kwargs: 其他执行参数
            
        Returns:
            任务执行结果
        """
        if isinstance(task_names, str):
            task_names = [task_names]
        
        # 计算开始日期和结束日期
        end_date = datetime.date.today().strftime('%Y%m%d')
        start_date = (datetime.date.today() - datetime.timedelta(days=days_lookback)).strftime('%Y%m%d')
        
        self.logger.info(f"开始增量更新任务: {task_names}, 日期范围: {start_date} - {end_date}")
        
        return await self.full_update(task_names, start_date, end_date, **kwargs)
    
    def get_all_registered_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有注册的任务信息
        
        Returns:
            任务信息字典，键为任务名称，值为任务信息
        """
        result = {}
        
        for task_name, task_class in self.registered_tasks.items():
            # 获取任务的基本信息
            task_info = {
                'name': task_name,
                'description': getattr(task_class, 'description', ''),
                'table_name': getattr(task_class, 'table_name', ''),
                'dependencies': getattr(task_class, 'dependencies', []),
                'dependent_tasks': list(self.dependency_graph.get(task_name, set()))
            }
            
            result[task_name] = task_info
        
        return result
    
    def get_task_dependency_tree(self, task_name: str = None) -> Dict[str, Any]:
        """
        获取任务依赖树
        
        Args:
            task_name: 任务名称，如果为None则返回所有任务的依赖树
            
        Returns:
            任务依赖树
        """
        if task_name is not None and task_name not in self.registered_tasks:
            raise ValueError(f"任务'{task_name}'未注册")
        
        def build_tree(name):
            dependencies = self.get_dependencies(name)
            return {
                'name': name,
                'dependencies': [build_tree(dep) for dep in dependencies]
            }
        
        if task_name:
            return build_tree(task_name)
        else:
            # 找出所有没有被依赖的任务（顶层任务）
            all_tasks = set(self.registered_tasks.keys())
            dependent_tasks = set()
            for deps in self.dependency_graph.values():
                dependent_tasks.update(deps)
            
            top_level_tasks = all_tasks - dependent_tasks
            
            return [build_tree(task) for task in top_level_tasks]
    
    async def validate_task_dependencies(self) -> Dict[str, List[str]]:
        """
        验证任务依赖关系是否有效
        
        Returns:
            无效依赖字典，键为任务名称，值为无效依赖列表
        """
        invalid_dependencies = {}
        
        for task_name, task_class in self.registered_tasks.items():
            dependencies = getattr(task_class, 'dependencies', [])
            invalid = [dep for dep in dependencies if dep not in self.registered_tasks]
            
            if invalid:
                invalid_dependencies[task_name] = invalid
        
        return invalid_dependencies
    
    async def run_scheduled_update(self, schedule_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据调度配置执行定时更新
        
        Args:
            schedule_config: 调度配置，格式为：
                {
                    'task_name': {
                        'type': 'full' | 'incremental',
                        'days_lookback': 7,  # 仅对增量更新有效
                        'params': {...}  # 其他执行参数
                    },
                    ...
                }
            
        Returns:
            任务执行结果
        """
        results = {}
        
        for task_name, config in schedule_config.items():
            if task_name not in self.registered_tasks:
                self.logger.warning(f"任务'{task_name}'未注册，跳过调度")
                continue
            
            update_type = config.get('type', 'incremental')
            params = config.get('params', {})
            
            if update_type == 'full':
                start_date = config.get('start_date')
                end_date = config.get('end_date')
                result = await self.full_update(task_name, start_date, end_date, **params)
            else:  # incremental
                days_lookback = config.get('days_lookback', 7)
                result = await self.incremental_update(task_name, days_lookback, **params)
            
            results[task_name] = result
        
        return results
    
    def clear_task_history(self, days_to_keep: int = 30) -> int:
        """
        清理任务执行历史
        
        Args:
            days_to_keep: 保留的天数
            
        Returns:
            清理的记录数
        """
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
        keys_to_remove = []
        
        for task_id, record in self.task_history.items():
            start_time = record.get('start_time')
            if start_time and start_time < cutoff_date:
                keys_to_remove.append(task_id)
        
        for key in keys_to_remove:
            del self.task_history[key]
        
        self.logger.info(f"清理了{len(keys_to_remove)}条任务执行历史记录")
        return len(keys_to_remove)
