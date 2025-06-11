#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据采集任务基类

专门用于从外部API获取数据的任务类型。
继承自统一的BaseTask，但专注于数据采集的特定需求。
"""

from ..common.task_system.base_task import BaseTask


class FetchTask(BaseTask):
    """数据采集任务基类
    
    专门用于从外部数据源（如API、文件等）获取数据的任务。
    相比通用的BaseTask，FetchTask专注于数据采集的特定需求。
    """
    
    # 设置任务类型为fetch
    task_type: str = "fetch"
    
    # fetch任务特有的属性
    api_rate_limit = None     # API调用频率限制 (calls/second)
    retry_times = 3           # 重试次数
    timeout = 30              # 请求超时时间(秒)
    
    def __init__(self, db_connection, api_token=None):
        """初始化采集任务
        
        Args:
            db_connection: 数据库连接
            api_token: API令牌（如果需要）
        """
        super().__init__(db_connection)
        self.api_token = api_token
        
        # 设置采集任务专用的日志记录器
        import logging
        self.logger = logging.getLogger(f"fetch_task.{self.name}")
    
    async def fetch_data(self, stop_event=None, **kwargs):
        """从外部数据源获取数据
        
        这是采集任务的核心方法，子类必须实现具体的数据获取逻辑。
        """
        raise NotImplementedError("FetchTask子类必须实现fetch_data方法")
    
    def validate_api_response(self, response_data):
        """验证API响应数据的有效性
        
        Args:
            response_data: API返回的原始数据
            
        Returns:
            bool: 数据是否有效
        """
        if response_data is None:
            self.logger.warning("API响应为None")
            return False
            
        # 如果是pandas DataFrame
        if hasattr(response_data, 'empty'):
            if response_data.empty:
                self.logger.warning("API响应为空DataFrame")
                return False
        
        # 如果是list或dict
        elif isinstance(response_data, (list, dict)):
            if not response_data:
                self.logger.warning("API响应为空列表或字典")
                return False
        
        return True
    
    def handle_api_error(self, error, context=""):
        """处理API请求错误
        
        Args:
            error: 错误对象
            context: 错误上下文信息
        """
        error_msg = f"API请求失败 {context}: {str(error)}"
        self.logger.error(error_msg)
        
        # 可以在这里添加特定的错误处理逻辑
        # 比如根据错误类型决定是否重试
        
        return {"status": "api_error", "error": error_msg, "task": self.name}
    
    async def pre_execute(self, stop_event=None, **kwargs):
        """采集任务执行前的准备工作"""
        await super().pre_execute(stop_event=stop_event, **kwargs)
        
        # 检查API令牌（如果需要）
        if hasattr(self, 'requires_token') and self.requires_token:
            if not self.api_token:
                raise ValueError(f"任务 {self.name} 需要API令牌但未提供")
        
        self.logger.info(f"开始执行数据采集任务: {self.name}")
    
    async def post_execute(self, result, stop_event=None, **kwargs):
        """采集任务执行后的清理工作"""
        await super().post_execute(result, stop_event=stop_event, **kwargs)
        
        # 记录采集统计信息
        if result.get("status") == "success":
            rows = result.get("rows", 0)
            self.logger.info(f"数据采集完成: {self.name}, 获取数据 {rows} 行")
        else:
            self.logger.warning(f"数据采集未成功: {self.name}, 状态: {result.get('status')}")


# 为了向后兼容，保持Task类的存在（从fetchers.base_task导入）
# 但建议新的采集任务继承FetchTask
from .base_task import Task 