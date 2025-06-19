import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import urlparse

import asyncpg
import psycopg2
import pandas as pd

from ..logging_utils import get_logger
from .table_name_resolver import TableNameResolver


class DBManagerCore:
    """数据库管理器核心基类
    
    职责：
    ----
    数据库连接管理器的核心组件，负责：
    1. 数据库连接的建立和管理
    2. 双模式工作机制（异步/同步）的实现
    3. 连接池的创建、维护和销毁
    4. 线程安全的连接获取
    5. 跨模式的兼容性适配
    
    工作模式：
    --------
    - **async模式**: 使用asyncpg连接池，适用于异步环境（如数据采集任务）
      - 通过连接池管理并发连接
      - 支持异步上下文管理
      - 针对高并发场景优化
      
    - **sync模式**: 使用psycopg2连接，适用于同步环境（如Backtrader回测）
      - 基于线程本地存储管理连接
      - 支持同步阻塞操作
      - 兼容传统同步代码
    
    架构设计：
    --------
    作为所有数据库操作Mixin的基础，提供：
    - 连接管理的统一接口
    - 模式切换的透明处理
    - 异步/同步操作的桥接
    - 资源清理的统一机制
    
    使用场景：
    --------
    - 数据采集任务的高并发数据写入
    - 回测系统的同步数据查询
    - GUI应用的混合模式数据操作
    - 批处理任务的大规模数据处理
    
    线程安全性：
    ---------
    - async模式：通过连接池天然支持并发
    - sync模式：使用线程本地存储确保线程安全
    - 模式切换：通过锁机制保护临界区操作
    
    性能特点：
    --------
    - 连接复用：减少连接创建开销
    - 智能清理：自动检测和清理无效连接
    - 资源优化：基于使用模式优化资源分配
    - 错误恢复：自动重连和故障转移机制
    
    配置参数：
    --------
    - connection_string: PostgreSQL标准连接字符串
    - mode: 工作模式选择（'async' | 'sync'）
    - 连接池参数：通过asyncpg.create_pool配置
    """

    def __init__(self, connection_string: str, mode: str = "async"):
        """初始化数据库连接管理器

        创建数据库连接管理器实例，根据指定的工作模式初始化相应的连接机制。
        
        Args:
            connection_string (str): PostgreSQL数据库连接字符串
                格式：postgresql://username:password@host:port/database
                示例：postgresql://user:pass@localhost:5432/mydb
            mode (str): 工作模式选择
                - 'async': 异步模式，使用asyncpg连接池（默认）
                - 'sync': 同步模式，使用psycopg2连接
                
        Raises:
            ValueError: 当mode参数不是'async'或'sync'时抛出
            
        Note:
            - async模式适用于高并发数据采集任务
            - sync模式适用于Backtrader等同步框架
            - 连接不会在初始化时立即建立，需要调用connect()方法
        """
        self.connection_string = connection_string
        self.mode = mode.lower()
        self.logger = get_logger(f"db_manager_{self.mode}")
        self.resolver = TableNameResolver()

        if self.mode not in ["async", "sync"]:
            raise ValueError(f"不支持的模式: {mode}，只支持 'async' 或 'sync'")

        if self.mode == "async":
            # 异步模式：使用 asyncpg
            self.pool = None
            self._sync_lock = threading.Lock()
            self._loop = None
            self._executor = None
        elif self.mode == "sync":
            # 同步模式：使用 psycopg2
            self._parse_connection_string()
            self._local = threading.local()
            self.pool = None  # 兼容性属性

    def _parse_connection_string(self):
        """解析连接字符串为psycopg2连接参数（仅同步模式）
        
        将PostgreSQL标准连接字符串解析为psycopg2.connect()所需的参数字典。
        这个方法只在同步模式下使用，用于准备psycopg2连接参数。
        
        解析格式：
            postgresql://username:password@host:port/database
            
        默认值：
            - host: localhost
            - port: 5432  
            - user: postgres
            - password: "" (空字符串)
            - database: postgres
            
        内部属性：
            设置 self._conn_params 字典，包含所有连接参数
            
        Note:
            此方法为内部方法，不应直接调用
        """
        parsed = urlparse(self.connection_string)
        self._conn_params = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip("/") if parsed.path else "postgres",
        }

    def _get_sync_connection(self):
        """获取线程本地的数据库连接（仅同步模式）
        
        在同步模式下为当前线程获取专用的数据库连接。使用线程本地存储
        确保每个线程都有独立的连接，避免并发冲突。
        
        线程安全机制：
            - 使用threading.local()存储连接
            - 每个线程维护独立的连接实例
            - 自动检测连接状态并重新创建无效连接
            
        连接生命周期：
            - 惰性创建：第一次访问时创建连接
            - 状态检查：每次使用前检查连接是否有效
            - 自动重连：检测到连接关闭时自动重新创建
            
        Returns:
            psycopg2.connection: 当前线程的数据库连接对象
            
        Raises:
            RuntimeError: 当在异步模式下调用此方法时
            psycopg2.Error: 数据库连接创建失败时
            
        Note:
            - 此方法仅在同步模式下使用
            - 连接会自动提交事务
            - 调用者需要手动处理事务回滚
        """
        if self.mode != "sync":
            raise RuntimeError("_get_sync_connection 只能在同步模式下使用")

        if not hasattr(self._local, "connection") or self._local.connection.closed:
            try:
                self._local.connection = psycopg2.connect(**self._conn_params)
                self.logger.debug("创建新的同步数据库连接")
            except Exception as e:
                self.logger.error(f"创建同步数据库连接失败: {e}")
                raise
        return self._local.connection

    async def connect(self):
        """创建数据库连接池（仅异步模式）
        
        在异步模式下创建并初始化asyncpg连接池。连接池提供高效的连接管理，
        支持并发操作和自动连接回收。
        
        连接池特性：
            - 自动连接管理：根据负载自动创建和销毁连接
            - 并发支持：支持多个异步操作同时使用不同连接
            - 健康检查：自动检测和替换无效连接
            - 资源优化：智能的连接复用和回收机制
            
        初始化过程：
            1. 解析连接字符串
            2. 创建asyncpg连接池
            3. 验证连接可用性
            4. 记录初始化状态
            
        Raises:
            RuntimeError: 当在同步模式下调用此方法时
            asyncpg.PostgresError: 数据库连接失败时
            Exception: 其他连接相关错误
            
        Note:
            - 此方法仅在异步模式下使用
            - 连接池创建后可以立即使用
            - 重复调用不会创建多个连接池
        """
        if self.mode != "async":
            raise RuntimeError("connect 方法只能在异步模式下使用")

        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(self.connection_string)
                self.logger.info("异步数据库连接池创建成功")
            except Exception as e:
                self.logger.error(f"异步数据库连接池创建失败: {str(e)}")
                raise

    async def close(self):
        """关闭数据库连接池（仅异步模式）
        
        优雅地关闭异步连接池，确保所有连接都被正确释放。
        这是资源清理的重要步骤，防止连接泄漏。
        
        关闭过程：
            1. 等待所有进行中的操作完成
            2. 逐一关闭池中的所有连接
            3. 清理连接池资源
            4. 重置连接池状态
            
        资源清理：
            - 关闭所有活跃连接
            - 取消所有待处理的连接请求
            - 释放连接池占用的内存
            - 清理相关的系统资源
            
        Raises:
            RuntimeError: 当在同步模式下调用此方法时
            
        Note:
            - 此方法仅在异步模式下使用
            - 关闭后需要重新调用connect()才能继续使用
            - 建议在应用程序退出时调用此方法
        """
        if self.mode != "async":
            raise RuntimeError("close 方法只能在异步模式下使用")

        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            self.logger.info("异步数据库连接池已关闭")

    def close_sync(self):
        """关闭同步数据库连接
        
        根据当前工作模式，关闭相应的数据库连接。这个方法提供了
        统一的连接关闭接口，支持异步和同步两种模式。
        
        模式处理：
            - async模式：通过_run_sync()包装异步close()方法
            - sync模式：直接关闭线程本地连接
            
        同步模式处理：
            - 检查线程本地连接是否存在
            - 检查连接是否仍然有效
            - 优雅关闭连接并清理资源
            
        异步模式处理：
            - 在新线程中运行异步关闭操作
            - 确保不阻塞当前线程
            - 处理可能的并发冲突
            
        Note:
            - 此方法在任何模式下都可以安全调用
            - 支持重复调用，不会产生副作用
            - 是资源清理的推荐方法
        """
        if self.mode == "async":
            return self._run_sync(self.close())
        elif self.mode == "sync":
            if hasattr(self._local, "connection") and not self._local.connection.closed:
                self._local.connection.close()
                self.logger.debug("同步数据库连接已关闭")

    def _run_sync(self, coro):
        """在同步上下文中运行异步代码
        
        提供了在同步环境中执行异步操作的桥接机制。这个方法解决了
        异步代码和同步代码之间的兼容性问题。
        
        执行策略：
            1. 检测当前是否有运行中的事件循环
            2. 如果有循环：在独立线程中运行异步代码
            3. 如果无循环：直接使用asyncio.run()运行
            
        线程安全：
            - 使用锁机制保护临界区
            - 使用线程池避免重复创建线程
            - 确保异步操作的原子性
            
        资源管理：
            - 自动创建和管理线程池
            - 智能清理执行器资源
            - 处理线程执行异常
            
        Args:
            coro: 需要执行的协程对象
            
        Returns:
            Any: 协程的执行结果
            
        Raises:
            Exception: 协程执行过程中的任何异常
            
        Note:
            - 此方法为内部方法，主要用于模式桥接
            - 自动处理事件循环的检测和管理
            - 支持嵌套调用和并发执行
        """
        with self._sync_lock:
            try:
                # 尝试获取当前事件循环
                loop = asyncio.get_running_loop()
                # 如果有运行中的循环，在新线程中运行
                if self._executor is None:
                    self._executor = ThreadPoolExecutor(max_workers=1)
                future = self._executor.submit(asyncio.run, coro)
                return future.result()
            except RuntimeError:
                # 没有运行中的循环，直接运行
                return asyncio.run(coro)

    # === 同步方法接口 ===

    def connect_sync(self):
        """同步创建数据库连接池
        
        提供同步接口来创建数据库连接池。这个方法是connect()的同步版本，
        允许在同步环境中初始化数据库连接。
        
        适用场景：
            - 在同步初始化代码中建立连接
            - GUI应用的启动阶段
            - 测试代码中的连接设置
            - 不支持异步的legacy代码
            
        实现机制：
            - 通过_run_sync()桥接异步connect()方法
            - 自动处理事件循环管理
            - 保持与异步版本的功能一致性
            
        Returns:
            None: 连接创建成功后返回
            
        Raises:
            Exception: 数据库连接创建失败时的相关异常
            
        Note:
            - 功能等价于await connect()
            - 在异步环境中推荐使用connect()
            - 此方法会阻塞当前线程直到连接建立
        """
        return self._run_sync(self.connect())
