import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import urlparse

import asyncpg
import psycopg2

from ..logging_utils import get_logger


class DBManagerCore:
    """数据库管理器核心基类 - 负责连接管理和模式切换

    支持两种工作模式：
    - async: 使用 asyncpg，适用于异步环境（如 fetchers）
    - sync: 使用 psycopg2，适用于同步环境（如 Backtrader）
    """

    def __init__(self, connection_string: str, mode: str = "async"):
        """初始化数据库连接管理器

        Args:
            connection_string (str): 数据库连接字符串，格式为：
                `postgresql://username:password@host:port/database`
            mode (str): 工作模式，'async' 或 'sync'
        """
        self.connection_string = connection_string
        self.mode = mode.lower()
        self.logger = get_logger(f"db_manager_{self.mode}")

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
        """解析连接字符串为psycopg2连接参数（仅同步模式）"""
        parsed = urlparse(self.connection_string)
        self._conn_params = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip("/") if parsed.path else "postgres",
        }

    def _get_sync_connection(self):
        """获取线程本地的数据库连接（仅同步模式）"""
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
        """创建数据库连接池（仅异步模式）"""
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
        """关闭数据库连接池（仅异步模式）"""
        if self.mode != "async":
            raise RuntimeError("close 方法只能在异步模式下使用")

        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            self.logger.info("异步数据库连接池已关闭")

    def close_sync(self):
        """关闭同步数据库连接"""
        if self.mode == "async":
            return self._run_sync(self.close())
        elif self.mode == "sync":
            if hasattr(self._local, "connection") and not self._local.connection.closed:
                self._local.connection.close()
                self.logger.debug("同步数据库连接已关闭")

    def _run_sync(self, coro):
        """在同步上下文中运行异步代码"""
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
        """同步创建数据库连接池"""
        return self._run_sync(self.connect())
