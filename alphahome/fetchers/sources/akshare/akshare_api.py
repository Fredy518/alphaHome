#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare API 封装层

提供对 akshare 库的异步封装，包括：
- 同步到异步的适配（akshare 原生是同步库）
- 请求间隔控制（避免触发限流/IP封禁）
- 重试机制
- 统一的错误处理和日志记录
"""

import asyncio
import logging
import time
from functools import partial
from typing import Any, Callable, Dict, Optional

import pandas as pd

try:
    import akshare as ak
except ImportError:
    ak = None  # akshare not installed; will be checked on use

from .stock_limitup_reason_ext import stock_limitup_reason
from .index_cons_csindex_ext import index_stock_cons_csindex

class AkShareAPIError(Exception):
    """AkShare API 调用错误"""
    pass


class AkShareRateLimitError(AkShareAPIError):
    """AkShare 限流错误"""
    pass


class AkShareAPI:
    """
    AkShare API 客户端

    封装 akshare 库的同步调用，提供异步接口和限流保护。

    特点：
    1. akshare 是同步库，使用 asyncio.to_thread 在异步环境中运行
    2. 实现请求间隔控制，默认每次请求间隔 1-2 秒
    3. 自动重试机制，处理网络波动
    4. 统一的错误处理和日志记录

    使用示例：
        api = AkShareAPI(logger=logging.getLogger("akshare"))
        df = await api.call("bond_zh_us_rate", start_date="19901219")
    """

    # 默认配置
    DEFAULT_REQUEST_INTERVAL = 1.5  # 默认请求间隔（秒）
    DEFAULT_MAX_RETRIES = 3  # 默认最大重试次数
    DEFAULT_RETRY_DELAY = 5  # 默认重试等待时间（秒）

    # 已知的限流相关错误关键词
    RATE_LIMIT_KEYWORDS = [
        "频繁", "限流", "rate limit", "too many requests",
        "请求过于频繁", "访问频率", "ip", "banned", "blocked"
    ]


    # extra akshare-like functions in this project
    EXTRA_FUNCS: Dict[str, Callable[..., Any]] = {
        "stock_limitup_reason": stock_limitup_reason,
        # Override buggy akshare impl for .xls reading
        "index_stock_cons_csindex": index_stock_cons_csindex,
    }
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        request_interval: float = DEFAULT_REQUEST_INTERVAL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
    ):
        """
        初始化 AkShare API 客户端

        Args:
            logger: 日志记录器实例
            request_interval: 请求间隔时间（秒），默认 1.5 秒
            max_retries: 最大重试次数，默认 3 次
            retry_delay: 重试等待时间（秒），默认 5 秒
        """
        self.logger = logger or logging.getLogger(__name__)
        self.request_interval = request_interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # 上次请求时间（用于控制请求间隔）
        self._last_request_time: float = 0.0

        # 请求锁（确保请求间隔控制的原子性）
        self._request_lock = asyncio.Lock()

        # 验证 akshare 是否已安装
        if ak is None:
            self.logger.warning(
                "akshare 库未安装，请使用 'pip install akshare' 安装"
            )

    def _check_akshare_available(self):
        """检查 akshare 库是否可用"""
        if ak is None:
            raise AkShareAPIError(
                "akshare 库未安装，请使用 'pip install akshare' 安装"
            )

    async def _wait_for_rate_limit(self):
        """等待请求间隔，避免触发限流"""
        async with self._request_lock:
            current_time = time.monotonic()
            elapsed = current_time - self._last_request_time

            if elapsed < self.request_interval:
                wait_time = self.request_interval - elapsed
                self.logger.debug(f"等待 {wait_time:.2f} 秒以满足请求间隔要求")
                await asyncio.sleep(wait_time)

            self._last_request_time = time.monotonic()

    def _is_rate_limit_error(self, error: Exception) -> bool:
        """判断是否为限流相关错误"""
        error_str = str(error).lower()
        return any(keyword in error_str for keyword in self.RATE_LIMIT_KEYWORDS)

    async def call(
        self,
        func_name: str,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        调用 akshare 函数

        Args:
            func_name: akshare 函数名称（如 "bond_zh_us_rate"）
            stop_event: 可选的停止事件，用于取消操作
            **kwargs: 传递给 akshare 函数的参数

        Returns:
            DataFrame 或 None（如果调用失败）

        Raises:
            AkShareAPIError: API 调用错误
            asyncio.CancelledError: 操作被取消
        """
        # 优先使用项目中扩展的 akshare 风格函数
        if func_name in self.EXTRA_FUNCS:
            func = self.EXTRA_FUNCS[func_name]
        else:
            self._check_akshare_available()

            # ��ȡ akshare ����
            func = getattr(ak, func_name, None)
            if func is None:
                raise AkShareAPIError(f"akshare �в����ں���: {func_name}")

        self.logger.info(f"���� akshare.{func_name}������: {kwargs}")

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            # 检查停止事件
            if stop_event and stop_event.is_set():
                self.logger.warning(f"akshare.{func_name} 调用被取消")
                raise asyncio.CancelledError("操作被用户取消")

            try:
                # 等待请求间隔
                await self._wait_for_rate_limit()

                # 在线程池中执行同步的 akshare 调用
                # 使用 partial 绑定参数
                bound_func = partial(func, **kwargs)
                result = await asyncio.to_thread(bound_func)

                # 验证返回结果
                if result is None:
                    self.logger.warning(f"akshare.{func_name} 返回 None")
                    return None

                if isinstance(result, pd.DataFrame):
                    self.logger.info(
                        f"akshare.{func_name} 成功返回 {len(result)} 行数据"
                    )

                    # 特殊处理 bond_zh_us_rate 的列名编码问题
                    if func_name == "bond_zh_us_rate" and len(result.columns) == 13:
                        # 直接根据列的位置重命名，避免编码问题
                        column_names = [
                            "date",           # 0: 日期
                            "CN_2y",          # 1: 中国国债收益率2年
                            "CN_5y",          # 2: 中国国债收益率5年
                            "CN_10y",         # 3: 中国国债收益率10年
                            "CN_30y",         # 4: 中国国债收益率30年
                            "CN_10y_2y_spread", # 5: 中国国债收益率10年-2年
                            "CN_GDP",         # 6: 中国GDP增长率
                            "US_2y",          # 7: 美国国债收益率2年
                            "US_5y",          # 8: 美国国债收益率5年
                            "US_10y",         # 9: 美国国债收益率10年
                            "US_30y",         # 10: 美国国债收益率30年
                            "US_10y_2y_spread", # 11: 美国国债收益率10年-2年
                            "US_GDP",         # 12: 美国GDP增长率
                        ]
                        result.columns = column_names
                        self.logger.debug(f"已重命名 bond_zh_us_rate 列名，共 {len(column_names)} 列")

                    return result
                else:
                    self.logger.warning(
                        f"akshare.{func_name} 返回非 DataFrame 类型: {type(result)}"
                    )
                    return result

            except asyncio.CancelledError:
                raise  # 直接传播取消事件

            except Exception as e:
                last_error = e
                error_msg = str(e)

                # 检查是否为限流错误
                if self._is_rate_limit_error(e):
                    self.logger.warning(
                        f"akshare.{func_name} 疑似触发限流（尝试 {attempt}/{self.max_retries}）: {error_msg}"
                    )
                    # 限流错误使用更长的等待时间
                    wait_time = self.retry_delay * (attempt + 1)
                else:
                    self.logger.warning(
                        f"akshare.{func_name} 调用失败（尝试 {attempt}/{self.max_retries}）: {error_msg}"
                    )
                    wait_time = self.retry_delay * attempt

                # 如果还有重试机会，等待后重试
                if attempt < self.max_retries:
                    self.logger.info(f"将在 {wait_time:.1f} 秒后重试...")

                    # 分段等待，支持取消
                    for _ in range(int(wait_time)):
                        if stop_event and stop_event.is_set():
                            raise asyncio.CancelledError("操作被用户取消")
                        await asyncio.sleep(1)

        # 所有重试都失败
        self.logger.error(
            f"akshare.{func_name} 在 {self.max_retries} 次尝试后仍然失败: {last_error}"
        )
        raise AkShareAPIError(
            f"akshare.{func_name} 调用失败: {last_error}"
        ) from last_error

    async def call_with_callback(
        self,
        func_name: str,
        callback: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        调用 akshare 函数并应用回调处理

        Args:
            func_name: akshare 函数名称
            callback: 可选的回调函数，用于处理返回的 DataFrame
            stop_event: 可选的停止事件
            **kwargs: 传递给 akshare 函数的参数

        Returns:
            处理后的 DataFrame 或 None
        """
        result = await self.call(func_name, stop_event=stop_event, **kwargs)

        if result is not None and callback is not None:
            try:
                result = callback(result)
                self.logger.debug(f"回调处理完成，结果行数: {len(result) if isinstance(result, pd.DataFrame) else 'N/A'}")
            except Exception as e:
                self.logger.error(f"回调处理失败: {e}", exc_info=True)
                raise AkShareAPIError(f"数据回调处理失败: {e}") from e

        return result

    def get_available_functions(self) -> list:
        """
        获取 akshare 中所有可用的函数列表

        Returns:
            函数名称列表
        """
        self._check_akshare_available()

        return [
            name for name in dir(ak)
            if not name.startswith('_') and callable(getattr(ak, name, None))
        ]

    def function_exists(self, func_name: str) -> bool:
        """
        检查指定函数是否存在于 akshare 中

        Args:
            func_name: 函数名称

        Returns:
            是否存在
        """
        self._check_akshare_available()
        return hasattr(ak, func_name) and callable(getattr(ak, func_name))
