import asyncio
import collections
import logging
import os
import time
from typing import Any, Dict, List, Optional, Union

import aiohttp
import pandas as pd


class TushareAPI:
    """Tushare API 客户端，负责处理与 Tushare 的 HTTP 通信"""

    # --- 新的速率和并发控制配置 ---
    # 1. 每分钟最大请求数 (用于滑动窗口速率控制)
    _api_max_requests_per_minute: Dict[str, int] = {
        "daily": 800,  # 股票日线数据 (示例值，请根据Tushare文档调整)
        "stock_basic": 200,  # 股票基本信息 (示例值)
        "trade_cal": 100,  # 交易日历 (示例值)
        "index_weight": 500,  # 指数成分和权重 (明确设为每分钟500次)
        # ... 其他API ...
    }
    _default_max_requests_per_minute: int = 100  # 未指定API的默认每分钟请求数

    # 2. 并发请求数上限 (用于 asyncio.Semaphore)
    _api_concurrency_limits: Dict[str, int] = {
        "daily": 80,  # 示例并发
        "stock_basic": 20,  # 示例并发
        "trade_cal": 10,
        "index_weight": 50,  # 示例并发 (例如，设为50，而不是速率的500)
        # ... 其他API ...
    }
    _default_concurrency_limit: int = 20  # 未指定API的默认并发数

    _rate_limit_window_seconds: int = 60  # 速率控制的时间窗口 (60秒 = 1分钟)

    # --- 运行时实例存储 ---
    _api_semaphores: Dict[str, asyncio.Semaphore] = {}  # 并发信号量实例
    _api_request_timestamps: Dict[str, collections.deque] = {}  # 滑动窗口时间戳记录

    # 旧的配置 (将被上面的新配置取代或整合)
    # _api_rate_limits = { ... } # 将被 _api_max_requests_per_minute 和 _api_concurrency_limits 取代
    # _default_limit = 50        # 将被 _default_max_requests_per_minute 和 _default_concurrency_limit 取代

    def __init__(self, token=None, logger=None):
        """初始化 TushareAPI 客户端"""
        self.token = token or os.environ.get("TUSHARE_TOKEN")
        if not self.token:
            raise ValueError(
                "必须提供Tushare API令牌，可以通过参数传入或设置TUSHARE_TOKEN环境变量"
            )

        self.url = "http://api.tushare.pro"
        self.logger = logger or logging.getLogger(__name__)

        # 为所有预定义的API初始化信号量和时间戳队列 (类级别共享，但在此确保实例创建)
        # 合并已知API列表，避免重复
        all_known_apis = set(self._api_max_requests_per_minute.keys()) | set(
            self._api_concurrency_limits.keys()
        )

        for api_name in all_known_apis:
            # 初始化信号量
            if api_name not in TushareAPI._api_semaphores:
                limit = self._api_concurrency_limits.get(
                    api_name, self._default_concurrency_limit
                )
                TushareAPI._api_semaphores[api_name] = asyncio.Semaphore(limit)
                if self.logger:
                    self.logger.debug(
                        f"为 API {api_name} 创建并发信号量，限制: {limit}"
                    )

            # 初始化时间戳队列
            if api_name not in TushareAPI._api_request_timestamps:
                TushareAPI._api_request_timestamps[api_name] = collections.deque()
                if self.logger:
                    self.logger.debug(f"为 API {api_name} 创建速率控制时间戳队列")

    # @classmethod (set_api_rate_limit, set_default_rate_limit) 需要更新以适应新配置结构
    # 例如: set_api_max_requests(api_name, count_per_minute) 和 set_api_concurrency(api_name, count)

    async def _wait_for_rate_limit_slot(self, api_name: str):
        """使用滑动窗口日志算法等待速率限制的空位"""
        if api_name not in TushareAPI._api_request_timestamps:
            # 对于动态遇到的、未在预定义列表中的API，也需要初始化
            TushareAPI._api_request_timestamps[api_name] = collections.deque()
            if self.logger:
                self.logger.debug(
                    f"动态为 API {api_name} 创建速率控制时间戳队列 (使用默认速率)"
                )

        timestamps_deque = TushareAPI._api_request_timestamps[api_name]
        limit_per_window = self._api_max_requests_per_minute.get(
            api_name, self._default_max_requests_per_minute
        )

        while True:
            current_time = time.monotonic()

            while (
                timestamps_deque
                and timestamps_deque[0]
                <= current_time - self._rate_limit_window_seconds
            ):
                timestamps_deque.popleft()

            if len(timestamps_deque) < limit_per_window:
                timestamps_deque.append(current_time)
                self.logger.debug(
                    f"速率控制 ({api_name}): 允许请求。窗口内 {self._rate_limit_window_seconds}s 请求数: {len(timestamps_deque)}/{limit_per_window}"
                )
                break
            else:
                time_to_wait = (
                    (timestamps_deque[0] + self._rate_limit_window_seconds)
                    - current_time
                    + 0.01
                )
                if time_to_wait <= 0:
                    time_to_wait = 0.05  # 最小等待时间，避免潜在的CPU空转

                self.logger.debug(
                    f"速率控制 ({api_name}): 超出限制 ({len(timestamps_deque)}/{limit_per_window})。等待 {time_to_wait:.2f} 秒... (队列首位时间戳: {timestamps_deque[0] if timestamps_deque else 'N/A'})"
                )
                await asyncio.sleep(time_to_wait)

    def _get_semaphore_for_api(self, api_name: str) -> asyncio.Semaphore:
        """获取或创建指定API的并发信号量"""
        if api_name not in TushareAPI._api_semaphores:
            limit = self._api_concurrency_limits.get(
                api_name, self._default_concurrency_limit
            )
            TushareAPI._api_semaphores[api_name] = asyncio.Semaphore(limit)
            if self.logger:
                self.logger.debug(
                    f"动态为 API {api_name} 创建并发信号量，限制: {limit}"
                )
        return TushareAPI._api_semaphores[api_name]

    async def query(
        self,
        api_name: str,
        params: Dict = None,
        fields: List[str] = None,
        page_size: int = None,
    ) -> pd.DataFrame:
        """向 Tushare 发送异步 HTTP 请求，支持自动分页、并发控制和速率限制。
        每个实际的HTTP POST请求都会受到速率和并发控制。
        """
        params = params or {}
        all_data = []
        offset = 0

        # 确保 page_size 有一个值
        effective_page_size = (
            page_size if page_size is not None and page_size > 0 else 5000
        )

        has_more = True
        consecutive_empty_pages = 0  # 新增：连续空页计数器
        max_consecutive_empty_before_stop = 3  # 新增：连续多少次空页后停止的阈值
        request_count = 0  # 用于日志记录分页次数

        # 分页循环
        while has_more:
            request_count += 1
            self.logger.debug(
                f"TushareAPI.query ({api_name}): 开始第 {request_count} 次分页请求. Offset: {offset}, EffectivePageSize: {effective_page_size}, Params: {params}"
            )  # 新增日志

            # 1. 等待速率限制槽位 (针对本次分页的HTTP请求)
            await self._wait_for_rate_limit_slot(api_name)

            # 2. 获取并发信号量 (针对本次分页的HTTP请求)
            current_semaphore = self._get_semaphore_for_api(
                api_name
            )  # 获取/创建当前API的信号量

            # 使用信号量控制并发
            async with current_semaphore:
                if self.logger:  # 避免在非debug模式下过于频繁的日志
                    self.logger.debug(
                        f"并发控制 ({api_name}): 获取 Semaphore 许可 (当前并发上限: {current_semaphore._value if hasattr(current_semaphore, '_value') else 'N/A'})"
                    )

                try:
                    page_params = params.copy()
                    if "limit" not in page_params:  # API可能用 'limit'
                        page_params["limit"] = effective_page_size
                    if "offset" not in page_params:  # API可能用 'offset'
                        page_params["offset"] = offset

                    if offset > 0:
                        self.logger.info(
                            f"分页请求 ({api_name}): offset={offset}, limit={page_params['limit']}, params={params}"
                        )
                    else:
                        self.logger.debug(
                            f"首次请求 ({api_name}): offset={offset}, limit={page_params['limit']}, params={params}"
                        )

                    payload = {
                        "api_name": api_name,
                        "token": self.token,
                        "params": page_params,
                        "fields": fields or "",
                    }

                    async with aiohttp.ClientSession() as session:
                        async with session.post(self.url, json=payload) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                self.logger.error(
                                    f"Tushare API 请求失败 ({api_name}): 状态码: {response.status}, URL: {self.url}, Payload: {payload}, 响应: {error_text}"
                                )
                                raise ValueError(
                                    f"Tushare API 请求失败({api_name})，状态码: {response.status}, 响应: {error_text}"
                                )

                            result = await response.json()
                            if result.get("code") != 0:
                                error_msg = result.get("msg", "未知错误")
                                self.logger.error(
                                    f"Tushare API 返回错误 ({api_name}): Code: {result.get('code')}, Msg: {error_msg}, Payload: {payload}"
                                )
                                raise ValueError(
                                    f"Tushare API 返回错误 ({api_name}): Code: {result.get('code')}, Msg: {error_msg}"
                                )

                            data = result.get("data", {})
                            if not data:
                                break  # 无数据则终止分页

                            columns = data.get("fields", [])
                            items = data.get("items", [])

                            self.logger.debug(
                                f"TushareAPI.query ({api_name}): 第 {request_count} 次分页请求返回 {len(items)} 条记录."
                            )  # 新增日志

                            if not items:  # 如果本次分页未获取到任何条目
                                consecutive_empty_pages += 1
                                self.logger.debug(
                                    f"({api_name}) 本次分页获取 0 条记录. Offset: {offset}. 已连续空页: {consecutive_empty_pages}"
                                )
                                if (
                                    not all_data and consecutive_empty_pages >= 1
                                ):  # 如果一开始就没数据，且已尝试1次以上空页
                                    self.logger.info(
                                        f"({api_name}) 首次/早期分页即连续 {consecutive_empty_pages} 次返回空数据，提前结束分页。Params: {params}"
                                    )
                                    has_more = False  # 强制结束
                                elif (
                                    consecutive_empty_pages
                                    >= max_consecutive_empty_before_stop
                                ):
                                    self.logger.info(
                                        f"({api_name}) 连续 {consecutive_empty_pages} 次分页返回空数据，提前结束分页。Offset: {offset}, Params: {params}"
                                    )
                                    has_more = False  # 强制结束

                                if not has_more:  # 如果决定要结束了
                                    break  # 跳出 while has_more 循环
                                # 如果只是单次空页，但未达到退出条件，循环会继续（除非下面 len(items) < effective_page_size 也为真）
                            else:  # 本次分页获取到数据
                                consecutive_empty_pages = 0  # 重置计数器
                                df = pd.DataFrame(items, columns=columns)
                                all_data.append(df)
                                self.logger.debug(
                                    f"({api_name}) 本次分页获取 {len(items)} 条记录. Offset: {offset}, PageSize requested: {page_params['limit']}"
                                )

                            # 判断是否还有更多数据的标准逻辑 (基于Tushare分页行为)
                            if len(items) < effective_page_size:
                                has_more = False  # 这是最后一页了
                                self.logger.debug(
                                    f"TushareAPI.query ({api_name}): 第 {request_count} 次分页判断为最后一页 (items: {len(items)} < effective_page_size: {effective_page_size})."
                                )  # 新增日志
                            else:
                                offset += len(
                                    items
                                )  #  Tushare的offset是基于条目数，不是页数
                                self.logger.debug(
                                    f"TushareAPI.query ({api_name}): 第 {request_count} 次分页后，仍有数据。新 offset: {offset}. (items: {len(items)} == effective_page_size: {effective_page_size})"
                                )  # 新增日志

                finally:  # 确保信号量被释放
                    if self.logger:  # 避免在非debug模式下过于频繁的日志
                        self.logger.debug(
                            f"并发控制 ({api_name}): 已释放 Semaphore 许可"
                        )

        if not all_data:
            return pd.DataFrame()
        combined_data = pd.concat(all_data, ignore_index=True)
        self.logger.info(
            f"API {api_name} (参数: {params}) 通过分页共获取 {len(combined_data)} 条记录。"
        )
        return combined_data

    # 旧的 set_api_rate_limit 和 set_default_rate_limit 需要更新或移除
    # @classmethod
    # def set_api_rate_limit(cls, api_name: str, limit: int): ...
    # @classmethod
    # def set_default_rate_limit(cls, limit: int): ...


# --- 使用示例 (辅助理解，非类一部分) ---
# async def main():
#     api = TushareAPI(token="YOUR_TOKEN", logger=logging.getLogger("test_api"))
#     logging.basicConfig(level=logging.DEBUG)

#     # 示例：获取某股票日线数据 (假设daily的每分钟请求数限制为800, 并发为80)
#     try:
#         # 启动多个并发请求来测试速率和并发控制
#         tasks = []
#         for i in range(10): # 尝试启动10个并发的 query 调用
#             # 每个 query 调用自身也可能分页
#             tasks.append(api.query(api_name="daily", params={"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20230110"}, page_size=3)) # 小page_size触发分页
#         results = await asyncio.gather(*tasks, return_exceptions=True)
#         for i, res in enumerate(results):
#             if isinstance(res, Exception):
#                 print(f"Task {i} failed: {res}")
#             # else:
#                 # print(f"Task {i} got data: \n{res.head()}")
#     except Exception as e:
#         print(f"Error: {e}")

# if __name__ == "__main__":
# asyncio.run(main())
