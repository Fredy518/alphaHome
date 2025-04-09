import os
import logging
import asyncio
import aiohttp
import pandas as pd
from typing import Dict, List, Any, Optional, Union

class TushareAPI:
    """Tushare API 客户端，负责处理与 Tushare 的 HTTP 通信"""
    
    # 每个 API 的特定速率限制
    _api_rate_limits = {
        # 示例：不同 API 的不同限制
        "daily": 500,        # 股票日线数据
        "stock_basic": 100,  # 股票基本信息
        "trade_cal": 50,     # 交易日历
        # 可以根据 Tushare 文档添加更多 API 的限制
    }
    
    # 默认速率限制（用于未特别指定的 API）
    _default_limit = 50
    
    # 每个 API 的信号量字典（类级别共享）
    _api_semaphores = {}
    
    def __init__(self, token=None, logger=None):
        """初始化 TushareAPI 客户端
        
        Args:
            token (str, optional): Tushare API 令牌，如果不提供则从环境变量获取
            logger (logging.Logger, optional): 日志记录器
        """
        self.token = token or os.environ.get('TUSHARE_TOKEN')
        if not self.token:
            raise ValueError("必须提供Tushare API令牌，可以通过参数传入或设置TUSHARE_TOKEN环境变量")
            
        self.url = "http://api.tushare.pro"
        self.logger = logger or logging.getLogger(__name__)
        
        # 初始化时为所有预设的 API 速率限制创建信号量
        for api_name, limit in self._api_rate_limits.items():
            if api_name not in self._api_semaphores:
                self._api_semaphores[api_name] = asyncio.Semaphore(limit)
                if self.logger:
                    self.logger.debug(f"为 API {api_name} 创建信号量，限制: {limit}")
    
    @classmethod
    def set_api_rate_limit(cls, api_name: str, limit: int):
        """设置特定 API 的速率限制
        
        Args:
            api_name (str): API 名称
            limit (int): 允许的并发请求数量
        """
        cls._api_rate_limits[api_name] = limit
        cls._api_semaphores[api_name] = asyncio.Semaphore(limit)
    
    @classmethod
    def set_default_rate_limit(cls, limit: int):
        """设置默认的 API 速率限制
        
        Args:
            limit (int): 默认的并发请求数量限制
        """
        cls._default_limit = limit
    
    def _get_semaphore(self, api_name: str) -> asyncio.Semaphore:
        """获取指定 API 的信号量
        
        如果 API 没有特定的信号量，则创建一个使用默认限制的信号量
        
        Args:
            api_name (str): API 名称
            
        Returns:
            asyncio.Semaphore: 对应的信号量
        """
        if api_name not in self._api_semaphores:
            limit = self._api_rate_limits.get(api_name, self._default_limit)
            self._api_semaphores[api_name] = asyncio.Semaphore(limit)
        return self._api_semaphores[api_name]
        
    async def query(self, api_name: str, params: Dict = None, fields: List[str] = None, page_size: int = None) -> pd.DataFrame:
        """向 Tushare 发送异步 HTTP 请求，支持自动分页
        
        Args:
            api_name (str): Tushare API 名称
            params (dict, optional): 请求参数
            fields (List[str], optional): 需要获取的字段列表
            page_size (int, optional): 每页数据量，默认为5000
            
        Returns:
            pd.DataFrame: 响应数据转换为 DataFrame
            
        Raises:
            ValueError: 当 API 请求失败或返回错误时
        """
        params = params or {}
        
        # 获取该 API 的信号量
        semaphore = self._get_semaphore(api_name)
        limit = self._api_rate_limits.get(api_name, self._default_limit)
        
        # 初始化结果列表和分页参数
        all_data = []
        offset = 0
        # 如果未指定page_size，则使用默认值5000
        if page_size is None:
            page_size = 5000  # Tushare API 默认每页最多返回 6000 条记录，我们设置为 5000 以确保安全
        has_more = True
        
        # 使用信号量控制并发
        async with semaphore:
            self.logger.debug(f"获取 API {api_name} 的 Semaphore 许可，当前限制: {limit}")
            try:
                while has_more:
                    # 添加分页参数
                    page_params = params.copy()
                    if 'limit' not in page_params:
                        page_params['limit'] = page_size
                    if 'offset' not in page_params:
                        page_params['offset'] = offset
                    
                    # 添加必要的认证信息
                    payload = {
                        "api_name": api_name,
                        "token": self.token,
                        "params": page_params,
                        "fields": fields or ""
                    }
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(self.url, json=payload) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                self.logger.error(f"Tushare API 请求失败: {error_text}")
                                raise ValueError(f"Tushare API 请求失败，状态码: {response.status}")
                            
                            result = await response.json()
                            
                            if result.get('code') != 0:
                                self.logger.error(f"Tushare API 返回错误: {result.get('msg')}")
                                raise ValueError(f"Tushare API 返回错误: {result.get('msg')}")
                            
                            # 将结果转换为 DataFrame
                            data = result.get('data', {})
                            if not data:
                                break
                                
                            columns = data.get('fields', [])
                            items = data.get('items', [])
                            
                            if not items:
                                break
                            
                            df = pd.DataFrame(items, columns=columns)
                            all_data.append(df)
                            
                            # 判断是否有更多数据
                            if len(items) < page_size:
                                has_more = False
                            else:
                                offset += len(items)
                                self.logger.debug(f"已获取 {offset} 条记录，继续获取下一页")
                
                # 合并所有分页数据
                if not all_data:
                    return pd.DataFrame()
                    
                combined_data = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"API {api_name} 共获取 {len(combined_data)} 条记录")
                return combined_data
            finally:
                self.logger.debug(f"已释放 API {api_name} 的 Semaphore 许可")
