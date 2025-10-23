# -*- coding: utf-8 -*-

"""
Pytdx API 封装类

借鉴hikyuu的实现，提供通达信pytdx数据源的API接口封装。
"""

import asyncio
import logging
import os
import sys
import importlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date
import pandas as pd

from pytdx.hq import TdxHq_API, TDXParams
from pytdx.config.hosts import hq_hosts

from ....common.logging_utils import get_logger

logger = get_logger(__name__)


class PytdxAPI:
    """Pytdx API 封装类"""

    def __init__(self, host: Optional[str] = None, port: int = 7709, timeout: int = 10):
        """
        初始化Pytdx API

        Args:
            host: 通达信服务器地址，如果为None则自动选择最佳服务器
            port: 端口号，默认7709
            timeout: 连接超时时间，默认10秒
        """
        # 借鉴hikyuu的实现，尝试加载用户目录下的hosts配置
        self._load_custom_hosts()

        self.host = host
        self.port = port
        self.timeout = timeout
        self.api = None
        self.connected = False

    def _load_custom_hosts(self):
        """借鉴hikyuu的实现，加载用户目录下的自定义hosts配置"""
        global hq_hosts
        try:
            # 尝试获取用户目录下的hosts配置
            config_path = "{}/.hikyuu".format(os.path.expanduser('~'))
            host_file = f"{config_path}/hosts.py"
            if os.path.exists(host_file):
                if config_path not in sys.path:
                    sys.path.append(config_path)
                tmp = importlib.import_module('hosts')
                hq_hosts = tmp.hq_hosts
                logger.info(f"成功加载自定义hosts配置: {host_file}")
            else:
                logger.debug(f"未找到自定义hosts文件: {host_file}")
        except Exception as e:
            logger.warning(f"加载自定义hosts配置失败: {e}")

    async def connect(self) -> bool:
        """连接到通达信服务器"""
        try:
            if self.api is None:
                self.api = TdxHq_API(multithread=False)

            # 如果没有指定host，自动选择最佳服务器
            if self.host is None:
                self.host, self.port = await self._find_best_server()

            logger.info(f"连接到通达信服务器: {self.host}:{self.port}")
            if self.api.connect(self.host, self.port, time_out=self.timeout):
                self.connected = True
                logger.info("成功连接到通达信服务器")
                return True
            else:
                logger.error("连接通达信服务器失败")
                return False

        except Exception as e:
            logger.error(f"连接通达信服务器时出错: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        if self.api and self.connected:
            self.api.disconnect()
            self.connected = False
            logger.info("已断开通达信服务器连接")

    async def _find_best_server(self) -> Tuple[str, int]:
        """查找最佳的通达信服务器，借鉴hikyuu的search_best_tdx实现"""
        try:
            # 借鉴hikyuu的实现，使用并发ping测试
            ping_results = await self._concurrent_ping_test()

            if not ping_results:
                logger.warning("所有服务器ping测试均失败，使用默认服务器")
                return hq_hosts[0][1], hq_hosts[0][2]

            # 对ping成功的服务器进行数据一致性验证
            validated_servers = await self._validate_server_consistency(ping_results)

            if validated_servers:
                best_server = validated_servers[0]  # 已按响应时间排序，取最优的
                logger.info(f"找到最佳服务器: {best_server[2]}:{best_server[3]}, 响应时间: {best_server[1]:.2f}s")
                return best_server[2], best_server[3]
            else:
                # 如果一致性验证失败，使用ping最快的服务器
                best_server = ping_results[0]
                logger.warning(f"数据一致性验证失败，使用ping最快的服务器: {best_server[2]}:{best_server[3]}")
                return best_server[2], best_server[3]

        except Exception as e:
            logger.error(f"查找最佳服务器时出错: {e}")
            return hq_hosts[0][1], hq_hosts[0][2]

    async def _concurrent_ping_test(self) -> List[Tuple[bool, float, str, int]]:
        """并发ping测试所有服务器"""
        import concurrent.futures
        import time

        def ping_host(host_info):
            """同步ping函数，借鉴hikyuu的ping实现"""
            host, port = host_info[1], host_info[2]
            api = TdxHq_API(multithread=False)
            success = False
            start_time = time.time()

            try:
                if api.connect(host, port, time_out=self.timeout):
                    # 测试获取少量数据来验证连接
                    test_data = api.get_security_bars(7, 0, '000001', 0, 1)
                    if test_data:
                        success = True
                    api.disconnect()
            except Exception as e:
                logger.debug(f"ping测试服务器 {host}:{port} 失败: {e}")

            end_time = time.time()
            return (success, end_time - start_time, host, port)

        try:
            # 并发测试所有服务器
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures_list = [executor.submit(ping_host, host_info) for host_info in hq_hosts]
                # 增加超时时间到30秒，并处理超时情况
                try:
                    results = [future.result() for future in concurrent.futures.as_completed(futures_list, timeout=30)]
                except concurrent.futures.TimeoutError:
                    # 如果超时，获取已完成的结果
                    logger.warning("并发ping测试部分超时，处理已完成的结果")
                    results = []
                    for future in futures_list:
                        if future.done():
                            try:
                                results.append(future.result())
                            except:
                                pass

            # 过滤成功的结果并按响应时间排序
            successful_results = [r for r in results if r[0]]
            successful_results.sort(key=lambda x: x[1])  # 按响应时间排序

            logger.info(f"ping测试完成，{len(successful_results)}/{len(hq_hosts)} 个服务器响应正常")
            return successful_results

        except Exception as e:
            logger.error(f"并发ping测试出错: {e}")
            return []

    async def _validate_server_consistency(self, ping_results: List[Tuple[bool, float, str, int]]) -> List[Tuple[bool, float, str, int]]:
        """验证服务器数据一致性，借鉴hikyuu的实现"""
        if len(ping_results) < 2:
            return ping_results

        try:
            # 使用测试股票'159915'来验证数据一致性
            values = {}
            validated_servers = []

            for result in ping_results[:10]:  # 只验证前10个最快的服务器
                success, response_time, host, port = result
                api = TdxHq_API(multithread=False)

                try:
                    if api.connect(host, port, time_out=self.timeout):
                        # 获取测试数据
                        test_data = api.get_security_bars(9, 0, '159915', 0, 1)
                        if test_data and len(test_data) > 0:
                            close_price = test_data[0]['close']
                            if close_price not in values:
                                values[close_price] = [result]
                            else:
                                values[close_price].append(result)
                        api.disconnect()
                except Exception as e:
                    logger.debug(f"验证服务器 {host}:{port} 数据一致性失败: {e}")

            # 选择返回相同数据最多的服务器组
            if values:
                max_consistent_servers = max(values.values(), key=len)
                logger.info(f"数据一致性验证完成，{len(max_consistent_servers)} 个服务器返回一致数据")
                return max_consistent_servers
            else:
                logger.warning("数据一致性验证失败")
                return []

        except Exception as e:
            logger.error(f"数据一致性验证出错: {e}")
            return []

    def _market_to_pytdx_market(self, market: str) -> int:
        """转换市场代码为pytdx格式"""
        market_map = {'SZ': 0, 'SH': 1, 'BJ': 2}
        return market_map.get(market.upper(), 0)

    async def get_stock_daily_bars(self, market: str, code: str, start: int = 0, count: int = 800) -> Optional[List[Dict]]:
        """
        获取股票日线数据

        Args:
            market: 市场代码 ('SH', 'SZ', 'BJ')
            code: 股票代码
            start: 开始位置
            count: 获取数量

        Returns:
            日线数据列表，每个元素包含OHLCV等信息
        """
        if not self.connected or not self.api:
            logger.error("未连接到通达信服务器")
            return None

        try:
            pytdx_market = self._market_to_pytdx_market(market)
            bars = self.api.get_security_bars(TDXParams.KLINE_TYPE_RI_K, pytdx_market, code, start, count)

            if bars:
                # 转换数据格式
                result = []
                for bar in bars:
                    result.append({
                        'datetime': bar['datetime'],
                        'open': bar['open'],
                        'high': bar['high'],
                        'low': bar['low'],
                        'close': bar['close'],
                        'volume': bar['vol'],  # 注意：pytdx中使用'vol'字段名
                        'amount': bar['amount']
                    })
                return result
            else:
                logger.warning(f"未获取到 {market}{code} 的日线数据")
                return None

        except Exception as e:
            logger.error(f"获取 {market}{code} 日线数据时出错: {e}")
            return None

    async def get_stock_list_from_db(self, db_connection, market: str = None) -> List[Tuple]:
        """
        从postgresql的tushare.stock_basic表获取股票列表

        借鉴hikyuu的get_stock_list实现，返回格式：
        (stockid, marketid, code, valid, type)

        Args:
            db_connection: 数据库连接对象
            market: 市场代码 ('SH', 'SZ', 'BJ')，如果为None则返回所有市场

        Returns:
            股票列表，格式与hikyuu的get_stock_list一致
        """
        try:
            # 构建查询SQL
            query = """
                SELECT ts_code, symbol, name, market, exchange, list_status
                FROM tushare.stock_basic
                WHERE list_status = 'L'
            """

            if market:
                if market.upper() == 'SH':
                    query += " AND market = '主板' AND exchange = 'SSE'"
                elif market.upper() == 'SZ':
                    query += " AND market IN ('主板', '中小板', '创业板') AND exchange = 'SZSE'"
                elif market.upper() == 'BJ':
                    query += " AND exchange = 'BSE'"

            query += " ORDER BY ts_code"

            # 执行查询 (使用正确的异步API)
            rows = await db_connection.fetch(query)

            stock_list = []
            stockid_counter = 1

            for row in rows:
                # 处理不同格式的行数据
                if isinstance(row, dict):
                    ts_code = row.get('ts_code')
                    symbol = row.get('symbol')
                    name = row.get('name')
                    market_name = row.get('market')
                    exchange = row.get('exchange')
                    list_status = row.get('list_status')
                else:
                    # 如果是元组或其他格式
                    ts_code, symbol, name, market_name, exchange, list_status = row

                # 解析市场代码
                if exchange == 'SSE':
                    market_code = 'SH'
                    marketid = 1  # SH
                elif exchange == 'SZSE':
                    market_code = 'SZ'
                    marketid = 0  # SZ
                elif exchange == 'BSE':
                    market_code = 'BJ'
                    marketid = 2  # BJ
                else:
                    continue  # 跳过不支持的交易所

                # 解析股票类型 (简化为股票类型)
                stktype = 1  # 默认股票类型

                # 构建hikyuu格式的元组
                # (stockid, marketid, code, valid, type)
                stock_record = (
                    stockid_counter,  # stockid: 自增ID
                    marketid,         # marketid: 市场ID (0=SZ, 1=SH, 2=BJ)
                    symbol,           # code: 股票代码 (如'000001')
                    1,                # valid: 是否有效 (1=有效)
                    stktype           # type: 股票类型
                )

                stock_list.append(stock_record)
                stockid_counter += 1

            logger.info(f"从数据库获取到 {len(stock_list)} 只股票 ({market or '全市场'})")
            return stock_list

        except Exception as e:
            logger.error(f"获取股票列表时出错: {e}")
            return []

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        self.disconnect()
