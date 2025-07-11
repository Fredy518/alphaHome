import aiohttp
import asyncio
import logging
from typing import Any, Dict, Optional, List, Union

from alphahome.common.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class iFindConnectionError(Exception):
    """iFind 连接或认证失败时抛出的异常。"""
    pass


class iFindRequestError(Exception):
    """iFind API 请求失败时抛出的异常。"""
    pass


class iFindAPI:
    """
    同花顺 iFind HTTP API 的异步包装器, 负责token管理和请求发送。
    """
    def __init__(self, config_manager: ConfigManager):
        config = config_manager.load_config()
        ifind_config = config.get("api", {}).get("ifind", {})

        self.base_url: Optional[str] = ifind_config.get("base_url")
        self.refresh_token: Optional[str] = ifind_config.get("refresh_token")

        if not self.base_url or "your_ifind_base_url" in self.base_url:
            raise iFindConnectionError("iFind 'base_url' 未在配置文件中设置。")
        if not self.refresh_token or "your_ifind_refresh_token" in self.refresh_token:
            raise iFindConnectionError("iFind 'refresh_token' 未在配置文件中设置。")

        self._access_token: Optional[str] = None
        self._token_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        logger.info("iFindAPI (Token-based) 已初始化。")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _refresh_access_token(self) -> None:
        """
        使用 refresh_token 获取新的 access_token。
        """
        async with self._token_lock:
            # 再次检查，防止在等待锁的过程中token已被其他协程刷新
            if self._access_token:
                return

            logger.info("正在刷新 iFind access token...")
            session = await self._get_session()
            auth_url = f"{self.base_url}get_access_token"
            
            assert self.refresh_token is not None
            headers = {"refresh_token": self.refresh_token}

            try:
                async with session.post(auth_url, headers=headers) as response:
                    response.raise_for_status()
                    
                    # 先获取原始内容用于调试
                    content = await response.text()
                    logger.debug(f"Token 刷新响应内容: {content}")
                    
                    try:
                        import json
                        result = json.loads(content)
                    except json.JSONDecodeError as json_error:
                        logger.error(f"无法解析 JSON 响应。Content-Type: {response.content_type}, 响应内容: {content}")
                        raise json_error
                    
                    if result.get("data") and result["data"].get("access_token"):
                        self._access_token = result["data"]["access_token"]
                        logger.info("iFind access token 刷新成功。")
                    else:
                        err_msg = f"刷新 access token 失败: {result.get('errmsg', '未知错误')}"
                        logger.error(err_msg)
                        raise iFindConnectionError(err_msg)
            except (aiohttp.ClientError, Exception) as e:
                logger.error(f"刷新 access token 时发生网络或解析错误: {e}", exc_info=True)
                raise iFindConnectionError(f"刷新 token 失败: {e}") from e

    async def request(self, endpoint: str, payload: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
        """
        向指定的 iFind API endpoint 发送一个已认证的请求。
        处理 access_token 的获取和自动重试。
        """
        for attempt in range(2): # 最多尝试2次 (初次 + 1次重试)
            if self._access_token is None:
                await self._refresh_access_token()

            session = await self._get_session()
            request_url = f"{self.base_url}{endpoint}"
            
            assert self._access_token is not None
            headers = {"access_token": self._access_token}

            logger.debug(f"向 {request_url} 发送请求, payload: {payload}")

            try:
                # 设置请求超时
                request_timeout = aiohttp.ClientTimeout(total=timeout)
                async with session.post(request_url, json=payload, headers=headers, timeout=request_timeout) as response:
                    response.raise_for_status()
                    
                    # 获取原始内容用于调试
                    content = await response.text()
                    logger.debug(f"数据请求响应内容: {content}")
                    
                    try:
                        import json
                        result = json.loads(content)
                    except json.JSONDecodeError as json_error:
                        logger.error(f"无法解析数据请求的 JSON 响应。Content-Type: {response.content_type}, 响应内容: {content}")
                        raise json_error

                    # 检查特定的token失效错误码 (假设为-401，需要根据实际情况调整)
                    if result.get("errorcode") == -401 and attempt == 0:
                        logger.warning("Access token 可能已失效，将强制刷新后重试。")
                        self._access_token = None # 强制刷新
                        continue # 进入下一次循环以重试
                    
                    if result.get("errorcode") != 0:
                        raise iFindRequestError(f"API Error Code: {result.get('errorcode')}, Msg: {result.get('errmsg')}")

                    return result
            
            except asyncio.TimeoutError:
                logger.error(f"请求 endpoint '{endpoint}' 超时（超过 {timeout} 秒）。")
                raise iFindRequestError(f"请求 endpoint '{endpoint}' 超时。") from asyncio.TimeoutError

            except (aiohttp.ClientError, Exception) as e:
                logger.error(f"请求 endpoint '{endpoint}' 时发生错误: {e}", exc_info=True)
                raise iFindRequestError(f"请求 endpoint '{endpoint}' 失败: {e}") from e
        
        # 如果两次尝试都失败
        raise iFindRequestError("请求失败，经过一次重试后仍然失败。")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("iFind aiohttp session closed.")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ==============================================
    # 便捷方法 - 对应各个 iFind API 端点
    # ==============================================
    
    async def basic_data_service(self, codes: str, 
                               indicators: Union[List[str], str],
                               indiparams: Optional[List[List[str]]] = None) -> Dict[str, Any]:
        """
        基础数据服务：获取证券基本信息、财务指标、盈利预测、日频行情等数据
        
        Args:
            codes: 股票代码，多个代码用逗号分隔，如 "000001.SZ,600000.SH"
            indicators: 指标列表，可以是：
                - List[str]: ["ths_stock_short_name_stock", "ths_pe_ttm_stock"]
                - str: "ths_stock_short_name_stock;ths_pe_ttm_stock" (分号分隔)
            indiparams: 每个指标对应的参数列表，如 [[""], ["100", ""]]
                - 如果为 None，所有指标使用空参数 [""]
                - 长度应与 indicators 匹配
        
        Returns:
            Dict[str, Any]: API响应数据
            
        Example:
            # 简单用法
            result = await api.basic_data_service(
                codes="000001.SZ,600000.SH",
                indicators=["ths_stock_short_name_stock", "ths_pe_ttm_stock"]
            )
            
            # 带参数用法
            result = await api.basic_data_service(
                codes="000001.SZ",
                indicators=["ths_close_price_stock"],
                indiparams=[["", "100", ""]]  # 第二个参数是报告期
            )
        """
        # 处理 indicators 参数
        if isinstance(indicators, str):
            # 支持分号分隔的字符串格式
            indicator_list = indicators.split(';')
        else:
            indicator_list = indicators
        
        # 处理 indiparams 参数
        if indiparams is None:
            # 如果没有提供参数，为每个指标使用空参数
            indiparams = [[""] for _ in indicator_list]
        elif len(indiparams) != len(indicator_list):
            # 参数长度不匹配时的处理
            raise ValueError(f"indiparams 长度 ({len(indiparams)}) 与 indicators 长度 ({len(indicator_list)}) 不匹配")
        
        # 构造 indipara JSON 字符串
        import json
        indipara_list = []
        for indicator, params in zip(indicator_list, indiparams):
            indipara_list.append({
                "indicator": indicator,
                "indiparams": params
            })
        
        indipara_str = json.dumps(indipara_list)
        
        # 调用原始 API
        payload = {"codes": codes, "indipara": indipara_str}
        return await self.request("basic_data_service", payload)
    
    async def high_frequency(self, codes: str, indicators: str, 
                           starttime: str, endtime: str) -> Dict[str, Any]:
        """
        高频序列：获取分钟数据
        
        Args:
            codes: 股票代码，如 "000001.SZ"
            indicators: 指标，如 "open,high,low,close,volume,amount,changeRatio"
            starttime: 开始时间，格式 "2022-07-05 09:15:00"
            endtime: 结束时间，格式 "2022-07-05 15:15:00"
        """
        payload = {
            "codes": codes,
            "indicators": indicators,
            "starttime": starttime,
            "endtime": endtime
        }
        return await self.request("high_frequency", payload)
    
    async def real_time_quotation(self, codes: str, indicators: str = "latest") -> Dict[str, Any]:
        """
        实时行情：获取最新行情数据
        
        Args:
            codes: 股票代码，如 "300033.SZ"
            indicators: 指标，默认 "latest"
        """
        payload = {"codes": codes, "indicators": indicators}
        return await self.request("real_time_quotation", payload)
    
    async def cmd_history_quotation(self, codes: str, indicators: str,
                                  startdate: str, enddate: str, 
                                  functionpara: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        历史行情：获取历史的日频行情数据
        
        Args:
            codes: 股票代码，多个代码用逗号分隔，如 "000001.SZ,600000.SH"
            indicators: 指标，如 "open,high,low,close"
            startdate: 开始日期，格式 "2021-07-05"
            enddate: 结束日期，格式 "2022-07-05"
            functionpara: 功能参数，如 {"Fill": "Blank"}
        """
        payload = {
            "codes": codes,
            "indicators": indicators,
            "startdate": startdate,
            "enddate": enddate
        }
        if functionpara: 
            payload["functionpara"] = functionpara # type: ignore
        return await self.request("cmd_history_quotation", payload)
    
    async def date_sequence(self, codes: str, startdate: str, enddate: str,
                          indipara: List[Dict[str, Any]], 
                          functionpara: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        日期序列：与基础数据指标相同，可以同时获取多日数据
        
        Args:
            codes: 股票代码，多个代码用逗号分隔，如 "000001.SZ,600000.SH"
            startdate: 开始日期，格式 "20220605"
            enddate: 结束日期，格式 "20220705"
            indipara: 指标参数列表，如 [{"indicator": "ths_close_price_stock", "indiparams": ["", "100", ""]}]
            functionpara: 功能参数，如 {"Fill": "Blank"}
        """
        payload = {
            "codes": codes,
            "startdate": startdate,
            "enddate": enddate,
            "indipara": indipara
        }
        if functionpara:
            payload["functionpara"] = functionpara
        return await self.request("date_sequence", payload)
    
    async def data_pool(self, reportname: str, functionpara: Dict[str, Any],
                       outputpara: str) -> Dict[str, Any]:
        """
        专题报表：如提取全部A股代码等报表数据
        
        Args:
            reportname: 报表名称，如 "p03425"
            functionpara: 功能参数，如 {"date": "20220706", "blockname": "001005010", "iv_type": "allcontract"}
            outputpara: 输出参数，如 "p03291_f001,p03291_f002,p03291_f003,p03291_f004"
        """
        payload = {
            "reportname": reportname,
            "functionpara": functionpara,
            "outputpara": outputpara
        }
        return await self.request("data_pool", payload)
    
    async def edb_service(self, indicators: str, startdate: str, enddate: str) -> Dict[str, Any]:
        """
        经济数据库
        
        Args:
            indicators: 指标代码，如 "G009035746"
            startdate: 开始日期，格式 "2022-04-01"
            enddate: 结束日期，格式 "2022-05-01"
        """
        payload = {
            "indicators": indicators,
            "startdate": startdate,
            "enddate": enddate
        }
        return await self.request("edb_service", payload)
    
    async def snap_shot(self, codes: str, indicators: str,
                       starttime: str, endtime: str) -> Dict[str, Any]:
        """
        日内快照：tick数据
        
        Args:
            codes: 股票代码，如 "000001.SZ"
            indicators: 指标，如 "open,high,low,latest,bid1,ask1,bidSize1,askSize1"
            starttime: 开始时间，格式 "2022-07-06 09:15:00"
            endtime: 结束时间，格式 "2022-07-06 15:15:00"
        """
        payload = {
            "codes": codes,
            "indicators": indicators,
            "starttime": starttime,
            "endtime": endtime
        }
        return await self.request("snap_shot", payload)
    
    async def report_query(self, codes: str, functionpara: Dict[str, Any],
                          beginrDate: str, endrDate: str, outputpara: str) -> Dict[str, Any]:
        """
        公告查询
        
        Args:
            codes: 股票代码，多个代码用逗号分隔，如 "000001.SZ,600000.SH"
            functionpara: 功能参数，如 {"reportType": "901"}
            beginrDate: 开始日期，格式 "2021-01-01"
            endrDate: 结束日期，格式 "2022-07-06"
            outputpara: 输出参数，如 "reportDate:Y,thscode:Y,secName:Y,ctime:Y,reportTitle:Y,pdfURL:Y,seq:Y"
        """
        payload = {
            "codes": codes,
            "functionpara": functionpara,
            "beginrDate": beginrDate,
            "endrDate": endrDate,
            "outputpara": outputpara
        }
        return await self.request("report_query", payload)
    
    async def smart_stock_picking(self, searchstring: str, searchtype: str = "stock") -> Dict[str, Any]:
        """
        智能选股
        
        Args:
            searchstring: 搜索条件，如 "涨跌幅"
            searchtype: 搜索类型，默认 "stock"
        """
        payload = {
            "searchstring": searchstring,
            "searchtype": searchtype
        }
        return await self.request("smart_stock_picking", payload)
    
    async def get_trade_dates(self, marketcode: str, functionpara: Dict[str, Any],
                            startdate: str) -> Dict[str, Any]:
        """
        日期查询函数、日期偏移函数：根据交易所查询交易日
        
        Args:
            marketcode: 市场代码，如 "212001"
            functionpara: 功能参数，如 {"dateType": "0", "period": "D", "offset": "-10", "dateFormat": "0", "output": "sequencedate"}
            startdate: 开始日期，格式 "2022-07-05"
        """
        payload = {
            "marketcode": marketcode,
            "functionpara": functionpara,
            "startdate": startdate
        }
        return await self.request("get_trade_dates", payload) 