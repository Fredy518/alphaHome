from abc import ABC, abstractmethod
import json
import pandas as pd
from typing import Dict, Any, Optional, List
import asyncio
from alphahome.fetchers.base.fetcher_task import FetcherTask
from alphahome.fetchers.sources.ifind.ifind_api import iFindAPI, iFindRequestError
from alphahome.common.config_manager import ConfigManager

class iFindTask(FetcherTask, ABC):
    """
    所有 iFind 数据获取任务的基类。
    继承自 FetcherTask，遵循项目标准架构。
    
    参考 TushareTask 的设计：
    - 使用类属性而非方法来定义必需配置
    - 保持简洁的抽象接口
    - 在 __init__ 中验证必需属性
    - 与任务工厂系统兼容
    """
    
    data_source = "ifind"
    task_type: str = "fetch"
    
    # 必须由具体任务定义的属性
    api_endpoint: Optional[str] = None  # iFind API 端点名称
    indicators: Optional[str] = None    # 默认指标列表（分号分隔）
    
    def __init__(self, db_connection, **kwargs):
        """
        初始化 iFindTask。
        
        Args:
            db_connection: 数据库连接（与 TushareTask 保持一致）
            **kwargs: 传递给 FetcherTask 的参数
        """
        super().__init__(db_connection, **kwargs)
        
        # 验证必需属性（参考 TushareTask 的设计）
        if self.api_endpoint is None or self.indicators is None:
            raise ValueError("iFindTask 子类必须定义 api_endpoint 和 indicators 属性")
        
        # 内部创建 ConfigManager（而不是要求外部传递）
        self.config_manager = ConfigManager()
        # 改为懒加载，避免在任务不执行时创建会话
        self.api: Optional[iFindAPI] = None
        
        # iFind 特有的配置
        self.codes_to_fetch: List[str] = kwargs.get("codes", [])

    async def _get_api(self) -> iFindAPI:
        """懒加载 iFindAPI 实例，确保只在需要时创建。"""
        if self.api is None:
            self.logger.debug("首次使用，创建 iFindAPI 实例...")
            self.api = iFindAPI(self.config_manager)
        return self.api
        
    async def get_batch_list(self, **kwargs) -> List[List[str]]:
        """
        生成股票代码批次列表。
        iFind API 按股票代码分批，而不是按日期。
        
        这是抽象方法，但提供默认实现，子类可以重写以自定义批处理逻辑。
        """
        # 优先使用运行时提供的参数
        codes = kwargs.get("codes", self.codes_to_fetch)
        
        if not codes:
            self.logger.warning(f"任务 {self.name}: 没有提供股票代码列表。")
            return []
        
        batch_size = kwargs.get('batch_size', 100)
        batches = [codes[i:i + batch_size] for i in range(0, len(codes), batch_size)]
        
        self.logger.info(f"任务 {self.name}: 生成了 {len(batches)} 个批次，总计 {len(codes)} 个股票代码。")
        return batches
        
    async def prepare_params(self, batch: List[str]) -> Dict[str, Any]:
        """
        为给定的股票代码批次准备API请求参数。
        通用实现，子类通常不需要重写。
        """
        if not batch:
            raise ValueError("批次不能为空")
            
        # 使用类属性而非方法调用
        assert self.api_endpoint is not None, "api_endpoint must be defined"
        assert self.indicators is not None, "indicators must be defined"
        
        # 构造 iFind API 所需的参数格式
        params = {
            "endpoint": self.api_endpoint,
            "codes": batch,
            "indicators": self.indicators
        }
        
        self.logger.debug(f"任务 {self.name}: 为批次 {batch[:3]}{'...' if len(batch) > 3 else ''} 准备参数")
        return params
        
    async def fetch_batch(self, params: Dict[str, Any], stop_event: Optional[asyncio.Event] = None) -> Optional[pd.DataFrame]:
        """
        获取单个批次的数据。
        通用实现，支持不同的 iFind API 端点，并增加了中断和超时处理。
        """
        api = await self._get_api()
        try:
            # 创建一个在 stop_event 设置时完成的 future
            stop_waiter = asyncio.create_task(stop_event.wait()) if stop_event else None

            # 准备API调用协程并将其创建为Task
            api_call_coro = self._execute_api_call(api, params)
            api_task = asyncio.create_task(api_call_coro)

            # 等待网络请求或停止信号
            tasks_to_wait = [api_task]
            if stop_waiter:
                tasks_to_wait.append(stop_waiter)
            
            done, pending = await asyncio.wait(tasks_to_wait, return_when=asyncio.FIRST_COMPLETED)

            if stop_waiter and stop_waiter in done:
                # 是停止事件触发了返回
                self.logger.warning(f"任务 {self.name} 在获取批次期间被取消。")
                api_task.cancel() # 取消网络请求任务
                return None

            # 检查是否有待处理的任务（即stop_waiter），并取消它
            if pending:
                for task in pending:
                    task.cancel()

            # 网络请求完成
            result_task = done.pop()
            json_response = await result_task

            if json_response:
                return self.extract_data(json_response)
                
        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 的批次获取操作被取消。")
            return None
        except iFindRequestError as e:
            self.logger.error(f"任务 {self.name}: iFind API 请求失败: {e}")
            return None
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 获取批次数据时发生未知错误: {e}", exc_info=True)
            raise
        finally:
            # 确保stop_waiter被取消，防止资源泄漏
            if 'stop_waiter' in locals() and stop_waiter and not stop_waiter.done():
                stop_waiter.cancel()

        return None

    async def _execute_api_call(self, api: iFindAPI, params: Dict[str, Any]) -> Dict[str, Any]:
        """封装实际的API调用逻辑，以便于被asyncio.wait管理"""
        endpoint = params["endpoint"]
        
        if endpoint == "basic_data_service":
            codes_param = params["codes"]
            if isinstance(codes_param, list):
                codes_str = ",".join(codes_param)
            else:
                codes_str = str(codes_param)

            indicators = params["indicators"]
            indiparams = params.get("indiparams")
            
            if isinstance(indicators, str):
                indicator_list = indicators.split(';')
            else:
                indicator_list = indicators
            
            return await api.basic_data_service(
                codes=codes_str,
                indicators=indicator_list,
                indiparams=indiparams
            )
            
        elif endpoint == "data_pool":
            reportname = params.get("reportname", "")
            functionpara = params.get("functionpara", {})
            outputpara = params.get("outputpara", "")
            
            return await api.data_pool(
                reportname=reportname,
                functionpara=functionpara,
                outputpara=outputpara
            )
            
        else:
            codes = params["codes"]
            if isinstance(codes, list):
                codes_str = ",".join(codes)
            else:
                codes_str = str(codes)
                
            indicators = params["indicators"]
            indicators_list = indicators.split(';') if isinstance(indicators, str) else indicators
            indipara_str = json.dumps([{"indicator": ind} for ind in indicators_list])
            payload = {
                "codes": codes_str, 
                "indipara": indipara_str
            }
            return await api.request(endpoint, payload)

    def extract_data(self, json_response: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        从API响应中提取数据并转换为DataFrame。
        支持不同端点的响应格式。
        """
        # 检查错误码
        if json_response.get("errorcode") != 0:
            self.logger.error(f"iFind API 返回错误: {json_response.get('errmsg', '未知错误')}")
            return None
        
        # 判断数据格式类型
        if "tables" in json_response:
            # basic_data_service 等端点的格式
            return self._extract_tables_format(json_response)
        elif "data" in json_response:
            # data_pool 端点的格式
            return self._extract_data_pool_format(json_response)
        else:
            self.logger.warning("未知的响应数据格式")
            return None
    
    def _extract_tables_format(self, json_response: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """提取 tables 格式的数据（basic_data_service 等）"""
        tables = json_response.get("tables", [])
        if not tables:
            self.logger.warning("iFind API响应中没有找到'tables'数据。")
            return None

        # 将嵌套的表格数据转换为扁平的行记录
        rows = []
        for table_entry in tables:
            thscode = table_entry.get("thscode")
            table_data = table_entry.get("table", {})
            
            # 获取所有指标的值
            row = {"thscode": thscode}
            
            # 为每个指标提取第一个值（通常每个指标只有一个值）
            for indicator, values in table_data.items():
                if values and len(values) > 0:
                    row[indicator] = values[0]
                else:
                    row[indicator] = None
            
            rows.append(row)

        # 转换为DataFrame
        if rows:
            df = pd.DataFrame(rows)
            self.logger.debug(f"成功提取到 {len(df)} 行数据")
            return df
        else:
            self.logger.warning("处理后没有有效的数据行。")
            return None
    
    def _extract_data_pool_format(self, json_response: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """提取 data_pool 格式的数据"""
        data = json_response.get("data", {})
        if not data:
            self.logger.warning("iFind API响应中没有找到'data'数据。")
            return None
        
        # data_pool 返回的是列式存储格式
        tables = data.get("tables", [])
        if not tables:
            self.logger.warning("data_pool 响应中没有找到 tables 数据。")
            return None
        
        # 获取第一个表格（通常只有一个）
        table = tables[0] if len(tables) > 0 else {}
        table_data = table.get("table", {})
        
        if not table_data:
            self.logger.warning("data_pool 表格中没有数据。")
            return None
        
        # data_pool 的数据是列式存储：每个字段对应一个数组
        # 我们需要将其转换为行式存储（每行一个记录）
        field_names = list(table_data.keys())
        if not field_names:
            self.logger.warning("没有找到字段名。")
            return None
        
        # 获取数据长度（假设所有字段长度相同）
        first_field = field_names[0]
        data_length = len(table_data[first_field])
        
        if data_length == 0:
            self.logger.warning("数据为空。")
            return None
        
        # 构建行数据
        rows = []
        for i in range(data_length):
            row = {}
            for field_name in field_names:
                field_data = table_data[field_name]
                if i < len(field_data):
                    value = field_data[i]
                    # 过滤掉 "--" 值，将其转换为 None
                    row[field_name] = None if value == "--" else value
                else:
                    row[field_name] = None
            rows.append(row)
        
        # 转换为DataFrame
        try:
            df = pd.DataFrame(rows)
            # 过滤掉所有字段都为空的行
            df = df.dropna(how='all')
            self.logger.debug(f"成功提取到 {len(df)} 行数据")
            return df
        except Exception as e:
            self.logger.error(f"转换 data_pool 数据为DataFrame失败: {e}")
            return None
        
    async def _post_execute(self, result, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行后的清理工作"""
        # 在任务执行完毕后，无论成功、失败还是取消，都尝试关闭API会话
        if self.api and self.api._session and not self.api._session.closed:
            await self.api.close()
            self.logger.debug(f"任务 {self.name}: iFind API 会话已关闭。")
        await super()._post_execute(result, stop_event, **kwargs)