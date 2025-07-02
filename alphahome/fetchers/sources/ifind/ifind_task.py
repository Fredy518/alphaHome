from abc import ABC, abstractmethod
import json
import pandas as pd
from typing import Dict, Any, Optional, List
import asyncio
from alphahome.fetchers.base.fetcher_task import FetcherTask
from alphahome.fetchers.sources.ifind.ifind_api import iFindAPI
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
        self.api = iFindAPI(self.config_manager)
        
        # iFind 特有的配置
        self.codes_to_fetch: List[str] = kwargs.get("codes", [])
        
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
        通用实现，支持不同的 iFind API 端点。
        """
        try:
            endpoint = params["endpoint"]
            
            # 根据不同的端点调用不同的API方法
            if endpoint == "basic_data_service":
                codes = params["codes"]
                indicators = params["indicators"]
                
                # 使用改进的 basic_data_service API
                if isinstance(indicators, str):
                    indicator_list = indicators.split(';')
                else:
                    indicator_list = indicators
                
                # 调用改进的 API（自动处理 indipara 构造）
                json_response = await self.api.basic_data_service(
                    codes=",".join(codes),
                    indicators=indicator_list
                )
                
            elif endpoint == "data_pool":
                # data_pool 端点的参数处理
                reportname = params.get("reportname", "")
                functionpara = params.get("functionpara", {})
                outputpara = params.get("outputpara", "")
                
                # 调用 data_pool API
                json_response = await self.api.data_pool(
                    reportname=reportname,
                    functionpara=functionpara,
                    outputpara=outputpara
                )
                
            else:
                # 其他端点使用原有逻辑
                codes = params["codes"]
                indicators = params["indicators"]
                indicators_list = indicators.split(';') if isinstance(indicators, str) else indicators
                indipara_str = json.dumps([{"indicator": ind} for ind in indicators_list])
                payload = {
                    "codes": ",".join(codes), 
                    "indipara": indipara_str
                }
                json_response = await self.api.request(endpoint, payload)
            
            if json_response:
                return self.extract_data(json_response)
                
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 获取批次数据失败: {e}")
            raise
            
        return None
        
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
            self.logger.info(f"成功提取到 {len(df)} 行数据")
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
            self.logger.info(f"成功提取到 {len(df)} 行数据")
            return df
        except Exception as e:
            self.logger.error(f"转换 data_pool 数据为DataFrame失败: {e}")
            return None
        
    async def _post_execute(self, result, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行后的清理工作"""
        await self.api.close()
        await super()._post_execute(result, stop_event, **kwargs)