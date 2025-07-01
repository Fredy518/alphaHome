#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
iFind 股票基础信息任务

获取上市公司基本资料，使用同花顺 iFind 的 data_pool 接口。
报表名称：p00005（上市公司基本资料）

数据来源：同花顺 iFind API
更新频率：按需更新
"""

import logging
from typing import Dict, Any, Optional, List

from alphahome.fetchers.sources.ifind.ifind_task import iFindTask
from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import ConfigManager
from alphahome.common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class iFindStockBasicTask(iFindTask):
    """获取上市公司基本资料（data_pool: p00005）"""

    # 1. 核心属性
    name: str = "ifind_stock_basic"
    description = "获取上市公司基本资料（同花顺 iFind）"
    table_name: str = "ifind_stock_basic"
    primary_keys = ["p00005_f023"]  # A股代码作为主键
    date_column = None  # 基础信息不按日期更新
    data_source = "ifind"

    # 2. iFindTask 必需属性（data_pool 端点不使用这些属性，但需要设置以满足基类要求）
    api_endpoint: str = "data_pool"
    indicators: str = ""  # data_pool 不使用 indicators，使用 outputpara
    
    # 3. data_pool 特有属性
    reportname: str = "p00005"  # 上市公司基本资料报表
    functionpara = {
        "thscode": "",  # 空字符串表示获取所有股票
        "rowFilter": "",
        "codeNameColumn": "p00005_f001,p00005_f023,p00005_f024,p00005_f025"
    }
    outputpara: str = "p00005_f001,p00005_f002,p00005_f003,p00005_f023,p00005_f024,p00005_f025,p00005_f026,p00005_f008,p00005_f007,p00005_f006,p00005_f009,p00005_f010,p00005_f020,p00005_f011,p00005_f012,p00005_f014,p00005_f015,p00005_f016,p00005_f027,p00005_f028,p00005_f029"
    
    # 4. 数据库表结构定义（根据真实字段调整）
    schema_def = {
        "p00005_f001": "VARCHAR(100)",  # 公司名称
        "p00005_f002": "VARCHAR(50)",   # 日期_成立
        "p00005_f003": "VARCHAR(50)",   # 日期_上市
        "p00005_f023": "VARCHAR(20)",   # 已发行股票代码_A股
        "p00005_f024": "VARCHAR(20)",   # 已发行股票代码_B股
        "p00005_f025": "VARCHAR(20)",   # 已发行股票代码_H股
        "p00005_f026": "VARCHAR(50)",   # 其他字段
        "p00005_f008": "VARCHAR(50)",   # 其他字段
        "p00005_f007": "TEXT",          # 备注信息（DT_MEMO 类型）
        "p00005_f006": "TEXT",          # 备注信息（DT_MEMO 类型）
        "p00005_f009": "VARCHAR(50)",   # 其他字段
        "p00005_f010": "VARCHAR(50)",   # 其他字段
        "p00005_f020": "VARCHAR(100)",  # 其他字段
        "p00005_f011": "VARCHAR(100)",  # 其他字段
        "p00005_f012": "VARCHAR(100)",  # 其他字段
        "p00005_f014": "VARCHAR(100)",  # 其他字段
        "p00005_f015": "VARCHAR(50)",   # 其他字段
        "p00005_f016": "VARCHAR(50)",   # 其他字段
        "p00005_f027": "VARCHAR(50)",   # 其他字段
        "p00005_f028": "VARCHAR(50)",   # 其他字段
        "p00005_f029": "VARCHAR(100)"   # 其他字段
    }
    
    # 5. 代码级默认配置
    default_concurrent_limit = 1  # data_pool 通常一次性获取所有数据
    default_page_size = 5000
    update_type = "full"  # 基础信息通常是全量更新

    def __init__(self, db_manager: DBManager, config_manager: ConfigManager, **kwargs):
        super().__init__(db_manager, config_manager, **kwargs)

    async def prepare_params(self, batch: List[str]) -> Dict[str, Any]:
        """
        为 data_pool 端点准备参数。
        重写基类方法以支持 data_pool 的参数结构。
        """
        # data_pool 端点不按股票代码分批，而是一次性获取所有数据
        params = {
            "endpoint": self.api_endpoint,
            "reportname": self.reportname,
            "functionpara": self.functionpara.copy(),
            "outputpara": self.outputpara
        }
        
        self.logger.debug(f"任务 {self.name}: 准备 data_pool 参数")
        return params

    async def get_batch_list(self, **kwargs) -> List[List[str]]:
        """
        data_pool 端点通常不需要分批，返回单个批次。
        """
        # data_pool 获取所有数据，不需要按股票代码分批
        return [[]]  # 单个空批次，表示获取所有数据

    def get_display_name(self) -> str:
        """返回任务的显示名称"""
        return f"iFind股票基础信息 ({len(self.codes_to_fetch)} 只股票)" 