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
import pandas as pd
from typing import Dict, Any, Optional, List

from alphahome.fetchers.sources.ifind.ifind_task import iFindTask
from alphahome.common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class iFindStockBasicTask(iFindTask):
    """获取上市公司基本资料（data_pool: p00005）"""

    # 1. 核心属性
    name: str = "ifind_stock_basic"
    description = "获取上市公司基本资料（同花顺 iFind）"
    table_name: str = "ifind_stock_basic"
    primary_keys = ["stock_code_a"]  # 使用映射后的字段名作为主键
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
    
    # 4. 字段映射：将无意义的 p00005_fXXX 字段名映射为有意义的英文字段名
    column_mapping = {
        "p00005_f001": "company_name",        # 公司名称
        "p00005_f002": "establish_date",      # 成立日期
        "p00005_f003": "listing_date",        # 上市日期
        "p00005_f023": "stock_code_a",        # A股代码
        "p00005_f024": "stock_code_b",        # B股代码
        "p00005_f025": "stock_code_h",        # H股代码
        "p00005_f026": "exchange_market",     # 交易市场
        "p00005_f008": "industry_category",   # 行业分类
        "p00005_f007": "business_scope",      # 经营范围
        "p00005_f006": "company_profile",     # 公司简介
        "p00005_f009": "registered_address",  # 注册地址
        "p00005_f010": "office_address",      # 办公地址
        "p00005_f020": "company_website",     # 公司网站
        "p00005_f011": "legal_representative", # 法定代表人
        "p00005_f012": "registered_capital",  # 注册资本
        "p00005_f014": "total_share_capital", # 总股本
        "p00005_f015": "company_type",        # 公司类型
        "p00005_f016": "listing_status",      # 上市状态
        "p00005_f027": "isin_code",          # ISIN代码
        "p00005_f028": "market_sector",       # 市场板块
        "p00005_f029": "listing_board"        # 上市板块
    }
    
    # 5. 数据库表结构定义（使用映射后的字段名）
    schema_def = {
        "company_name": "VARCHAR(100)",        # 公司名称
        "establish_date": "DATE",              # 成立日期
        "listing_date": "DATE",                # 上市日期
        "stock_code_a": "VARCHAR(20)",         # A股代码（主键）
        "stock_code_b": "VARCHAR(20)",         # B股代码
        "stock_code_h": "VARCHAR(20)",         # H股代码
        "exchange_market": "VARCHAR(50)",      # 交易市场
        "industry_category": "VARCHAR(100)",   # 行业分类
        "business_scope": "TEXT",              # 经营范围
        "company_profile": "TEXT",             # 公司简介
        "registered_address": "VARCHAR(200)",  # 注册地址
        "office_address": "VARCHAR(200)",      # 办公地址
        "company_website": "VARCHAR(100)",     # 公司网站
        "legal_representative": "VARCHAR(50)", # 法定代表人
        "registered_capital": "VARCHAR(50)",   # 注册资本
        "total_share_capital": "VARCHAR(50)",  # 总股本
        "company_type": "VARCHAR(50)",         # 公司类型
        "listing_status": "VARCHAR(50)",       # 上市状态
        "isin_code": "VARCHAR(50)",           # ISIN代码
        "market_sector": "VARCHAR(50)",        # 市场板块
        "listing_board": "VARCHAR(50)"        # 上市板块
    }
    
    # 6. 代码级默认配置
    default_concurrent_limit = 1  # data_pool 通常一次性获取所有数据
    default_page_size = 5000
    update_type = "full"  # 基础信息通常是全量更新

    def __init__(self, db_connection, **kwargs):
        """
        初始化 iFindStockBasicTask。
        
        Args:
            db_connection: 数据库连接（与其他任务保持一致）
            **kwargs: 传递给父类的参数
        """
        super().__init__(db_connection, **kwargs)

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

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从API获取的原始数据，执行字段映射。
        
        Args:
            data: 从API获取的原始DataFrame
            **kwargs: 额外参数
            
        Returns:
            pd.DataFrame: 处理后的数据，包含字段映射
        """
        if data is None or data.empty:
            return data
        
        # 执行字段映射
        if self.column_mapping:
            # 只重命名存在的列
            rename_mapping = {}
            for old_col, new_col in self.column_mapping.items():
                if old_col in data.columns:
                    rename_mapping[old_col] = new_col
            
            if rename_mapping:
                data = data.rename(columns=rename_mapping)
                self.logger.info(f"字段映射完成，重命名了 {len(rename_mapping)} 个字段")
                self.logger.debug(f"字段映射详情: {rename_mapping}")
        
        # 数据类型转换和清理
        if 'establish_date' in data.columns:
            data['establish_date'] = pd.to_datetime(data['establish_date'], errors='coerce')
        
        if 'listing_date' in data.columns:
            data['listing_date'] = pd.to_datetime(data['listing_date'], errors='coerce')
        
        # 清理空值和无效数据
        for col in data.columns:
            if data[col].dtype == 'object':
                # 将 "--" 和空字符串转换为 None
                data[col] = data[col].replace(['--', '', 'nan'], None)
        
        return data

    def get_display_name(self) -> str:
        """返回任务的显示名称"""
        return f"iFind股票基础信息 ({len(self.codes_to_fetch)} 只股票)" 