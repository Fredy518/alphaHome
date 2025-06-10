#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股票分红送股数据任务

接口文档: https://tushare.pro/document/2?doc_id=103
数据说明:
- 获取股票分红送股数据
- 支持两种批处理策略:
  1. 全量模式: 按股票代码分批获取，过滤 div_proc='实施'
  2. 增量模式: 按实施公告日期(imp_ann_date)分批获取

权限要求: 需要至少2000积分
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, date
import logging
import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ...tools.batch_utils import generate_single_date_batches, generate_stock_code_batches
from ...task_decorator import task_register


@task_register()
class TushareStockDividendTask(TushareTask):
    """获取股票分红送股数据 (dividend)
    
    实现要求:
    - 全量更新: 使用ts_code作为batch单位，批量获取全部数据（div_proc='实施'）
    - 增量模式: 使用ex_date字段进行更新，调用generate_single_date_batches方法
    """
    
    # 1. 核心属性
    name = "tushare_stock_dividend"
    description = "获取股票分红送股数据"
    table_name = "tushare_stock_dividend"
    primary_keys = ["ts_code", "ex_date"]
    date_column = "ex_date"  # 除权除息日
    default_start_date = "20050101"  # 默认开始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 降低并发，避免频率限制
    default_page_size = 2000
    
    # 2. TushareTask 特有属性
    api_name = 'dividend'
    # Tushare dividend 接口返回的字段
    fields = [
        'ts_code',           # TS代码
        'end_date',          # 分红年度  
        'ann_date',          # 预案公告日
        'div_proc',          # 实施进度
        'stk_div',           # 每股送转
        'stk_bo_rate',       # 每股送股比例
        'stk_co_rate',       # 每股转增比例
        'cash_div',          # 每股分红（税后）
        'cash_div_tax',      # 每股分红（税前）
        'record_date',       # 股权登记日
        'ex_date',           # 除权除息日
        'pay_date',          # 派息日
        'div_listdate',      # 红股上市日
        'imp_ann_date',      # 实施公告日
        'base_date',         # 基准日
        'base_share'         # 基准股本（万）
    ]
    
    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        'stk_div': float,
        'stk_bo_rate': float,
        'stk_co_rate': float,
        'cash_div': float,
        'cash_div_tax': float,
        'base_share': float
    }
    
    # 5. 数据库表结构
    schema = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE"},
        "ann_date": {"type": "DATE"},
        "div_proc": {"type": "VARCHAR(10)"},
        "stk_div": {"type": "NUMERIC(10,4)"},
        "stk_bo_rate": {"type": "NUMERIC(10,4)"},
        "stk_co_rate": {"type": "NUMERIC(10,4)"},
        "cash_div": {"type": "NUMERIC(10,4)"},
        "cash_div_tax": {"type": "NUMERIC(10,4)"},
        "record_date": {"type": "DATE"},
        "ex_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pay_date": {"type": "DATE"},
        "div_listdate": {"type": "DATE"},
        "imp_ann_date": {"type": "DATE"},
        "base_date": {"type": "DATE"},
        "base_share": {"type": "NUMERIC(15,2)"}
        # update_time 会自动添加
    }
    
    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_dividend_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_dividend_imp_ann_date", "columns": "imp_ann_date"},
        {"name": "idx_stock_dividend_div_proc", "columns": "div_proc"},
        {"name": "idx_stock_dividend_ex_date", "columns": "ex_date"},
        {"name": "idx_stock_dividend_update_time", "columns": "update_time"}
    ]
    
    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        策略说明:
        1. 全量模式(force_full=True或提供start_date+end_date): 按股票代码分批
        2. 增量模式(智能增量): 按实施公告日期分批
        
        Args:
            **kwargs: 包含start_date, end_date, force_full等参数
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        force_full = kwargs.get('force_full', False)
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        
        self.logger.info(f"开始生成批处理列表 - force_full: {force_full}, start_date: {start_date}, end_date: {end_date}")
        
        # 策略1: 强制全量模式 - 按股票代码分批
        if force_full:
            self.logger.info("使用按股票代码分批策略（强制全量模式）")
            return await self._get_code_based_batches(**kwargs)
        
        # 策略2: 增量模式或指定日期范围 - 按实施公告日期分批  
        else:
            self.logger.info("使用按日期分批策略（增量模式）")
            return await self._get_date_based_batches(**kwargs)
    
    async def _get_code_based_batches(self, **kwargs) -> List[Dict]:
        """按股票代码分批获取数据（全量模式）
        
        使用通用的generate_stock_code_batches工具函数为每个股票代码生成单独批次
        全量模式下只传入ts_code参数，不传入日期参数以获取全部历史分红数据
        
        Returns:
            List[Dict]: 批处理参数列表，每个包含单个ts_code
        """
        try:
            # 全量模式下添加fields参数，显式传递所有输出字段
            additional_params = {
                'fields': ','.join(self.schema.keys())
            }
            
            # 使用通用工具函数生成单个股票代码批次
            batch_list = await generate_stock_code_batches(
                db_connection=self.db,
                table_name='tushare_stock_basic',
                code_column='ts_code',
                filter_condition=None,  # 获取所有股票，不过滤
                api_instance=self.api,
                additional_params=additional_params,
                logger=self.logger
            )
            
            self.logger.info(f"按股票代码分批完成，共生成 {len(batch_list)} 个批次")
            return batch_list
            
        except Exception as e:
            self.logger.error(f"按股票代码分批失败: {e}", exc_info=True)
            raise
    
    async def _get_date_based_batches(self, **kwargs) -> List[Dict]:
        """按日期分批获取数据（增量模式）
        
        使用ex_date字段按日期分批，调用generate_single_date_batches方法
        
        Returns:
            List[Dict]: 批处理参数列表，每个包含单个日期
        """
        try:
            # 确定日期范围
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')
            
            if not start_date or not end_date:
                self.logger.error("增量模式需要提供start_date和end_date")
                return []
            
            self.logger.info(f"按日期分批: {start_date} 到 {end_date}")
            
            # 准备额外参数，添加fields字段
            additional_params = {
                'fields': ','.join(self.schema.keys())
            }
            
            # 使用batch_utils中的generate_single_date_batches方法
            batch_list = await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field='ex_date',  # 使用除权除息日字段
                additional_params=additional_params,
                logger=self.logger
            )
            
            self.logger.info(f"按日期分批完成，共生成 {len(batch_list)} 个批次")
            return batch_list
            
        except Exception as e:
            self.logger.error(f"按日期分批失败: {e}", exc_info=True)
            raise
    
    
    
    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数
        
        处理批次参数，转换为Tushare API所需的格式
        
        Args:
            batch_params: 批次参数字典
            
        Returns:
            Dict: API调用参数
        """
        api_params = batch_params.copy()
        
        # 添加fields参数，确保获取所有需要的字段
        api_params['fields'] = ','.join(self.fields)
        
        # 清理无效参数
        if 'start_date' in api_params and 'end_date' in api_params:
            # dividend接口不直接支持日期范围，需要转换为其他参数
            # 这里我们保留，让API层处理
            pass
        
        self.logger.debug(f"准备API参数: {api_params}")
        return api_params
    
    async def fetch_batch(self, batch_params: Dict) -> Optional[pd.DataFrame]:
        """重写批次获取方法，添加div_proc过滤
        
        获取数据后过滤div_proc='实施'的记录
        
        Args:
            batch_params: 批次参数
            
        Returns:
            Optional[pd.DataFrame]: 过滤后的数据
        """
        # 调用父类方法获取数据
        data = await super().fetch_batch(batch_params)
        
        # 对数据进行div_proc过滤
        if data is not None and not data.empty and 'div_proc' in data.columns:
            original_count = len(data)
            filtered_data = data[data['div_proc'] == '实施']
            filtered_count = len(filtered_data)
            
            ts_code = batch_params.get('ts_code', '未知')
            self.logger.debug(f"股票 {ts_code}: 获取到 {original_count} 条数据，过滤后 {filtered_count} 条已实施分红")
            
            return filtered_data
        
        return data


# 导出任务类
__all__ = ['TushareStockDividendTask'] 