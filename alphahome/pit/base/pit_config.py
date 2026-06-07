#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据表统一配置
================

为所有PIT数据表管理器提供统一的配置管理

Author: AI Assistant
Date: 2025-08-11
"""

from typing import Dict, List, Any
from datetime import datetime, timedelta

class PITConfig:
    """PIT数据表统一配置"""
    
    # ===========================================
    # 数据表配置
    # ===========================================
    
    # PIT数据表列表
    PIT_TABLES = {
        'pit_income_quarterly': {
            'description': 'PIT利润表数据',
            'tushare_table': 'fina_income',
            'key_fields': ['ts_code', 'end_date', 'ann_date'],
            'data_fields': ['revenue', 'oper_cost', 'n_income_attr_p', 'operate_profit'],
            # 扩展字段（动态建表用）
            'extended_fields': {
                'n_income': 'numeric(20,4)',
                'total_profit': 'numeric(20,4)',
                'net_profit_mid': 'numeric(20,4)',
                'conversion_status': 'varchar(20)',
                'year': 'integer',
                'quarter': 'integer'
            },
            'has_historical_data': True,
            'supports_incremental': True,
            # express 归母净利回填历史回看窗口（单位：月）
            'INCOME_EXPRESS_FILL_LOOKBACK_MONTHS': 9
        },
        'pit_balance_quarterly': {
            'description': 'PIT资产负债表数据',
            # express 缺失字段历史回看窗口（单位：月），用于PIT填充
            'BALANCE_EXPRESS_FILL_LOOKBACK_MONTHS': 9,
            'tushare_table': 'fina_balancesheet',
            'key_fields': ['ts_code', 'end_date', 'ann_date'],
            'data_fields': ['tot_assets', 'tot_liab', 'tot_equity'],
            'has_historical_data': True,
            'supports_incremental': True,
            # 行业字段适用性：按申万一级行业屏蔽不适用字段
            'INDUSTRY_FIELD_ALLOWLIST': {
                '银行': {
                    'exclude_fields': ['total_cur_assets', 'total_cur_liab', 'inventories']
                }
            },
            # 扩展字段（动态建表用），类型需为标准SQL类型
            'extended_fields': {
                'total_cur_assets': 'numeric(20,4)',
                'total_cur_liab': 'numeric(20,4)',
                'inventories': 'numeric(20,4)'
            }
        },
        'pit_industry_classification': {
            'description': 'PIT行业分类数据',
            'tushare_tables': ['index_swmember', 'index_cimember'],
            'key_fields': ['ts_code', 'obs_date', 'data_source'],
            'data_fields': ['industry_level1', 'industry_level2', 'industry_level3'],
            'has_historical_data': True,
            'supports_incremental': True,
            'snapshot_mode': True  # 月度快照模式
        },
        'pit_financial_indicators': {
            'description': 'PIT财务指标（标准版本）',
            'source_tables': ['pit_income_quarterly', 'pit_balance_quarterly'],
            'key_fields': ['ts_code', 'end_date', 'ann_date'],
            'data_fields': [
                'gpa_ttm',           # 总资产利润率TTM
                'roe_excl_ttm',      # 净资产收益率(扣除少数股东权益)TTM
                'roa_excl_ttm',      # 总资产报酬率(扣除少数股东权益)TTM
                'revenue_yoy_growth',          # 营收同比增长率(%)
                'n_income_yoy_growth',         # 归属母公司股东的净利润同比增长率(%)
                'operate_profit_yoy_growth'    # 经营利润同比增长率(%)
            ],
            'has_historical_data': True,
            'supports_incremental': True,
            'depends_on': ['pit_income_quarterly', 'pit_balance_quarterly']
        }
    }
    
    # ===========================================
    # 执行配置
    # ===========================================
    
    # 默认批次大小
    DEFAULT_BATCH_SIZES = {
        'pit_income_quarterly': 1000,
        'pit_balance_quarterly': 1000,
        'pit_industry_classification': 1000,
        'pit_financial_indicators': 500
    }
    
    # 默认日期范围
    DEFAULT_DATE_RANGES = {
        'incremental_days': 7,      # 增量更新默认天数
        'backfill_start': '2010-01-01',  # 历史回填起始日期
        'max_backfill_days': 30     # 单次回填最大天数
    }
    
    # ===========================================
    # 数据质量配置
    # ===========================================
    
    # 数据质量等级
    DATA_QUALITY_LEVELS = ['high', 'normal', 'low', 'invalid']
    
    # 数据源类型
    DATA_SOURCES = {
        'tushare': 'tushare数据源',
        'manual': '手动数据源',
        'calculated': '计算数据源'
    }
    
    # ===========================================
    # 日志配置
    # ===========================================
    
    # 日志级别
    LOG_LEVELS = {
        'DEBUG': 10,
        'INFO': 20,
        'WARNING': 30,
        'ERROR': 40,
        'CRITICAL': 50
    }
    
    # 日志格式
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # 日志文件路径模板
    LOG_FILE_TEMPLATE = 'logs/pit_{table_name}_{date}.log'
    
    # ===========================================
    # 数据库配置
    # ===========================================
    
    # 数据库schema
    PIT_SCHEMA = 'pgs_factors'
    TUSHARE_SCHEMA = 'tushare'
    
    # 连接池配置
    DB_POOL_CONFIG = {
        'min_connections': 1,
        'max_connections': 10,
        'connection_timeout': 30
    }
    
    # ===========================================
    # 性能配置
    # ===========================================
    
    # 并发配置
    CONCURRENCY_CONFIG = {
        'max_workers': 4,           # 最大并发数
        'chunk_size': 100,          # 数据块大小
        'timeout_seconds': 300      # 超时时间
    }
    
    # 内存配置
    MEMORY_CONFIG = {
        'max_memory_mb': 2048,      # 最大内存使用
        'gc_threshold': 1000        # 垃圾回收阈值
    }
    
    @classmethod
    def get_table_config(cls, table_name: str) -> Dict[str, Any]:
        """获取指定表的配置"""
        if table_name not in cls.PIT_TABLES:
            raise ValueError(f"未知的PIT表: {table_name}")
        return cls.PIT_TABLES[table_name]
    
    @classmethod
    def get_batch_size(cls, table_name: str) -> int:
        """获取指定表的批次大小"""
        return cls.DEFAULT_BATCH_SIZES.get(table_name, 1000)
    
    @classmethod
    def get_log_file_path(cls, table_name: str) -> str:
        """获取日志文件路径"""
        date_str = datetime.now().strftime('%Y%m%d')
        return cls.LOG_FILE_TEMPLATE.format(table_name=table_name, date=date_str)
    
    @classmethod
    def get_incremental_date_range(cls, days: int = None) -> tuple:
        """获取增量更新的日期范围"""
        if days is None:
            days = cls.DEFAULT_DATE_RANGES['incremental_days']
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    @classmethod
    def get_backfill_date_range(cls, start_date: str = None, end_date: str = None) -> tuple:
        """获取历史回填的日期范围"""
        if start_date is None:
            start_date = cls.DEFAULT_DATE_RANGES['backfill_start']
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        return start_date, end_date
    
    @classmethod
    def validate_table_name(cls, table_name: str) -> bool:
        """验证表名是否有效"""
        return table_name in cls.PIT_TABLES
    
    @classmethod
    def get_table_dependencies(cls, table_name: str) -> List[str]:
        """获取表的依赖关系"""
        config = cls.get_table_config(table_name)
        return config.get('depends_on', [])
    
    @classmethod
    def get_execution_order(cls, table_names: List[str]) -> List[str]:
        """根据依赖关系确定执行顺序"""
        # 简单的拓扑排序
        ordered = []
        remaining = table_names.copy()
        
        while remaining:
            # 找到没有未满足依赖的表
            ready = []
            for table in remaining:
                deps = cls.get_table_dependencies(table)
                if all(dep in ordered or dep not in table_names for dep in deps):
                    ready.append(table)
            
            if not ready:
                # 如果没有可执行的表，说明存在循环依赖
                raise ValueError(f"检测到循环依赖: {remaining}")
            
            # 添加到执行顺序并从剩余列表中移除
            for table in ready:
                ordered.append(table)
                remaining.remove(table)
        
        return ordered
