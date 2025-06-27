#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务基类

专门用于处理已存储数据的任务类型。
继承自统一的BaseTask，专注于数据处理的特定需求。
"""

import pandas as pd
from typing import Dict, List, Any, Union, Optional
from ..common.task_system.base_task import BaseTask
from abc import abstractmethod
from ..common.task_system import BaseTask
from .base.block_processor import BlockProcessorMixin
import asyncio


class ProcessorTask(BaseTask, BlockProcessorMixin):
    """
    数据处理任务的统一基类。

    它整合了 BlockProcessorMixin，为所有处理器任务提供了可选的分块处理能力。
    一个处理器任务必须实现以下两种执行逻辑之一：
    1. 对于非分块任务：实现 `process` 方法。
    2. 对于分块任务：
        a. 设置 `is_block_processor = True`。
        b. 实现 `get_data_blocks` 方法来定义如何分块。
        c. 实现 `process_block` 方法来处理单个块。
    """
    
    # 设置任务类型为processor
    task_type: str = "processor"
    
    # processor任务特有的属性  
    source_tables: List[str] = []        # 源数据表列表
    dependencies: List[str] = []         # 依赖的其他任务
    calculation_method = None            # 计算方法标识
    batch_size = 1000                    # 批处理大小
    
    def __init__(self, db_connection):
        """初始化处理任务
        
        Args:
            db_connection: 数据库连接
        """
        super().__init__(db_connection)
        
        # 设置处理任务专用的日志记录器
        import logging
        self.logger = logging.getLogger(f"processor_task.{self.name}")
        
        # 验证必要的配置
        if not self.source_tables:
            self.logger.warning(f"处理任务 {self.name} 未定义source_tables")
    
    async def fetch_data(self, stop_event=None, **kwargs):
        """从数据库获取源数据
        
        重写基类方法，从数据库表中获取数据而不是外部API。
        支持单表和多表查询。
        """
        if len(self.source_tables) == 1:
            # 单表查询
            return await self._fetch_single_table(**kwargs)
        else:
            # 多表查询
            return await self._fetch_multiple_tables(**kwargs)
    
    async def _fetch_single_table(self, **kwargs):
        """从单个表获取数据"""
        table_name = self.source_tables[0]
        
        # 构建基础查询
        query = f"SELECT * FROM {table_name}"
        conditions = []
        params = []
        
        # 添加日期范围条件
        if 'start_date' in kwargs and 'end_date' in kwargs and self.date_column:
            conditions.append(f"{self.date_column} >= %s AND {self.date_column} <= %s")
            params.extend([kwargs['start_date'], kwargs['end_date']])
        
        # 添加股票代码条件（如果适用）
        if 'ts_code' in kwargs:
            conditions.append("ts_code = %s")
            params.append(kwargs['ts_code'])
        
        # 组装完整查询
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # 添加排序
        if self.date_column:
            query += f" ORDER BY {self.date_column}"
        
        self.logger.info(f"执行单表查询: {table_name}")
        self.logger.debug(f"查询SQL: {query}")
        
        try:
            result = await self.db.fetch_dataframe(query, params)
            self.logger.info(f"从表 {table_name} 获取 {len(result)} 行数据")
            return result
        except Exception as e:
            self.logger.error(f"查询表 {table_name} 失败: {str(e)}")
            raise
    
    async def _fetch_multiple_tables(self, **kwargs):
        """从多个表获取数据"""
        # 子类可以重写此方法实现具体的多表联合查询逻辑
        # 这里提供一个基础实现，分别获取各表数据
        tables_data = {}
        
        for table_name in self.source_tables:
            self.logger.info(f"获取表 {table_name} 的数据")
            
            # 构建基础查询
            query = f"SELECT * FROM {table_name}"
            conditions = []
            params = []
            
            # 添加日期范围条件
            if 'start_date' in kwargs and 'end_date' in kwargs:
                # 假设每个表都有date_column，子类可重写更复杂的逻辑
                date_col = self.date_column or 'trade_date'
                conditions.append(f"{date_col} >= %s AND {date_col} <= %s")
                params.extend([kwargs['start_date'], kwargs['end_date']])
            
            # 添加股票代码条件（如果适用）
            if 'ts_code' in kwargs:
                conditions.append("ts_code = %s")
                params.append(kwargs['ts_code'])
            
            # 组装完整查询
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            try:
                table_data = await self.db.fetch_dataframe(query, params)
                tables_data[table_name] = table_data
                self.logger.info(f"从表 {table_name} 获取 {len(table_data)} 行数据")
            except Exception as e:
                self.logger.error(f"查询表 {table_name} 失败: {str(e)}")
                raise
        
        return tables_data
    
    def process_data(self, data, stop_event=None, **kwargs):
        """
        处理数据（重写基类的扩展点）

        专门处理数据计算逻辑，实现了 ProcessorTask 特有的数据处理流程。

        Args:
            data: 源数据，可能是DataFrame或多个表的数据字典
            stop_event: 停止事件
            **kwargs: 其他参数

        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 首先调用核心计算方法（ProcessorTask特有逻辑）
        if isinstance(data, dict):
            # 多表数据
            result = self._calculate_from_multiple_sources(data, **kwargs)
        else:
            # 单表数据
            result = self._calculate_from_single_source(data, **kwargs)

        # 然后应用基类的通用处理逻辑（如transformations）
        result = super().process_data(result, stop_event=stop_event, **kwargs)

        return result
    
    def _calculate_from_single_source(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """从单个数据源计算
        
        子类应重写此方法实现具体的计算逻辑。
        """
        raise NotImplementedError("ProcessorTask子类必须实现_calculate_from_single_source方法")
    
    def _calculate_from_multiple_sources(self, data: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """从多个数据源计算
        
        子类应重写此方法实现具体的多表联合计算逻辑。
        """
        raise NotImplementedError("ProcessorTask子类必须实现_calculate_from_multiple_sources方法")
    
    def validate_data(self, data):
        """验证处理后的数据"""
        # 调用基类验证
        base_valid = super().validate_data(data)
        
        # 添加处理任务特有的验证逻辑
        if isinstance(data, pd.DataFrame):
            # 检查关键列是否存在
            required_columns = getattr(self, 'required_columns', [])
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                self.logger.error(f"处理结果缺少必要列: {missing_columns}")
                return False
            
            # 检查数据量是否合理
            if len(data) == 0:
                self.logger.warning("处理结果为空")
                return False
        
        return base_valid
    
    async def pre_execute(self, stop_event=None, **kwargs):
        """处理任务执行前的准备工作"""
        await super().pre_execute(stop_event=stop_event, **kwargs)
        
        # 检查依赖任务是否完成（这里提供基础框架，具体实现可在子类中完善）
        if self.dependencies:
            self.logger.info(f"检查依赖任务: {self.dependencies}")
            # TODO: 实现依赖检查逻辑
        
        # 验证源表是否存在
        for table_name in self.source_tables:
            table_exists = await self._check_table_exists(table_name)
            if not table_exists:
                raise ValueError(f"源表 {table_name} 不存在")
        
        self.logger.info(f"开始执行数据处理任务: {self.name}")
    
    async def _check_table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        # 使用 db_manager 的 table_exists 方法，支持 schema-aware 查询
        try:
            # 创建一个虚拟任务对象用于解析表名
            class TableProxy:
                def __init__(self, table_name):
                    self.table_name = table_name
                    self.data_source = None  # 默认使用 public schema
                    
            proxy = TableProxy(table_name)
            return await self.db.table_exists(proxy)
        except Exception as e:
            self.logger.error(f"检查表 {table_name} 是否存在时出错: {str(e)}")
            return False
    
    async def post_execute(self, result, stop_event=None, **kwargs):
        """处理任务执行后的清理工作"""
        await super().post_execute(result, stop_event=stop_event, **kwargs)
        
        # 记录处理统计信息
        if result.get("status") == "success":
            rows = result.get("rows", 0)
            self.logger.info(f"数据处理完成: {self.name}, 生成数据 {rows} 行")
            self.logger.info(f"源表: {self.source_tables}, 目标表: {self.table_name}")
        else:
            self.logger.warning(f"数据处理未成功: {self.name}, 状态: {result.get('status')}")
    
    def get_processing_info(self) -> Dict[str, Any]:
        """获取处理任务的详细信息"""
        return {
            "name": self.name,
            "type": self.task_type,
            "source_tables": self.source_tables,
            "target_table": self.table_name,
            "dependencies": self.dependencies,
            "calculation_method": self.calculation_method,
            "description": self.description,
        }

    async def run(self, **kwargs: Any) -> None:
        """
        任务执行的统一入口点。
        根据 `is_block_processor` 属性，决定是按块执行还是整体执行。
        """
        if self.is_block_processor:
            # 对于分块任务，调用分块处理驱动方法
            # 注意：run_all_blocks 是同步的，如果需要异步块处理，未来需要调整
            self.run_all_blocks(**kwargs)
        else:
            # 对于非分块任务，调用 process 方法
            # 注意：process 方法可以是异步的
            if asyncio.iscoroutinefunction(self.process):
                await self.process(**kwargs)
            else:
                self.process(**kwargs)

    # --- 标准（非分块）处理接口 ---
    @abstractmethod
    def process(self, **kwargs: Any) -> Optional[pd.DataFrame]:
        """
        处理非分块任务的核心逻辑。
        如果任务不是分块任务，则必须实现此方法。
        """
        raise NotImplementedError("非分块处理器必须实现 process 方法。")

    # --- Mixin 方法的占位符实现 ---
    # ProcessorTask 本身不是一个具体的分块任务，
    # 所以它提供了 Mixin 抽象方法的基础实现，以满足 ABC 的要求。
    # 需要分块的子类必须重写这些方法。

    def get_data_blocks(self, **kwargs) -> pd.DataFrame:
        if self.is_block_processor:
            raise NotImplementedError("分块处理器必须重写 get_data_blocks 方法。")
        # 非分块任务不需要实现此方法，返回一个空列表
        return []

    def process_block(self, block_params: Dict[str, Any]) -> Optional[pd.DataFrame]:
        if self.is_block_processor:
            raise NotImplementedError("分块处理器必须重写 process_block 方法。")
        # 非分块任务不需要实现此方法
        return None 