#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
块处理器基类

提供基于块的数据处理功能，允许将大型数据集分割成更小的块进行处理。
适用于内存受限的环境或需要增量处理的场景。
"""

import abc
import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Callable, Iterator
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import asyncio
import math


class BlockProcessor(abc.ABC):
    """块处理器基类
    
    提供基于块的数据处理功能，允许将大型数据集分割成更小的块进行处理。
    支持日期块、代码块或自定义块分割。
    
    使用场景:
    1. 处理大型数据集，需要控制内存使用
    2. 增量处理，只处理新数据
    3. 并行处理，提高性能
    
    示例:
    ```python
    class DailyBarProcessor(BlockProcessor):
        async def process_block(self, data):
            # 处理一个日期块的数据
            result = data.copy()
            # ... 计算技术指标等
            return result
            
    processor = DailyBarProcessor(
        db_connection,
        date_column='trade_date',
        block_type='date',
        block_size=timedelta(days=30)
    )
    
    # 处理过去一年的数据
    start_date = datetime.now().date() - timedelta(days=365)
    end_date = datetime.now().date()
    result = await processor.execute(start_date=start_date, end_date=end_date)
    ```
    """
    
    def __init__(self, 
                db_connection,
                date_column: Optional[str] = None,
                code_column: Optional[str] = None,
                block_type: str = 'date',
                block_size: Union[int, timedelta] = 30,
                max_blocks_in_memory: int = 3,
                config: Optional[Dict[str, Any]] = None):
        """初始化块处理器
        
        Args:
            db_connection: 数据库连接
            date_column: 日期列名，用于日期块
            code_column: 代码列名，用于代码块
            block_type: 块类型，可选值: 'date', 'code', 'custom'
            block_size: 块大小，对于日期块为timedelta，对于代码块为每块的代码数量
            max_blocks_in_memory: 内存中最大同时处理的块数量
            config: 额外配置参数
        """
        self.db = db_connection
        self.date_column = date_column
        self.code_column = code_column
        self.block_type = block_type
        self.block_size = block_size
        self.max_blocks_in_memory = max_blocks_in_memory
        self.config = config or {}
        self.name = self.__class__.__name__
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(f"block_processor.{self.name}")
        logger.setLevel(logging.INFO)
        return logger
        
    async def execute(self, **kwargs) -> Dict[str, Any]:
        """执行块处理
        
        根据参数和块类型，将数据分割成块并依次处理。
        
        对于日期块，参数包括:
        - start_date: 开始日期
        - end_date: 结束日期
        
        对于代码块，参数包括:
        - codes: 代码列表
        - date_range: 可选的日期范围
        
        对于自定义块，参数由子类定义
        
        Args:
            **kwargs: 处理参数
            
        Returns:
            Dict[str, Any]: 处理结果汇总
        """
        try:
            # 块分割
            blocks = await self._split_into_blocks(**kwargs)
            
            # 处理每个块
            total_rows_processed = 0
            processed_blocks = 0
            results = []
            
            for block_id, block_params in blocks:
                self.logger.info(f"处理块 {block_id}: {block_params}")
                
                # 获取块数据
                block_data_fetched = await self._fetch_block_data(block_params)
                
                is_block_empty = False
                if block_data_fetched is None:
                    is_block_empty = True
                elif isinstance(block_data_fetched, pd.DataFrame):
                    if block_data_fetched.empty:
                        is_block_empty = True
                elif isinstance(block_data_fetched, dict):
                    main_data_df = block_data_fetched.get('stock_data')
                    if main_data_df is None or main_data_df.empty:
                        self.logger.info(f"块 {block_id} 的 'stock_data' 为空或不存在。")
                        is_block_empty = True
                else:
                    self.logger.warning(f"块 {block_id} 的 _fetch_block_data 返回了未知类型的数据: {type(block_data_fetched)}")
                    is_block_empty = True 

                if is_block_empty:
                    self.logger.warning(f"块 {block_id} 数据无效或主要数据为空，跳过处理。")
                    continue
                    
                # 处理块
                block_result = await self.process_block(block_data_fetched, **kwargs)
                
                # 保存结果
                if block_result is None or block_result.empty:
                    self.logger.warning(f"块 {block_id} 的 process_block 返回了空结果，不保存。")
                elif kwargs.get('save_result', True) and hasattr(self, 'result_table') and self.result_table:
                    save_result = await self._save_block_result(block_result, block_params, **kwargs)
                    results.append(save_result)
                else:
                    results.append({"block_id": block_id, "rows": len(block_result)})
                    
                total_rows_processed += len(block_result)
                processed_blocks += 1
                
                # 日志
                self.logger.info(f"块 {block_id} 处理完成，处理了 {len(block_result)} 行")
                
            # 汇总结果
            return {
                "status": "success",
                "total_rows_processed": total_rows_processed,
                "blocks_processed": processed_blocks,
                "block_results": results
            }
            
        except Exception as e:
            self.logger.error(f"块处理执行失败: {str(e)}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e)
            }
            
    async def _split_into_blocks(self, **kwargs) -> List[Tuple[str, Dict[str, Any]]]:
        """将处理任务拆分成多个块
        
        Args:
            **kwargs: 处理参数
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: 块列表，每个元素为(块ID, 块参数)
        """
        if self.block_type == 'date':
            return await self._split_into_date_blocks(**kwargs)
        elif self.block_type == 'code':
            return await self._split_into_code_blocks(**kwargs)
        elif self.block_type == 'custom':
            return await self._split_into_custom_blocks(**kwargs)
        else:
            raise ValueError(f"未知的块类型: {self.block_type}")
            
    async def _split_into_date_blocks(self, **kwargs) -> List[Tuple[str, Dict[str, Any]]]:
        """按日期拆分成块
        
        Args:
            **kwargs: 处理参数，包括start_date和end_date
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: 日期块列表
        """
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        
        if not start_date or not end_date:
            raise ValueError("日期块处理需要提供start_date和end_date参数")
            
        # 确保日期格式一致
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y%m%d").date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
            
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y%m%d").date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()
            
        # 确保块大小为timedelta
        block_size = self.block_size
        if isinstance(block_size, int):
            block_size = timedelta(days=block_size)
            
        # 生成日期块
        blocks = []
        current_start = start_date
        block_id = 1
        
        while current_start <= end_date:
            current_end = min(current_start + block_size, end_date)
            
            block_params = {
                'start_date': current_start,
                'end_date': current_end
            }
            
            # 添加代码过滤条件（如果有）
            if 'codes' in kwargs:
                block_params['codes'] = kwargs['codes']
                
            blocks.append((f"date_block_{block_id}", block_params))
            
            current_start = current_end + timedelta(days=1)
            block_id += 1
            
        return blocks
        
    async def _split_into_code_blocks(self, **kwargs) -> List[Tuple[str, Dict[str, Any]]]:
        """按代码拆分成块
        
        Args:
            **kwargs: 处理参数，包括codes和可选的date_range
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: 代码块列表
        """
        codes = kwargs.get('codes')
        
        if not codes:
            raise ValueError("代码块处理需要提供codes参数")
            
        # 确定每块的代码数量
        block_size = self.block_size
        if not isinstance(block_size, int):
            block_size = 10  # 默认每块10个代码
            
        # 生成代码块
        blocks = []
        for i in range(0, len(codes), block_size):
            block_codes = codes[i:i + block_size]
            block_id = f"code_block_{i//block_size + 1}"
            
            block_params = {'codes': block_codes}
            
            # 添加日期过滤条件（如果有）
            if 'start_date' in kwargs and 'end_date' in kwargs:
                block_params['start_date'] = kwargs['start_date']
                block_params['end_date'] = kwargs['end_date']
                
            blocks.append((block_id, block_params))
            
        return blocks
        
    async def _split_into_custom_blocks(self, **kwargs) -> List[Tuple[str, Dict[str, Any]]]:
        """自定义块拆分
        
        子类应重写此方法以提供自定义块拆分逻辑。
        
        Args:
            **kwargs: 处理参数
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: 自定义块列表
        """
        raise NotImplementedError("子类必须实现_split_into_custom_blocks方法")
        
    async def _fetch_block_data(self, block_params: Dict[str, Any]) -> pd.DataFrame:
        """获取块数据
        
        根据块参数从数据库获取块数据。
        
        Args:
            block_params: 块参数
            
        Returns:
            pd.DataFrame: 块数据
        """
        from ..utils.query_builder import QueryBuilder
        
        # 构建查询
        query_builder = QueryBuilder(self.source_table)
        
        # 添加日期条件
        if 'start_date' in block_params and 'end_date' in block_params and self.date_column:
            query_builder.add_condition(f"{self.date_column} >= $start_date")
            query_builder.add_condition(f"{self.date_column} <= $end_date")
            
        # 添加代码条件
        if 'codes' in block_params and self.code_column:
            query_builder.add_in_condition(self.code_column, "$codes")
            
        # 添加排序
        if self.date_column:
            query_builder.add_order_by(self.date_column)
            
        if self.code_column:
            query_builder.add_order_by(self.code_column)
            
        # 构建查询和参数
        query, params = query_builder.build(block_params)
        
        # 获取数据
        try:
            rows = await self.db.fetch_all(query, params)
            
            if not rows:
                return pd.DataFrame()
                
            # 转换为DataFrame
            df = pd.DataFrame([dict(row) for row in rows])
            self.logger.info(f"获取了 {len(df)} 行数据")
            return df
            
        except Exception as e:
            self.logger.error(f"获取块数据失败: {str(e)}", exc_info=True)
            raise
            
    @abc.abstractmethod
    async def process_block(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """处理单个数据块
        
        子类必须实现此方法，定义块处理逻辑。
        
        Args:
            data: 块数据
            **kwargs: 额外参数
            
        Returns:
            pd.DataFrame: 处理后的块数据
        """
        raise NotImplementedError("子类必须实现process_block方法")
        
    async def _save_block_result(self, 
                              data: pd.DataFrame, 
                              block_params: Dict[str, Any],
                              **kwargs) -> Dict[str, Any]:
        """保存块处理结果
        
        Args:
            data: 处理后的块数据
            block_params: 块参数
            **kwargs: 额外参数
            
        Returns:
            Dict[str, Any]: 保存结果
        """
        if not hasattr(self, 'result_table') or not self.result_table:
            raise ValueError("未定义结果表名")
            
        if data is None or data.empty:
            return {"rows_saved": 0}
            
        # 转换为记录列表
        records = data.to_dict('records')
        
        # 清除可能存在的旧结果
        if kwargs.get('replace_existing', True):
            await self._clear_existing_results(data, block_params)
            
        try:
            # 保存数据
            if hasattr(self.db, 'copy_records_to_table'):
                # 使用批量插入
                rows_saved = await self.db.copy_records_to_table(self.result_table, records)
            else:
                # 使用逐条插入
                for record in records:
                    await self.db.insert(self.result_table, record)
                rows_saved = len(records)
                
            self.logger.info(f"保存了 {rows_saved} 行数据到表 {self.result_table}")
            return {"rows_saved": rows_saved}
            
        except Exception as e:
            self.logger.error(f"保存数据到表 {self.result_table} 时出错: {str(e)}", exc_info=True)
            raise
            
    async def _clear_existing_results(self, data: pd.DataFrame, block_params: Dict[str, Any]):
        """清除可能存在的旧结果
        
        根据块参数清除结果表中的旧数据。
        
        Args:
            data: 新数据
            block_params: 块参数
        """
        if not hasattr(self, 'result_table') or not self.result_table:
            return
            
        conditions = []
        params = {}
        
        # 日期条件
        if 'start_date' in block_params and 'end_date' in block_params and self.date_column:
            conditions.append(f"{self.date_column} BETWEEN $min_date AND $max_date")
            params['min_date'] = block_params['start_date']
            params['max_date'] = block_params['end_date']
            
        # 代码条件
        if 'codes' in block_params and self.code_column:
            conditions.append(f"{self.code_column} = ANY($codes)")
            params['codes'] = block_params['codes']
            
        if conditions:
            query = f"DELETE FROM {self.result_table} WHERE " + " AND ".join(conditions)
            self.logger.info(f"清除旧结果: {query}")
            await self.db.execute(query, params) 