#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Excel 数据源任务基类

用于从本地 Excel 文件读取数据并保存到数据库。
适用于补充难以从常规数据源获取的数据。
"""

import abc
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from alphahome.fetchers.base.fetcher_task import FetcherTask


class ExcelTask(FetcherTask, abc.ABC):
    """
    从 Excel 文件读取数据的抽象任务基类。

    特点：
    - 不需要API调用，直接读取本地文件
    - 支持单一批次模式（通常Excel文件作为整体处理）
    - 自动读取第一个sheet，无需配置sheet名称
    - 需要子类指定文件路径等配置
    """

    data_source = "excel"
    
    # Excel 特有配置
    excel_file_path: Optional[str] = None  # Excel 文件路径
    header_row: Optional[int] = 0  # 表头所在行，默认第一行
    skip_rows: Optional[List[int]] = None  # 要跳过的行
    use_cols: Optional[List[str]] = None  # 要读取的列
    
    # 数据处理配置
    column_mapping: Optional[Dict[str, str]] = None  # 列名映射
    date_columns: Optional[List[str]] = None  # 需要解析为日期的列
    
    def __init__(
        self,
        db_connection,
        excel_file_path: Optional[str] = None,
        **kwargs,
    ):
        """
        初始化 ExcelTask。
        
        Args:
            db_connection: 数据库连接
            excel_file_path: Excel 文件路径（可选，用于覆盖类属性）
            **kwargs: 其他参数传递给父类
        """
        super().__init__(db_connection, **kwargs)
        
        # 允许通过构造函数参数覆盖类属性
        if excel_file_path:
            self.excel_file_path = excel_file_path
            
        # 验证文件路径
        if not self.excel_file_path:
            raise ValueError(f"任务 {self.name} 必须指定 excel_file_path")
            
        self.excel_file_path = Path(self.excel_file_path)
        
        if not self.excel_file_path.exists():
            raise FileNotFoundError(f"Excel 文件不存在: {self.excel_file_path}")
            
        self.logger.info(f"Excel任务初始化: {self.name}, 文件: {self.excel_file_path}")

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成批次列表。

        对于 Excel 文件，通常只有一个批次（整个文件），读取第一个sheet。

        Returns:
            List[Dict]: 批次列表，每个批次包含读取参数
        """
        # 默认返回单一批次，包含基本的读取参数
        batch = {
            "file_path": str(self.excel_file_path),
        }

        self.logger.info(
            f"任务 {self.name}: Excel文件模式，生成单一批次。"
        )

        return [batch]

    async def _determine_date_range(self) -> Optional[Dict[str, str]]:
        """
        确定日期范围。
        
        对于 Excel 任务，由于不支持增量更新，总是使用全量更新模式。
        如果表不存在或查询失败，直接返回默认日期范围。
        
        Returns:
            Optional[Dict[str, str]]: 日期范围字典 {"start_date": ..., "end_date": ...} 或 None
        """
        from datetime import datetime
        
        self.logger.info(f"'{self.name}' - 确定日期范围 (Excel任务使用全量更新模式)...")
        
        # Excel 任务强制使用全量更新，不依赖数据库中的日期
        # 这避免了表不存在时的查询错误
        start_date = self.default_start_date
        end_date = datetime.now().strftime("%Y%m%d")
        
        return {"start_date": start_date, "end_date": end_date}

    async def prepare_params(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备批次参数。
        
        对于 Excel 任务，直接返回批次参数即可。
        
        Args:
            batch: 批次信息
            
        Returns:
            Dict[str, Any]: 准备好的参数
        """
        return batch.copy()

    async def fetch_batch(
        self,
        params: Dict[str, Any],
        stop_event: Optional[object] = None
    ) -> Optional[pd.DataFrame]:
        """
        读取单个批次的数据（从Excel文件第一个sheet）。

        Args:
            params: 批次参数，包含文件路径等
            stop_event: 停止事件（Excel读取通常很快，可能用不上）

        Returns:
            pd.DataFrame: 读取的数据，如果失败返回 None
        """
        try:
            file_path = params.get("file_path", self.excel_file_path)

            self.logger.info(
                f"开始读取 Excel 文件第一个sheet: {file_path}"
            )

            # 构建 pandas read_excel 参数，固定读取第一个sheet
            read_kwargs = {
                "sheet_name": 0,
                "header": self.header_row,
            }
            
            if self.skip_rows:
                read_kwargs["skiprows"] = self.skip_rows
                
            if self.use_cols:
                read_kwargs["usecols"] = self.use_cols
                
            if self.date_columns:
                read_kwargs["parse_dates"] = self.date_columns
            
            # 读取 Excel 文件
            data = pd.read_excel(file_path, **read_kwargs)
            
            if data is None or data.empty:
                self.logger.warning(f"Excel 文件为空或无数据: {file_path}")
                return None
                
            self.logger.info(f"成功读取 {len(data)} 行数据")
            
            # 应用列名映射
            if self.column_mapping:
                data = data.rename(columns=self.column_mapping)
                self.logger.debug(f"应用列名映射: {self.column_mapping}")
            
            # 数据清洗和转换（子类可以重写 process_data 方法进行更复杂的处理）
            processed_data = self.process_data(data)
            
            return processed_data
            
        except Exception as e:
            self.logger.error(
                f"读取 Excel 批次数据失败，参数: {params}。错误: {e}", 
                exc_info=True
            )
            raise

    def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """
        处理读取的数据。
        
        默认实现：
        - 去除空行
        - 去除前后空格
        
        子类可以重写此方法进行更复杂的数据处理。
        
        Args:
            data: 原始数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 去除完全为空的行
        data = data.dropna(how='all')
        
        # 去除字符串列的前后空格
        str_columns = data.select_dtypes(include=['object']).columns
        for col in str_columns:
            data[col] = data[col].str.strip() if data[col].dtype == 'object' else data[col]
        
        self.logger.debug(f"数据处理完成，剩余 {len(data)} 行")
        
        return data

    def supports_incremental_update(self) -> bool:
        """
        Excel 任务默认不支持智能增量更新。
        
        Excel 文件通常是完整的数据快照，不适合增量更新。
        如果特定任务需要支持增量更新，可以在子类中重写此方法。
        
        Returns:
            bool: False 表示不支持增量更新
        """
        return False

    def get_incremental_skip_reason(self) -> str:
        """
        返回不支持智能增量更新的原因说明。
        
        Returns:
            str: 跳过原因说明
        """
        return "Excel文件任务不支持智能增量更新，请使用全量更新模式"
