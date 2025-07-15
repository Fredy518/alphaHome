#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
扩展的 BatchPlanner 功能模块

基于现有 BatchPlanner 架构，集成智能时间批处理策略和多维度分批能力。
提供更广泛的批处理场景支持，同时保持完全的向后兼容性。
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union
from itertools import groupby
import pandas as pd

from .batch_planner import BatchPlanner, Source, Partition, Map, PartitionStrategy, MapStrategy

T = TypeVar("T")
BatchResult = Dict[str, Any]


class SmartTimePartition:
    """
    智能时间分区策略，集成四级智能拆分算法
    
    四级智能拆分策略：
    - ≤31天：单批次策略（边界条件优化）
    - 32天-3个月：月度拆分（精细粒度）
    - 4个月-2年：季度拆分（平衡效率和精度）
    - 2-10年：半年度拆分（提高长期更新效率）
    - >10年：年度拆分（超长期数据优化）
    """
    
    @staticmethod
    def create(date_format: str = "%Y%m%d") -> PartitionStrategy:
        """
        创建智能时间分区策略
        
        Args:
            date_format: 日期格式，默认为 "%Y%m%d"
            
        Returns:
            PartitionStrategy: 分区策略函数
        """
        def smart_time_partitioner(date_range_data: Iterable[str]) -> List[List[str]]:
            """
            智能时间分区器
            
            Args:
                date_range_data: 包含 [start_date, end_date] 的可迭代对象
                
            Returns:
                List[List[str]]: 分区后的时间批次列表
            """
            date_list = list(date_range_data)
            if len(date_list) != 2:
                raise ValueError("SmartTimePartition expects exactly 2 elements: [start_date, end_date]")
            
            start_date_str, end_date_str = date_list
            
            try:
                start_dt = datetime.strptime(start_date_str, date_format)
                end_dt = datetime.strptime(end_date_str, date_format)
            except ValueError as e:
                raise ValueError(f"Invalid date format. Expected {date_format}: {e}")
            
            # 边界条件检查
            if start_dt > end_dt:
                raise ValueError(f"Start date {start_date_str} is later than end date {end_date_str}")
            
            # 智能确定批次频率
            freq, freq_desc = SmartTimePartition._determine_batch_frequency(start_dt, end_dt)
            
            # 生成智能时间批次
            time_batches = SmartTimePartition._generate_smart_time_batches(
                start_dt, end_dt, freq, date_format
            )
            
            # 转换为分区格式
            partitions = []
            for batch in time_batches:
                partitions.append([batch["start_date"], batch["end_date"]])
            
            return partitions
        
        return smart_time_partitioner
    
    @staticmethod
    def _determine_batch_frequency(start_dt: datetime, end_dt: datetime) -> tuple[str, str]:
        """确定批次频率"""
        total_days = (end_dt - start_dt).days
        total_months = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month)
        
        # 边界条件处理：对于小于1个完整月的时间跨度，使用单批次策略
        if total_days <= 31:
            return "SINGLE", "单批次"
        
        # 四级智能批次大小策略
        if total_months <= 3:
            return "MS", "月度"
        elif total_months <= 24:
            return "QS", "季度"
        elif total_months <= 120:
            return "6MS", "半年度"
        else:
            return "YS", "年度"
    
    @staticmethod
    def _generate_smart_time_batches(
        start_dt: datetime, 
        end_dt: datetime, 
        freq: str, 
        date_format: str
    ) -> List[Dict[str, str]]:
        """生成智能时间批次"""
        if freq == "SINGLE":
            return [{
                "start_date": start_dt.strftime(date_format),
                "end_date": end_dt.strftime(date_format)
            }]
        
        # 生成日期范围
        date_ranges = SmartTimePartition._generate_date_ranges(start_dt, end_dt, freq)
        
        # 转换为批次
        batches = []
        for i, period_start in enumerate(date_ranges):
            period_end = SmartTimePartition._calculate_period_end(period_start, freq, end_dt)
            
            batches.append({
                "start_date": period_start.strftime(date_format),
                "end_date": period_end.strftime(date_format)
            })
        
        return batches
    
    @staticmethod
    def _generate_date_ranges(start_dt: datetime, end_dt: datetime, freq: str) -> List[datetime]:
        """生成日期范围"""
        if freq == "6MS":
            # 半年度：手动生成
            date_ranges = []
            current = start_dt.replace(day=1)
            while current <= end_dt:
                date_ranges.append(current)
                if current.month <= 6:
                    current = current.replace(month=current.month + 6)
                else:
                    current = current.replace(year=current.year + 1, month=current.month - 6)
            return date_ranges
        elif freq == "YS":
            # 年度：手动生成
            date_ranges = []
            current = start_dt.replace(month=1, day=1)
            while current <= end_dt:
                date_ranges.append(current)
                current = current.replace(year=current.year + 1)
            return date_ranges
        else:
            # 月度和季度：使用pandas
            date_ranges = list(pd.date_range(start=start_dt, end=end_dt, freq=freq))
            return date_ranges if date_ranges else [start_dt]
    
    @staticmethod
    def _calculate_period_end(period_start: datetime, freq: str, end_dt: datetime) -> datetime:
        """计算批次结束日期"""
        if freq == "MS":
            period_end = period_start + pd.offsets.MonthEnd(1)
        elif freq == "QS":
            period_end = period_start + pd.offsets.QuarterEnd(1)
        elif freq == "6MS":
            if period_start.month <= 6:
                period_end = period_start.replace(month=6, day=30)
            else:
                period_end = period_start.replace(month=12, day=31)
        elif freq == "YS":
            period_end = period_start.replace(month=12, day=31)
        else:
            period_end = period_start + pd.offsets.MonthEnd(1)
        
        return min(period_end, end_dt)


class StatusPartition:
    """按股票状态分区策略"""
    
    @staticmethod
    def create(field: str = "list_status") -> PartitionStrategy:
        """
        创建按状态分区策略
        
        Args:
            field: 状态字段名，默认为 "list_status"
            
        Returns:
            PartitionStrategy: 分区策略函数
        """
        def status_partitioner(data: Iterable[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
            """按状态分区"""
            data_list = list(data)
            if not data_list:
                return []
            
            # 按状态字段分组
            sorted_data = sorted(data_list, key=lambda x: x.get(field, ""))
            grouped = groupby(sorted_data, key=lambda x: x.get(field, ""))
            
            partitions = []
            for status, group in grouped:
                group_list = list(group)
                if group_list:
                    partitions.append(group_list)
            
            return partitions
        
        return status_partitioner


class MarketPartition:
    """按市场类型分区策略"""
    
    @staticmethod
    def create(field: str = "market") -> PartitionStrategy:
        """
        创建按市场分区策略
        
        Args:
            field: 市场字段名，默认为 "market"
            
        Returns:
            PartitionStrategy: 分区策略函数
        """
        def market_partitioner(data: Iterable[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
            """按市场分区"""
            data_list = list(data)
            if not data_list:
                return []
            
            # 按市场字段分组
            sorted_data = sorted(data_list, key=lambda x: x.get(field, ""))
            grouped = groupby(sorted_data, key=lambda x: x.get(field, ""))
            
            partitions = []
            for market, group in grouped:
                group_list = list(group)
                if group_list:
                    partitions.append(group_list)
            
            return partitions
        
        return market_partitioner


class CompositePartition:
    """组合维度分区策略"""
    
    @staticmethod
    def create(partition_strategies: List[PartitionStrategy]) -> PartitionStrategy:
        """
        创建组合分区策略
        
        Args:
            partition_strategies: 分区策略列表
            
        Returns:
            PartitionStrategy: 组合分区策略函数
        """
        def composite_partitioner(data: Iterable[T]) -> List[List[T]]:
            """组合分区器"""
            current_partitions = [list(data)]
            
            # 依次应用每个分区策略
            for strategy in partition_strategies:
                new_partitions = []
                for partition in current_partitions:
                    sub_partitions = strategy(partition)
                    new_partitions.extend(sub_partitions)
                current_partitions = new_partitions
            
            return current_partitions
        
        return composite_partitioner


class ExtendedMap:
    """扩展的映射策略"""
    
    @staticmethod
    def to_smart_time_range(start_field: str = "start_date", end_field: str = "end_date") -> MapStrategy:
        """
        智能时间范围映射策略

        Args:
            start_field: 开始日期字段名
            end_field: 结束日期字段名

        Returns:
            MapStrategy: 映射策略函数
        """
        def smart_time_mapper(batch: List[str]) -> BatchResult:
            # batch 应该是一个包含两个日期字符串的列表 [start_date, end_date]
            if len(batch) != 2:
                raise ValueError(f"Smart time range mapping expects exactly 2 elements [start_date, end_date], got {len(batch)}")

            start_date, end_date = batch
            return {
                start_field: start_date,
                end_field: end_date
            }

        return smart_time_mapper
    
    @staticmethod
    def to_grouped_dict(group_field: str, items_field: str = "items") -> MapStrategy:
        """
        分组字典映射策略
        
        Args:
            group_field: 分组字段名
            items_field: 项目列表字段名
            
        Returns:
            MapStrategy: 映射策略函数
        """
        def grouped_mapper(batch: List[Dict[str, Any]]) -> BatchResult:
            if not batch:
                return {}
            
            # 获取分组值（假设同一批次中的分组值相同）
            group_value = batch[0].get(group_field)
            
            return {
                group_field: group_value,
                items_field: batch
            }
        
        return grouped_mapper

    @staticmethod
    def with_custom_func(func: Callable[[List[Any]], BatchResult]) -> MapStrategy:
        """
        自定义映射函数策略

        Args:
            func: 自定义映射函数

        Returns:
            MapStrategy: 映射策略函数
        """
        return func


class ExtendedBatchPlanner(BatchPlanner):
    """
    扩展的 BatchPlanner，提供智能批处理和性能统计功能

    完全兼容原有 BatchPlanner API，同时提供：
    1. 智能时间批处理集成
    2. 批次优化效果统计
    3. 性能监控和日志记录
    """

    def __init__(
        self,
        source: Callable[..., Any],
        partition_strategy: PartitionStrategy,
        map_strategy: MapStrategy,
        enable_stats: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        """
        初始化扩展的 BatchPlanner

        Args:
            source: 数据源
            partition_strategy: 分区策略
            map_strategy: 映射策略
            enable_stats: 是否启用统计功能
            logger: 日志记录器
        """
        super().__init__(source, partition_strategy, map_strategy)
        self.enable_stats = enable_stats
        self.logger = logger or logging.getLogger(__name__)
        self._stats = {}

    async def generate(self, *args, **kwargs) -> List[BatchResult]:
        """
        生成批次列表，带性能统计

        Args:
            *args, **kwargs: 传递给父类的参数

        Returns:
            List[BatchResult]: 批次结果列表
        """
        import time
        start_time = time.time()

        # 调用父类方法
        batches = await super().generate(*args, **kwargs)

        # 统计信息
        if self.enable_stats:
            end_time = time.time()
            self._stats = {
                "batch_count": len(batches),
                "generation_time": end_time - start_time,
                "timestamp": datetime.now().isoformat()
            }

            # 如果是智能时间分区，添加优化统计
            if isinstance(self.partition_strategy, type(SmartTimePartition.create())):
                self._add_smart_time_stats(args, kwargs, batches)

            self.logger.info(
                f"BatchPlanner generated {len(batches)} batches in {end_time - start_time:.3f}s"
            )

        return batches

    def _add_smart_time_stats(self, args: tuple, kwargs: dict, batches: List[BatchResult]):
        """添加智能时间分区的统计信息"""
        try:
            # 尝试从参数中提取时间范围
            start_date = kwargs.get("start_date") or (args[0] if args else None)
            end_date = kwargs.get("end_date") or (args[1] if len(args) > 1 else None)

            if start_date and end_date:
                # 计算原始批次数量（假设按天分批）
                start_dt = datetime.strptime(str(start_date), "%Y%m%d")
                end_dt = datetime.strptime(str(end_date), "%Y%m%d")
                total_days = (end_dt - start_dt).days + 1
                estimated_original_batches = max(1, total_days)

                # 计算优化效果
                current_batches = len(batches)
                reduction_rate = (estimated_original_batches - current_batches) / estimated_original_batches * 100

                self._stats.update({
                    "smart_time_optimization": {
                        "original_estimated_batches": estimated_original_batches,
                        "optimized_batches": current_batches,
                        "reduction_rate": reduction_rate,
                        "time_span_days": total_days
                    }
                })
        except Exception as e:
            self.logger.warning(f"Failed to calculate smart time stats: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self._stats.copy()


class TimeRangeSource:
    """时间范围数据源，专门用于智能时间分区"""

    @staticmethod
    def create(start_date: str, end_date: str) -> Callable[[], List[str]]:
        """
        创建时间范围数据源

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Callable: 数据源函数
        """
        def time_range_source() -> List[str]:
            return [start_date, end_date]

        return time_range_source


class StockListSource:
    """股票列表数据源，支持多维度查询"""

    @staticmethod
    def create(
        db_manager=None,
        api_instance=None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Callable[[], List[Dict[str, Any]]]:
        """
        创建股票列表数据源

        Args:
            db_manager: 数据库管理器
            api_instance: API实例
            filters: 过滤条件

        Returns:
            Callable: 数据源函数
        """
        async def stock_list_source() -> List[Dict[str, Any]]:
            """获取股票列表"""
            stocks = []

            # 尝试从数据库获取
            if db_manager:
                try:
                    query = "SELECT ts_code, list_status, market, exchange FROM stock_basic"
                    if filters:
                        conditions = []
                        for key, value in filters.items():
                            conditions.append(f"{key} = '{value}'")
                        if conditions:
                            query += " WHERE " + " AND ".join(conditions)

                    result = await db_manager.fetch_all(query)
                    stocks = [dict(row) for row in result]
                except Exception as e:
                    logging.warning(f"Failed to fetch from database: {e}")

            # 尝试从API获取
            if not stocks and api_instance:
                try:
                    df = await api_instance.query(
                        api_name="stock_basic",
                        fields=["ts_code", "list_status", "market", "exchange"],
                        params=filters or {}
                    )
                    if df is not None and not df.empty:
                        stocks = df.to_dict('records')
                except Exception as e:
                    logging.warning(f"Failed to fetch from API: {e}")

            # 兜底数据
            if not stocks:
                stocks = [
                    {"ts_code": "000001.SZ", "list_status": "L", "market": "主板", "exchange": "SZSE"},
                    {"ts_code": "600519.SH", "list_status": "L", "market": "主板", "exchange": "SSE"},
                    {"ts_code": "300750.SZ", "list_status": "L", "market": "创业板", "exchange": "SZSE"},
                ]

            return stocks

        return stock_list_source


# 便利函数，用于快速创建常用的批处理配置
def create_smart_time_planner(
    start_date: str,
    end_date: str,
    start_field: str = "start_date",
    end_field: str = "end_date",
    date_format: str = "%Y%m%d",
    enable_stats: bool = True
) -> ExtendedBatchPlanner:
    """
    创建智能时间批处理规划器

    Args:
        start_date: 开始日期
        end_date: 结束日期
        start_field: 开始日期字段名
        end_field: 结束日期字段名
        date_format: 日期格式
        enable_stats: 是否启用统计

    Returns:
        ExtendedBatchPlanner: 批处理规划器
    """
    return ExtendedBatchPlanner(
        source=TimeRangeSource.create(start_date, end_date),
        partition_strategy=SmartTimePartition.create(date_format),
        map_strategy=ExtendedMap.to_smart_time_range(start_field, end_field),
        enable_stats=enable_stats
    )


def create_stock_status_planner(
    db_manager=None,
    api_instance=None,
    status_field: str = "list_status",
    enable_stats: bool = True
) -> ExtendedBatchPlanner:
    """
    创建按股票状态分批的规划器

    Args:
        db_manager: 数据库管理器
        api_instance: API实例
        status_field: 状态字段名
        enable_stats: 是否启用统计

    Returns:
        ExtendedBatchPlanner: 批处理规划器
    """
    return ExtendedBatchPlanner(
        source=StockListSource.create(db_manager, api_instance),
        partition_strategy=StatusPartition.create(status_field),
        map_strategy=ExtendedMap.to_grouped_dict(status_field, "stocks"),
        enable_stats=enable_stats
    )
