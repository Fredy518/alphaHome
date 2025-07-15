#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能批次拆分混入类 (Smart Batch Mixin)

提供四级智能批次拆分策略，可以被任何需要时间范围批次处理的任务继承使用。

重构说明：
- 保持原有接口完全不变，确保现有任务的兼容性
- 内部实现基于 ExtendedBatchPlanner，消除代码重复
- 统一底层算法，确保行为一致性
"""

from datetime import datetime
from typing import List, Dict, Any, Tuple
import asyncio
import logging


class SmartBatchMixin:
    """
    智能批次拆分混入类

    提供四级智能批次拆分策略：
    - ≤31天：单批次策略（边界条件优化）
    - 32天-3个月：月度拆分（精细粒度）
    - 4个月-2年：季度拆分（平衡效率和精度）
    - 2-10年：半年度拆分（提高长期更新效率）
    - >10年：年度拆分（超长期数据优化）

    使用方法：
    1. 在任务类中继承此混入类
    2. 在 get_batch_list 方法中调用 generate_smart_time_batches
    3. 根据返回的时间批次生成具体的任务批次

    重构说明：
    - 保持原有接口完全不变
    - 内部基于 ExtendedBatchPlanner 实现，消除代码重复
    - 统一底层算法，确保行为一致性
    """

    def generate_smart_time_batches(self, start_date_str: str, end_date_str: str) -> List[Dict[str, str]]:
        """
        生成智能时间批次列表

        这是 SmartBatchMixin 的主要接口方法，保持原有签名和行为不变。
        内部实现基于 ExtendedBatchPlanner 的 SmartTimePartition。

        Args:
            start_date_str: 开始日期字符串，格式为 YYYYMMDD
            end_date_str: 结束日期字符串，格式为 YYYYMMDD

        Returns:
            List[Dict[str, str]]: 时间批次列表，每个批次包含 start_date 和 end_date
        """
        try:
            # 导入 ExtendedBatchPlanner 相关组件
            from ...common.planning.extended_batch_planner import (
                ExtendedBatchPlanner, SmartTimePartition, ExtendedMap, TimeRangeSource
            )

            # 创建智能时间批处理规划器
            planner = ExtendedBatchPlanner(
                source=TimeRangeSource.create(start_date_str, end_date_str),
                partition_strategy=SmartTimePartition.create(),
                map_strategy=ExtendedMap.to_smart_time_range(),
                enable_stats=False  # 保持轻量级，统计通过单独方法提供
            )

            # 生成批次（同步调用，内部处理异步）
            try:
                # 尝试在现有事件循环中运行
                loop = asyncio.get_running_loop()
                # 如果已有事件循环，需要使用 run_in_executor 或其他方式
                # 但为了简化，我们直接创建新的事件循环
                import threading
                result = [None]
                exception = [None]

                def run_async():
                    try:
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        result[0] = new_loop.run_until_complete(planner.generate())
                        new_loop.close()
                    except Exception as e:
                        exception[0] = e

                thread = threading.Thread(target=run_async)
                thread.start()
                thread.join()

                if exception[0]:
                    raise exception[0]
                batches = result[0]

            except RuntimeError:
                # 没有运行中的事件循环，直接创建新的
                batches = asyncio.run(planner.generate())

            return batches

        except Exception as e:
            # 获取日志记录器
            logger = getattr(self, 'logger', logging.getLogger(__name__))
            logger.error(f"SmartBatchMixin: 生成智能时间批次失败: {e}", exc_info=True)

            # 回退到单批次策略
            return [{
                "start_date": start_date_str,
                "end_date": end_date_str
            }]

    def get_batch_optimization_stats(self, start_date_str: str, end_date_str: str) -> Dict[str, Any]:
        """
        获取批次优化统计信息

        保持原有接口不变，内部基于 ExtendedBatchPlanner 的统计功能。

        Args:
            start_date_str: 开始日期字符串
            end_date_str: 结束日期字符串

        Returns:
            Dict[str, Any]: 包含优化统计信息的字典
        """
        try:
            # 导入 ExtendedBatchPlanner 相关组件
            from ...common.planning.extended_batch_planner import (
                ExtendedBatchPlanner, SmartTimePartition, ExtendedMap, TimeRangeSource
            )

            # 创建带统计功能的规划器
            planner = ExtendedBatchPlanner(
                source=TimeRangeSource.create(start_date_str, end_date_str),
                partition_strategy=SmartTimePartition.create(),
                map_strategy=ExtendedMap.to_smart_time_range(),
                enable_stats=True
            )

            # 生成批次以获取统计信息（同步调用）
            try:
                # 尝试在现有事件循环中运行
                loop = asyncio.get_running_loop()
                # 使用线程来运行异步代码
                import threading
                result = [None]
                exception = [None]

                def run_async():
                    try:
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        result[0] = new_loop.run_until_complete(planner.generate())
                        new_loop.close()
                    except Exception as e:
                        exception[0] = e

                thread = threading.Thread(target=run_async)
                thread.start()
                thread.join()

                if exception[0]:
                    raise exception[0]

            except RuntimeError:
                # 没有运行中的事件循环，直接创建新的
                asyncio.run(planner.generate())

            # 获取统计信息
            stats = planner.get_stats()

            # 转换为 SmartBatchMixin 期望的格式
            if "smart_time_optimization" in stats:
                opt_stats = stats["smart_time_optimization"]
                return {
                    "strategy": self._get_strategy_name(opt_stats.get("time_span_days", 0)),
                    "reduction_rate": opt_stats.get("reduction_rate", 0),
                    "original_batches": opt_stats.get("original_estimated_batches", 0),
                    "optimized_batches": opt_stats.get("optimized_batches", 0),
                    "generation_time": stats.get("generation_time", 0)
                }
            else:
                # 基础统计信息
                return {
                    "strategy": "未知",
                    "reduction_rate": 0,
                    "original_batches": 1,
                    "optimized_batches": stats.get("batch_count", 1),
                    "generation_time": stats.get("generation_time", 0)
                }

        except Exception as e:
            logger = getattr(self, 'logger', logging.getLogger(__name__))
            logger.error(f"SmartBatchMixin: 获取优化统计失败: {e}", exc_info=True)

            # 返回默认统计信息
            return {
                "strategy": "单批次",
                "reduction_rate": 0,
                "original_batches": 1,
                "optimized_batches": 1,
                "generation_time": 0
            }

    def _get_strategy_name(self, total_days: int) -> str:
        """
        根据天数获取策略名称

        Args:
            total_days: 总天数

        Returns:
            str: 策略名称
        """
        if total_days <= 31:
            return "单批次"

        total_months = total_days // 30  # 粗略估算月数

        if total_months <= 3:
            return "月度"
        elif total_months <= 24:
            return "季度"
        elif total_months <= 120:
            return "半年度"
        else:
            return "年度"


