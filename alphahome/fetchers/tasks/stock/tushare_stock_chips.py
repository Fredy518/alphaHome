#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
每日筹码及胜率 (cyq_perf) 更新任务
获取A股每日筹码平均成本和胜率情况。
数据从2018年开始。
参考文档: https://tushare.pro/document/2?doc_id=293
"""

import asyncio  # 添加 asyncio 导入
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.batch_utils import (  # 改回使用 generate_single_date_batches
    generate_single_date_batches,
)


@task_register()
class TushareStockChipsTask(TushareTask):
    """获取A股每日筹码平均成本和胜率情况"""

    # 1. 核心属性
    name = "tushare_stock_chips"
    description = "获取A股每日筹码平均成本和胜率情况"
    table_name = "tushare_stock_chips"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20180101"  # 数据从2018年开始

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 5000  # API限制单次最大5000条

    # 2. 自定义索引
    indexes = [
        # 主键 ("ts_code", "trade_date") 索引由基类自动处理
        {"name": "idx_stock_chips_update_time", "columns": "update_time"}
    ]

    # 3. Tushare特有属性
    api_name = "cyq_perf"
    fields = [
        "ts_code",
        "trade_date",
        "his_low",
        "his_high",
        "cost_5pct",
        "cost_15pct",
        "cost_50pct",
        "cost_85pct",
        "cost_95pct",
        "weight_avg",
        "winner_rate",
    ]

    # 4. 数据类型转换 (日期字段由基类自动处理，其他多为float)
    transformations = {
        "his_low": float,
        "his_high": float,
        "cost_5pct": float,
        "cost_15pct": float,
        "cost_50pct": float,
        "cost_85pct": float,
        "cost_95pct": float,
        "weight_avg": float,
        "winner_rate": float,
    }

    # 5. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {}

    # 6. 表结构定义 (包含注释)
    schema_def = {
        "ts_code": {
            "type": "VARCHAR(10)",
            "constraints": "NOT NULL",
            "comment": "股票代码",
        },
        "trade_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "交易日期",
        },
        "his_low": {"type": "FLOAT", "comment": "历史最低价"},
        "his_high": {"type": "FLOAT", "comment": "历史最高价"},
        "cost_5pct": {"type": "FLOAT", "comment": "5分位成本"},
        "cost_15pct": {"type": "FLOAT", "comment": "15分位成本"},
        "cost_50pct": {"type": "FLOAT", "comment": "50分位成本"},
        "cost_85pct": {"type": "FLOAT", "comment": "85分位成本"},
        "cost_95pct": {"type": "FLOAT", "comment": "95分位成本"},
        "weight_avg": {"type": "FLOAT", "comment": "加权平均成本"},
        "winner_rate": {"type": "FLOAT", "comment": "胜率"},
        # update_time 会由基类自动添加
    }

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """生成批处理参数列表 (使用单日期批次工具)。"""
        start_date_overall = kwargs.get("start_date")
        end_date_overall = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的股票代码
        exchange = kwargs.get("exchange", "SSE")  # 传递 exchange 给日历工具

        # 确定总体起止日期
        if not start_date_overall:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date_overall = (
                    pd.to_datetime(latest_db_date) + pd.Timedelta(days=1)
                ).strftime("%Y%m%d")
            else:
                start_date_overall = self.default_start_date
            self.logger.info(
                f"任务 {self.name}: 未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date_overall}"
            )

        if not end_date_overall:
            end_date_overall = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date_overall}"
            )

        if pd.to_datetime(start_date_overall) > pd.to_datetime(end_date_overall):
            self.logger.info(
                f"任务 {self.name}: 起始日期 ({start_date_overall}) 晚于结束日期 ({end_date_overall})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 使用单日期批次工具生成批处理列表，范围: {start_date_overall} 到 {end_date_overall}, 股票代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_single_date_batches(
                start_date=start_date_overall,
                end_date=end_date_overall,
                date_field="trade_date",  # API cyq_perf 使用 trade_date 参数
                ts_code=ts_code,  # 可选的股票代码，为 None 时获取全市场，generate_single_date_batches 会处理
                exchange=exchange,  # 传递 exchange 给日历工具
                logger=self.logger,
            )
            # generate_single_date_batches 返回的批次已经是 {'trade_date': 'YYYYMMDD', 'ts_code': 'XXX'(可选)} 格式
            self.logger.info(
                f"任务 {self.name}: 成功生成 {len(batch_list)} 个单日期批次。"
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成单日期批次时出错: {e}", exc_info=True
            )
            return []

    # process_data 和 prepare_params 可以使用基类默认实现，除非有特殊需求
    # 例如，如果API返回的数据列名与期望的不一致，可以在process_data中调整
    # 如果API参数需要特殊构造，可以在prepare_params中调整

    async def pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行前的准备工作"""
        await super().pre_execute(stop_event=stop_event, **kwargs)
        # 可以在这里添加特定于此任务的预处理逻辑

    async def post_execute(
        self,
        result: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
        **kwargs,
    ):
        """任务执行后的清理工作"""
        await super().post_execute(result, stop_event=stop_event, **kwargs)
        # 可以在这里添加特定于此任务的后处理逻辑


# 可以在此处添加一些测试代码，例如:
# async def main():
#     from alphahome.fetchers.db_manager import DBManager
#     from alphahome.config import load_config
#     import os
#
#     # 加载配置 (假设配置文件在项目根目录的 config.json)
#     # 请根据您的项目结构调整路径
#     project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
#     config_path = os.path.join(project_root, 'config.json')
#     config = load_config(config_path)
#
#     # 初始化数据库连接
#     db_manager = DBManager(config['database'])
#     await db_manager.initialize()
#
#     # 创建任务实例
#     task = TushareStockChipsTask(db_connection=db_manager)
#     task.set_config(config.get('tasks', {}).get(task.name, {})) # 应用任务特定配置
#
#     try:
#         # 执行任务 (例如，获取特定日期范围的数据)
#         # result = await task.execute(start_date="20230101", end_date="20230105")
#         # 或者执行智能增量更新
#         result = await task.smart_incremental_update()
#         print(f"任务 {task.name} 执行结果: {result}")
#
#     except Exception as e:
#         print(f"执行任务时发生错误: {e}")
#     finally:
#         await db_manager.close()
#
# if __name__ == '__main__':
#     # 注意：直接运行此脚本可能需要配置PYTHONPATH或进行相对导入调整
#     # setup_logging() # 如果有全局日志设置函数
#     # asyncio.run(main())
pass
