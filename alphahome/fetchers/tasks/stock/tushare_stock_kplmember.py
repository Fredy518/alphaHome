#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
开盘啦题材成分 (kpl_concept_cons) 全量更新任务
获取开盘啦概念题材的成分股数据，每次执行时替换数据库中的旧数据。
此任务支持全量和增量更新模式。

{{ AURA-X: [Optimize] - 参考 dcmember 模式优化开盘啦题材成分任务. Source: tushare.pro/document/2?doc_id=351 }}
{{ AURA-X: [Note] - 支持全量和增量更新模式 }}
{{ AURA-X: [Algorithm] - 数据分组算法：按 [ts_code, con_code] 分组，计算 in_date/out_date }}
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockKplMemberTask(TushareTask):
    """获取开盘啦题材成分信息 (支持全量和增量更新)"""

    # 1. 核心属性
    name = "tushare_stock_kplmember"
    description = "获取开盘啦题材成分数据"
    table_name = "stock_kplmember"
    primary_keys = ["ts_code", "con_code", "trade_date"]  # 主键包含交易日期
    date_column = "trade_date"  # 使用 trade_date 作为日期列
    default_start_date = "20241014"  # 根据开盘啦数据起始时间设置
    data_source = "tushare"
    domain = "stock"  # 业务域标识，属于股票相关数据

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 3000  # 单次最大可获取3000条数据

    # 2. TushareTask 特有属性
    api_name = "kpl_concept_cons"
    # Tushare kpl_concept_cons 接口实际返回的字段（精简版）
    fields = [
        "ts_code",
        "name",
        "con_name",
        "con_code",
        "trade_date",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {}

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "con_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "con_name": {"type": "VARCHAR(100)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 复合主键 (ts_code, con_code, trade_date) 已自动创建索引
        {"name": "idx_kplmember_ts_code", "columns": "ts_code"},
        {"name": "idx_kplmember_con_code", "columns": "con_code"},
        {"name": "idx_kplmember_trade_date", "columns": "trade_date"},
        {
            "name": "idx_kplmember_update_time",
            "columns": "update_time",
        },  # update_time 索引
    ]

    # 8. 分批配置
    batch_trade_days = 1  # 每个批次的交易日数量 (1个交易日)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。

        简化逻辑：直接返回月末交易日的批次列表
        全量更新：获取所有历史月份的月末交易日
        智能增量：从数据库最新日期的下一个月末开始
        手动增量：使用用户指定的日期范围
        """
        update_type = kwargs.get("update_type", UpdateTypes.FULL)

        if update_type == UpdateTypes.MANUAL:
            # 手动增量：使用用户指定的日期范围
            start_date_param = kwargs.get("start_date")
            end_date_param = kwargs.get("end_date")

            if not start_date_param or not end_date_param:
                self.logger.error("手动增量模式需要提供 start_date 和 end_date 参数")
                return []

            try:
                start_date = pd.to_datetime(start_date_param)
                end_date = pd.to_datetime(end_date_param)
            except Exception as e:
                self.logger.error(f"日期参数格式错误: {e}")
                return []

            if start_date > end_date:
                self.logger.error("起始日期不能晚于结束日期")
                return []

            # 生成指定日期范围内的所有月末交易日
            months = pd.date_range(start=start_date, end=end_date, freq='ME')

            batch_list = []
            for month_end in months:
                trade_date = month_end.strftime('%Y%m%d')
                batch_list.append({"trade_date": trade_date})

            self.logger.info(f"任务 {self.name}: 手动增量模式，从 {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}，生成 {len(batch_list)} 个批次")
            return batch_list

        elif update_type == UpdateTypes.FULL:
            # 全量更新：生成从开始日期到当前的所有月末交易日
            start_date = pd.to_datetime(self.default_start_date)
            end_date = pd.Timestamp.now()

            # 生成月份列表
            months = pd.date_range(start=start_date, end=end_date, freq='ME')

            batch_list = []
            for month_end in months:
                # 获取该月的月末交易日（简化处理，直接使用月末日期）
                trade_date = month_end.strftime('%Y%m%d')
                batch_list.append({"trade_date": trade_date})

            self.logger.info(f"任务 {self.name}: 全量更新模式，生成 {len(batch_list)} 个月的批次")
            return batch_list

        else:
            # 智能增量：从数据库最新日期的下一个月末开始
            latest_date = await self.get_latest_date_for_task()
            if latest_date:
                # 从数据库最新日期的下一个月末开始
                next_month = (latest_date.replace(day=1) + pd.DateOffset(months=1))
                start_date = next_month
                self.logger.info(f"任务 {self.name}: 智能增量模式，从数据库最新日期 {latest_date.strftime('%Y-%m-%d')} 的下一个月末开始")
            else:
                # 如果没有历史数据，使用默认起始日期
                start_date = pd.to_datetime(self.default_start_date)
                self.logger.info(f"任务 {self.name}: 智能增量模式，无历史数据，使用默认起始日期 {start_date.strftime('%Y-%m-%d')}")

            end_date = pd.Timestamp.now()

            # 生成从start_date到end_date的所有月末交易日
            months = pd.date_range(start=start_date, end=end_date, freq='ME')

            batch_list = []
            for month_end in months:
                trade_date = month_end.strftime('%Y%m%d')
                batch_list.append({"trade_date": trade_date})

            self.logger.info(f"任务 {self.name}: 智能增量模式，生成 {len(batch_list)} 个批次")
            return batch_list


    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['con_code'].notna(), "题材代码不能为空"),
        (lambda df: df['name'].notna(), "股票名称不能为空"),
        (lambda df: df['con_name'].notna(), "题材名称不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "股票代码不能为空字符串"),
        (lambda df: ~(df['con_code'].astype(str).str.strip().eq('') | df['con_code'].isna()), "题材代码不能为空字符串"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "股票名称不能为空字符串"),
        (lambda df: ~(df['con_name'].astype(str).str.strip().eq('') | df['con_name'].isna()), "题材名称不能为空字符串"),
    ]


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接和 Tushare 客户端在基类中正确初始化。
3. 确保数据库中存在名为 'stock_kplmember' 的表，且结构与 Tushare API 返回数据匹配。
4. 任务会自动注册到 TaskFactory。
5. 使用相应的运行脚本来执行此任务。

{{ AURA-X: [Note] - 本任务需要 5000 积分权限，每分钟可调取 200 次 }}
{{ AURA-X: [Important] - 此任务支持全量和增量更新模式 }}

技术说明 (简化版):
- 数据结构：直接保存 Tushare API 返回的月末成分股数据
- 全量更新：获取从开始日期到当前的所有月末交易日数据
- 智能增量：从数据库最新日期的下一个月末开始获取数据
- 手动增量：使用用户指定的日期范围获取数据
- 主键：(ts_code, con_code, trade_date) 确保每只股票在每个月末的唯一性
- 数据字段：ts_code, con_code, name, con_name, trade_date
- 处理逻辑：不进行复杂的分组合并，直接保存原始数据
"""
