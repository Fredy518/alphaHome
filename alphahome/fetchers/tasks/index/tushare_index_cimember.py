#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中信行业成分 (ci_index_member) 数据任务
获取最新的 (is_new='Y') 和历史的 (is_new='N') 中信行业成分信息，
并使用 UPSERT 更新到数据库。

说明：该接口不支持真正的增量更新。
在 SMART 模式下，采用"超过1个月未更新且当天为非交易日"时才执行全量更新，否则跳过。
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ...tools.calendar import is_trade_day
from alphahome.common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes


@task_register()
class TushareIndexCiMemberTask(TushareTask):
    """获取中信(CITIC)行业成分数据 (UPSERT)"""

    # 1. 核心属性
    name = "tushare_index_cimember"
    description = "获取中信(CITIC)行业成分数据 (含历史, UPSERT)"
    table_name = "index_cimember"
    primary_keys = ["ts_code", "l3_code", "in_date"]  # <-- 修改主键为 l3_code
    date_column = None  # <-- 明确一下没有主日期列用于增量
    data_source = "tushare"
    domain = "index"  # 业务域标识

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 2
    default_page_size = 4000

    # 2. TushareTask 特有属性
    api_name = "ci_index_member"
    # 根据 Tushare 文档更新字段列表
    fields = [
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "ts_code",
        "name",
        "in_date",
        "out_date",
        "is_new",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列由基类处理)
    transformations = {}

    # 5. 数据库表结构
    schema_def = {
        "index_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "con_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"},
        "l1_code": {"type": "VARCHAR(20)"},
        "l1_name": {"type": "VARCHAR(50)"},
        "l2_code": {"type": "VARCHAR(20)"},  # <-- 移除 NOT NULL
        "l2_name": {"type": "VARCHAR(50)"},
        "l3_code": {
            "type": "VARCHAR(20)",
            "constraints": "NOT NULL",
        },  # <-- 添加 NOT NULL
        "l3_name": {"type": "VARCHAR(100)"},  # 新增
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},  # 主键部分
        "name": {"type": "VARCHAR(100)"},
        "out_date": {"type": "DATE"},  # 新增
        "is_new": {"type": "VARCHAR(1)"},  # 新增
        # update_time 会自动添加 (默认)
    }

    # 6. 自定义索引
    indexes = [
        # 主键 ("ts_code", "l3_code", "in_date") 已自动创建索引
        {"name": "idx_index_cimember_l1", "columns": "l1_code"},
        {"name": "idx_index_cimember_l3", "columns": "l3_code"},
        {
            "name": "idx_index_cimember_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        
        - 全量更新：获取最新(Y)和历史(N)两个批次
        - 智能增量：检查是否超过1个月未更新且当天为非交易日；满足才执行全量更新，否则跳过
        - 手动增量：跳过执行（接口不支持按日期范围查询）
        """
        update_type = kwargs.get("update_type", UpdateTypes.FULL)
        
        if update_type == UpdateTypes.SMART:
            should_update = await self._should_perform_full_update()
            if not should_update:
                self.logger.info(
                    f"任务 {self.name}: 智能增量 - 不满足全量更新条件（超过1个月未更新且当天为非交易日），跳过执行"
                )
                return []
            self.logger.info(f"任务 {self.name}: 智能增量 - 满足全量更新条件，转为全量更新")
        
        elif update_type != UpdateTypes.FULL:
            self.logger.warning(
                f"任务 {self.name}: 此任务仅支持全量更新 (FULL) 和智能增量 (SMART)。"
                f"当前更新类型为 '{update_type}'，任务将跳过执行。"
            )
            return []
        
        self.logger.info(f"任务 {self.name}: 生成获取最新(Y)和历史(N)行业成分的批次。")
        return [{"is_new": "Y"}, {"is_new": "N"}]  # <-- 返回两个批次

    async def _should_perform_full_update(self) -> bool:
        """
        检查数据表是否满足全量更新的条件：
        1) 数据表超过1个月未更新（以 max(update_time) 为准）
        2) 当天为非交易日
        
        Returns:
            bool: 同时满足两个条件返回 True，否则返回 False
        """
        try:
            query = f"SELECT MAX(update_time) as last_update FROM {self.data_source}.{self.table_name}"
            result = await self.db.fetch(query)
            
            if not result or not result[0]["last_update"]:
                # 没有任何数据/更新时间，认为需要更新
                self.logger.info(f"表 {self.table_name} 没有更新时间记录，需要执行全量更新")
                return True
            
            last_update = result[0]["last_update"]
            current_time = datetime.now()
            time_diff = current_time - last_update
            
            is_over_one_month = time_diff > timedelta(days=30)
            
            today_str = current_time.strftime("%Y%m%d")
            is_trading_day = await is_trade_day(today_str)
            is_non_trading_day = not is_trading_day
            
            self.logger.info(
                f"表 {self.table_name} 最后更新时间为 {last_update}，"
                f"距离现在 {time_diff.days} 天，是否超过1个月: {is_over_one_month}，"
                f"当天 {today_str} 是否为非交易日: {is_non_trading_day}"
            )
            
            return bool(is_over_one_month and is_non_trading_day)
        
        except Exception as e:
            self.logger.error(f"检查表 {self.table_name} 更新时间失败: {e}", exc_info=True)
            # 检查失败时，为安全起见，默认允许更新（避免长期不更新）
            return True

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['l3_code'].notna(), "中信三级行业代码不能为空"),
        (lambda df: df['in_date'].notna(), "纳入日期不能为空"),
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "股票代码不能为空字符串"),
        (lambda df: ~(df['l3_code'].astype(str).str.strip().eq('') | df['l3_code'].isna()), "中信三级行业代码不能为空字符串"),
        (lambda df: ~(df['in_date'].astype(str).str.strip().eq('') | df['in_date'].isna()), "纳入日期不能为空字符串"),
        (lambda df: df['l3_code'].astype(str) != 'null', "中信三级行业代码不能为null值"),
    ]

    # 可选：如果需要在保存前进行额外处理，可以覆盖 process_data
    # async def process_data(self, df: pd.DataFrame, batch_params: Dict) -> pd.DataFrame:
    #     df = await super().process_data(df, batch_params) # 调用基类处理
    #     # 在这里添加特定于此任务的处理
    #     return df

    # 可选：如果需要在任务执行后进行操作，可以覆盖 post_execute
    # async def post_execute(self, results: List[pd.DataFrame], **kwargs: Any) -> None:
    #     self.logger.info(f"任务 {self.name} 完成。")
