#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
同花顺概念和行业指数 (ths_index) 全量更新任务
获取同花顺板块指数基本信息，每次执行时替换数据库中的旧数据。
继承自 TushareTask，利用 pre_execute 清空表。
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ...tools.calendar import is_trade_day
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockThsIndexTask(TushareTask):
    """获取同花顺概念和行业指数基本信息

    说明：该接口不支持真正的增量更新，且数据量相对较大。
    因此在 SMART 模式下，采用“超过1个月未更新且当天为非交易日”时才执行全量更新，否则跳过。
    """

    # 1. 核心属性
    name = "tushare_stock_thsindex"
    description = "获取同花顺概念和行业指数基本信息"
    table_name = "stock_thsindex"
    primary_keys = ["ts_code"]
    date_column = None  # 全量任务
    default_start_date = "19900101"  # 全量任务，设置一个早期默认起始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识，属于股票相关数据

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "ths_index"
    # Tushare ths_index 接口实际返回的字段
    fields = [
        "ts_code",
        "name",
        "count",
        "exchange",
        "list_date",
        "type",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "count": lambda x: pd.to_numeric(x, errors="coerce"),
        # list_date 由覆盖的 process_data 方法处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "count": {"type": "INTEGER"},
        "exchange": {"type": "VARCHAR(10)"},
        "list_date": {"type": "DATE"},
        "type": {"type": "VARCHAR(10)"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 主键 ts_code 已自动创建索引
        {"name": "idx_ths_index_exchange", "columns": "exchange"},
        {"name": "idx_ths_index_type", "columns": "type"},
        {"name": "idx_ths_index_list_date", "columns": "list_date"},
        {
            "name": "idx_ths_index_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。

        - 全量更新：直接执行全量更新
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

        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        return [{}]  # 触发一次不带参数的 API 调用

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
        (lambda df: df['ts_code'].notna(), "指数代码不能为空"),
        (lambda df: df['name'].notna(), "指数名称不能为空"),
        (lambda df: df['count'].notna(), "成分个数不能为空"),
        (lambda df: df['exchange'].notna(), "交易所不能为空"),
        (lambda df: df['type'].notna(), "指数类型不能为空"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "指数名称不能为空字符串"),
        (lambda df: ~(df['exchange'].astype(str).str.strip().eq('') | df['exchange'].isna()), "交易所不能为空字符串"),
        (lambda df: ~(df['type'].astype(str).str.strip().eq('') | df['type'].isna()), "指数类型不能为空字符串"),
        (lambda df: df['count'] >= 0, "成分个数不能为负数"),
    ]


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'ths_index' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. (可能需要) 在 TaskFactory 中注册此任务。
5. 使用相应的运行脚本来执行此任务。
"""
