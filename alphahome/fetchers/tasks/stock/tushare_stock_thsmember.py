#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
同花顺概念板块成分 (ths_member) 更新任务
获取同花顺概念板块的成分股列表，每次执行时替换数据库中的旧数据。
支持全量更新和智能增量：
- 全量更新：直接执行全量更新
- 智能增量：检查数据表是否同时满足两个条件：超过1个月未更新且当天为非交易日，如果同时满足则自动转为全量更新，否则跳过执行
- 手动增量：跳过执行

{{ AURA-X: [Modify] - 修改智能增量逻辑，支持基于时间+交易日判断的自动更新. Source: context7-mcp }}
{{ AURA-X: [Note] - 智能增量需要同时满足超过1个月未更新且当天为非交易日两个条件 }}
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ...tools.calendar import is_trade_day
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockThsMemberTask(TushareTask):
    """获取同花顺概念板块成分信息 (仅支持全量更新)"""

    # 1. 核心属性
    name = "tushare_stock_thsmember"
    description = "获取同花顺概念板块成分列表"
    table_name = "stock_thsmember"
    primary_keys = ["ts_code", "con_code"]
    date_column = None  # 全量任务
    default_start_date = "19900101"  # 全量任务，设置一个早期默认起始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识，属于股票相关数据

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "ths_member"
    # Tushare ths_member 接口实际返回的字段
    fields = [
        "ts_code",
        "con_code",
        "con_name",
        "weight",
        "in_date",
        "out_date",
        "is_new",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "weight": 'float',
        # in_date, out_date 由覆盖的 process_data 方法处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "con_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "con_name": {"type": "VARCHAR(100)"},
        "weight": {"type": "NUMERIC(10,4)"},
        "in_date": {"type": "DATE"},
        "out_date": {"type": "DATE"},
        "is_new": {"type": "VARCHAR(1)"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 复合主键 (ts_code, con_code) 已自动创建索引
        {"name": "idx_thsmember_ts_code", "columns": "ts_code"},
        {"name": "idx_thsmember_con_code", "columns": "con_code"},
        {"name": "idx_thsmember_is_new", "columns": "is_new"},
        {
            "name": "idx_thsmember_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。

        {{ AURA-X: [Important] - 支持全量更新和智能增量 }}
        - 全量更新：直接执行全量更新
        - 智能增量：检查数据表是否同时满足两个条件（超过1个月未更新且当天为非交易日），如果同时满足则转为全量更新，否则跳过执行
        - 手动增量：跳过执行

        对于需要更新的情况，从 stock_thsindex 表获取所有同花顺板块的 ts_code，
        然后为每个 ts_code 生成批处理参数。
        """
        # 检查更新类型
        update_type = kwargs.get("update_type", UpdateTypes.FULL)

        # {{ AURA-X: [Check] - 处理不同的更新类型 }}
        if update_type == UpdateTypes.SMART:
            # 智能增量：检查是否需要全量更新
            should_update = await self._should_perform_full_update()
            if not should_update:
                self.logger.info(f"任务 {self.name}: 智能增量 - 数据表不满足全量更新条件，跳过执行")
                return []
            else:
                self.logger.info(f"任务 {self.name}: 智能增量 - 数据表满足全量更新条件，转为全量更新")
                # 继续执行全量更新逻辑
        elif update_type != UpdateTypes.FULL:
            # 手动增量或其他不支持的模式
            self.logger.warning(
                f"任务 {self.name}: 此任务仅支持全量更新 (FULL) 和智能增量 (SMART)。"
                f"当前更新类型为 '{update_type}'，任务将跳过执行。"
                f"如需更新此数据，请使用全量更新或智能增量。"
            )
            # 返回空列表，任务会跳过执行
            return []
        
        # {{ AURA-X: [Query] - 从 stock_thsindex 表获取所有板块代码 }}
        ts_codes = await self._get_all_thsmember_ts_codes()
        
        if not ts_codes:
            self.logger.warning("未找到任何同花顺板块代码，任务将跳过执行")
            return []
        
        # 为每个 ts_code 生成批处理参数
        batch_list = [{"ts_code": ts_code} for ts_code in ts_codes]
        
        self.logger.info(f"任务 {self.name}: 为 {len(batch_list)} 个板块生成批处理列表")
        return batch_list

    async def _get_all_thsmember_ts_codes(self) -> List[str]:
        """
        从 tushare.stock_thsindex 表获取所有同花顺板块的 ts_code。
        
        Returns:
            List[str]: 板块代码列表
        """
        try:
            # 查询 tushare.stock_thsindex 表获取所有 ts_code
            query = "SELECT ts_code FROM tushare.stock_thsindex ORDER BY ts_code"
            result = await self.db.fetch(query)
            
            if result:
                ts_codes = [row["ts_code"] for row in result]
                self.logger.info(f"从 tushare.stock_thsindex 表获取到 {len(ts_codes)} 个板块代码")
                return ts_codes
            else:
                self.logger.warning("tushare.stock_thsindex 表为空，未找到任何板块代码")
                return []
                
        except Exception as e:
            self.logger.error(f"查询 tushare.stock_thsindex 表失败: {e}", exc_info=True)
            return []

    async def _should_perform_full_update(self) -> bool:
        """
        检查数据表是否满足全量更新的条件：
        1. 数据表超过1个月未更新
        2. 当天为非交易日

        只有同时满足两个条件才返回True。

        Returns:
            bool: 如果同时满足两个条件返回True，否则返回False
        """
        try:
            # 查询数据表中最大的 update_time
            query = f"SELECT MAX(update_time) as last_update FROM {self.data_source}.{self.table_name}"
            result = await self.db.fetch(query)

            if not result or not result[0]["last_update"]:
                # 如果没有数据或 update_time 为 NULL，认为需要更新
                self.logger.info(f"表 {self.table_name} 没有更新时间记录，需要执行全量更新")
                return True

            last_update = result[0]["last_update"]
            current_time = datetime.now()

            # 计算时间差
            time_diff = current_time - last_update

            # 检查是否超过1个月 (30天)
            one_month = timedelta(days=30)
            is_over_one_month = time_diff > one_month

            # 检查当天是否为非交易日
            today_str = current_time.strftime("%Y%m%d")
            is_trading_day = await is_trade_day(today_str)
            is_non_trading_day = not is_trading_day

            self.logger.info(
                f"表 {self.table_name} 最后更新时间为 {last_update}，"
                f"距离现在 {time_diff.days} 天，是否超过1个月: {is_over_one_month}，"
                f"当天 {today_str} 是否为非交易日: {is_non_trading_day}"
            )

            # 只有同时满足两个条件才执行全量更新
            if is_over_one_month and is_non_trading_day:
                self.logger.info(
                    f"表 {self.table_name} 满足全量更新条件：超过1个月未更新且当天为非交易日"
                )
                return True
            else:
                if not is_over_one_month:
                    self.logger.info(
                        f"表 {self.table_name} 未超过1个月未更新，跳过更新"
                    )
                elif not is_non_trading_day:
                    self.logger.info(
                        f"表 {self.table_name} 当天为交易日，跳过更新"
                    )
                return False

        except Exception as e:
            self.logger.error(f"检查表 {self.table_name} 更新时间失败: {e}", exc_info=True)
            # 如果检查失败，为了安全起见，认为需要更新
            return True

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "指数代码不能为空"),
        (lambda df: df['con_code'].notna(), "成分股代码不能为空"),
        (lambda df: df['con_name'].notna(), "成分股名称不能为空"),
        (lambda df: ~(df['con_name'].astype(str).str.strip().eq('') | df['con_name'].isna()), "成分股名称不能为空字符串"),
        (
            lambda df: (df['weight'].isna()) | (df['weight'] >= 0),
            "权重不能为负数",
        ),
    ]


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'stock_thsmember' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. 任务会自动注册到 TaskFactory。
5. 使用相应的运行脚本来执行此任务。

{{ AURA-X: [Note] - 本任务需要 5000 积分权限，每分钟可调取 200 次 }}
{{ AURA-X: [Important] - 支持全量更新和智能增量模式，手动增量模式会跳过执行 }}

技术说明:
- 由于板块成分变动频繁，且 Tushare API 不提供增量查询机制，此任务支持全量更新和智能增量
- 全量更新：直接执行全量更新
- 智能增量：检查数据表是否同时满足两个条件：max(update_time) 超过1个月且当天为非交易日，如果同时满足则自动转为全量更新，否则跳过执行
- 手动增量：由于 API 限制，不支持手动增量，任务会跳过执行
- 非交易日判断：通过交易日历表检查当天是否为工作日且开市，避免在交易日频繁更新
- 全量更新时从 stock_thsindex 表获取所有板块代码，为每个板块生成独立的批处理参数
- 数据库表使用复合主键 (ts_code, con_code) 确保每个板块-成分股组合的唯一性
"""

