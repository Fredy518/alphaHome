#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
同花顺概念板块成分 (ths_member) 全量更新任务
获取同花顺概念板块的成分股列表，每次执行时替换数据库中的旧数据。
此任务仅支持全量更新模式，智能增量和手动增量模式会跳过执行。

{{ AURA-X: [Create] - 基于 Tushare ths_member API 创建同花顺概念板块成分任务. Source: tushare.pro/document/2?doc_id=261 }}
{{ AURA-X: [Note] - 此任务仅支持全量更新，其他更新模式将跳过执行 }}
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
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
        
        {{ AURA-X: [Important] - 此任务仅支持全量更新模式 }}
        如果用户选择智能增量或手动增量模式，任务将跳过执行并在日志中给出提示。
        
        对于全量更新模式，从 stock_thsindex 表获取所有同花顺板块的 ts_code，
        然后为每个 ts_code 生成批处理参数。
        """
        # 检查更新类型
        update_type = kwargs.get("update_type", UpdateTypes.FULL)
        
        # {{ AURA-X: [Check] - 验证更新类型是否为全量更新 }}
        if update_type != UpdateTypes.FULL:
            self.logger.warning(
                f"任务 {self.name}: 此任务仅支持全量更新模式 (FULL)。"
                f"当前更新类型为 '{update_type}'，任务将跳过执行。"
                f"如需更新此数据，请使用全量更新模式。"
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
{{ AURA-X: [Important] - 此任务仅支持全量更新模式，智能增量和手动增量模式会跳过执行 }}

技术说明:
- 由于板块成分变动频繁，且 Tushare API 不提供增量查询机制，此任务设计为仅支持全量更新
- 在 get_batch_list 方法中检查 update_type，如果不是 FULL 模式，则返回空列表跳过执行
- 全量更新时从 stock_thsindex 表获取所有板块代码，为每个板块生成独立的批处理参数
- 数据库表使用复合主键 (ts_code, con_code) 确保每个板块-成分股组合的唯一性
"""

