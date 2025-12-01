#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债转股价变动 (cb_price_chg) 全量更新任务
获取所有可转债的转股价变动信息并替换数据库中的旧数据。
继承自 TushareTask。

接口文档: https://tushare.pro/document/2?doc_id=246
权限要求: 本接口需单独开权限（跟积分没关系）
限量: 单次最大2000，总量不限制
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareCBondPriceChgTask(TushareTask):
    """获取可转债转股价变动 (全量更新)"""

    # 1. 核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_price_chg"
    description = "获取可转债转股价变动"
    table_name = "cbond_price_chg"
    primary_keys = ["ts_code", "change_date"]  # 主键：转债代码 + 变动日期
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = "19700101"  # 全量任务需要一个默认起始日期来满足基类方法调用

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 全量更新，设置为串行执行以简化
    default_page_size = 2000  # 单次最大2000条
    update_type = "full"  # 明确指定为全量更新任务类型

    # 2. TushareTask 特有属性
    api_name = "cb_price_chg"
    fields = [
        "ts_code",  # 转债代码
        "bond_short_name",  # 转债简称
        "publish_date",  # 公告日期
        "change_date",  # 变动日期
        "convert_price_initial",  # 初始转股价格
        "convertprice_bef",  # 修正前转股价格
        "convertprice_aft",  # 修正后转股价格
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "convert_price_initial": lambda x: pd.to_numeric(x, errors="coerce"),
        "convertprice_bef": lambda x: pd.to_numeric(x, errors="coerce"),
        "convertprice_aft": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},  # 转债代码
        "bond_short_name": {"type": "VARCHAR(100)"},  # 转债简称
        "publish_date": {"type": "DATE"},  # 公告日期
        "change_date": {"type": "DATE", "constraints": "NOT NULL"},  # 变动日期
        "convert_price_initial": {"type": "NUMERIC(15,4)"},  # 初始转股价格
        "convertprice_bef": {"type": "NUMERIC(15,4)"},  # 修正前转股价格
        "convertprice_aft": {"type": "NUMERIC(15,4)"},  # 修正后转股价格
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_price_chg_ts_code", "columns": "ts_code"},
        {"name": "idx_cbond_price_chg_change_date", "columns": "change_date"},
        {"name": "idx_cbond_price_chg_publish_date", "columns": "publish_date"},
        {
            "name": "idx_cbond_price_chg_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        从 tushare.cbond_basic 表获取所有可转债代码，每10个 ts_code 组成一个多值字符串分批获取。
        """
        # 从 cbond_basic 表获取所有转债代码
        query = 'SELECT DISTINCT ts_code FROM "tushare"."cbond_basic" ORDER BY ts_code'
        records = await self.db.fetch(query)
        
        if not records:
            self.logger.warning(f"任务 {self.name}: 未找到任何转债代码，返回空批次列表。")
            return []

        ts_codes = [r["ts_code"] for r in records]
        total_count = len(ts_codes)
        batch_size = 10  # 每批10个 ts_code
        
        # 按每10个分组
        batches = []
        for i in range(0, total_count, batch_size):
            batch_codes = ts_codes[i:i + batch_size]
            ts_codes_str = ",".join(batch_codes)
            batches.append({"ts_code": ts_codes_str})
        
        self.logger.info(
            f"任务 {self.name}: 全量获取模式，从 cbond_basic 表获取到 {total_count} 个转债代码，"
            f"按每 {batch_size} 个分组，生成 {len(batches)} 个批次。"
        )
        
        return batches

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "转债代码不能为空"),
        (lambda df: df["change_date"].notna(), "变动日期不能为空"),
        (lambda df: df["convert_price_initial"].fillna(0) >= 0 if "convert_price_initial" in df.columns else True, "初始转股价格不能为负数"),
        (lambda df: df["convertprice_bef"].fillna(0) >= 0 if "convertprice_bef" in df.columns else True, "修正前转股价格不能为负数"),
        (lambda df: df["convertprice_aft"].fillna(0) >= 0 if "convertprice_aft" in df.columns else True, "修正后转股价格不能为负数"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def pre_execute(self, stop_event: Optional[Any] = None, **kwargs):
        """
        预执行处理。
        对于全量更新模式，清空表数据。
        """
        self.logger.info(f"任务 {self.name}: 全量更新模式，预先清空表数据。")
        await self.clear_table()


"""
使用方法:
1. 全量更新：设置 update_type="full" 或通过GUI选择"全量更新"
   - 会清空现有数据，重新获取所有可转债转股价变动信息
   - 不管增量还是全量模式，都进行全量更新
   - 从 tushare.cbond_basic 表获取所有转债代码，每10个 ts_code 组成一个多值字符串分批获取
2. 权限要求：本接口需单独开权限（跟积分没关系）
3. 单次限量：2000条，总量不限制

注意事项:
- 只处理可转债转股价变动信息，不包含其他数据
- 全量更新会清空现有数据并重新获取
- 主键为 (ts_code, change_date)，同一个转债可能有多次转股价变动记录
- 数据包含初始转股价格、修正前转股价格、修正后转股价格等信息
- 每批处理10个转债代码，避免单次请求数据量过大
"""

