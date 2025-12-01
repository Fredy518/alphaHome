#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债转股结果 (cb_share) 全量更新任务
获取所有可转债的转股结果信息并替换数据库中的旧数据。
继承自 TushareTask。

接口文档: https://tushare.pro/document/2?doc_id=247
权限要求: 用户需要至少2000积分才可以调取，但有流量控制，5000积分以上频次相对较高
限量: 单次最大2000，总量不限制

!!! 重要提示：接口 Bug !!!
该任务使用的 cb_share 接口存在 bug，暂时无法使用 ann_date、start_date、end_date 等日期参数。
因此增量更新功能实际无法使用，使用增量更新模式将获取空数据框。
目前只能使用全量更新模式（update_type="full"），通过 ts_code 参数获取所有数据。
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
class TushareCBondShareTask(TushareTask):
    """获取可转债转股结果 (全量更新)"""

    # 1. 核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_share"
    description = "获取可转债转股结果"
    table_name = "cbond_share"
    primary_keys = ["ts_code", "end_date"]  # 主键：转债代码 + 统计截止日期
    date_column = "end_date"  # 增量更新使用的日期字段（使用统计截止日期）
    default_start_date = "19900101"  # 默认起始日期
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 设置为串行执行以简化
    default_page_size = 2000  # 单次最大2000条

    # 2. TushareTask 特有属性
    api_name = "cb_share"
    fields = [
        "ts_code",  # 债券代码
        "bond_short_name",  # 债券简称
        "publish_date",  # 公告日期
        "end_date",  # 统计截止日期
        "issue_size",  # 可转债发行总额
        "convert_price_initial",  # 初始转换价格
        "convert_price",  # 本次转换价格
        "convert_val",  # 本次转股金额
        "convert_vol",  # 本次转股数量
        "convert_ratio",  # 本次转股比例
        "acc_convert_val",  # 累计转股金额
        "acc_convert_vol",  # 累计转股数量
        "acc_convert_ratio",  # 累计转股比例
        "remain_size",  # 可转债剩余金额
        "total_shares",  # 转股后总股本
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "issue_size": lambda x: pd.to_numeric(x, errors="coerce"),
        "convert_price_initial": lambda x: pd.to_numeric(x, errors="coerce"),
        "convert_price": lambda x: pd.to_numeric(x, errors="coerce"),
        "convert_val": lambda x: pd.to_numeric(x, errors="coerce"),
        "convert_vol": lambda x: pd.to_numeric(x, errors="coerce"),
        "convert_ratio": lambda x: pd.to_numeric(x, errors="coerce"),
        "acc_convert_val": lambda x: pd.to_numeric(x, errors="coerce"),
        "acc_convert_vol": lambda x: pd.to_numeric(x, errors="coerce"),
        "acc_convert_ratio": lambda x: pd.to_numeric(x, errors="coerce"),
        "remain_size": lambda x: pd.to_numeric(x, errors="coerce"),
        "total_shares": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},  # 债券代码
        "bond_short_name": {"type": "VARCHAR(100)"},  # 债券简称
        "publish_date": {"type": "DATE"},  # 公告日期
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},  # 统计截止日期
        "issue_size": {"type": "NUMERIC(20,2)"},  # 可转债发行总额
        "convert_price_initial": {"type": "NUMERIC(15,4)"},  # 初始转换价格
        "convert_price": {"type": "NUMERIC(15,4)"},  # 本次转换价格
        "convert_val": {"type": "NUMERIC(20,2)"},  # 本次转股金额
        "convert_vol": {"type": "NUMERIC(20,2)"},  # 本次转股数量
        "convert_ratio": {"type": "NUMERIC(10,4)"},  # 本次转股比例
        "acc_convert_val": {"type": "NUMERIC(20,2)"},  # 累计转股金额
        "acc_convert_vol": {"type": "NUMERIC(20,2)"},  # 累计转股数量
        "acc_convert_ratio": {"type": "NUMERIC(10,4)"},  # 累计转股比例
        "remain_size": {"type": "NUMERIC(20,2)"},  # 可转债剩余金额
        "total_shares": {"type": "NUMERIC(20,2)"},  # 转股后总股本
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_share_ts_code", "columns": "ts_code"},
        {"name": "idx_cbond_share_end_date", "columns": "end_date"},
        {"name": "idx_cbond_share_publish_date", "columns": "publish_date"},
        {
            "name": "idx_cbond_share_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        从 tushare.cbond_basic 表获取所有可转债代码，每20个 ts_code 组成一个多值字符串分批获取。
        
        注意：由于 cb_share 接口存在 bug，无法使用 ann_date、start_date、end_date 等日期参数，
        因此增量更新功能实际无法使用，使用增量更新模式将获取空数据框。
        目前只能使用全量更新模式（update_type="full"）。
        """
        update_type = kwargs.get("update_type", "full")
        
        # 从 cbond_basic 表获取所有转债代码
        query = 'SELECT DISTINCT ts_code FROM "tushare"."cbond_basic" ORDER BY ts_code'
        records = await self.db.fetch(query)
        
        if not records:
            self.logger.warning(f"任务 {self.name}: 未找到任何转债代码，返回空批次列表。")
            return []

        ts_codes = [r["ts_code"] for r in records]
        total_count = len(ts_codes)
        batch_size = 20  # 每批20个 ts_code
        
        # 处理日期参数
        start_date = None
        end_date = None
        
        if update_type != "full":
            # 增量更新模式：使用 start_date 和 end_date 参数
            # !!! 警告：接口 Bug !!!
            # cb_share 接口存在 bug，无法使用 ann_date、start_date、end_date 等日期参数
            # 使用增量更新模式将获取空数据框，请使用全量更新模式
            self.logger.warning(
                f"任务 {self.name}: 警告！cb_share 接口存在 bug，无法使用日期参数。"
                f"增量更新模式将获取空数据框，建议使用全量更新模式（update_type='full'）。"
            )
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")
            
            # 如果没有提供日期，使用默认值或从数据库获取最新日期
            if not start_date:
                if update_type == "smart":
                    # 智能增量模式：从数据库获取最新 end_date，回看 smart_lookback_days 天
                    latest_date = await self.get_latest_date()
                    if latest_date:
                        from datetime import timedelta
                        start_date_obj = latest_date - timedelta(days=self.smart_lookback_days)
                        start_date = start_date_obj.strftime("%Y%m%d")
                    else:
                        start_date = self.default_start_date
                else:
                    start_date = self.default_start_date
                self.logger.info(
                    f"任务 {self.name}: 未提供 start_date，使用: {start_date}"
                )
            
            if not end_date:
                from datetime import datetime
                end_date = datetime.now().strftime("%Y%m%d")
                self.logger.info(
                    f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}"
                )
        
        # 按每20个分组
        batches = []
        for i in range(0, total_count, batch_size):
            batch_codes = ts_codes[i:i + batch_size]
            ts_codes_str = ",".join(batch_codes)
            
            batch_params = {"ts_code": ts_codes_str}
            
            # 增量模式：添加日期参数
            if update_type != "full" and start_date and end_date:
                batch_params["start_date"] = start_date
                batch_params["end_date"] = end_date
            
            batches.append(batch_params)
        
        if update_type == "full":
            self.logger.info(
                f"任务 {self.name}: 全量获取模式，从 cbond_basic 表获取到 {total_count} 个转债代码，"
                f"按每 {batch_size} 个分组，生成 {len(batches)} 个批次。"
            )
        else:
            self.logger.info(
                f"任务 {self.name}: 增量更新模式，从 cbond_basic 表获取到 {total_count} 个转债代码，"
                f"按每 {batch_size} 个分组，日期范围: {start_date} ~ {end_date}，生成 {len(batches)} 个批次。"
            )
        
        return batches

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "债券代码不能为空"),
        (lambda df: df["end_date"].notna(), "统计截止日期不能为空"),
        (lambda df: df["issue_size"].fillna(0) >= 0 if "issue_size" in df.columns else True, "可转债发行总额不能为负数"),
        (lambda df: df["convert_price_initial"].fillna(0) >= 0 if "convert_price_initial" in df.columns else True, "初始转换价格不能为负数"),
        (lambda df: df["convert_price"].fillna(0) >= 0 if "convert_price" in df.columns else True, "本次转换价格不能为负数"),
        (lambda df: df["convert_val"].fillna(0) >= 0 if "convert_val" in df.columns else True, "本次转股金额不能为负数"),
        (lambda df: df["convert_vol"].fillna(0) >= 0 if "convert_vol" in df.columns else True, "本次转股数量不能为负数"),
        (lambda df: df["convert_ratio"].fillna(0).between(0, 100) if "convert_ratio" in df.columns else True, "本次转股比例应在0-100之间"),
        (lambda df: df["acc_convert_ratio"].fillna(0).between(0, 100) if "acc_convert_ratio" in df.columns else True, "累计转股比例应在0-100之间"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def pre_execute(self, stop_event: Optional[Any] = None, **kwargs):
        """
        预执行处理。
        对于全量更新模式，清空表数据。
        """
        update_type = kwargs.get("update_type", "full")
        if update_type == "full":
            self.logger.info(f"任务 {self.name}: 全量更新模式，预先清空表数据。")
            await self.clear_table()


"""
使用方法:
1. 全量更新：设置 update_type="full" 或通过GUI选择"全量更新"
   - 会清空现有数据，重新获取所有可转债转股结果信息
   - 从 tushare.cbond_basic 表获取所有转债代码，每20个 ts_code 组成一个多值字符串分批获取
   - 这是目前唯一可用的更新模式
2. 增量更新：设置 update_type="smart" 或 "incremental"
   - !!! 警告：由于接口 Bug，增量更新功能暂时无法使用 !!!
   - cb_share 接口存在 bug，无法使用 ann_date、start_date、end_date 等日期参数
   - 使用增量更新模式将获取空数据框，请使用全量更新模式
3. 权限要求：用户需要至少2000积分才可以调取，但有流量控制，5000积分以上频次相对较高
4. 单次限量：2000条，总量不限制

注意事项:
- 只处理可转债转股结果信息，不包含其他数据
- 全量更新会清空现有数据并重新获取
- !!! 重要：由于接口 Bug，增量更新功能暂时无法使用，只能使用全量更新模式 !!!
- 主键为 (ts_code, end_date)，同一个转债可能有多次转股结果记录
- 数据包含本次转股金额、转股数量、转股比例，以及累计转股金额、转股数量、转股比例等信息
- 每批处理20个转债代码，避免单次请求数据量过大
"""

