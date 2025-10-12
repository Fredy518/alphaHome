#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中债收益率曲线 (yc_cb) 更新任务
获取中债收益率曲线，目前可获取中债国债收益率曲线即期和到期收益率曲线数据。
继承自 TushareTask。
全部使用交易日分批策略。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入批次生成工具函数
from ...sources.tushare.batch_utils import generate_trade_day_batches


@task_register()
class TushareMacroYieldCurveTask(TushareTask):
    """获取中债收益率曲线数据"""

    # 1. 核心属性
    domain = "macro"  # 业务域标识
    name = "tushare_macro_yieldcurve"
    description = "获取中债收益率曲线，包括即期和到期收益率曲线数据"
    table_name = "macro_yieldcurve"
    primary_keys = ["trade_date", "ts_code", "curve_type", "curve_term"]  # 复合主键确保唯一性
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20160613"  # 起始日期20160613

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 默认并发限制，与API速率限制匹配
    default_page_size = 2000  # 单次最大2000条数据

    # 2. TushareTask 特有属性
    api_name = "yc_cb"
    # Tushare yc_cb 接口实际返回的字段 (根据Tushare文档)
    fields = ["trade_date", "ts_code", "curve_name", "curve_type", "curve_term", "yield"]

    # 3. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "ts_code": str,
        "curve_name": str,
        "curve_type": str,
        "curve_term": float,  # 期限转换为浮点数
        "yield": float,  # 收益率转换为浮点数
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(20)"},  # 曲线编码
        "curve_name": {"type": "TEXT"},  # 曲线名称
        "curve_type": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},  # 曲线类型：0-到期，1-即期
        "curve_term": {"type": "NUMERIC(10,4)", "constraints": "NOT NULL"},  # 期限(年)
        "yield": {"type": "NUMERIC(10,6)"},  # 收益率(%)
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_yieldcurve_trade_date", "columns": "trade_date"},
        {"name": "idx_yieldcurve_ts_code", "columns": "ts_code"},
        {"name": "idx_yieldcurve_curve_type", "columns": "curve_type"},
        {"name": "idx_yieldcurve_update_time", "columns": "update_time"},
    ]

    # 7. 交易日分批配置
    batch_trade_days = 10  # 每个批次包含10个交易日，适应每分钟20次的速率限制

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        全部使用交易日分批策略。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code", "1001.CB")  # 默认国债收益率曲线编码
        exchange = kwargs.get("exchange", "SSE")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 曲线代码: {ts_code}"
        )

        try:
            # 使用交易日分批策略
            additional_params = {
                "fields": ",".join(self.fields),
                "ts_code": ts_code  # 固定使用国债收益率曲线
            }

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days,
                ts_code=None,  # ts_code通过additional_params传递
                exchange=exchange,
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个交易日批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成批次失败") from e

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从API获取的原始数据（重写基类扩展点）
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.info(
                f"任务 {self.name}: process_data 接收到空 DataFrame，跳过处理。"
            )
            return df

        # 首先调用基类的数据处理方法（应用基础转换）
        df = super().process_data(df, **kwargs)

        # 特殊处理：确保curve_type为字符串格式
        if 'curve_type' in df.columns:
            df['curve_type'] = df['curve_type'].astype(str)

        self.logger.info(
            f"任务 {self.name}: process_data 完成，返回 DataFrame (行数: {len(df)})。"
        )
        return df

    # 8. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['curve_type'].notna(), "曲线类型不能为空"),
        (lambda df: df['curve_term'].notna(), "期限不能为空"),
        (lambda df: ~(df['trade_date'].astype(str).str.strip().eq('') | df['trade_date'].isna()), "交易日期不能为空字符串"),
        # 期限范围验证（通常在0-50年之间）
        (lambda df: (df['curve_term'] >= 0) & (df['curve_term'] <= 50), "期限应在0-50年范围内"),
        # 收益率合理性验证（通常在-10%到50%之间）
        (lambda df: df['yield'].isna() | ((df['yield'] >= -10) & (df['yield'] <= 50)), "收益率应在-10%到50%范围内"),
        # 曲线类型验证（应为0或1）
        (lambda df: df['curve_type'].isin(['0', '1']), "曲线类型应为0（到期）或1（即期）"),
    ]

    # 9. 验证模式配置 - 使用报告模式记录验证结果但保留所有数据
    validation_mode = "report"