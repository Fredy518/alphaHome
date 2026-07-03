#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""事件类宏观任务基类。

akshare 多个宏观经济指标接口（美联储/欧央行/日央行利率决议、非农、失业率、
核心 PCE 等）输出同构：`商品/日期/今值/预测值/前值`，日期为事件公告日，
最新未发布事件的「今值」为 NaN。本基类封装这类接口共享的 process_data 逻辑：
仅保留 schema_def 定义的列、规整日期、丢弃未发布（今值为 NaN）的行、按 date 去重。

子类只需声明 name/table_name/api_name/column_mapping/schema_def/validations，
无需实现 process_data。设计参照 AkShareNoDateSingleBatchTask（全历史、不接受日期参数）。
"""

import pandas as pd

from .akshare_task import AkShareNoDateSingleBatchTask


class AkShareMacroEventTask(AkShareNoDateSingleBatchTask):
    """事件类宏观任务基类（商品/日期/今值/预测值/前值 同构接口）。

    子类约定：column_mapping 将「日期→date, 今值→rate, 预测值→rate_forecast,
    前值→rate_prev」，schema_def 含这些列。本基类据此清洗。
    """

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用基础转换后，仅保留 schema_def 列、规整日期、丢弃未发布行并去重。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # 仅保留 schema_def 定义的列（丢弃「商品」等未映射的源列，避免 COPY 失败）
        keep = [c for c in self.schema_def.keys() if c in data.columns]
        data = data[keep].copy()

        # 规整日期列为 date 类型（FULL 模式下基类不做此转换）
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
            data = data.dropna(subset=["date"])

        # 丢弃尚未发布事件（今值为 NaN 的最新行）
        if "rate" in data.columns:
            data = data.dropna(subset=["rate"]).copy()

        # 去重（按 date 保留最后一条）
        if "date" in data.columns:
            data = data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        return data
