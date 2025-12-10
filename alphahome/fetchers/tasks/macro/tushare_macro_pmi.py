#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
采购经理人指数 (cn_pmi) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=229
数据说明:
- 获取中国采购经理人指数(PMI)数据
- 单次最大2000条，一次可以提取全部数据

权限要求: 需要至少2000积分
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareMacroPmiTask(TushareTask):
    """获取中国采购经理人指数(PMI)数据（全量单批）"""

    # 1. 核心属性
    name = "tushare_macro_pmi"
    description = "获取中国采购经理人指数(PMI)"
    table_name = "macro_pmi"
    primary_keys = ["month"]
    date_column = None
    default_start_date = "20050101"
    data_source = "tushare"
    domain = "macro"
    single_batch = True
    update_type = "full"

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "cn_pmi"
    fields = [
        "month",
        # 制造业PMI
        "pmi010000", "pmi010100", "pmi010200", "pmi010300",
        "pmi010400", "pmi010401", "pmi010402", "pmi010403",
        "pmi010500", "pmi010501", "pmi010502", "pmi010503",
        "pmi010600", "pmi010601", "pmi010602", "pmi010603",
        "pmi010700", "pmi010701", "pmi010702", "pmi010703",
        "pmi010800", "pmi010801", "pmi010802", "pmi010803",
        "pmi010900", "pmi011000", "pmi011100", "pmi011200",
        "pmi011300", "pmi011400", "pmi011500", "pmi011600",
        "pmi011700", "pmi011800", "pmi011900", "pmi012000",
        # 非制造业PMI
        "pmi020100", "pmi020101", "pmi020102",
        "pmi020200", "pmi020201", "pmi020202",
        "pmi020300", "pmi020301", "pmi020302",
        "pmi020400", "pmi020401", "pmi020402",
        "pmi020500", "pmi020501", "pmi020502",
        "pmi020600", "pmi020601", "pmi020602",
        "pmi020700", "pmi020800", "pmi020900", "pmi021000",
        # 综合PMI
        "pmi030000",
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {field: float for field in fields if field != "month"}

    # 5. 数据库表结构
    schema_def = {
        "month": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        # 制造业PMI
        "pmi010000": {"type": "NUMERIC(10,4)"},  # 制造业PMI
        "pmi010100": {"type": "NUMERIC(10,4)"},  # 大型企业
        "pmi010200": {"type": "NUMERIC(10,4)"},  # 中型企业
        "pmi010300": {"type": "NUMERIC(10,4)"},  # 小型企业
        "pmi010400": {"type": "NUMERIC(10,4)"},  # 生产指数
        "pmi010401": {"type": "NUMERIC(10,4)"},
        "pmi010402": {"type": "NUMERIC(10,4)"},
        "pmi010403": {"type": "NUMERIC(10,4)"},
        "pmi010500": {"type": "NUMERIC(10,4)"},  # 新订单指数
        "pmi010501": {"type": "NUMERIC(10,4)"},
        "pmi010502": {"type": "NUMERIC(10,4)"},
        "pmi010503": {"type": "NUMERIC(10,4)"},
        "pmi010600": {"type": "NUMERIC(10,4)"},  # 供应商配送时间指数
        "pmi010601": {"type": "NUMERIC(10,4)"},
        "pmi010602": {"type": "NUMERIC(10,4)"},
        "pmi010603": {"type": "NUMERIC(10,4)"},
        "pmi010700": {"type": "NUMERIC(10,4)"},  # 原材料库存指数
        "pmi010701": {"type": "NUMERIC(10,4)"},
        "pmi010702": {"type": "NUMERIC(10,4)"},
        "pmi010703": {"type": "NUMERIC(10,4)"},
        "pmi010800": {"type": "NUMERIC(10,4)"},  # 从业人员指数
        "pmi010801": {"type": "NUMERIC(10,4)"},
        "pmi010802": {"type": "NUMERIC(10,4)"},
        "pmi010803": {"type": "NUMERIC(10,4)"},
        "pmi010900": {"type": "NUMERIC(10,4)"},  # 新出口订单
        "pmi011000": {"type": "NUMERIC(10,4)"},  # 进口
        "pmi011100": {"type": "NUMERIC(10,4)"},  # 采购量
        "pmi011200": {"type": "NUMERIC(10,4)"},  # 主要原材料购进价格
        "pmi011300": {"type": "NUMERIC(10,4)"},  # 出厂价格
        "pmi011400": {"type": "NUMERIC(10,4)"},  # 产成品库存
        "pmi011500": {"type": "NUMERIC(10,4)"},  # 在手订单
        "pmi011600": {"type": "NUMERIC(10,4)"},  # 生产经营活动预期
        "pmi011700": {"type": "NUMERIC(10,4)"},  # 装备制造业
        "pmi011800": {"type": "NUMERIC(10,4)"},  # 高技术制造业
        "pmi011900": {"type": "NUMERIC(10,4)"},  # 基础原材料制造业
        "pmi012000": {"type": "NUMERIC(10,4)"},  # 消费品制造业
        # 非制造业PMI
        "pmi020100": {"type": "NUMERIC(10,4)"},  # 商务活动
        "pmi020101": {"type": "NUMERIC(10,4)"},  # 建筑业
        "pmi020102": {"type": "NUMERIC(10,4)"},  # 服务业
        "pmi020200": {"type": "NUMERIC(10,4)"},  # 新订单指数
        "pmi020201": {"type": "NUMERIC(10,4)"},
        "pmi020202": {"type": "NUMERIC(10,4)"},
        "pmi020300": {"type": "NUMERIC(10,4)"},  # 投入品价格指数
        "pmi020301": {"type": "NUMERIC(10,4)"},
        "pmi020302": {"type": "NUMERIC(10,4)"},
        "pmi020400": {"type": "NUMERIC(10,4)"},  # 销售价格指数
        "pmi020401": {"type": "NUMERIC(10,4)"},
        "pmi020402": {"type": "NUMERIC(10,4)"},
        "pmi020500": {"type": "NUMERIC(10,4)"},  # 从业人员指数
        "pmi020501": {"type": "NUMERIC(10,4)"},
        "pmi020502": {"type": "NUMERIC(10,4)"},
        "pmi020600": {"type": "NUMERIC(10,4)"},  # 业务活动预期指数
        "pmi020601": {"type": "NUMERIC(10,4)"},
        "pmi020602": {"type": "NUMERIC(10,4)"},
        "pmi020700": {"type": "NUMERIC(10,4)"},  # 新出口订单
        "pmi020800": {"type": "NUMERIC(10,4)"},  # 在手订单
        "pmi020900": {"type": "NUMERIC(10,4)"},  # 存货
        "pmi021000": {"type": "NUMERIC(10,4)"},  # 供应商配送时间
        # 综合PMI
        "pmi030000": {"type": "NUMERIC(10,4)"},  # 综合PMI产出指数
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_macro_pmi_month", "columns": "month"},
        {"name": "idx_macro_pmi_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["month"].notna(), "月份不能为空"),
        (lambda df: df["month"].astype(str).str.match(r"^\d{6}$"), "月份格式必须为YYYYMM"),
        (lambda df: df["month_end_date"].notna(), "month_end_date 生成失败"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """全量模式：返回单个空参数批次"""
        self.logger.info(f"任务 {self.name}: 采用全量模式，单批次拉取所有数据")
        return [{"fields": ",".join(self.fields or [])}]

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """生成 month_end_date，排序并返回"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        df = super().process_data(df, **kwargs)

        if "month" not in df.columns:
            self.logger.error(f"任务 {self.name}: 缺少 month 列")
            return pd.DataFrame()

        def _month_to_end_date(month_str: Optional[str]):
            if pd.isna(month_str):
                return None
            text = str(month_str).strip()
            if len(text) != 6 or not text.isdigit():
                return None
            year = int(text[:4])
            month = int(text[4:6])
            try:
                next_month = month % 12 + 1
                next_year = year + (month // 12)
                first_day_next = datetime(next_year, next_month, 1)
                end_day = first_day_next - pd.Timedelta(days=1)
                return end_day.date()
            except Exception:
                return None

        df["month_end_date"] = df["month"].apply(_month_to_end_date)

        invalid = df["month_end_date"].isna().sum()
        if invalid:
            self.logger.warning(f"任务 {self.name}: {invalid} 行 month_end_date 转换失败")
            df = df[df["month_end_date"].notna()].copy()

        df = df.sort_values("month_end_date").reset_index(drop=True)
        self.logger.info(f"任务 {self.name}: 处理后共 {len(df)} 行数据")
        return df


__all__ = ["TushareMacroPmiTask"]
