#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
国际指数日线行情 (index_global) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=211
数据说明:
- 获取国际主要指数日线行情
- 单次最大提取4000行情数据，可循环获取，总量不限制

权限要求: 需要至少6000积分
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_trade_day_batches
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes


@task_register()
class TushareIndexGlobalTask(TushareTask):
    """获取国际指数日线行情数据 (index_global)"""

    # 1. 核心属性
    name = "tushare_index_global"
    description = "获取国际主要指数日线行情"
    table_name = "index_global"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19820920"
    data_source = "tushare"
    domain = "index"

    # --- 默认配置 ---
    default_concurrent_limit = 3
    default_page_size = 4000

    # 2. TushareTask 特有属性
    api_name = "index_global"
    fields = [
        "ts_code",      # TS指数代码
        "trade_date",   # 交易日
        "open",         # 开盘点位
        "close",        # 收盘点位
        "high",         # 最高点位
        "low",          # 最低点位
        "pre_close",    # 昨日收盘点
        "change",       # 涨跌点位
        "pct_chg",      # 涨跌幅
        "swing",        # 振幅
        "vol",          # 成交量
        "amount",       # 成交额
    ]

    # 3. 列名映射 (vol -> volume)
    column_mapping: Dict[str, str] = {
        "vol": "volume",
    }

    # 4. 数据类型转换
    transformations = {
        "open": lambda x: pd.to_numeric(x, errors="coerce"),
        "close": lambda x: pd.to_numeric(x, errors="coerce"),
        "high": lambda x: pd.to_numeric(x, errors="coerce"),
        "low": lambda x: pd.to_numeric(x, errors="coerce"),
        "pre_close": lambda x: pd.to_numeric(x, errors="coerce"),
        "change": lambda x: pd.to_numeric(x, errors="coerce"),
        "pct_chg": lambda x: pd.to_numeric(x, errors="coerce"),
        "swing": lambda x: pd.to_numeric(x, errors="coerce"),
        "volume": lambda x: pd.to_numeric(x, errors="coerce"),
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "open": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "pre_close": {"type": "NUMERIC(15,4)"},
        "change": {"type": "NUMERIC(15,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "swing": {"type": "NUMERIC(10,4)"},
        "volume": {"type": "NUMERIC(20,4)"},
        "amount": {"type": "NUMERIC(20,4)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_index_global_ts_code", "columns": "ts_code"},
        {"name": "idx_index_global_trade_date", "columns": "trade_date"},
        {"name": "idx_index_global_update_time", "columns": "update_time"},
    ]

    # 7. 国际指数代码与中文名称映射
    global_index_names: Dict[str, str] = {
        "XIN9": "富时中国A50指数",
        "HSI": "恒生指数",
        "HKTECH": "恒生科技指数",
        "HKAH": "恒生AH股H指数",
        "DJI": "道琼斯工业指数",
        "SPX": "标普500指数",
        "IXIC": "纳斯达克指数",
        "FTSE": "富时100指数",
        "FCHI": "法国CAC40指数",
        "GDAXI": "德国DAX指数",
        "N225": "日经225指数",
        "KS11": "韩国综合指数",
        "AS51": "澳大利亚标普200指数",
        "SENSEX": "印度孟买SENSEX指数",
        "IBOVESPA": "巴西IBOVESPA指数",
        "RTS": "俄罗斯RTS指数",
        "TWII": "台湾加权指数",
        "CKLSE": "马来西亚指数",
        "SPTSX": "加拿大S&P/TSX指数",
        "CSX5P": "STOXX欧洲50指数",
        "RUT": "罗素2000指数",
    }

    @property
    def global_index_codes(self) -> List[str]:
        """获取国际指数代码列表"""
        return list(self.global_index_names.keys())

    # 8. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "指数代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
    ]

    # 9. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """生成批处理参数列表"""
        update_type = kwargs.get("update_type", "incremental")

        if update_type == UpdateTypes.FULL:
            return await self._get_full_batch_list(**kwargs)
        else:
            return await self._get_incremental_batch_list(**kwargs)

    async def _get_full_batch_list(self, **kwargs: Any) -> List[Dict]:
        """全量更新：按指数代码分批"""
        self.logger.info(f"任务 {self.name}: 全量更新模式，按指数代码分批处理")

        batch_list = []
        end_date = datetime.now().strftime("%Y%m%d")

        for ts_code in self.global_index_codes:
            batch_list.append({
                "ts_code": ts_code,
                "start_date": self.default_start_date,
                "end_date": end_date,
            })

        self.logger.info(f"生成了 {len(batch_list)} 个批次")
        return batch_list

    async def _get_incremental_batch_list(self, **kwargs: Any) -> List[Dict]:
        """增量更新：按指数代码和日期范围分批"""
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if not start_date:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date = (latest_db_date + timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = self.default_start_date

        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
            self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
            return []

        self.logger.info(f"任务 {self.name}: 增量更新 {start_date} ~ {end_date}")

        batch_list = []
        for ts_code in self.global_index_codes:
            batch_list.append({
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date,
            })

        self.logger.info(f"生成了 {len(batch_list)} 个批次")
        return batch_list

    async def pre_execute(self):
        """预执行处理"""
        update_type = getattr(self, "update_type", "incremental")
        if update_type == UpdateTypes.FULL:
            self.logger.info(f"任务 {self.name}: 全量更新模式，清空表数据")
            await self.clear_table()

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """数据处理：添加中文名称，过滤无效数据"""
        if df is None or df.empty:
            return df

        # 调用父类处理（列名映射、类型转换等）
        df = super().process_data(df, **kwargs)

        # 添加中文名称
        df["name"] = df["ts_code"].map(self.global_index_names)

        # 过滤 close 为 NaN 的记录
        before_count = len(df)
        df = df[df["close"].notna()]
        after_count = len(df)
        if before_count > after_count:
            self.logger.debug(f"过滤了 {before_count - after_count} 条 close 为空的记录")

        return df


__all__ = ["TushareIndexGlobalTask"]
