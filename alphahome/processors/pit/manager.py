#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PIT 域服务（processors schema）
=============================

该模块提供基于 processors 命名空间的 PIT 数据域服务骨架，
采用“域服务 + 任务包装”模式：
1) ensure_tables_exist: 建表/索引治理
2) full_rebuild: 全量重建（同步）
3) incremental_update: 增量更新（同步、幂等）
4) validate: 数据质量校验（同步）

本文件是首个最小实现骨架：
- 仅保留 schema 准备与接口雏形；
- 后续将逐步迁移 research 中的等价逻辑到此（但写入 processors schema）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import logging
import time
from datetime import datetime, timedelta

import pandas as pd

from ..domain.base_manager import BaseDomainManager


class PITManager(BaseDomainManager):
    """processors 下的 PIT 域服务。

    说明：
    - 统一依赖 DBManager（双模），避免重复数据库封装。
    - 默认 schema = "processors"；可按需通过构造参数或子类重写扩展。
    """

    def __init__(self, db_manager: "DBManager", batch_size: int = 500) -> None:
        super().__init__(db_manager, batch_size=batch_size)
        self.logger = logging.getLogger("processors.pit_manager")

    # ------------------------------------------------------------------
    # 基本结构治理
    # ------------------------------------------------------------------
    async def ensure_tables_exist(self) -> None:
        """确保 processors 命名空间与核心 PIT 表存在（异步）。"""
        schema = self.schema_name

        # 根据模式选择执行方式（兼容双模 DBManager）
        is_async_mode = getattr(self.db, "mode", "async") == "async"

        # 1) 确保 schema 存在
        if is_async_mode:
            await self.db.ensure_schema_exists(schema)  # type: ignore[attr-defined]
        else:
            self._ensure_namespace_sync()

        # 2) 创建 processors.pit_income_quarterly
        create_income_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.pit_income_quarterly (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(10) NOT NULL,
            end_date DATE NOT NULL,
            ann_date DATE NOT NULL,
            data_source VARCHAR(20),
            year INT,
            quarter INT,
            n_income_attr_p FLOAT,
            n_income FLOAT,
            revenue FLOAT,
            operate_profit FLOAT,
            total_profit FLOAT,
            income_tax FLOAT,
            oper_cost FLOAT,
            total_cogs FLOAT,
            fin_exp FLOAT,
            interest_expense FLOAT,
            net_profit_mid FLOAT,
            conversion_status VARCHAR(20) DEFAULT 'SINGLE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, end_date, ann_date)
        );
        """

        income_index_sqls = [
            f'CREATE INDEX IF NOT EXISTS idx_pit_income_q_stock ON {schema}.pit_income_quarterly(ts_code);',
            f'CREATE INDEX IF NOT EXISTS idx_pit_income_q_end ON {schema}.pit_income_quarterly(end_date);',
            f'CREATE INDEX IF NOT EXISTS idx_pit_income_q_ann ON {schema}.pit_income_quarterly(ann_date);',
        ]

        # 3) 创建 processors.pit_balance_quarterly
        create_balance_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.pit_balance_quarterly (
            id SERIAL PRIMARY KEY,
            ts_code VARCHAR(10) NOT NULL,
            end_date DATE NOT NULL,
            ann_date DATE NOT NULL,
            data_source VARCHAR(20),
            year INT,
            quarter INT,
            tot_assets FLOAT,
            tot_liab FLOAT,
            tot_equity FLOAT,
            total_cur_assets FLOAT,
            total_cur_liab FLOAT,
            inventories FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, end_date, ann_date)
        );
        """

        balance_index_sqls = [
            f'CREATE INDEX IF NOT EXISTS idx_pit_balance_q_stock ON {schema}.pit_balance_quarterly(ts_code);',
            f'CREATE INDEX IF NOT EXISTS idx_pit_balance_q_end ON {schema}.pit_balance_quarterly(end_date);',
            f'CREATE INDEX IF NOT EXISTS idx_pit_balance_q_ann ON {schema}.pit_balance_quarterly(ann_date);',
        ]

        # 执行 DDL
        if is_async_mode:
            await self.db.execute(create_income_sql)  # type: ignore[attr-defined]
            for sql in income_index_sqls:
                await self.db.execute(sql)  # type: ignore[attr-defined]

            await self.db.execute(create_balance_sql)  # type: ignore[attr-defined]
            for sql in balance_index_sqls:
                await self.db.execute(sql)  # type: ignore[attr-defined]
        else:
            self.db.execute_sync(create_income_sql)  # type: ignore[attr-defined]
            for sql in income_index_sqls:
                self.db.execute_sync(sql)  # type: ignore[attr-defined]

            self.db.execute_sync(create_balance_sql)  # type: ignore[attr-defined]
            for sql in balance_index_sqls:
                self.db.execute_sync(sql)  # type: ignore[attr-defined]

        self.logger.info("processors.PIT 基础表检查/创建完成")

    # ------------------------------------------------------------------
    # 数据域功能
    # ------------------------------------------------------------------
    def get_stock_universe(self, stocks: Optional[List[str]] = None) -> List[str]:
        """获取股票池（来自原始层 tushare 表）。"""
        if stocks:
            return stocks

        query = (
            "SELECT DISTINCT ts_code FROM ("
            " SELECT ts_code FROM tushare.fina_income"
            " UNION"
            " SELECT ts_code FROM tushare.fina_express"
            " UNION"
            " SELECT ts_code FROM tushare.fina_forecast"
            ") t ORDER BY ts_code"
        )
        rows = self.db.fetch_sync(query)
        return [row["ts_code"] if isinstance(row, dict) else row[0] for row in rows] if rows else []

    def process_report_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理损益表 report 数据 → processors.pit_income_quarterly。"""
        where = ["ts_code = ANY(%s)"]
        params: List[Any] = [stocks]
        if start_date:
            where.append("ann_date >= %s")
            params.append(start_date)
        where_clause = " AND ".join(where)

        query = f"""
        WITH latest_income AS (
            SELECT ts_code, end_date, ann_date,
                   n_income_attr_p, n_income, revenue, operate_profit, total_profit,
                   income_tax, oper_cost, total_cogs, fin_exp, fin_exp_int_exp as interest_expense,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_income
            WHERE {where_clause}
        )
        SELECT ts_code,
               end_date::date as end_date,
               ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               n_income_attr_p::numeric  as n_income_attr_p,
               n_income::numeric  as n_income,
               revenue::numeric  as revenue,
               operate_profit::numeric  as operate_profit,
               total_profit::numeric  as total_profit,
               income_tax::numeric as income_tax,
               oper_cost::numeric as oper_cost,
               total_cogs::numeric as total_cogs,
               fin_exp::numeric as fin_exp,
               interest_expense::numeric as interest_expense
        FROM latest_income
        WHERE rn = 1
        ORDER BY ts_code, end_date
        """

        rows = self.db.fetch_sync(query, tuple(params))
        if not rows:
            return 0

        df = pd.DataFrame(rows)
        # 单季化：同时处理 n_income 与 n_income_attr_p 及相关累计字段
        df_single_income = self._convert_to_single_quarter(df, "n_income")
        df_single_attr = self._convert_to_single_quarter(df, "n_income_attr_p")

        merge_keys = ["ts_code", "end_date", "ann_date", "year", "quarter"]
        df_single = df_single_income.copy()
        if "n_income_attr_p_single" in df_single_attr.columns:
            df_single = df_single.merge(
                df_single_attr[merge_keys + ["n_income_attr_p_single"]],
                on=merge_keys,
                how="left",
            )

        insert_df = pd.DataFrame(
            {
                "ts_code": df_single["ts_code"],
                "end_date": df_single["end_date"],
                "ann_date": df_single["ann_date"],
                "data_source": "report",
                "year": df_single["year"],
                "quarter": df_single["quarter"],
                "n_income": df_single.get("n_income_single"),
                "n_income_attr_p": df_single.get("n_income_attr_p_single"),
                "revenue": df_single.get("revenue_single"),
                "operate_profit": df_single.get("operate_profit_single"),
                "total_profit": df_single.get("total_profit_single"),
                "income_tax": df_single.get("income_tax_single"),
                "oper_cost": df_single.get("oper_cost_single"),
                "total_cogs": df_single.get("total_cogs_single"),
                "fin_exp": df_single.get("fin_exp_single"),
                "interest_expense": df_single.get("interest_expense_single"),
                "net_profit_mid": None,
                "conversion_status": df_single.get("conversion_status", "SINGLE"),
            }
        )

        # UPSERT 到 processors.pit_income_quarterly
        self.db._run_sync(
            self.db.upsert(
                df=insert_df,
                target=f"{self.schema_name}.pit_income_quarterly",
                conflict_columns=["ts_code", "end_date", "ann_date"],
                update_columns=None,
                timestamp_column="updated_at",
            )
        )
        return len(insert_df)

    def process_express_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理快报 express 数据 → processors.pit_income_quarterly。"""
        where = ["ts_code = ANY(%s)"]
        params: List[Any] = [stocks]
        if start_date:
            where.append("ann_date >= %s")
            params.append(start_date)
        where_clause = " AND ".join(where)

        query = f"""
        WITH latest_express AS (
            SELECT ts_code, end_date, ann_date, n_income, revenue,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_express
            WHERE {where_clause}
        )
        SELECT ts_code,
               end_date::date as end_date,
               ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               n_income::numeric as n_income,
               revenue::numeric as revenue
        FROM latest_express
        WHERE rn = 1
        ORDER BY ts_code, end_date
        """

        rows = self.db.fetch_sync(query, tuple(params))
        if not rows:
            return 0
        df = pd.DataFrame(rows)

        df_single_income = self._convert_to_single_quarter(df, "n_income")
        df_revenue_single = self._convert_to_single_quarter(df, "revenue")

        insert_df = pd.DataFrame(
            {
                "ts_code": df_single_income["ts_code"],
                "end_date": df_single_income["end_date"],
                "ann_date": df_single_income["ann_date"],
                "data_source": "express",
                "year": df_single_income["year"],
                "quarter": df_single_income["quarter"],
                "n_income": df_single_income.get("n_income_single"),
                "n_income_attr_p": df_single_income.get("n_income_attr_p"),
                "revenue": df_revenue_single.get("revenue_single"),
                "net_profit_mid": None,
                "conversion_status": df_single_income.get("conversion_status", "SINGLE"),
            }
        )

        self.db._run_sync(
            self.db.upsert(
                df=insert_df,
                target=f"{self.schema_name}.pit_income_quarterly",
                conflict_columns=["ts_code", "end_date", "ann_date"],
                update_columns=None,
                timestamp_column="updated_at",
            )
        )
        return len(insert_df)

    def process_forecast_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理预告 forecast 数据 → processors.pit_income_quarterly。"""
        where = ["ts_code = ANY(%s)"]
        params: List[Any] = [stocks]
        if start_date:
            where.append("ann_date >= %s")
            params.append(start_date)
        where_clause = " AND ".join(where)

        query = f"""
        WITH latest_forecast AS (
            SELECT ts_code, end_date, ann_date,
                   net_profit_min, net_profit_max,
                   p_change_min, p_change_max, last_parent_net,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_forecast
            WHERE {where_clause}
        )
        SELECT ts_code,
               end_date::date as end_date,
               ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               CASE 
                   WHEN net_profit_min IS NOT NULL AND net_profit_max IS NOT NULL 
                        AND NOT (net_profit_min = 'NaN' OR net_profit_max = 'NaN') THEN
                       ((net_profit_min + net_profit_max)/2.0)::numeric * 10000
                   WHEN (net_profit_min IS NULL OR net_profit_max IS NULL 
                         OR net_profit_min = 'NaN' OR net_profit_max = 'NaN') 
                        AND p_change_min IS NOT NULL AND p_change_max IS NOT NULL 
                        AND last_parent_net IS NOT NULL 
                        AND NOT (p_change_min = 'NaN' OR p_change_max = 'NaN' OR last_parent_net = 'NaN') THEN
                       (last_parent_net * (1 + (p_change_min + p_change_max)/200.0))::numeric * 10000
                   ELSE NULL
               END as net_profit_mid,
               CASE 
                   WHEN net_profit_min IS NOT NULL AND net_profit_max IS NOT NULL 
                        AND NOT (net_profit_min = 'NaN' OR net_profit_max = 'NaN') THEN 'direct'
                   WHEN (net_profit_min IS NULL OR net_profit_max IS NULL 
                         OR net_profit_min = 'NaN' OR net_profit_max = 'NaN') 
                        AND p_change_min IS NOT NULL AND p_change_max IS NOT NULL 
                        AND last_parent_net IS NOT NULL 
                        AND NOT (p_change_min = 'NaN' OR p_change_max = 'NaN' OR last_parent_net = 'NaN') THEN 'calculated'
                   ELSE 'unavailable'
               END as data_source_type
        FROM latest_forecast
        WHERE rn = 1 AND (
            (net_profit_min IS NOT NULL AND net_profit_max IS NOT NULL AND NOT (net_profit_min = 'NaN' OR net_profit_max = 'NaN'))
            OR
            (p_change_min IS NOT NULL AND p_change_max IS NOT NULL AND last_parent_net IS NOT NULL AND NOT (p_change_min = 'NaN' OR p_change_max = 'NaN' OR last_parent_net = 'NaN'))
        )
        ORDER BY ts_code, end_date
        """

        rows = self.db.fetch_sync(query, tuple(params))
        if not rows:
            return 0

        df = pd.DataFrame(rows)

        # 读取 processors 层已入库的 report 单季值用于 YTD 还原
        report_query = f"""
        SELECT ts_code,
               EXTRACT(YEAR FROM end_date)::int AS year,
               EXTRACT(QUARTER FROM end_date)::int AS quarter,
               n_income_attr_p AS n_income_attr_p_single
        FROM {self.schema_name}.pit_income_quarterly
        WHERE ts_code = ANY(%s) AND data_source = 'report'
        """
        report_rows = self.db.fetch_sync(report_query, (stocks,))
        report_df = pd.DataFrame(report_rows) if report_rows else pd.DataFrame(
            columns=["ts_code", "year", "quarter", "n_income_attr_p_single"]
        )

        merged = self._compute_forecast_single_quarter(df, report_df)

        insert_df = pd.DataFrame(
            {
                "ts_code": merged["ts_code"],
                "end_date": merged["end_date"],
                "ann_date": merged["ann_date"],
                "data_source": merged["data_source_type"].apply(lambda x: f"forecast_{x}"),
                "year": merged["year"],
                "quarter": merged["quarter"],
                "n_income": None,
                "n_income_attr_p": merged["n_income_attr_p_single"],
                "revenue": None,
                "operate_profit": None,
                "total_profit": None,
                "income_tax": None,
                "oper_cost": None,
                "total_cogs": None,
                "fin_exp": None,
                "interest_expense": None,
                "net_profit_mid": merged["net_profit_mid"],
                "conversion_status": "FORECAST",
            }
        )

        self.db._run_sync(
            self.db.upsert(
                df=insert_df,
                target=f"{self.schema_name}.pit_income_quarterly",
                conflict_columns=["ts_code", "end_date", "ann_date"],
                update_columns=None,
                timestamp_column="updated_at",
            )
        )
        return len(insert_df)

    def process_balance_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理资产负债表 report 数据 → processors.pit_balance_quarterly。"""
        where = ["ts_code = ANY(%s)"]
        params: List[Any] = [stocks]
        if start_date:
            where.append("ann_date >= %s")
            params.append(start_date)
        where_clause = " AND ".join(where)

        query = f"""
        WITH latest_balance AS (
            SELECT ts_code, end_date, ann_date,
                   total_assets, total_liab, (total_assets - total_liab) as tot_equity,
                   total_cur_assets, total_cur_liab, inventories,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_balancesheet
            WHERE {where_clause}
        )
        SELECT ts_code,
               end_date::date as end_date,
               ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               total_assets::numeric as tot_assets,
               total_liab::numeric as tot_liab,
               tot_equity::numeric as tot_equity,
               total_cur_assets::numeric as total_cur_assets,
               total_cur_liab::numeric as total_cur_liab,
               inventories::numeric as inventories
        FROM latest_balance
        WHERE rn = 1
        ORDER BY ts_code, end_date
        """

        rows = self.db.fetch_sync(query, tuple(params))
        if not rows:
            return 0
        df = pd.DataFrame(rows)

        insert_df = pd.DataFrame(
            {
                "ts_code": df["ts_code"],
                "end_date": df["end_date"],
                "ann_date": df["ann_date"],
                "data_source": "report",
                "year": df["year"],
                "quarter": df["quarter"],
                "tot_assets": df["tot_assets"],
                "tot_liab": df["tot_liab"],
                "tot_equity": df["tot_equity"],
                "total_cur_assets": df.get("total_cur_assets"),
                "total_cur_liab": df.get("total_cur_liab"),
                "inventories": df.get("inventories"),
            }
        )

        self.db._run_sync(
            self.db.upsert(
                df=insert_df,
                target=f"{self.schema_name}.pit_balance_quarterly",
                conflict_columns=["ts_code", "end_date", "ann_date"],
                update_columns=None,
                timestamp_column="updated_at",
            )
        )
        return len(insert_df)

    # ------------------------------------------------------------------
    # 全量/增量/校验（同步接口雏形）
    # ------------------------------------------------------------------
    def full_rebuild(
        self,
        stocks: Optional[List[str]] = None,
        date_range: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ) -> Dict[str, Any]:
        """全量重建 processors.PIT 表。"""
        # 建表
        self.db._run_sync(self.ensure_tables_exist())

        target_stocks = self.get_stock_universe(stocks)
        start_date = date_range[0] if date_range else None

        total = {
            "report_income": 0,
            "report_balance": 0,
            "express": 0,
            "forecast": 0,
        }

        for i in range(0, len(target_stocks), self.batch_size):
            batch = target_stocks[i : i + self.batch_size]
            total["report_income"] += self.process_report_data(batch, start_date)
            total["report_balance"] += self.process_balance_data(batch, start_date)
            total["express"] += self.process_express_data(batch, start_date)
            total["forecast"] += self.process_forecast_data(batch, start_date)

        return {"status": "success", "schema": self.schema_name, **total}

    def incremental_update(
        self,
        since: Optional[str] = None,
        stocks: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """增量更新 processors.PIT 表。"""
        self.db._run_sync(self.ensure_tables_exist())

        target_stocks = self.get_stock_universe(stocks)
        # 默认取最近30天
        if since is None:
            since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        total = {
            "report_income": 0,
            "report_balance": 0,
            "express": 0,
            "forecast": 0,
        }

        for i in range(0, len(target_stocks), self.batch_size):
            batch = target_stocks[i : i + self.batch_size]
            total["report_income"] += self.process_report_data(batch, since)
            total["report_balance"] += self.process_balance_data(batch, since)
            total["express"] += self.process_express_data(batch, since)
            total["forecast"] += self.process_forecast_data(batch, since)

        return {"status": "success", "schema": self.schema_name, "since": since, **total}

    def validate(self) -> Dict[str, Any]:
        """基础数据质量校验（记录数/最新 ann_date/重复键检测）。"""
        self._ensure_namespace_sync()

        stats: Dict[str, Any] = {"schema": self.schema_name}

        # 计数与最大最小日期
        count_income = self.db.fetch_val_sync(
            f"SELECT COUNT(*) FROM {self.schema_name}.pit_income_quarterly"
        )
        count_balance = self.db.fetch_val_sync(
            f"SELECT COUNT(*) FROM {self.schema_name}.pit_balance_quarterly"
        )
        max_ann_income = self.db.fetch_val_sync(
            f"SELECT MAX(ann_date) FROM {self.schema_name}.pit_income_quarterly"
        )
        max_ann_balance = self.db.fetch_val_sync(
            f"SELECT MAX(ann_date) FROM {self.schema_name}.pit_balance_quarterly"
        )

        # 重复键检测（理论上不应存在）
        dup_income = self.db.fetch_val_sync(
            f"""
            SELECT COUNT(*) FROM (
              SELECT ts_code, end_date, ann_date, COUNT(*) c
              FROM {self.schema_name}.pit_income_quarterly
              GROUP BY ts_code, end_date, ann_date
              HAVING COUNT(*) > 1
            ) t
            """
        )
        dup_balance = self.db.fetch_val_sync(
            f"""
            SELECT COUNT(*) FROM (
              SELECT ts_code, end_date, ann_date, COUNT(*) c
              FROM {self.schema_name}.pit_balance_quarterly
              GROUP BY ts_code, end_date, ann_date
              HAVING COUNT(*) > 1
            ) t
            """
        )

        stats.update(
            {
                "income_rows": int(count_income or 0),
                "balance_rows": int(count_balance or 0),
                "income_latest_ann_date": max_ann_income,
                "balance_latest_ann_date": max_ann_balance,
                "income_dup_keys": int(dup_income or 0),
                "balance_dup_keys": int(dup_balance or 0),
                "status": "success",
            }
        )
        return stats

    # ------------------------------------------------------------------
    # 工具函数：单季化与预告转换
    # ------------------------------------------------------------------
    def _convert_to_single_quarter(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        """将累计数据转换为单季数据，并标注 conversion_status。"""
        if df is None or df.empty:
            return df

        result = df.copy()
        result = result.sort_values(["ts_code", "year", "quarter"])
        result["conversion_status"] = "SINGLE"

        cumulative_fields = [
            value_col,
            "revenue",
            "operate_profit",
            "total_profit",
            "income_tax",
            "oper_cost",
            "total_cogs",
            "fin_exp",
            "interest_expense",
        ]

        warned = set()
        for field in cumulative_fields:
            if field in result.columns:
                result[field] = pd.to_numeric(result[field], errors="coerce").fillna(0.0)
                result[f"{field}_single"] = 0.0

        grouped = result.groupby(["ts_code", "year"])  # type: ignore
        for (ts_code, year), group in grouped:
            group = group.sort_values("quarter")
            quarters = sorted(group["quarter"].tolist())
            mask = (result["ts_code"] == ts_code) & (result["year"] == year)

            if quarters == [1]:
                result.loc[mask, "conversion_status"] = "SINGLE"
            elif quarters == [4]:
                result.loc[mask, "conversion_status"] = "ANNUAL"
            elif len(quarters) == 1 and quarters[0] in [2, 3]:
                result.loc[mask, "conversion_status"] = "CUMULATIVE"
            elif quarters == [1, 2]:
                result.loc[mask & (result["quarter"] == 1), "conversion_status"] = "SINGLE"
                result.loc[mask & (result["quarter"] == 2), "conversion_status"] = "SINGLE"
            elif quarters == [1, 4]:
                result.loc[mask & (result["quarter"] == 1), "conversion_status"] = "SINGLE"
                result.loc[mask & (result["quarter"] == 4), "conversion_status"] = "CALCULATED"
            elif quarters == [2, 4]:
                result.loc[mask & (result["quarter"] == 2), "conversion_status"] = "CUMULATIVE"
                result.loc[mask & (result["quarter"] == 4), "conversion_status"] = "CALCULATED"
            elif quarters == [3, 4]:
                result.loc[mask & (result["quarter"] == 3), "conversion_status"] = "CUMULATIVE"
                result.loc[mask & (result["quarter"] == 4), "conversion_status"] = "SINGLE"
            elif len(quarters) >= 3:
                result.loc[mask, "conversion_status"] = "SINGLE"
            else:
                result.loc[mask, "conversion_status"] = "CUMULATIVE"
                key = (ts_code, year)
                if key not in warned:
                    self.logger.warning(f"{ts_code} {year} 季报模式异常: {quarters}")
                    warned.add(key)

            for field in cumulative_fields:
                if field in result.columns:
                    self._process_single_field_for_group(result, group, field, quarters, mask)

        return result

    def _process_single_field_for_group(
        self,
        result: pd.DataFrame,
        group: pd.DataFrame,
        field: str,
        quarters: List[int],
        mask: pd.Series,
    ) -> None:
        if quarters == [1]:
            result.loc[mask & (result["quarter"] == 1), f"{field}_single"] = group[group["quarter"] == 1][
                field
            ].iloc[0]
        elif quarters == [4]:
            result.loc[mask & (result["quarter"] == 4), f"{field}_single"] = group[group["quarter"] == 4][
                field
            ].iloc[0]
        elif len(quarters) == 1 and quarters[0] in [2, 3]:
            q = quarters[0]
            result.loc[mask & (result["quarter"] == q), f"{field}_single"] = group[group["quarter"] == q][
                field
            ].iloc[0]
        elif quarters == [1, 2]:
            q1 = float(group[group["quarter"] == 1][field].iloc[0] or 0)
            q2 = float(group[group["quarter"] == 2][field].iloc[0] or 0)
            result.loc[mask & (result["quarter"] == 1), f"{field}_single"] = q1
            result.loc[mask & (result["quarter"] == 2), f"{field}_single"] = q2 - q1
        elif quarters == [1, 4]:
            q1 = float(group[group["quarter"] == 1][field].iloc[0] or 0)
            q4 = float(group[group["quarter"] == 4][field].iloc[0] or 0)
            result.loc[mask & (result["quarter"] == 1), f"{field}_single"] = q1
            result.loc[mask & (result["quarter"] == 4), f"{field}_single"] = q4 - q1
        elif quarters == [2, 4]:
            q2 = float(group[group["quarter"] == 2][field].iloc[0] or 0)
            q4 = float(group[group["quarter"] == 4][field].iloc[0] or 0)
            result.loc[mask & (result["quarter"] == 2), f"{field}_single"] = q2
            result.loc[mask & (result["quarter"] == 4), f"{field}_single"] = q4 - q2
        elif quarters == [3, 4]:
            q3 = float(group[group["quarter"] == 3][field].iloc[0] or 0)
            q4 = float(group[group["quarter"] == 4][field].iloc[0] or 0)
            result.loc[mask & (result["quarter"] == 3), f"{field}_single"] = q3
            result.loc[mask & (result["quarter"] == 4), f"{field}_single"] = q4 - q3
        elif len(quarters) >= 3:
            prev = 0.0
            for q in quarters:
                cur = float(group[group["quarter"] == q][field].iloc[0] or 0)
                single = cur if q == 1 else cur - prev
                result.loc[mask & (result["quarter"] == q), f"{field}_single"] = single
                prev = cur
        else:
            for q in quarters:
                val = group[group["quarter"] == q][field].iloc[0]
                result.loc[mask & (result["quarter"] == q), f"{field}_single"] = val

    def _compute_forecast_single_quarter(self, forecast_df: pd.DataFrame, report_df: pd.DataFrame) -> pd.DataFrame:
        """根据预告中值与已入库 report 单季值，计算 forecast 单季归母净利润。"""
        if forecast_df is None or forecast_df.empty:
            return forecast_df

        result = forecast_df.copy()
        if report_df is not None and not report_df.empty:
            r = report_df.sort_values(["ts_code", "year", "quarter"])  # type: ignore
            r["n_income_attr_p_single"] = pd.to_numeric(
                r["n_income_attr_p_single"], errors="coerce"
            ).fillna(0.0)
            r["ytd_sum_before"] = (
                r.groupby(["ts_code", "year"])  # type: ignore
                ["n_income_attr_p_single"].cumsum()
                - r["n_income_attr_p_single"]
            )
        else:
            r = pd.DataFrame(columns=["ts_code", "year", "quarter", "ytd_sum_before"])  # type: ignore

        merged = result.merge(
            r[["ts_code", "year", "quarter", "ytd_sum_before"]],
            on=["ts_code", "year", "quarter"],
            how="left",
        )
        merged["ytd_sum_before"] = merged["ytd_sum_before"].fillna(0.0)
        merged["net_profit_mid"] = pd.to_numeric(merged["net_profit_mid"], errors="coerce").fillna(0.0)
        merged["ytd_sum_before"] = pd.to_numeric(merged["ytd_sum_before"], errors="coerce").fillna(0.0)
        merged["n_income_attr_p_single"] = merged.apply(
            lambda row: float(row["net_profit_mid"]) if row["quarter"] == 1 else float(row["net_profit_mid"]) - float(row["ytd_sum_before"]),
            axis=1,
        )
        return merged


