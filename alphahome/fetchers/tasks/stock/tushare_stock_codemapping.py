#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
独立任务：tushare_stock_codemapping（不修改 stock_basic）

!!! 废弃说明 !!!
该任务已被标记为废弃，不再推荐在生产环境中使用；如需代码映射能力，请评估最新任务体系或与数据团队确认替代方案。

目的：
- 基于现有 `tushare.stock_basic` 快照数据与 `tushare.stock_daily` 交易日数据，
  推断新旧代码之间的对应关系（旧→新），并写入 `tushare.stock_code_mapping`。

说明：
- 不修改 `tushare_stock_basic.py`，避免增加其复杂度。
- 可独立运行；无需添加到 __init__.py。使用时显式 import 即可触发 @task_register 注册，
  或直接实例化任务类运行。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Import BaseTask at module level to avoid circular imports
try:
    from alphahome.common.task_system.base_task import BaseTask
    from alphahome.common.task_system.task_decorator import task_register
except ImportError:
    # If full alphahome package imports fail, we'll handle it in main
    BaseTask = None
    task_register = None


def _normalize_name(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


@task_register()
class TushareStockCodeMappingTask(BaseTask):
    """从 stock_basic 与 stock_daily 推断代码映射并入库的独立任务。"""

    # 基础元数据
    task_type: str = "processor"
    name: str = "tushare_stock_codemapping"

    # 目标表（映射表）
    table_name: str = "stock_code_mapping"
    data_source: str = "tushare"
    domain: str = "stock"

    # 主键与结构
    primary_keys = ["ts_code_new", "ts_code_old"]
    auto_add_update_time: bool = True

    # 目标表结构与索引
    schema_def = {
        "ts_code_new": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "symbol_new": {"type": "VARCHAR(15)"},
        "ts_code_old": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "symbol_old": {"type": "VARCHAR(15)"},
        "exchange": {"type": "VARCHAR(10)"},
        "fullname": {"type": "VARCHAR(255)"},
        "name_new": {"type": "VARCHAR(100)"},
        "name_old": {"type": "VARCHAR(100)"},
        "list_date": {"type": "DATE"},
        "switch_date": {"type": "DATE", "constraints": "NOT NULL"},
        "reason": {"type": "VARCHAR(100)"},
        # update_time 会自动添加
    }

    indexes = [
        {"name": "idx_scm_old", "columns": "ts_code_old"},
        {"name": "idx_scm_new", "columns": "ts_code_new"},
        {"name": "idx_scm_switch", "columns": "switch_date"},
    ]

    # 可配置项的默认值
    DEFAULT_HINT: str = "2025-09-29"
    DEFAULT_WINDOW_DAYS: int = 10
    DEFAULT_REASON: str = "exchange-wide code renumbering"

    # 源表常量
    _BASIC_FULL: str = '"tushare"."stock_basic"'
    _DAILY_FULL: str = '"tushare"."stock_daily"'

    async def _fetch_data(self, stop_event: Optional[Any] = None, **kwargs) -> Optional[pd.DataFrame]:
        """拉取 stock_basic 快照，自连匹配候选对，再基于 stock_daily 推断 switch_date 并产出映射DF。"""
        # 读取配置
        cfg: Dict[str, Any] = getattr(self, "task_config", {}) or {}
        code_switch_date_cfg: Optional[str] = cfg.get("code_switch_date")
        hint_str: str = cfg.get("code_switch_date_hint", self.DEFAULT_HINT)
        window_days: int = int(cfg.get("switch_date_window_days", self.DEFAULT_WINDOW_DAYS))
        mapping_reason: str = cfg.get("mapping_reason", self.DEFAULT_REASON)
        exchange_filter: Optional[str] = cfg.get("exchange_filter")

        # 1) 读取 stock_basic 快照
        basic_cols = [
            "ts_code",
            "symbol",
            "exchange",
            "fullname",
            "name",
            "list_date",
            "update_time",
        ]
        where_clause = ""
        params: List[Any] = []
        if exchange_filter:
            where_clause = " WHERE exchange = $1"
            params.append(exchange_filter)

        query_basic = f"SELECT {', '.join(basic_cols)} FROM {self._BASIC_FULL}{where_clause}"
        records = await self.db.fetch(query_basic, *params)
        if not records:
            self.logger.info("stock_basic 表为空，跳过映射推断。")
            return pd.DataFrame()

        df = pd.DataFrame([dict(r) for r in records])

        # 统一列类型
        for c in ["ts_code", "symbol", "exchange", "fullname", "name"]:
            if c in df.columns:
                df[c] = df[c].astype(str)
        # list_date/ update_time 规范化
        if "list_date" in df.columns:
            df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").dt.date
        if "update_time" in df.columns:
            df["update_time"] = pd.to_datetime(df["update_time"], errors="coerce")

        # 名称规范化列
        df["_norm_name"] = df["name"].map(_normalize_name)

        # 2) 自连接构造候选对（同 exchange & list_date；名称匹配；代码不同）
        merged = df.merge(
            df,
            on=["exchange", "list_date"],
            suffixes=("_a", "_b"),
            how="inner",
        )
        # 名称匹配：fullname 完全相等或 规范化 name 完全相等
        name_match = (
            (merged["fullname_a"].notna() & merged["fullname_b"].notna() & (merged["fullname_a"] == merged["fullname_b"]))
            | (merged["_norm_name_a"] == merged["_norm_name_b"])  # type: ignore
        )
        code_changed = (merged["ts_code_a"] != merged["ts_code_b"]) | (merged["symbol_a"] != merged["symbol_b"])  # type: ignore
        candidates = merged.loc[name_match & code_changed].copy()

        if candidates.empty:
            self.logger.info("未发现潜在的新旧代码配对候选，结束。")
            return pd.DataFrame()

        # 3) 方向判定：优先 code_switch_date；否则按 update_time 较大为新
        # 先准备 hint 日期与窗口范围
        def _to_date(s: Optional[str]) -> Optional[date]:
            if not s:
                return None
            try:
                return pd.to_datetime(s).date()
            except Exception:
                return None

        cfg_switch_date: Optional[date] = _to_date(code_switch_date_cfg)
        hint_date: date = _to_date(hint_str) or date(2025, 9, 29)
        begin_date: date = hint_date - timedelta(days=window_days)
        end_date: date = hint_date + timedelta(days=window_days)

        # 方向选择函数
        def choose_orientation(row) -> Tuple[str, str, str, str]:
            ts_a, sym_a, ut_a = row["ts_code_a"], row["symbol_a"], row["update_time_a"]
            ts_b, sym_b, ut_b = row["ts_code_b"], row["symbol_b"], row["update_time_b"]

            # 先用配置的 code_switch_date 判定
            if cfg_switch_date is not None:
                ua = ut_a.date() if pd.notna(ut_a) else None
                ub = ut_b.date() if pd.notna(ut_b) else None
                a_is_new = ua is not None and ua >= cfg_switch_date
                b_is_new = ub is not None and ub >= cfg_switch_date
                if a_is_new != b_is_new:
                    # 只有一方满足，则其为新
                    if a_is_new:
                        return ts_a, sym_a, ts_b, sym_b
                    else:
                        return ts_b, sym_b, ts_a, sym_a
                # 若都满足或都不满足，则继续按更新时间大小判定

            # 回退：按 update_time 大小判定
            ut_a_val = pd.to_datetime(ut_a) if pd.notna(ut_a) else pd.Timestamp.min
            ut_b_val = pd.to_datetime(ut_b) if pd.notna(ut_b) else pd.Timestamp.min
            if ut_a_val >= ut_b_val:
                return ts_a, sym_a, ts_b, sym_b
            else:
                return ts_b, sym_b, ts_a, sym_a

        # 应用方向判定
        orient = candidates.apply(choose_orientation, axis=1, result_type="expand")
        orient.columns = ["ts_code_new", "symbol_new", "ts_code_old", "symbol_old"]

        # 组装基础映射DF（名称需与方向一致）
        new_is_a = orient["ts_code_new"].astype(str) == candidates["ts_code_a"].astype(str)
        name_new_series = candidates["name_a"].where(new_is_a, candidates["name_b"]).astype(str)
        name_old_series = candidates["name_b"].where(new_is_a, candidates["name_a"]).astype(str)

        mapping_df = pd.DataFrame({
            "ts_code_new": orient["ts_code_new"],
            "symbol_new": orient["symbol_new"],
            "ts_code_old": orient["ts_code_old"],
            "symbol_old": orient["symbol_old"],
            "exchange": candidates["exchange"],
            "fullname": candidates["fullname_a"],  # 两侧相等
            "name_new": name_new_series,
            "name_old": name_old_series,
            "list_date": candidates["list_date"],
        })

        # 去除自配对/重复方向（同一对可能出现两次）
        mapping_df = mapping_df[mapping_df["ts_code_new"] != mapping_df["ts_code_old"]]
        mapping_df.drop_duplicates(subset=["ts_code_new", "ts_code_old"], keep="last", inplace=True)

        if mapping_df.empty:
            self.logger.info("方向判定后无有效配对，结束。")
            return mapping_df

        # 4) 基于 stock_daily 推断 switch_date（共同交易日）
        # 先构建涉及的全部代码集合
        all_codes = sorted(set(mapping_df["ts_code_new"]).union(set(mapping_df["ts_code_old"])))

        # 查询窗口内的交易日存在性
        # 使用 ANY($3::text[]) 绑定数组参数
        daily_sql = (
            f"SELECT ts_code, trade_date FROM {self._DAILY_FULL} "
            f"WHERE trade_date BETWEEN $1 AND $2 AND ts_code = ANY($3::text[])"
        )
        daily_records = await self.db.fetch(daily_sql, begin_date, end_date, all_codes)
        daily_df = pd.DataFrame([dict(r) for r in daily_records]) if daily_records else pd.DataFrame(columns=["ts_code", "trade_date"]) 
        if not daily_df.empty:
            daily_df["trade_date"] = pd.to_datetime(daily_df["trade_date"], errors="coerce").dt.date

        # 构建每个ts_code的交易日集合
        code_to_days: Dict[str, set] = {}
        if not daily_df.empty:
            for ts, grp in daily_df.groupby("ts_code"):
                code_to_days[ts] = set(grp["trade_date"].tolist())

        # 对每一对进行 switch_date 推断
        switch_dates: List[date] = []
        for _, row in mapping_df.iterrows():
            if cfg_switch_date is not None:
                switch_dates.append(cfg_switch_date)
                continue

            days_old = code_to_days.get(row["ts_code_old"], set())
            days_new = code_to_days.get(row["ts_code_new"], set())

            # 共同交易日优先使用 hint 当天
            if hint_date in days_old and hint_date in days_new:
                switch_dates.append(hint_date)
                continue

            # 否则在窗口内寻找最早共同交易日
            common_days = sorted(list(days_old.intersection(days_new)))
            if common_days:
                switch_dates.append(common_days[0])
                continue

            # 回退：使用“新记录的 update_time 的日期部分”
            # 从 candidates 中查回对应新代码的 update_time_a / b
            # 为简单起见，使用 hint_date 作为最后保底（避免空值），再尽量从 df 中找新代码的最新更新时间
            new_code = row["ts_code_new"]
            new_ut = df.loc[df["ts_code"] == new_code, "update_time"].max()
            if pd.notna(new_ut):
                switch_dates.append(pd.to_datetime(new_ut).date())
            else:
                switch_dates.append(hint_date)

        mapping_df["switch_date"] = switch_dates
        mapping_df["reason"] = mapping_reason

        return mapping_df

    def process_data(self, data: pd.DataFrame, stop_event: Optional[Any] = None, **kwargs) -> pd.DataFrame:
        """轻量规范化与日志统计。"""
        if data is None or data.empty:
            self.logger.info("没有生成任何映射数据。")
            return pd.DataFrame()

        # 规范化类型
        for c in [
            "ts_code_new",
            "symbol_new",
            "ts_code_old",
            "symbol_old",
            "exchange",
            "fullname",
            "name_new",
            "name_old",
            "reason",
        ]:
            if c in data.columns:
                data[c] = data[c].astype(str)

        if "list_date" in data.columns:
            data["list_date"] = pd.to_datetime(data["list_date"], errors="coerce").dt.date
        if "switch_date" in data.columns:
            data["switch_date"] = pd.to_datetime(data["switch_date"], errors="coerce").dt.date

        before = len(data)
        data.drop_duplicates(subset=["ts_code_new", "ts_code_old"], keep="last", inplace=True)
        after = len(data)
        if before != after:
            self.logger.info(f"去重：移除 {before - after} 条重复映射记录。")

        self.logger.info(f"映射生成完成：共 {after} 条。")
        return data


if __name__ == "__main__":
    """
    直接运行此脚本，执行代码映射任务。

    使用方式B：直接实例化（完全独立）

    示例配置：
    - code_switch_date: 显式指定切换日期（最高优先级）
    - code_switch_date_hint: 启发式锚点日期（用于从 stock_daily 推断共同交易日，默认 2025-09-29）
    - switch_date_window_days: 在 hint 周围搜索共同交易日的窗口大小（默认 10 天）
    - mapping_reason: 映射原因描述
    - exchange_filter: 可选，仅处理特定交易所（如 'BJ'）

    运行命令：
        python alphahome/fetchers/tasks/stock/tushare_stock_codemapping.py
    """

    import asyncio
    import os
    import sys
    # Add the alphahome directory to Python path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

    # Import only what we need to avoid triggering full package imports
    from alphahome.common.db_manager import DBManager
    from alphahome.common.config_manager import get_database_url

    async def main():
        # 获取数据库连接字符串
        db_url = get_database_url()
        if not db_url:
            print("错误：无法获取数据库连接字符串，请检查配置文件。")
            return

        # 初始化数据库管理器
        db = DBManager(db_url, mode="async")
        await db.connect()

        try:
            # 创建任务实例
            task = TushareStockCodeMappingTask(
                db,
                task_config={
                    # 若你已知切换日，可显式指定（最高优先级）
                    # "code_switch_date": "2025-09-29",

                    # 启发式的锚点日期（用于从 tushare.stock_daily 推断共同交易日）
                    "code_switch_date_hint": "2025-09-29",
                    "switch_date_window_days": 10,

                    # 可选：只处理 BJ 交易所
                    # "exchange_filter": "BJ",

                    # 映射原因
                    "mapping_reason": "exchange-wide code renumbering",
                },
            )

            print("开始执行 tushare_stock_codemapping 任务...")
            result = await task.execute()
            print(f"任务执行完成，结果: {result}")

        except Exception as e:
            print(f"任务执行失败: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await db.close()

    # 运行异步主函数
    asyncio.run(main())
