#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
财务源表版本/重复特征审计（read-only）
=================================

用途：
- 快速检查 rawdata 财务表是否存在“多版本/同日重复”现象
- 为 PIT 口径（ann_date vs f_ann_date）与去重策略提供数据依据

覆盖表：
  rawdata.fina_indicator
  rawdata.fina_income
  rawdata.fina_balancesheet
  rawdata.fina_cashflow
  rawdata.fina_express
  rawdata.fina_forecast

用法：
  python scripts/analysis/audit_financial_table_versions.py
  python scripts/analysis/audit_financial_table_versions.py --database-url postgresql://...
  python scripts/analysis/audit_financial_table_versions.py --sample 10
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "Missing dependency psycopg2-binary. Install with `pip install psycopg2-binary`."
    ) from e

# 将项目根目录加入路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from alphahome.common.config_manager import get_database_url  # noqa: E402


@dataclass(frozen=True)
class TableSpec:
    schema: str
    table: str
    has_f_ann_date: bool = False

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.table}"


TABLES: Sequence[TableSpec] = (
    TableSpec("rawdata", "fina_indicator"),
    TableSpec("rawdata", "fina_income", has_f_ann_date=True),
    TableSpec("rawdata", "fina_balancesheet", has_f_ann_date=True),
    TableSpec("rawdata", "fina_cashflow", has_f_ann_date=True),
    TableSpec("rawdata", "fina_express"),
    TableSpec("rawdata", "fina_forecast"),
)


def _q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_type(cur, schema: str, table: str) -> str:
    cur.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    return str(row["table_type"]) if row else "UNKNOWN"


def _count_rows(cur, full: str) -> int:
    cur.execute(f"SELECT COUNT(*) AS n FROM {full}")
    return int(cur.fetchone()["n"])


def _dup_stats(cur, full: str, cols: Sequence[str]) -> tuple[int, int]:
    """
    返回 (dup_groups, max_dup)，其中 dup_groups 为 cnt>1 的 group 数。
    """
    group_cols = ", ".join(cols)
    where_cols = " AND ".join([f"{c} IS NOT NULL" for c in cols])
    cur.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE cnt > 1) AS dup_groups,
            COALESCE(MAX(cnt), 0) AS max_dup
        FROM (
            SELECT {group_cols}, COUNT(*) AS cnt
            FROM {full}
            WHERE {where_cols}
            GROUP BY {group_cols}
        ) g
        """
    )
    r = cur.fetchone()
    return int(r["dup_groups"]), int(r["max_dup"])


def _sample_ann_date_dups(cur, full: str, limit_groups: int) -> list[dict]:
    """
    抽样展示同 (ts_code,end_date,ann_date) 下存在多条的情况。
    """
    cur.execute(
        f"""
        WITH d AS (
            SELECT ts_code, end_date, ann_date, COUNT(*) AS cnt
            FROM {full}
            WHERE ts_code IS NOT NULL AND end_date IS NOT NULL AND ann_date IS NOT NULL
            GROUP BY ts_code, end_date, ann_date
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, ts_code, end_date, ann_date
            LIMIT %s
        )
        SELECT
            t.ts_code,
            t.end_date,
            t.ann_date,
            t.f_ann_date,
            t.update_time,
            t.report_type,
            t.comp_type
        FROM {full} t
        JOIN d USING (ts_code, end_date, ann_date)
        ORDER BY t.ts_code, t.end_date, t.ann_date, t.f_ann_date
        """,
        (limit_groups,),
    )
    return list(cur.fetchall())


def _print_table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    widths = [len(h) for h in headers]
    rows_list = [list(r) for r in rows]
    for r in rows_list:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))

    def fmt_row(r: Sequence[object]) -> str:
        return " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(r))

    print(fmt_row(headers))
    print("-+-".join("-" * w for w in widths))
    for r in rows_list:
        print(fmt_row(r))


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit rawdata finance table versions/dups (read-only)")
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="PostgreSQL URL; default: config.json database.url or env DATABASE_URL",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=5,
        help="Number of duplicated (ts_code,end_date,ann_date) groups to sample for income/balance/cashflow",
    )
    args = parser.parse_args()

    db_url = args.database_url or get_database_url() or os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: missing database url (config.json database.url or env DATABASE_URL).")
        return 2

    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        print("== Table Types ==")
        for t in TABLES:
            ttype = _table_type(cur, t.schema, t.table)
            print(f"- {t.full_name}: {ttype}")

        print("\n== Duplicate Stats ==")
        rows = []
        for t in TABLES:
            full = t.full_name
            total = _count_rows(cur, full)
            d1, m1 = _dup_stats(cur, full, ["ts_code", "end_date"])
            d2, m2 = _dup_stats(cur, full, ["ts_code", "end_date", "ann_date"])
            if t.has_f_ann_date:
                d3, m3 = _dup_stats(cur, full, ["ts_code", "end_date", "f_ann_date"])
                fstat = f"{d3}/{m3}"
            else:
                fstat = "-"
            rows.append([full, total, f"{d1}/{m1}", f"{d2}/{m2}", fstat])

        _print_table(
            headers=[
                "table",
                "rows",
                "dup_groups(ts_code,end_date)/max",
                "dup_groups(ts_code,end_date,ann_date)/max",
                "dup_groups(ts_code,end_date,f_ann_date)/max",
            ],
            rows=rows,
        )

        if args.sample > 0:
            print("\n== Samples: duplicated (ts_code,end_date,ann_date) groups ==")
            for t in TABLES:
                if not t.has_f_ann_date:
                    continue
                full = t.full_name
                samples = _sample_ann_date_dups(cur, full, limit_groups=args.sample)
                print(f"\n-- {full} --")
                if not samples:
                    print("(no duplicated groups on ann_date key)")
                    continue
                for r in samples[: min(len(samples), args.sample * 10)]:
                    print(
                        f"{r['ts_code']} {r['end_date']} "
                        f"ann={r['ann_date']} f_ann={r['f_ann_date']} "
                        f"update_time={r['update_time']} "
                        f"report_type={r.get('report_type')} comp_type={r.get('comp_type')}"
                    )

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
