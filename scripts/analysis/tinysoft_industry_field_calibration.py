#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TinySoft 行业字段口径校准脚本（C2）
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
import random
from typing import Any, List, Optional

import pandas as pd

from alphahome.common.config_manager import get_database_url, get_tinysoft_config
from alphahome.common.db_manager import DBManager
from alphahome.fetchers.sources.tinysoft.tinysoft_api import TinySoftAPI
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import ts_code_to_tinysoft_symbol


DEFAULT_CANDIDATES = [
    "getswhy1()",
    "getswhy2()",
    "getswhy3()",
    "getswindexcode()",
    "getbkbydate()",
]


@dataclass
class FieldStats:
    expression: str
    symbols_total: int = 0
    symbols_with_data: int = 0
    symbols_with_non_empty: int = 0
    total_rows: int = 0
    non_empty_rows: int = 0
    distinct_values: set[str] = field(default_factory=set)
    sample_values: Counter = field(default_factory=Counter)
    query_errors: int = 0

    @property
    def non_empty_ratio(self) -> float:
        if self.total_rows <= 0:
            return 0.0
        return self.non_empty_rows / self.total_rows

    @property
    def symbol_coverage(self) -> float:
        if self.symbols_total <= 0:
            return 0.0
        return self.symbols_with_non_empty / self.symbols_total


def _parse_csv_list(raw: str) -> List[str]:
    values = [x.strip() for x in str(raw or "").split(",")]
    return [x for x in values if x]


def _is_non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in {"none", "nan", "null"}


def _format_dt(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


async def _load_symbols(db: DBManager, sample_size: int, sample_mode: str, seed: int) -> List[str]:
    rows = await db.fetch(
        """
        SELECT ts_code
        FROM tushare.stock_basic
        WHERE list_status='L'
          AND ts_code ~ '^[0-9]{6}\\.(SH|SZ|BJ)$'
        ORDER BY ts_code
        """
    )
    codes = [str(r["ts_code"]) for r in rows]
    if not codes:
        return []
    if sample_size > 0 and sample_size < len(codes):
        if sample_mode == "random":
            rng = random.Random(seed)
            codes = sorted(rng.sample(codes, sample_size))
        else:
            codes = codes[:sample_size]
    return codes


async def _probe(
    api: TinySoftAPI,
    symbols: List[str],
    expression: str,
    begin_time: str,
    end_time: str,
    timeout_ms: int,
) -> FieldStats:
    stats = FieldStats(expression=expression, symbols_total=len(symbols))
    for ts_code in symbols:
        stock = ts_code_to_tinysoft_symbol(ts_code)
        try:
            df = await api.query(
                stock=stock,
                cycle="日线",
                begin_time=begin_time,
                end_time=end_time,
                fields=["date", "StockID", expression],
                timeout_ms=timeout_ms,
            )
        except Exception:
            stats.query_errors += 1
            continue

        if df is None or df.empty:
            continue
        stats.symbols_with_data += 1
        stats.total_rows += len(df)

        if expression in df.columns:
            col = expression
        elif len(df.columns) >= 3:
            col = str(df.columns[2])
        else:
            continue

        series = df[col]
        non_empty = series[series.map(_is_non_empty)]
        if non_empty.empty:
            continue
        stats.symbols_with_non_empty += 1
        stats.non_empty_rows += len(non_empty)
        texts = non_empty.astype(str).str.strip()
        stats.distinct_values.update(texts.tolist())
        for v in texts.head(20):
            stats.sample_values[str(v)] += 1
    return stats


def _choose(stats_list: List[FieldStats], candidates: List[str]) -> Optional[FieldStats]:
    if not stats_list:
        return None
    expr_rank = {expr: i for i, expr in enumerate(candidates)}
    ranked = sorted(
        stats_list,
        key=lambda s: (
            s.symbols_with_non_empty,
            s.non_empty_rows,
            len(s.distinct_values),
            -s.query_errors,
            -expr_rank.get(s.expression, 10_000),
        ),
        reverse=True,
    )
    return ranked[0]


def _build_report(
    *,
    start_date: str,
    end_date: str,
    sample_mode: str,
    sample_size: int,
    symbols_count: int,
    candidates: List[str],
    stats_list: List[FieldStats],
    recommended: Optional[FieldStats],
) -> str:
    lines: List[str] = []
    lines.append(f"# TinySoft 行业字段口径校准报告（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")
    lines.append("")
    lines.append("## 1. 运行参数")
    lines.append(f"- 区间：`{start_date}` ~ `{end_date}`")
    lines.append(f"- 样本模式：`{sample_mode}`")
    lines.append(f"- 样本数量（请求）：`{sample_size}`")
    lines.append(f"- 样本数量（实际）：`{symbols_count}`")
    lines.append(f"- 候选表达式：`{', '.join(candidates)}`")
    lines.append("")
    lines.append("## 2. 候选表达式对比")
    lines.append("")
    lines.append("| 表达式 | 总行数 | 非空行数 | 非空率 | 非空股票覆盖率 | 不同值个数 | 查询错误数 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for s in stats_list:
        lines.append(
            "| {expr} | {total} | {non_empty} | {ratio:.4f} | {cov:.4f} | {distinct_count} | {errors} |".format(
                expr=s.expression,
                total=s.total_rows,
                non_empty=s.non_empty_rows,
                ratio=s.non_empty_ratio,
                cov=s.symbol_coverage,
                distinct_count=len(s.distinct_values),
                errors=s.query_errors,
            )
        )
    lines.append("")
    lines.append("## 3. 推荐结果")
    if recommended is None:
        lines.append("- 无可用候选结果。")
    else:
        lines.append(f"- 推荐默认表达式：`{recommended.expression}`")
        lines.append(
            "- 推荐依据：非空股票覆盖率=`{:.4f}`，非空行数=`{}`，不同值个数=`{}`。".format(
                recommended.symbol_coverage,
                recommended.non_empty_rows,
                len(recommended.distinct_values),
            )
        )
        lines.append("- 样本值 Top10：")
        top_values = recommended.sample_values.most_common(10)
        if top_values:
            for value, count in top_values:
                lines.append(f"  - `{value}`: {count}")
        else:
            lines.append("  - 无非空样本值。")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TinySoft 行业字段口径校准脚本")
    parser.add_argument("--start-date", default="20240101", help="开始日期 YYYYMMDD")
    parser.add_argument("--end-date", default=datetime.now().strftime("%Y%m%d"), help="结束日期 YYYYMMDD")
    parser.add_argument("--sample-size", type=int, default=40, help="采样股票数")
    parser.add_argument("--sample-mode", choices=["first", "random"], default="first")
    parser.add_argument("--seed", type=int, default=20260303)
    parser.add_argument("--candidates", default=",".join(DEFAULT_CANDIDATES), help="候选表达式")
    parser.add_argument("--timeout-ms", type=int, default=45000)
    parser.add_argument(
        "--output",
        default=f"docs/tasks/tinysoft_industry_field_calibration_{datetime.now().strftime('%Y%m%d')}.md",
    )
    return parser.parse_args()


async def main(args: argparse.Namespace) -> int:
    start_date = _format_dt(args.start_date)
    end_date = _format_dt(args.end_date)
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        raise ValueError(f"start_date({start_date}) 不能晚于 end_date({end_date})")

    candidates = _parse_csv_list(args.candidates)
    if not candidates:
        raise ValueError("candidates 不能为空")

    db_url = get_database_url()
    if not db_url:
        raise ValueError("未读取到数据库连接串，请检查配置")

    db = DBManager(db_url)
    await db.connect()
    cfg = get_tinysoft_config() or {}
    api = TinySoftAPI(
        user=cfg.get("user"),
        password=cfg.get("password"),
        host=cfg.get("host", TinySoftAPI.DEFAULT_HOST),
        port=int(cfg.get("port", TinySoftAPI.DEFAULT_PORT) or TinySoftAPI.DEFAULT_PORT),
        ini_path=cfg.get("ini_path") or None,
        service=str(cfg.get("service") or ""),
        timeout_ms=int(cfg.get("timeout_ms", args.timeout_ms) or args.timeout_ms),
        request_interval=float(cfg.get("request_interval", 0.2) or 0.2),
    )

    begin_time = f"{start_date} 00:00:00"
    end_time = f"{end_date} 23:59:59"

    try:
        symbols = await _load_symbols(
            db=db,
            sample_size=max(1, int(args.sample_size)),
            sample_mode=args.sample_mode,
            seed=int(args.seed),
        )
        if not symbols:
            raise RuntimeError("未加载到股票列表，无法执行校准")

        print(f"Loaded symbols: {len(symbols)}")
        stats_list: List[FieldStats] = []
        for expr in candidates:
            print(f"Probing candidate: {expr}")
            stats = await _probe(
                api=api,
                symbols=symbols,
                expression=expr,
                begin_time=begin_time,
                end_time=end_time,
                timeout_ms=int(args.timeout_ms),
            )
            stats_list.append(stats)

        recommended = _choose(stats_list, candidates)
        report = _build_report(
            start_date=start_date,
            end_date=end_date,
            sample_mode=args.sample_mode,
            sample_size=int(args.sample_size),
            symbols_count=len(symbols),
            candidates=candidates,
            stats_list=stats_list,
            recommended=recommended,
        )
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report written: {args.output}")
        if recommended:
            print(f"Recommended expression: {recommended.expression}")
        return 0
    finally:
        await api.logout()
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(parse_args())))

