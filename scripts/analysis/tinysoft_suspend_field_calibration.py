#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TinySoft 停复牌字段口径校准脚本（C1.1）

目标：
1. 对比多个 TinySoft 字段表达式在“停复牌事件”上的命中效果
2. 输出非空率、事件关键词命中率、符号覆盖率与推荐表达式
3. 生成 Markdown 报告，供任务默认配置收敛使用

示例：
python scripts/analysis/tinysoft_suspend_field_calibration.py ^
  --start-date 20240101 --end-date 20260302 ^
  --sample-size 40 ^
  --candidates "spec(),spec,Spec(),停复牌,isstop(),status()" ^
  --output docs/tasks/tinysoft_suspend_field_calibration_20260303.md
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
import random
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from alphahome.common.config_manager import get_database_url, get_tinysoft_config
from alphahome.common.db_manager import DBManager
from alphahome.fetchers.sources.tinysoft.tinysoft_api import TinySoftAPI
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import ts_code_to_tinysoft_symbol


DEFAULT_CANDIDATES = [
    "spec()",
    "spec",
    "Spec()",
    "Spec",
    "停复牌",
    "停牌",
    "复牌",
    "isstop()",
    "status()",
]

DEFAULT_KEYWORDS = [
    "停牌",
    "复牌",
    "临停",
    "停市",
    "恢复交易",
]


@dataclass
class CandidateStats:
    expression: str
    service: str = "__default__"
    viewpoint: str = "__none__"
    symbols_total: int = 0
    symbols_with_data: int = 0
    symbols_with_non_empty: int = 0
    symbols_with_keyword_hit: int = 0
    total_rows: int = 0
    non_empty_rows: int = 0
    keyword_hit_rows: int = 0
    query_errors: int = 0
    sample_values: Counter = field(default_factory=Counter)

    @property
    def non_empty_ratio(self) -> float:
        if self.total_rows <= 0:
            return 0.0
        return self.non_empty_rows / self.total_rows

    @property
    def keyword_hit_ratio(self) -> float:
        if self.non_empty_rows <= 0:
            return 0.0
        return self.keyword_hit_rows / self.non_empty_rows

    @property
    def non_empty_symbol_coverage(self) -> float:
        if self.symbols_total <= 0:
            return 0.0
        return self.symbols_with_non_empty / self.symbols_total

    @property
    def keyword_symbol_coverage(self) -> float:
        if self.symbols_total <= 0:
            return 0.0
        return self.symbols_with_keyword_hit / self.symbols_total

    @property
    def combo_key(self) -> str:
        return f"{self.expression} | service={self.service} | viewpoint={self.viewpoint}"


def _parse_csv_list(raw: str) -> List[str]:
    values = [x.strip() for x in str(raw or "").split(",")]
    return [x for x in values if x]


def _is_non_empty_event(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    text = str(value).strip()
    if not text:
        return False
    if text.lower() in {"none", "nan", "null"}:
        return False
    return True


def _contains_keywords(text: str, keywords: Iterable[str]) -> bool:
    raw = str(text or "")
    return any(k in raw for k in keywords)


def _resolve_event_column(df: pd.DataFrame, expression: str) -> Optional[str]:
    if expression in df.columns:
        return expression
    if len(df.columns) >= 3:
        return str(df.columns[2])
    return None


def _format_dt(value: str) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


async def _load_symbols(
    db: DBManager,
    sample_size: int,
    sample_mode: str,
    seed: int,
) -> List[str]:
    rows = await db.fetch(
        """
        SELECT ts_code
        FROM tushare.stock_basic
        WHERE list_status = 'L'
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


async def _probe_candidate(
    api: TinySoftAPI,
    symbols: List[str],
    expression: str,
    service: str,
    viewpoint: Optional[Any],
    viewpoint_label: str,
    begin_time: str,
    end_time: str,
    keywords: List[str],
    timeout_ms: int,
) -> CandidateStats:
    stats = CandidateStats(
        expression=expression,
        service=service if service else "__default__",
        viewpoint=viewpoint_label,
        symbols_total=len(symbols),
    )
    for ts_code in symbols:
        tinysoft_symbol = ts_code_to_tinysoft_symbol(ts_code)
        try:
            df = await api.query(
                stock=tinysoft_symbol,
                cycle="日线",
                begin_time=begin_time,
                end_time=end_time,
                fields=["date", "StockID", expression],
                timeout_ms=timeout_ms,
                service=(service or ""),
                viewpoint=viewpoint,
            )
        except Exception:
            stats.query_errors += 1
            continue

        if df is None or df.empty:
            continue
        stats.symbols_with_data += 1
        stats.total_rows += len(df)

        event_col = _resolve_event_column(df, expression)
        if not event_col or event_col not in df.columns:
            continue

        series = df[event_col]
        non_empty = series[series.map(_is_non_empty_event)]
        if non_empty.empty:
            continue

        stats.symbols_with_non_empty += 1
        non_empty_text = non_empty.astype(str).str.strip()
        stats.non_empty_rows += len(non_empty_text)

        keyword_mask = non_empty_text.map(lambda x: _contains_keywords(x, keywords))
        keyword_hits = int(keyword_mask.sum())
        if keyword_hits > 0:
            stats.symbols_with_keyword_hit += 1
        stats.keyword_hit_rows += keyword_hits

        for value in non_empty_text.head(20):
            stats.sample_values[str(value)] += 1
    return stats


def _choose_recommended(stats_list: List[CandidateStats], expression_order: List[str]) -> Optional[CandidateStats]:
    if not stats_list:
        return None
    expr_rank = {expr: i for i, expr in enumerate(expression_order)}
    # 先按“能命中停复牌关键词”的能力排序，再看非空覆盖和稳定性
    ranked = sorted(
        stats_list,
        key=lambda s: (
            s.symbols_with_keyword_hit,
            s.keyword_hit_rows,
            s.symbols_with_non_empty,
            s.non_empty_rows,
            -s.query_errors,
            -expr_rank.get(s.expression, 10_000),
            1 if s.service == "__default__" else 0,
            1 if s.viewpoint == "__none__" else 0,
        ),
        reverse=True,
    )
    return ranked[0]


def _build_markdown_report(
    *,
    start_date: str,
    end_date: str,
    sample_mode: str,
    sample_size: int,
    symbols_count: int,
    candidates: List[str],
    services: List[str],
    viewpoints: List[str],
    keywords: List[str],
    stats_list: List[CandidateStats],
    recommended: Optional[CandidateStats],
) -> str:
    lines: List[str] = []
    lines.append(f"# TinySoft 停复牌字段口径校准报告（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")
    lines.append("")
    lines.append("## 1. 运行参数")
    lines.append(f"- 区间：`{start_date}` ~ `{end_date}`")
    lines.append(f"- 样本模式：`{sample_mode}`")
    lines.append(f"- 样本数量（请求）：`{sample_size}`")
    lines.append(f"- 样本数量（实际）：`{symbols_count}`")
    lines.append(f"- 候选表达式：`{', '.join(candidates)}`")
    lines.append(f"- 服务参数候选：`{', '.join(services)}`")
    lines.append(f"- viewpoint 候选：`{', '.join(viewpoints)}`")
    lines.append(f"- 事件关键词：`{', '.join(keywords)}`")
    lines.append("")
    lines.append("## 2. 候选表达式对比")
    lines.append("")
    lines.append("| 表达式 | service | viewpoint | 总行数 | 非空行数 | 非空率 | 关键词命中行数 | 命中率(相对非空) | 非空股票覆盖率 | 关键词股票覆盖率 | 查询错误数 |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for s in stats_list:
        lines.append(
            "| {expr} | {service} | {viewpoint} | {total} | {non_empty} | {non_empty_ratio:.4f} | {kw_rows} | {kw_ratio:.4f} | {sym_cov:.4f} | {kw_sym_cov:.4f} | {errors} |".format(
                expr=s.expression,
                service=s.service,
                viewpoint=s.viewpoint,
                total=s.total_rows,
                non_empty=s.non_empty_rows,
                non_empty_ratio=s.non_empty_ratio,
                kw_rows=s.keyword_hit_rows,
                kw_ratio=s.keyword_hit_ratio,
                sym_cov=s.non_empty_symbol_coverage,
                kw_sym_cov=s.keyword_symbol_coverage,
                errors=s.query_errors,
            )
        )

    lines.append("")
    lines.append("## 3. 推荐结果")
    if recommended is None:
        lines.append("- 无可用候选结果。")
    else:
        lines.append(f"- 推荐默认表达式：`{recommended.expression}`")
        lines.append(f"- 推荐 service：`{recommended.service}`")
        lines.append(f"- 推荐 viewpoint：`{recommended.viewpoint}`")
        lines.append(
            "- 推荐依据：关键词命中股票覆盖率=`{:.4f}`，关键词命中行数=`{}`，非空股票覆盖率=`{:.4f}`。".format(
                recommended.keyword_symbol_coverage,
                recommended.keyword_hit_rows,
                recommended.non_empty_symbol_coverage,
            )
        )
        top_values = recommended.sample_values.most_common(10)
        lines.append("- 该表达式样本值 Top10：")
        if top_values:
            for value, count in top_values:
                lines.append(f"  - `{value}`: {count}")
        else:
            lines.append("  - 无非空样本值。")

    lines.append("")
    lines.append("## 4. 结论提示")
    lines.append("- 若所有候选表达式关键词命中均为 0，说明当前 query 口径不足以直接识别停复牌事件。")
    lines.append("- 下一步建议：验证 TinySoft 帮助文档中的专用函数/服务参数，并做二次校准。")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TinySoft 停复牌字段口径校准脚本")
    parser.add_argument("--start-date", default="20240101", help="开始日期，格式 YYYYMMDD")
    parser.add_argument(
        "--end-date",
        default=datetime.now().strftime("%Y%m%d"),
        help="结束日期，格式 YYYYMMDD",
    )
    parser.add_argument("--sample-size", type=int, default=40, help="采样股票数")
    parser.add_argument(
        "--sample-mode",
        choices=["first", "random"],
        default="first",
        help="采样方式：first=顺序前N只；random=随机N只",
    )
    parser.add_argument("--seed", type=int, default=20260303, help="随机采样种子")
    parser.add_argument(
        "--candidates",
        default=",".join(DEFAULT_CANDIDATES),
        help="候选表达式，逗号分隔",
    )
    parser.add_argument(
        "--keywords",
        default=",".join(DEFAULT_KEYWORDS),
        help="事件关键词，逗号分隔",
    )
    parser.add_argument(
        "--services",
        default="__default__",
        help="service 候选，逗号分隔；__default__ 表示使用配置默认 service",
    )
    parser.add_argument(
        "--viewpoints",
        default="__none__",
        help="viewpoint 候选，逗号分隔；__none__ 表示不传 viewpoint 参数",
    )
    parser.add_argument("--timeout-ms", type=int, default=45000, help="单次查询超时（毫秒）")
    parser.add_argument(
        "--output",
        default=f"docs/tasks/tinysoft_suspend_field_calibration_{datetime.now().strftime('%Y%m%d')}.md",
        help="Markdown 报告输出路径",
    )
    return parser.parse_args()


async def main(args: argparse.Namespace) -> int:
    start_date = _format_dt(args.start_date)
    end_date = _format_dt(args.end_date)
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        raise ValueError(f"start_date({start_date}) 不能晚于 end_date({end_date})")

    candidates = _parse_csv_list(args.candidates)
    keywords = _parse_csv_list(args.keywords)
    services = _parse_csv_list(args.services)
    viewpoints = _parse_csv_list(args.viewpoints)
    if not candidates:
        raise ValueError("candidates 不能为空")
    if not keywords:
        raise ValueError("keywords 不能为空")
    if not services:
        services = ["__default__"]
    if not viewpoints:
        viewpoints = ["__none__"]

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
        stats_list: List[CandidateStats] = []
        for expr in candidates:
            for service_token in services:
                service = "" if service_token == "__default__" else service_token
                for viewpoint_token in viewpoints:
                    viewpoint: Optional[Any] = None
                    if viewpoint_token != "__none__":
                        if str(viewpoint_token).strip().lstrip("-").isdigit():
                            viewpoint = int(viewpoint_token)
                        else:
                            viewpoint = viewpoint_token
                    print(
                        f"Probing candidate: {expr} | service={service_token} | viewpoint={viewpoint_token}"
                    )
                    stats = await _probe_candidate(
                        api=api,
                        symbols=symbols,
                        expression=expr,
                        service=service,
                        viewpoint=viewpoint,
                        viewpoint_label=viewpoint_token,
                        begin_time=begin_time,
                        end_time=end_time,
                        keywords=keywords,
                        timeout_ms=int(args.timeout_ms),
                    )
                    stats_list.append(stats)

        recommended = _choose_recommended(stats_list, expression_order=candidates)
        report = _build_markdown_report(
            start_date=start_date,
            end_date=end_date,
            sample_mode=args.sample_mode,
            sample_size=int(args.sample_size),
            symbols_count=len(symbols),
            candidates=candidates,
            services=services,
            viewpoints=viewpoints,
            keywords=keywords,
            stats_list=stats_list,
            recommended=recommended,
        )
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)

        print(f"Report written: {args.output}")
        if recommended is not None:
            print(f"Recommended expression: {recommended.expression}")
        else:
            print("Recommended expression: None")
        return 0
    finally:
        await api.logout()
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(parse_args())))
