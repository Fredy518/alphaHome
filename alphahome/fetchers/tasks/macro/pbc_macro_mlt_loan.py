#!/usr/bin/env python

"""央行人民币中长期贷款月增量（住户 + 企业）。

旧宏观策略所用的 Wind 序列并不是单独的“企业中长期贷款”，而是金融统计
数据报告中住户与非金融企业两部分中长期贷款的合计。本任务保留两部分的
原始报告值、报告口径和官方发布时间，再按年内报告口径还原月增量。
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ...base.fetcher_task import FetcherTask
from .macro_release_calendar import (
    CalendarRecord,
    HttpClient,
    money_queries,
    resolve_pbc_release,
    strip_tags,
)

_HOUSEHOLD_LABELS = ("住户部门贷款", "住户贷款", "居民户贷款")
_ENTERPRISE_LABELS = (
    "企（事）业单位贷款",
    "企(事)业单位贷款",
    "非金融企业及机关团体贷款",
    "非金融企业及其他部门贷款",
)
_ALL_ENTITY_LABELS = (*_HOUSEHOLD_LABELS, *_ENTERPRISE_LABELS)
_AMOUNT_PATTERN = re.compile(
    r"中长期贷款(?P<direction>增加|减少)"
    r"(?P<amount>[0-9]+(?:\.[0-9]+)?)"
    r"(?P<unit>万亿元|亿元|万亿|亿)"
)
_MONTHLY_LOAN_ANCHOR = re.compile(
    r"(?:当月|本月|(?:[1-9]|1[0-2])月份?)人民币贷款(?:增加|减少)"
)
_YTD_LOAN_ANCHORS = (
    re.compile(r"前[一二三四五六七八九十两0-9]+个?月人民币贷款(?:增加|减少)"),
    re.compile(r"(?:一季度|上半年|前三季度|全年)人民币贷款(?:增加|减少)"),
    re.compile(r"1[—–－\-至][一二三四五六七八九十0-9]+月人民币贷款(?:增加|减少)"),
    re.compile(r"20[0-9]{2}年人民币贷款(?:增加|减少)"),
)
PBC_PUBLICATION_GRACE_DAYS = 45
PBC_KNOWN_UNAVAILABLE_PERIODS = frozenset(
    {
        pd.Timestamp("2011-05-31"),
        pd.Timestamp("2022-04-30"),
    }
)


@dataclass(frozen=True)
class PBCMLTReport:
    period_end_date: pd.Timestamp
    release_date: pd.Timestamp
    release_time: pd.Timestamp | None
    report_basis: str
    household_mlt_reported_100m_cny: float
    enterprise_mlt_reported_100m_cny: float
    source_title: str
    source_url: str
    query_text: str | None
    match_method: str
    search_rank: int | None
    source_hash: str


def _normalized_page_text(html: str) -> str:
    return re.sub(r"\s+", "", strip_tags(html or ""))


def _amount_to_100m(direction: str, amount: str, unit: str) -> float:
    value = float(amount)
    if unit.startswith("万亿"):
        value *= 10000.0
    if direction == "减少":
        value *= -1.0
    return round(value, 4)


def _find_entity_mlt(text: str, labels: tuple[str, ...]) -> tuple[float, int]:
    candidates: list[tuple[int, str]] = []
    for label in labels:
        candidates.extend(
            (match.start(), label) for match in re.finditer(re.escape(label), text)
        )

    for start, label in sorted(candidates):
        value_start = start + len(label)
        segment_end = min(len(text), value_start + 500)
        for boundary_label in (*_ALL_ENTITY_LABELS, "非银行业金融机构贷款"):
            boundary = text.find(boundary_label, value_start)
            if boundary >= 0:
                segment_end = min(segment_end, boundary)
        for punctuation in ("；", "。"):
            boundary = text.find(punctuation, value_start)
            if boundary >= 0:
                segment_end = min(segment_end, boundary)
        match = _AMOUNT_PATTERN.search(text[value_start:segment_end])
        if match is None:
            continue
        return (
            _amount_to_100m(
                match.group("direction"), match.group("amount"), match.group("unit")
            ),
            start,
        )
    raise ValueError(f"未找到{labels[0]}的中长期贷款金额")


def _detect_report_basis(
    text: str, period_end_date: pd.Timestamp, component_position: int
) -> str:
    if period_end_date.month == 1:
        return "monthly"

    # 报告标题通常先给年内累计数，随后再给“X月份”当月数。
    # 分部门的中长期贷款应跟随离它最近的明示口径锚点，不能因为
    # 往前 40 字仍可见“前七个月”就把 7 月单月值误判成累计值。
    prefix = text[:component_position]
    candidates: list[tuple[int, str]] = [
        (match.start(), "monthly") for match in _MONTHLY_LOAN_ANCHOR.finditer(prefix)
    ]
    for pattern in _YTD_LOAN_ANCHORS:
        candidates.extend((match.start(), "ytd") for match in pattern.finditer(prefix))
    if candidates:
        return max(candidates, key=lambda item: item[0])[1]

    # 早期页面偶尔省略完整锚点；没有明示累计描述时按当月值处理。
    return "monthly"


def parse_pbc_mlt_report(
    html: str,
    period_end_date: pd.Timestamp,
    calendar_record: CalendarRecord,
) -> PBCMLTReport:
    """从央行金融统计报告中提取住户和企业中长期贷款。"""

    text = _normalized_page_text(html)
    household, household_position = _find_entity_mlt(text, _HOUSEHOLD_LABELS)
    enterprise, enterprise_position = _find_entity_mlt(text, _ENTERPRISE_LABELS)
    report_basis = _detect_report_basis(
        text,
        pd.Timestamp(period_end_date),
        min(household_position, enterprise_position),
    )
    release_date = pd.Timestamp(calendar_record.release_date).normalize()
    release_time = (
        None
        if not calendar_record.release_time
        else pd.Timestamp(calendar_record.release_time)
    )
    return PBCMLTReport(
        period_end_date=pd.Timestamp(period_end_date).normalize(),
        release_date=release_date,
        release_time=release_time,
        report_basis=report_basis,
        household_mlt_reported_100m_cny=household,
        enterprise_mlt_reported_100m_cny=enterprise,
        source_title=calendar_record.source_title,
        source_url=calendar_record.source_url,
        query_text=calendar_record.query_text,
        match_method=calendar_record.match_method,
        search_rank=calendar_record.search_rank,
        source_hash=hashlib.sha256(html.encode("utf-8")).hexdigest(),
    )


def _empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _is_expected_unresolved_period(
    period_end_date: pd.Timestamp,
    *,
    as_of: pd.Timestamp | None = None,
) -> bool:
    period = pd.Timestamp(period_end_date).normalize()
    if period in PBC_KNOWN_UNAVAILABLE_PERIODS:
        return True
    current_date = (
        pd.Timestamp.now().normalize()
        if as_of is None
        else pd.Timestamp(as_of).normalize()
    )
    return period + pd.Timedelta(days=PBC_PUBLICATION_GRACE_DAYS) >= current_date


def _ensure_safe_normalization(
    frame: pd.DataFrame,
    allowed_historical_gaps: set[pd.Timestamp],
) -> None:
    gap_rows = frame.loc[
        frame["normalization_status"] == "missing_previous_ytd",
        "period_end_date",
    ]
    unexpected: list[pd.Timestamp] = []
    for value in gap_rows:
        period = pd.Timestamp(value).normalize()
        caused_by_allowed_gap = any(
            gap.year == period.year and gap < period for gap in allowed_historical_gaps
        )
        if not caused_by_allowed_gap:
            unexpected.append(period)
    if unexpected:
        periods = ", ".join(str(item.date()) for item in unexpected[:6])
        raise RuntimeError(
            "中长期贷款月增量存在非预期的累计链缺口，拒绝写入: " + periods
        )


def normalize_pbc_mlt_reports(reports: list[PBCMLTReport]) -> pd.DataFrame:
    """把月报/YTD 混合披露统一还原为月增量。"""

    rows: list[dict[str, Any]] = []
    year_state: dict[int, dict[str, Any]] = {}
    for report in sorted(reports, key=lambda item: item.period_end_date):
        year = report.period_end_date.year
        month = report.period_end_date.month
        state = year_state.setdefault(
            year,
            {
                "last_month": None,
                "household_ytd": None,
                "enterprise_ytd": None,
            },
        )
        is_contiguous = state["last_month"] == month - 1

        household_monthly: float | None
        enterprise_monthly: float | None
        household_ytd: float | None
        enterprise_ytd: float | None
        if report.report_basis == "monthly":
            household_monthly = report.household_mlt_reported_100m_cny
            enterprise_monthly = report.enterprise_mlt_reported_100m_cny
            if month == 1:
                household_ytd = household_monthly
                enterprise_ytd = enterprise_monthly
            elif is_contiguous and state["household_ytd"] is not None:
                household_ytd = round(state["household_ytd"] + household_monthly, 4)
                enterprise_ytd = round(state["enterprise_ytd"] + enterprise_monthly, 4)
            else:
                household_ytd = None
                enterprise_ytd = None
            normalization_status = "direct_monthly"
        else:
            household_ytd = report.household_mlt_reported_100m_cny
            enterprise_ytd = report.enterprise_mlt_reported_100m_cny
            if month == 1:
                household_monthly = household_ytd
                enterprise_monthly = enterprise_ytd
                normalization_status = "direct_january_ytd"
            elif is_contiguous and state["household_ytd"] is not None:
                household_monthly = round(household_ytd - state["household_ytd"], 4)
                enterprise_monthly = round(enterprise_ytd - state["enterprise_ytd"], 4)
                normalization_status = "derived_from_ytd"
            else:
                household_monthly = None
                enterprise_monthly = None
                normalization_status = "missing_previous_ytd"

        total_monthly = (
            None
            if household_monthly is None or enterprise_monthly is None
            else round(household_monthly + enterprise_monthly, 4)
        )
        rows.append(
            {
                "period_end_date": report.period_end_date.date(),
                "release_date": report.release_date.date(),
                "release_time": (
                    None
                    if report.release_time is None
                    else report.release_time.to_pydatetime()
                ),
                "report_basis": report.report_basis,
                "household_mlt_reported_100m_cny": report.household_mlt_reported_100m_cny,
                "enterprise_mlt_reported_100m_cny": report.enterprise_mlt_reported_100m_cny,
                "household_mlt_ytd_100m_cny": household_ytd,
                "enterprise_mlt_ytd_100m_cny": enterprise_ytd,
                "household_mlt_monthly_100m_cny": household_monthly,
                "enterprise_mlt_monthly_100m_cny": enterprise_monthly,
                "total_mlt_monthly_100m_cny": total_monthly,
                "normalization_status": normalization_status,
                "source_title": report.source_title,
                "source_url": report.source_url,
                "query_text": report.query_text,
                "match_method": report.match_method,
                "search_rank": report.search_rank,
                "source_hash": report.source_hash,
            }
        )
        state["last_month"] = month
        state["household_ytd"] = household_ytd
        state["enterprise_ytd"] = enterprise_ytd
    return pd.DataFrame(rows)


async def fetch_pbc_mlt_report(
    client: HttpClient, period_end_date: pd.Timestamp
) -> PBCMLTReport | None:
    queries = money_queries(period_end_date)
    calendar_record = await resolve_pbc_release(
        client, "money", period_end_date, queries
    )
    if calendar_record is None:
        return None
    html = await client.get_text(calendar_record.source_url)
    return parse_pbc_mlt_report(html, period_end_date, calendar_record)


@task_register()
class PBCMacroMLTLoanTask(FetcherTask):
    domain = "macro"
    name = "pbc_macro_mlt_loan"
    description = "央行人民币中长期贷款月增量（住户+企业，带官方发布时间）"
    table_name = "macro_mlt_loan"
    data_source = "pbc"
    primary_keys: ClassVar[list[str]] = ["period_end_date"]
    date_column = "period_end_date"
    default_start_date = "20110131"
    update_type = "smart"
    single_batch = True
    smart_lookback_days = 120

    default_concurrent_limit = 1
    default_max_retries = 1
    default_retry_delay = 3
    default_request_sleep = 0.10
    default_period_concurrency = 4

    schema_def: ClassVar[dict[str, dict[str, str]]] = {
        "period_end_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "统计期月末，不是可用日",
        },
        "release_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "央行报告官方发布日期，PIT 可用日锚点",
        },
        "release_time": {
            "type": "TIMESTAMP",
            "comment": "官方页面提供时保存精确发布时间，否则为空",
        },
        "report_basis": {
            "type": "VARCHAR(16)",
            "constraints": "NOT NULL",
            "comment": "monthly 或 ytd；描述原报告金额口径",
        },
        "household_mlt_reported_100m_cny": {
            "type": "NUMERIC(20,4)",
            "constraints": "NOT NULL",
            "comment": "报告中的住户中长期贷款，亿元；口径见 report_basis",
        },
        "enterprise_mlt_reported_100m_cny": {
            "type": "NUMERIC(20,4)",
            "constraints": "NOT NULL",
            "comment": "报告中的非金融企业中长期贷款，亿元；口径见 report_basis",
        },
        "household_mlt_ytd_100m_cny": {"type": "NUMERIC(20,4)"},
        "enterprise_mlt_ytd_100m_cny": {"type": "NUMERIC(20,4)"},
        "household_mlt_monthly_100m_cny": {"type": "NUMERIC(20,4)"},
        "enterprise_mlt_monthly_100m_cny": {"type": "NUMERIC(20,4)"},
        "total_mlt_monthly_100m_cny": {
            "type": "NUMERIC(20,4)",
            "comment": "住户+企业月增量；旧策略 long_loan_newadded 对应口径",
        },
        "normalization_status": {
            "type": "VARCHAR(32)",
            "constraints": "NOT NULL",
        },
        "source_title": {"type": "TEXT", "constraints": "NOT NULL"},
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
        "query_text": {"type": "TEXT"},
        "match_method": {"type": "VARCHAR(128)", "constraints": "NOT NULL"},
        "search_rank": {"type": "INTEGER"},
        "source_hash": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
    }

    indexes: ClassVar[list[dict[str, Any]]] = [
        {
            "name": "idx_pbc_macro_mlt_period",
            "columns": "period_end_date",
            "unique": True,
        },
        {"name": "idx_pbc_macro_mlt_release", "columns": "release_date"},
        {"name": "idx_pbc_macro_mlt_update", "columns": "update_time"},
    ]

    validations: ClassVar[list[tuple[Any, str]]] = [
        (lambda df: df["period_end_date"].notna(), "统计期不能为空"),
        (lambda df: df["release_date"].notna(), "发布日期不能为空"),
        (
            lambda df: pd.to_datetime(df["release_date"])
            > pd.to_datetime(df["period_end_date"]),
            "发布日期必须晚于统计期月末",
        ),
        (
            lambda df: df["report_basis"].isin(["monthly", "ytd"]),
            "报告口径必须为 monthly/ytd",
        ),
        (
            lambda df: df["source_url"].str.contains("pbc.gov.cn", na=False),
            "来源必须为央行官网",
        ),
        (
            lambda df: df["source_hash"].str.fullmatch(r"[0-9a-f]{64}", na=False),
            "来源哈希格式错误",
        ),
        (
            lambda df: df["total_mlt_monthly_100m_cny"]
            .dropna()
            .between(-100000, 200000),
            "中长期贷款月增量超出合理范围",
        ),
        (
            lambda df: df["total_mlt_monthly_100m_cny"].isna()
            | (
                (
                    df["total_mlt_monthly_100m_cny"]
                    - df["household_mlt_monthly_100m_cny"]
                    - df["enterprise_mlt_monthly_100m_cny"]
                ).abs()
                < 0.01
            ),
            "中长期贷款合计与住户/企业分项不一致",
        ),
    ]

    def _apply_config(self, task_config: dict) -> None:
        super()._apply_config(task_config)
        self.request_sleep = float(
            task_config.get("request_sleep", self.default_request_sleep)
        )
        self.period_concurrency = int(
            task_config.get("period_concurrency", self.default_period_concurrency)
        )

    def supports_incremental_update(self) -> bool:
        return True

    def get_incremental_skip_reason(self) -> str:
        return ""

    async def get_batch_list(self, **kwargs) -> list[dict[str, Any]]:
        return [
            {
                "start_date": kwargs.get("start_date", self.default_start_date),
                "end_date": kwargs.get(
                    "end_date", datetime.now(timezone.utc).strftime("%Y%m%d")
                ),
            }
        ]

    async def prepare_params(self, batch: dict[str, Any]) -> dict[str, Any]:
        return batch.copy()

    async def fetch_batch(
        self,
        params: dict[str, Any],
        stop_event: asyncio.Event | None = None,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(params["start_date"]).normalize()
        end_ts = pd.Timestamp(params["end_date"]).normalize()
        internal_start = pd.Timestamp(year=start_ts.year, month=1, day=1)
        periods = list(pd.date_range(internal_start, end_ts, freq="ME"))
        if not periods:
            return _empty_frame(list(self.schema_def))

        reports: list[PBCMLTReport] = []
        unresolved: list[pd.Timestamp] = []
        allowed_historical_gaps: set[pd.Timestamp] = set()
        unexpected_failures: list[tuple[pd.Timestamp, Exception | None]] = []
        semaphore = asyncio.Semaphore(max(self.period_concurrency, 1))

        async with HttpClient(request_sleep=self.request_sleep) as client:

            async def worker(
                period_end_date: pd.Timestamp,
            ) -> tuple[pd.Timestamp, PBCMLTReport | None, Exception | None]:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError
                async with semaphore:
                    try:
                        report = await fetch_pbc_mlt_report(client, period_end_date)
                        return period_end_date, report, None
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        return period_end_date, None, exc

            results = await asyncio.gather(*(worker(period) for period in periods))

        for period, report, error in results:
            if error is not None:
                if period in PBC_KNOWN_UNAVAILABLE_PERIODS:
                    self.logger.info("%s 为已知官网结构化缺口，跳过", period.date())
                    unresolved.append(period)
                    allowed_historical_gaps.add(period)
                else:
                    unexpected_failures.append((period, error))
            elif report is None:
                if _is_expected_unresolved_period(period):
                    unresolved.append(period)
                    if period in PBC_KNOWN_UNAVAILABLE_PERIODS:
                        allowed_historical_gaps.add(period)
                else:
                    unexpected_failures.append((period, None))
            else:
                reports.append(report)

        if unexpected_failures:
            sample = "; ".join(
                f"{period.date()}: {error or '官网未命中'}"
                for period, error in unexpected_failures[:4]
            )
            raise RuntimeError(
                f"{len(unexpected_failures)} 个已过发布宽限期的统计期解析失败，"
                f"拒绝保存部分结果。示例: {sample}"
            )
        if unresolved:
            self.logger.warning(
                "任务 %s: %s/%s 个统计期按已知缺口或发布宽限期跳过",
                self.name,
                len(unresolved),
                len(periods),
            )
        if not reports:
            return _empty_frame(list(self.schema_def))

        frame = normalize_pbc_mlt_reports(reports)
        _ensure_safe_normalization(frame, allowed_historical_gaps)
        frame = frame[
            (pd.to_datetime(frame["period_end_date"]) >= start_ts)
            & (pd.to_datetime(frame["period_end_date"]) <= end_ts)
        ]
        return frame[list(self.schema_def)].reset_index(drop=True)


__all__ = [
    "PBCMLTReport",
    "PBCMacroMLTLoanTask",
    "PBC_KNOWN_UNAVAILABLE_PERIODS",
    "PBC_PUBLICATION_GRACE_DAYS",
    "fetch_pbc_mlt_report",
    "normalize_pbc_mlt_reports",
    "parse_pbc_mlt_report",
]
