#!/usr/bin/env python

"""国家统计局房地产新开工、竣工和销售面积及其月同比。"""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ...base.fetcher_task import FetcherTask
from .macro_release_calendar import (
    NBS_LIST_URL,
    HttpClient,
    expand_table_grid,
    extract_links,
    extract_tables,
    last_day_of_month,
    parse_cn_date,
    strip_tags,
)

# 国家统计局当前“数据发布”列表只回溯到 2021 年 9 月附近。以下种子补齐
# 2021 年 2—8 月，使持续更新序列至少有一个完整的可比基年。
NBS_HOUSING_SEED_URLS = {
    "2021-02-28": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901027.html",
    "2021-03-31": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901049.html",
    "2021-04-30": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901101.html",
    "2021-05-31": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901128.html",
    "2021-06-30": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901157.html",
    "2021-07-31": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901193.html",
    "2021-08-31": "https://www.stats.gov.cn/xxgk/sjfb/zxfb2020/202109/t20210915_1822094.html",
}
NBS_SERIES_START = pd.Timestamp("2021-02-28")
NBS_DERIVED_YOY_START = pd.Timestamp("2022-02-28")
NBS_PUBLICATION_GRACE_DAYS = 45


@dataclass(frozen=True)
class NBSHousingReport:
    period_end_date: pd.Timestamp
    release_date: pd.Timestamp
    release_time: pd.Timestamp | None
    cumulative_area_10k_sqm: float
    cumulative_yoy_reported: float
    completed_cumulative_area_10k_sqm: float
    completed_cumulative_yoy_reported: float
    sold_cumulative_area_10k_sqm: float
    sold_cumulative_yoy_reported: float
    source_name: str
    source_title: str
    source_url: str
    match_method: str
    source_hash: str


def _clean_title(value: str) -> str:
    return " ".join(strip_tags(value or "").split()).strip()


def parse_nbs_housing_period(title: str) -> pd.Timestamp | None:
    text = _clean_title(title)
    range_match = re.search(
        r"(?P<year>20\d{2})年\s*1\s*[—–－\-至]\s*" r"(?P<month>\d{1,2})\s*月份?",
        text,
    )
    if range_match is not None:
        month = int(range_match.group("month"))
        if 1 <= month <= 12:
            return last_day_of_month(int(range_match.group("year")), month)

    special_match = re.search(
        r"(?P<year>20\d{2})年(?P<label>一季度|上半年|前三季度)", text
    )
    if special_match is not None:
        month_by_label = {"一季度": 3, "上半年": 6, "前三季度": 9}
        return last_day_of_month(
            int(special_match.group("year")),
            month_by_label[special_match.group("label")],
        )

    annual_match = re.search(r"(?P<year>20\d{2})年", text)
    if annual_match is not None and "房地产" in text:
        return last_day_of_month(int(annual_match.group("year")), 12)
    return None


def is_nbs_housing_report_title(title: str) -> bool:
    text = _clean_title(title)
    if "房地产" not in text:
        return False
    return (
        any(
            token in text
            for token in (
                "全国房地产市场基本情况",
                "全国房地产开发和销售情况",
                "全国房地产开发投资",
            )
        )
        and parse_nbs_housing_period(text) is not None
    )


def _extract_article_title(html: str, fallback_title: str | None = None) -> str:
    for match in re.finditer(
        r"<h[12][^>]*>(.*?)</h[12]>", html, flags=re.IGNORECASE | re.DOTALL
    ):
        title = _clean_title(match.group(1))
        if is_nbs_housing_report_title(title):
            return title

    title_match = re.search(
        r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL
    )
    if title_match is not None:
        title = _clean_title(title_match.group(1)).removesuffix(" - 国家统计局")
        if is_nbs_housing_report_title(title):
            return title
    if fallback_title and is_nbs_housing_report_title(fallback_title):
        return _clean_title(fallback_title)
    raise ValueError("页面标题不是全国房地产开发月度报告")


def _parse_release_timestamp(html: str) -> tuple[pd.Timestamp, pd.Timestamp | None]:
    flattened = " ".join(html.split())
    visible = strip_tags(flattened[:30000])
    patterns = [
        (r'<meta[^>]+name="PubDate"[^>]+content="([^"]+)"', html),
        (r"成文日期[^0-9]*(\d{4}年\d{1,2}月\d{1,2}日)", visible),
        (r"发布日期[^0-9]*(\d{4}年\d{1,2}月\d{1,2}日)", visible),
        (
            r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)",
            flattened[:30000],
        ),
    ]
    for pattern, haystack in patterns:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match is None:
            continue
        parsed = parse_cn_date(match.group(1))
        if parsed is None:
            continue
        has_time = bool(re.search(r"\d{1,2}:\d{2}", match.group(1)))
        return parsed.normalize(), parsed if has_time else None
    raise ValueError("页面缺少可信的官方发布日期")


def _parse_numeric_cell(value: str) -> float | None:
    text = re.sub(r"\s+", "", value or "").replace(",", "")
    match = re.search(r"-?[0-9]+(?:\.[0-9]+)?", text)
    return None if match is None else float(match.group(0))


def _extract_housing_metric_values(
    html: str, metric_label: str
) -> tuple[float, float, str]:
    for table in extract_tables(html):
        for row in expand_table_grid(table):
            if not row:
                continue
            label = re.sub(r"\s+", "", row[0])
            if metric_label not in label or "住宅" in label:
                continue
            values = [
                number
                for number in (_parse_numeric_cell(cell) for cell in row[1:])
                if number is not None
            ]
            if len(values) >= 2:
                return values[0], values[1], "official_table"

    text = re.sub(r"\s+", "", strip_tags(html))
    narrative = re.search(
        rf"{re.escape(metric_label)}(?:为)?(?P<amount>[0-9,.]+)"
        r"(?P<unit>万平方米|亿平方米)[，,；;]?"
        r"(?:(?:同比|比上年))?(?P<direction>增长|下降)"
        r"(?P<yoy>[0-9.]+)%",
        text,
    )
    if narrative is not None:
        area = float(narrative.group("amount").replace(",", ""))
        if narrative.group("unit") == "亿平方米":
            area *= 10000.0
        yoy = float(narrative.group("yoy"))
        if narrative.group("direction") == "下降":
            yoy *= -1.0
        return round(area, 4), round(yoy, 6), "official_narrative"

    unchanged = re.search(
        rf"{re.escape(metric_label)}(?:为)?(?P<amount>[0-9,.]+)"
        r"(?P<unit>万平方米|亿平方米)[^。；]{0,40}(?:同比)?持平",
        text,
    )
    if unchanged is not None:
        area = float(unchanged.group("amount").replace(",", ""))
        if unchanged.group("unit") == "亿平方米":
            area *= 10000.0
        return round(area, 4), 0.0, "official_narrative"
    raise ValueError(f"页面缺少{metric_label}累计值/同比")


def parse_nbs_housing_report(
    html: str, source_url: str, fallback_title: str | None = None
) -> NBSHousingReport:
    title = _extract_article_title(html, fallback_title=fallback_title)
    period_end_date = parse_nbs_housing_period(title)
    if period_end_date is None:
        raise ValueError("无法从标题识别统计期")
    release_date, release_time = _parse_release_timestamp(html)
    cumulative_area, cumulative_yoy, newstarts_method = _extract_housing_metric_values(
        html, "房屋新开工面积"
    )
    completed_area, completed_yoy, completed_method = _extract_housing_metric_values(
        html, "房屋竣工面积"
    )
    sold_area, sold_yoy, sold_method = _extract_housing_metric_values(
        html, "商品房销售面积"
    )
    match_method = "+".join(sorted({newstarts_method, completed_method, sold_method}))
    return NBSHousingReport(
        period_end_date=period_end_date.normalize(),
        release_date=release_date,
        release_time=release_time,
        cumulative_area_10k_sqm=cumulative_area,
        cumulative_yoy_reported=cumulative_yoy,
        completed_cumulative_area_10k_sqm=completed_area,
        completed_cumulative_yoy_reported=completed_yoy,
        sold_cumulative_area_10k_sqm=sold_area,
        sold_cumulative_yoy_reported=sold_yoy,
        source_name=urlparse(source_url).netloc.lower(),
        source_title=title,
        source_url=source_url,
        match_method=match_method,
        source_hash=hashlib.sha256(html.encode("utf-8")).hexdigest(),
    )


def _prefer_article_url(current: str | None, candidate: str) -> str:
    if current is None:
        return candidate
    if "/sj/zxfb/" in candidate and "/sj/zxfb/" not in current:
        return candidate
    return current


async def collect_nbs_housing_article_index(
    client: HttpClient,
    min_period: pd.Timestamp,
    max_period: pd.Timestamp,
    max_pages: int = 90,
) -> dict[pd.Timestamp, tuple[str, str]]:
    by_period: dict[pd.Timestamp, tuple[str, str]] = {}
    for period_text, url in NBS_HOUSING_SEED_URLS.items():
        period = pd.Timestamp(period_text).normalize()
        if min_period <= period <= max_period:
            by_period[period] = ("", url)

    failed_pages = 0
    cutoff_month = min_period.replace(day=1)
    for page_no in range(max(max_pages, 1)):
        url = (
            NBS_LIST_URL
            if page_no == 0
            else urljoin(NBS_LIST_URL, f"index_{page_no}.html")
        )
        try:
            html = await client.get_text(url)
        except Exception:  # noqa: BLE001 - continue past transient listing failures
            failed_pages += 1
            if failed_pages >= 3:
                break
            continue
        failed_pages = 0

        housing_publication_months: list[pd.Timestamp] = []
        seen_urls: set[str] = set()
        for href, title in extract_links(html):
            article_url = urljoin(url, href)
            if article_url in seen_urls or not is_nbs_housing_report_title(title):
                continue
            seen_urls.add(article_url)
            date_match = re.search(r"/(20\d{2})(\d{2})/", article_url)
            if date_match is not None:
                housing_publication_months.append(
                    pd.Timestamp(
                        year=int(date_match.group(1)),
                        month=int(date_match.group(2)),
                        day=1,
                    )
                )
            period = parse_nbs_housing_period(title)
            if period is None or not (min_period <= period <= max_period):
                continue
            current = by_period.get(period)
            chosen_url = _prefer_article_url(
                None if current is None else current[1], article_url
            )
            chosen_title = title if chosen_url == article_url else current[0]
            by_period[period] = (chosen_title, chosen_url)

        if (
            by_period
            and housing_publication_months
            and min(housing_publication_months) < cutoff_month
        ):
            break
    return by_period


def _empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _expected_published_periods(
    min_period: pd.Timestamp,
    max_period: pd.Timestamp,
    *,
    as_of: pd.Timestamp | None = None,
) -> set[pd.Timestamp]:
    start = max(pd.Timestamp(min_period).normalize(), NBS_SERIES_START)
    end = pd.Timestamp(max_period).normalize()
    if start > end:
        return set()
    current_date = (
        pd.Timestamp.now().normalize()
        if as_of is None
        else pd.Timestamp(as_of).normalize()
    )
    return {
        period.normalize()
        for period in pd.date_range(start, end, freq="ME")
        if period.month != 1
        and period + pd.Timedelta(days=NBS_PUBLICATION_GRACE_DAYS) <= current_date
    }


def _ensure_complete_derivations(
    frame: pd.DataFrame,
    *,
    output_start: pd.Timestamp,
    output_end: pd.Timestamp,
) -> None:
    broken_chain = frame.loc[
        frame["derivation_status"] == "missing_previous_cumulative",
        "period_end_date",
    ]
    if not broken_chain.empty:
        periods = ", ".join(
            str(pd.Timestamp(item).date()) for item in broken_chain.iloc[:6]
        )
        raise RuntimeError("房地产累计值存在非预期的连续月份缺口，拒绝写入: " + periods)

    period_values = pd.to_datetime(frame["period_end_date"])
    required_yoy_start = max(
        pd.Timestamp(output_start).normalize(), NBS_DERIVED_YOY_START
    )
    required_yoy_end = pd.Timestamp(output_end).normalize()
    yoy_columns = [
        "monthly_yoy_derived",
        "completed_monthly_yoy_derived",
        "sold_monthly_yoy_derived",
    ]
    missing_yoy = frame.loc[
        (period_values >= required_yoy_start)
        & (period_values <= required_yoy_end)
        & frame[yoy_columns].isna().any(axis=1),
        "period_end_date",
    ]
    if not missing_yoy.empty:
        periods = ", ".join(
            str(pd.Timestamp(item).date()) for item in missing_yoy.iloc[:6]
        )
        raise RuntimeError("房地产月同比派生链不完整，拒绝写入: " + periods)


def normalize_nbs_housing_reports(reports: list[NBSHousingReport]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    year_state: dict[int, dict[str, Any]] = {}
    metric_fields = {
        "newstarts": "cumulative_area_10k_sqm",
        "completed": "completed_cumulative_area_10k_sqm",
        "sold": "sold_cumulative_area_10k_sqm",
    }
    for report in sorted(reports, key=lambda item: item.period_end_date):
        year = report.period_end_date.year
        month = report.period_end_date.month
        state = year_state.setdefault(
            year,
            {
                "last_month": None,
                **{f"{metric}_cumulative": None for metric in metric_fields},
            },
        )
        if month == 1:
            bucket = "january"
            status = "direct_january"
        elif month == 2:
            bucket = "jan_feb_combined"
            status = "jan_feb_combined"
        elif state["last_month"] == month - 1:
            bucket = "calendar_month"
            status = "derived_from_cumulative"
        else:
            bucket = "calendar_month"
            status = "missing_previous_cumulative"

        monthly_values: dict[str, float | None] = {}
        for metric, field_name in metric_fields.items():
            cumulative_value = getattr(report, field_name)
            previous_value = state[f"{metric}_cumulative"]
            if month in (1, 2):
                monthly_value: float | None = cumulative_value
            elif state["last_month"] == month - 1 and previous_value is not None:
                monthly_value = round(cumulative_value - previous_value, 4)
            else:
                monthly_value = None
            monthly_values[metric] = monthly_value

        rows.append(
            {
                "period_end_date": report.period_end_date.date(),
                "release_date": report.release_date.date(),
                "release_time": (
                    None
                    if report.release_time is None
                    else report.release_time.to_pydatetime()
                ),
                "cumulative_area_10k_sqm": report.cumulative_area_10k_sqm,
                "cumulative_yoy_reported": report.cumulative_yoy_reported,
                "monthly_area_10k_sqm": monthly_values["newstarts"],
                "monthly_yoy_derived": None,
                "completed_cumulative_area_10k_sqm": (
                    report.completed_cumulative_area_10k_sqm
                ),
                "completed_cumulative_yoy_reported": (
                    report.completed_cumulative_yoy_reported
                ),
                "completed_monthly_area_10k_sqm": monthly_values["completed"],
                "completed_monthly_yoy_derived": None,
                "sold_cumulative_area_10k_sqm": report.sold_cumulative_area_10k_sqm,
                "sold_cumulative_yoy_reported": report.sold_cumulative_yoy_reported,
                "sold_monthly_area_10k_sqm": monthly_values["sold"],
                "sold_monthly_yoy_derived": None,
                "monthly_bucket": bucket,
                "derivation_status": status,
                "calculation_version": "nbs_housing_cumulative_difference_v2",
                "source_name": report.source_name,
                "source_title": report.source_title,
                "source_url": report.source_url,
                "match_method": report.match_method,
                "source_hash": report.source_hash,
            }
        )
        state["last_month"] = month
        for metric, field_name in metric_fields.items():
            state[f"{metric}_cumulative"] = getattr(report, field_name)

    by_period = {
        (
            pd.Timestamp(row["period_end_date"]).year,
            pd.Timestamp(row["period_end_date"]).month,
        ): row
        for row in rows
    }
    for row in rows:
        period = pd.Timestamp(row["period_end_date"])
        prior = by_period.get((period.year - 1, period.month))
        for metric in metric_fields:
            monthly_column = (
                "monthly_area_10k_sqm"
                if metric == "newstarts"
                else f"{metric}_monthly_area_10k_sqm"
            )
            yoy_column = (
                "monthly_yoy_derived"
                if metric == "newstarts"
                else f"{metric}_monthly_yoy_derived"
            )
            current_area = row[monthly_column]
            prior_area = None if prior is None else prior[monthly_column]
            if current_area is None or prior_area in (None, 0):
                continue
            row[yoy_column] = round((current_area / prior_area - 1.0) * 100.0, 6)
    return pd.DataFrame(rows)


@task_register()
class NBSMacroHousingNewStartsTask(FetcherTask):
    domain = "macro"
    name = "nbs_macro_housing_newstarts"
    description = "国家统计局房地产新开工、竣工和销售面积（官方发布时间）"
    table_name = "macro_housing_newstarts"
    data_source = "nbs"
    primary_keys: ClassVar[list[str]] = ["period_end_date"]
    date_column = "period_end_date"
    default_start_date = "20210228"
    update_type = "smart"
    single_batch = True
    smart_lookback_days = 120

    default_concurrent_limit = 1
    default_max_retries = 1
    default_retry_delay = 3
    default_request_sleep = 0.05
    default_article_concurrency = 4
    default_max_listing_pages = 90

    schema_def: ClassVar[dict[str, dict[str, str]]] = {
        "period_end_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "统计期月末；2 月代表 1—2 月合并桶",
        },
        "release_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "国家统计局官方发布日期，PIT 可用日锚点",
        },
        "release_time": {
            "type": "TIMESTAMP",
            "comment": "官方页面提供时保存精确发布时间，否则为空",
        },
        "cumulative_area_10k_sqm": {
            "type": "NUMERIC(20,4)",
            "constraints": "NOT NULL",
            "comment": "官方披露的年内累计房屋新开工面积，万平方米",
        },
        "cumulative_yoy_reported": {
            "type": "NUMERIC(20,6)",
            "constraints": "NOT NULL",
            "comment": "官方披露的累计同比，可能采用修订后的可比基数",
        },
        "monthly_area_10k_sqm": {
            "type": "NUMERIC(20,4)",
            "comment": "累计值差分；2 月为 1—2 月合并值",
        },
        "monthly_yoy_derived": {
            "type": "NUMERIC(20,6)",
            "comment": "月增量同比；旧策略 newstarts_area_yoy 对应口径",
        },
        "completed_cumulative_area_10k_sqm": {
            "type": "NUMERIC(20,4)",
            "constraints": "NOT NULL",
            "comment": "官方披露的年内累计房屋竣工面积，万平方米",
        },
        "completed_cumulative_yoy_reported": {
            "type": "NUMERIC(20,6)",
            "constraints": "NOT NULL",
        },
        "completed_monthly_area_10k_sqm": {"type": "NUMERIC(20,4)"},
        "completed_monthly_yoy_derived": {"type": "NUMERIC(20,6)"},
        "sold_cumulative_area_10k_sqm": {
            "type": "NUMERIC(20,4)",
            "constraints": "NOT NULL",
            "comment": "官方披露的年内累计商品房销售面积，万平方米",
        },
        "sold_cumulative_yoy_reported": {
            "type": "NUMERIC(20,6)",
            "constraints": "NOT NULL",
        },
        "sold_monthly_area_10k_sqm": {"type": "NUMERIC(20,4)"},
        "sold_monthly_yoy_derived": {"type": "NUMERIC(20,6)"},
        "monthly_bucket": {
            "type": "VARCHAR(32)",
            "constraints": "NOT NULL",
        },
        "derivation_status": {
            "type": "VARCHAR(40)",
            "constraints": "NOT NULL",
        },
        "calculation_version": {
            "type": "VARCHAR(64)",
            "constraints": "NOT NULL",
        },
        "source_name": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
        "source_title": {"type": "TEXT", "constraints": "NOT NULL"},
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
        "match_method": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
        "source_hash": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
    }

    indexes: ClassVar[list[dict[str, Any]]] = [
        {
            "name": "idx_nbs_housing_newstarts_period",
            "columns": "period_end_date",
            "unique": True,
        },
        {"name": "idx_nbs_housing_newstarts_release", "columns": "release_date"},
        {"name": "idx_nbs_housing_newstarts_update", "columns": "update_time"},
    ]

    validations: ClassVar[list[Any]] = [
        (lambda df: df["period_end_date"].notna(), "统计期不能为空"),
        (lambda df: df["release_date"].notna(), "发布日期不能为空"),
        (
            lambda df: pd.to_datetime(df["release_date"])
            > pd.to_datetime(df["period_end_date"]),
            "发布日期必须晚于统计期月末",
        ),
        (
            lambda df: df["cumulative_area_10k_sqm"] > 0,
            "累计新开工面积必须为正",
        ),
        (
            lambda df: df["monthly_area_10k_sqm"].isna()
            | (df["monthly_area_10k_sqm"] > 0),
            "月度新开工面积必须为正",
        ),
        (
            lambda df: df["completed_monthly_area_10k_sqm"].isna()
            | (df["completed_monthly_area_10k_sqm"] >= 0),
            "月度竣工面积不能为负",
        ),
        (
            lambda df: df["sold_monthly_area_10k_sqm"].isna()
            | (df["sold_monthly_area_10k_sqm"] >= 0),
            "月度商品房销售面积不能为负",
        ),
        (
            lambda df: df["cumulative_yoy_reported"].between(-100, 300),
            "官方累计同比超出合理范围",
        ),
        (
            lambda df: df["source_url"].str.contains("stats.gov.cn", na=False),
            "来源必须为国家统计局官网",
        ),
        (
            lambda df: df["source_hash"].str.fullmatch(r"[0-9a-f]{64}", na=False),
            "来源哈希格式错误",
        ),
    ]

    def _apply_config(self, task_config: dict) -> None:
        super()._apply_config(task_config)
        self.request_sleep = float(
            task_config.get("request_sleep", self.default_request_sleep)
        )
        self.article_concurrency = int(
            task_config.get("article_concurrency", self.default_article_concurrency)
        )
        self.max_listing_pages = int(
            task_config.get("max_listing_pages", self.default_max_listing_pages)
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
                    "end_date", pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y%m%d")
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
        internal_start = pd.Timestamp(year=start_ts.year - 1, month=1, day=1)

        async with HttpClient(request_sleep=self.request_sleep) as client:
            article_index = await collect_nbs_housing_article_index(
                client,
                internal_start,
                end_ts,
                max_pages=self.max_listing_pages,
            )
            expected_periods = _expected_published_periods(internal_start, end_ts)
            missing_periods = sorted(expected_periods - set(article_index))
            if missing_periods:
                sample = ", ".join(str(item.date()) for item in missing_periods[:8])
                raise RuntimeError(
                    f"国家统计局发布索引缺少 {len(missing_periods)} 个已过宽限期的"
                    f"统计期，拒绝保存部分结果。示例: {sample}"
                )
            if not article_index:
                return _empty_frame(list(self.schema_def))

            semaphore = asyncio.Semaphore(max(self.article_concurrency, 1))

            async def worker(
                period: pd.Timestamp, title: str, url: str
            ) -> tuple[pd.Timestamp, NBSHousingReport | None, Exception | None]:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError
                async with semaphore:
                    try:
                        html = await client.get_text(url)
                        report = parse_nbs_housing_report(
                            html, url, fallback_title=title or None
                        )
                        if report.period_end_date != period:
                            raise ValueError(
                                f"标题统计期 {report.period_end_date.date()} 与索引 {period.date()} 不一致"
                            )
                        return period, report, None
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        return period, None, exc

            results = await asyncio.gather(
                *(
                    worker(period, title, url)
                    for period, (title, url) in sorted(article_index.items())
                )
            )

        reports: list[NBSHousingReport] = []
        article_failures: list[tuple[pd.Timestamp, Exception]] = []
        for period, report, error in results:
            if error is not None:
                article_failures.append((period, error))
            elif report is not None:
                reports.append(report)
        if article_failures:
            sample = "; ".join(
                f"{period.date()}: {error}" for period, error in article_failures[:4]
            )
            raise RuntimeError(
                f"{len(article_failures)} 篇国家统计局文章抓取或解析失败，"
                f"拒绝保存部分结果。示例: {sample}"
            )
        if not reports:
            return _empty_frame(list(self.schema_def))

        frame = normalize_nbs_housing_reports(reports)
        _ensure_complete_derivations(
            frame,
            output_start=start_ts,
            output_end=end_ts,
        )
        frame = frame[
            (pd.to_datetime(frame["period_end_date"]) >= start_ts)
            & (pd.to_datetime(frame["period_end_date"]) <= end_ts)
        ]
        return frame[list(self.schema_def)].reset_index(drop=True)


__all__ = [
    "NBSHousingReport",
    "NBSMacroHousingNewStartsTask",
    "NBS_DERIVED_YOY_START",
    "NBS_PUBLICATION_GRACE_DAYS",
    "NBS_SERIES_START",
    "collect_nbs_housing_article_index",
    "is_nbs_housing_report_title",
    "normalize_nbs_housing_reports",
    "parse_nbs_housing_period",
    "parse_nbs_housing_report",
]
