#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""宏观 PIT 发布日历任务。"""

from __future__ import annotations

import asyncio
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

import aiohttp
import pandas as pd

from ...base.fetcher_task import FetcherTask
from ....common.task_system.task_decorator import task_register


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
PBC_SEARCH_URL = "https://wzdig.pbc.gov.cn/search/pcRender"
PBC_PAGE_ID = "c177a85bd02b4114bebebd210809f691"
NBS_LIST_URL = "https://www.stats.gov.cn/sj/zxfb/index.html"
PMI_SCHEDULE_URLS = {
    2013: "https://www.stats.gov.cn/sj/fbrc/ljxxfb/202308/t20230811_1941913.html",
    2014: "https://www.stats.gov.cn/sj/fbrc/ljxxfb/202302/t20230222_1918251.html",
    2015: "https://www.stats.gov.cn/sj/fbrc/202302/t20230202_1897047.html",
    2016: "https://www.stats.gov.cn/sj/fbrc/ljxxfb/202302/t20230222_1918253.html",
    2017: "https://www.stats.gov.cn/xxgk/sjfb/fbrcb/201708/t20170814_1759232.html",
    2018: "https://www.stats.gov.cn/sj/fbrc/202302/t20230202_1897050.html",
    2019: "https://www.stats.gov.cn/sj/fbrc/ljxxfb/202302/t20230222_1918256.html",
    2020: "https://www.stats.gov.cn/sj/fbrc/ljxxfb/202302/t20230222_1918257.html",
    2021: "https://www.stats.gov.cn/sj/fbrc/202302/t20230202_1897053.html",
}
PMI_MANUAL_SEEDS = {
    "2012-10-31": {
        "release_date": "2012-11-01",
        "release_time": "2012-11-01 09:00:00",
    },
    "2012-11-30": {
        "release_date": "2012-12-01",
        "release_time": "2012-12-01 09:00:00",
    },
}


@dataclass(frozen=True)
class SearchResult:
    rank: int
    title: str
    url: str
    search_date: Optional[pd.Timestamp]
    snippet: str


@dataclass(frozen=True)
class CalendarRecord:
    indicator_code: str
    period_end_date: str
    release_date: str
    release_time: Optional[str]
    source_name: str
    source_title: str
    source_url: str
    query_text: Optional[str]
    match_method: str
    search_rank: Optional[int]


@dataclass(frozen=True)
class UnresolvedRecord:
    indicator_code: str
    period_end_date: str
    attempted_queries: str
    reason: str


@dataclass(frozen=True)
class HtmlCell:
    text: str
    rowspan: int = 1
    colspan: int = 1


def normalize_text(text: str) -> str:
    cleaned = unescape(text or "")
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = cleaned.replace("\xa0", "")
    cleaned = cleaned.replace("月份", "月")
    cleaned = cleaned.replace("—", "")
    cleaned = cleaned.replace("–", "")
    cleaned = cleaned.replace("-", "")
    cleaned = cleaned.replace("－", "")
    cleaned = cleaned.replace("（", "")
    cleaned = cleaned.replace("）", "")
    cleaned = cleaned.replace("(", "")
    cleaned = cleaned.replace(")", "")
    cleaned = cleaned.replace("：", "")
    cleaned = cleaned.replace(":", "")
    cleaned = cleaned.replace("“", "")
    cleaned = cleaned.replace("”", "")
    cleaned = cleaned.replace("《", "")
    cleaned = cleaned.replace("》", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = re.sub(r"[^\u4e00-\u9fff0-9A-Za-z]", "", cleaned)
    return cleaned.lower()


def parse_cn_date(value: Optional[str]) -> Optional[pd.Timestamp]:
    if not value:
        return None
    normalized = (
        str(value).strip()
        .replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
    )
    if not normalized:
        return None
    try:
        ts = pd.to_datetime(normalized)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def last_day_of_month(year: int, month: int) -> pd.Timestamp:
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)


def month_to_end_date(month_str: Optional[str]) -> Optional[pd.Timestamp]:
    if not month_str:
        return None
    text = str(month_str).strip()
    if len(text) != 6 or not text.isdigit():
        return None
    return last_day_of_month(int(text[:4]), int(text[4:6]))


def period_end_from_release_date(release_date: pd.Timestamp) -> pd.Timestamp:
    normalized = pd.Timestamp(release_date).normalize()
    return normalized if normalized.is_month_end else normalized + pd.offsets.MonthEnd(-1)


def parse_schedule_release_time(cell_value: str) -> Optional[str]:
    match = re.search(r"(\d{1,2}:\d{2})", cell_value or "")
    return match.group(1) if match else None


def parse_schedule_release_days(cell_value: str) -> List[int]:
    return [int(item) for item in re.findall(r"(\d{1,2})\s*/", cell_value or "")]


def is_pmi_schedule_title(value: str) -> bool:
    normalized = normalize_text(value)
    return (
        "中国制造业采购经理指数月度报告" in normalized
        or "中国采购经理指数月度报告" in normalized
        or (
            "采购经理指数月度报告" in normalized
            and "综合pmi产出指数" in normalized
        )
    )


def pbc_period_labels(period_end_date: pd.Timestamp) -> List[str]:
    year = period_end_date.year
    month = period_end_date.month
    if month == 3:
        return [f"{year}年一季度"]
    if month == 6:
        return [f"{year}年上半年"]
    if month == 9:
        return [f"{year}年前三季度"]
    if month == 12:
        return [f"{year}年"]
    return [f"{year}年{month}月", f"{year}年{month}月份"]


def money_queries(period_end_date: pd.Timestamp) -> List[str]:
    return [f"{pbc_period_labels(period_end_date)[0]}金融统计数据报告"]


def credit_queries(period_end_date: pd.Timestamp) -> List[str]:
    labels = pbc_period_labels(period_end_date)
    if period_end_date.year <= 2014:
        suffixes = [
            "社会融资规模统计数据报告",
            "社会融资规模增量统计数据报告",
            "社会融资规模存量统计数据报告",
        ]
    else:
        suffixes = [
            "社会融资规模增量统计数据报告",
            "社会融资规模统计数据报告",
            "社会融资规模存量统计数据报告",
        ]
    queries: List[str] = []
    for label in labels:
        for suffix in suffixes:
            query = f"{label}{suffix}"
            if query not in queries:
                queries.append(query)
    return queries


def build_query_norms(indicator_code: str, period_end_date: pd.Timestamp, query: str) -> List[str]:
    normalized = [normalize_text(query)]
    if indicator_code == "credit":
        for label in pbc_period_labels(period_end_date):
            for suffix in [
                "社会融资规模增量统计数据报告",
                "社会融资规模统计数据报告",
                "社会融资规模存量统计数据报告",
            ]:
                candidate = normalize_text(f"{label}{suffix}")
                if candidate not in normalized:
                    normalized.append(candidate)
    return normalized


def strip_tags(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    return " ".join(unescape(text).split())


class LinkCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[tuple[str, str]] = []
        self._current_href: Optional[str] = None
        self._text_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag == "a":
            attr_map = dict(attrs)
            self._current_href = str(attr_map.get("href") or "").strip()
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._current_href is not None:
            title = " ".join("".join(self._text_parts).split())
            if title and self._current_href:
                self.links.append((self._current_href, title))
            self._current_href = None
            self._text_parts = []


class PBCSearchParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.results: List[SearchResult] = []
        self._block_depth = 0
        self._current: Optional[Dict[str, Any]] = None
        self._inside_h3 = 0
        self._capture_title = False
        self._capture_snippet = False
        self._inside_dates = False
        self._capture_date = False

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attr_map = dict(attrs)
        classes = set((attr_map.get("class") or "").split())
        if tag == "div" and "searchMod" in classes and self._current is None:
            self._current = {
                "title_parts": [],
                "url": "",
                "snippet_parts": [],
                "date_parts": [],
            }
            self._block_depth = 1
            return

        if self._current is None:
            return

        if tag == "div":
            self._block_depth += 1
        elif tag == "h3":
            self._inside_h3 += 1
        elif tag == "a" and self._inside_h3 > 0:
            self._capture_title = True
            self._current["url"] = str(attr_map.get("href") or "").strip()
        elif tag == "p" and "txtCon" in classes:
            self._capture_snippet = True
        elif tag == "p" and "dates" in classes:
            self._inside_dates = True
        elif tag == "span" and self._inside_dates:
            self._capture_date = True

    def handle_data(self, data: str) -> None:
        if self._current is None:
            return
        if self._capture_title:
            self._current["title_parts"].append(data)
        elif self._capture_snippet:
            self._current["snippet_parts"].append(data)
        elif self._capture_date:
            self._current["date_parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._current is None:
            return
        if tag == "a" and self._capture_title:
            self._capture_title = False
        elif tag == "span" and self._capture_date:
            self._capture_date = False
        elif tag == "p" and self._capture_snippet:
            self._capture_snippet = False
        elif tag == "p" and self._inside_dates:
            self._inside_dates = False
        elif tag == "h3" and self._inside_h3 > 0:
            self._inside_h3 -= 1
        elif tag == "div":
            self._block_depth -= 1
            if self._block_depth == 0:
                title = " ".join("".join(self._current["title_parts"]).split())
                url = str(self._current.get("url") or "").strip()
                snippet = " ".join("".join(self._current["snippet_parts"]).split())
                search_date = None
                for token in reversed([item for item in self._current["date_parts"] if item.strip()]):
                    search_date = parse_cn_date(token)
                    if search_date is not None:
                        break
                if title and url:
                    self.results.append(
                        SearchResult(
                            rank=len(self.results) + 1,
                            title=title,
                            url=url,
                            search_date=search_date,
                            snippet=snippet,
                        )
                    )
                self._current = None


class TableExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: List[List[List[HtmlCell]]] = []
        self._current_table: Optional[List[List[HtmlCell]]] = None
        self._current_row: Optional[List[HtmlCell]] = None
        self._current_cell: Optional[Dict[str, Any]] = None
        self._cell_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in ("td", "th") and self._current_row is not None:
            attr_map = dict(attrs)
            self._current_cell = {
                "rowspan": int(attr_map.get("rowspan") or 1),
                "colspan": int(attr_map.get("colspan") or 1),
            }
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._current_row is not None and self._current_cell is not None:
            text = " ".join("".join(self._cell_parts).split())
            self._current_row.append(
                HtmlCell(
                    text=text,
                    rowspan=int(self._current_cell["rowspan"]),
                    colspan=int(self._current_cell["colspan"]),
                )
            )
            self._current_cell = None
            self._cell_parts = []
        elif tag == "tr" and self._current_table is not None and self._current_row is not None:
            self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            self.tables.append(self._current_table)
            self._current_table = None


def expand_table_grid(rows: List[List[HtmlCell]]) -> List[List[str]]:
    grid: List[List[Optional[str]]] = []
    span_map: Dict[tuple[int, int], str] = {}
    for row_idx, row in enumerate(rows):
        grid.append([])
        col_idx = 0
        while (row_idx, col_idx) in span_map:
            grid[row_idx].append(span_map.pop((row_idx, col_idx)))
            col_idx += 1
        for cell in row:
            while (row_idx, col_idx) in span_map:
                grid[row_idx].append(span_map.pop((row_idx, col_idx)))
                col_idx += 1
            for col_span_idx in range(cell.colspan):
                target_col = col_idx + col_span_idx
                while len(grid[row_idx]) <= target_col:
                    grid[row_idx].append("")
                grid[row_idx][target_col] = cell.text
                for row_span_idx in range(1, cell.rowspan):
                    span_map[(row_idx + row_span_idx, target_col)] = cell.text
            col_idx += cell.colspan
        while (row_idx, col_idx) in span_map:
            grid[row_idx].append(span_map.pop((row_idx, col_idx)))
            col_idx += 1
    width = max((len(row) for row in grid), default=0)
    return [[row[col] if col < len(row) else "" for col in range(width)] for row in grid]


class HttpClient:
    def __init__(self, request_sleep: float = 0.0, timeout_seconds: int = 30) -> None:
        self.request_sleep = max(float(request_sleep), 0.0)
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "HttpClient":
        self.session = aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def get_text(self, url: str, *, params: Optional[Dict[str, str]] = None) -> str:
        if self.session is None:
            raise RuntimeError("HttpClient session is not initialized")
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                async with self.session.get(url, params=params) as response:
                    response.raise_for_status()
                    payload = await response.read()
                    text = ""
                    for encoding in [response.charset, "utf-8", "gb18030", "gbk"]:
                        if not encoding:
                            continue
                        try:
                            text = payload.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    if not text:
                        text = payload.decode("utf-8", errors="ignore")
                    if self.request_sleep:
                        await asyncio.sleep(self.request_sleep)
                    return text
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"Request failed for {url}: {last_error}")


def extract_pbc_search_results(html: str) -> List[SearchResult]:
    parser = PBCSearchParser()
    parser.feed(html)
    return parser.results


def extract_links(html: str) -> List[tuple[str, str]]:
    parser = LinkCollector()
    parser.feed(html)
    return parser.links


def extract_tables(html: str) -> List[List[List[HtmlCell]]]:
    parser = TableExtractor()
    parser.feed(html)
    return parser.tables


def resolve_nbs_schedule_page(release_year: int) -> Optional[tuple[str, str]]:
    if release_year not in PMI_SCHEDULE_URLS:
        return None
    return f"{release_year}年国家统计局主要统计信息发布日程表", PMI_SCHEDULE_URLS[release_year]


async def collect_pmi_article_index(client: HttpClient) -> Dict[pd.Timestamp, tuple[str, str]]:
    by_period: Dict[pd.Timestamp, tuple[str, str]] = {}
    empty_pages = 0
    for page_no in range(90):
        if page_no == 0:
            url = NBS_LIST_URL
        else:
            url = urljoin(NBS_LIST_URL, f"index_{page_no}.html")
        try:
            html = await client.get_text(url)
        except Exception:
            empty_pages += 1
            if empty_pages >= 3 and by_period:
                break
            continue

        page_hits = 0
        for href, title in extract_links(html):
            if "中国采购经理指数运行情况" not in title:
                continue
            match = re.search(r"(?P<year>\d{4})年(?P<month>\d{1,2})月中国采购经理指数运行情况", title)
            if match is None:
                continue
            period_end_date = last_day_of_month(int(match.group("year")), int(match.group("month")))
            by_period[period_end_date.normalize()] = (title, urljoin(url, href))
            page_hits += 1

        if page_hits == 0:
            empty_pages += 1
            if empty_pages >= 6 and by_period:
                break
        else:
            empty_pages = 0
    return by_period


async def fetch_pmi_page(client: HttpClient, url: str) -> tuple[str, Optional[str]]:
    html = await client.get_text(url)
    title_match = re.search(
        r"(?P<year>\d{4})年(?P<month>\d{1,2})月中国采购经理指数运行情况",
        html,
    )
    if title_match:
        title = title_match.group(0)
    else:
        html_title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        title = strip_tags(html_title_match.group(1)) if html_title_match else ""

    flattened = " ".join(html.split())
    leading_window = flattened[:12000]
    patterns = [
        r"成文日期[^0-9]*(\d{4}年\d{1,2}月\d{1,2}日)",
        r"发布日期[^0-9]*(\d{4}年\d{1,2}月\d{1,2}日)",
        r'<meta[^>]+name="PubDate"[^>]+content="([^"]+)"',
        r'class=["\']detail-title-des["\'][^>]*>\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)',
        r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)",
    ]
    for pattern in patterns:
        haystack = html if "meta" in pattern or "class=" in pattern else leading_window
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match:
            return title, match.group(1)
    return title, None


async def collect_pmi_schedule_map(
    client: HttpClient,
    periods: List[pd.Timestamp],
) -> Dict[pd.Timestamp, tuple[pd.Timestamp, Optional[str], str, str]]:
    if not periods:
        return {}

    period_set = {pd.Timestamp(item).normalize() for item in periods}
    release_years = range(min(period_set).year, max(period_set).year + 2)
    schedule_map: Dict[pd.Timestamp, tuple[pd.Timestamp, Optional[str], str, str]] = {}

    for release_year in release_years:
        resolved = resolve_nbs_schedule_page(release_year)
        if resolved is None:
            continue
        source_title, source_url = resolved
        try:
            html = await client.get_text(source_url)
        except Exception:
            continue

        target_grid: Optional[List[List[str]]] = None
        for table in extract_tables(html):
            grid = expand_table_grid(table)
            table_text = " ".join(cell for row in grid for cell in row if cell)
            if "采购经理指数" in table_text:
                target_grid = grid
                break
        if not target_grid:
            continue

        month_cols: Dict[int, int] = {}
        for row in target_grid[:3]:
            for col_idx, value in enumerate(row):
                match = re.fullmatch(r"(\d{1,2})\s*月", str(value).strip())
                if match:
                    month_cols[col_idx] = int(match.group(1))
            if month_cols:
                break
        if not month_cols:
            continue

        pmi_rows = [
            row for row in target_grid
            if any(is_pmi_schedule_title(cell) for cell in row[:3])
        ]
        if not pmi_rows:
            continue

        time_only_row = next(
            (
                row for row in pmi_rows
                if sum(
                    1
                    for col_idx in month_cols
                    if col_idx < len(row)
                    and parse_schedule_release_time(row[col_idx])
                    and not parse_schedule_release_days(row[col_idx])
                ) >= 6
            ),
            None,
        )
        date_rows = [
            row for row in pmi_rows
            if any(
                col_idx < len(row) and parse_schedule_release_days(row[col_idx])
                for col_idx in month_cols
            )
        ]

        for col_idx, month in month_cols.items():
            candidate_rows = [time_only_row] if time_only_row is not None else pmi_rows
            release_time = next(
                (
                    parse_schedule_release_time(row[col_idx])
                    for row in candidate_rows
                    if row is not None and col_idx < len(row) and parse_schedule_release_time(row[col_idx])
                ),
                None,
            )

            release_days: List[int] = []
            for row in date_rows:
                if col_idx >= len(row):
                    continue
                for day in parse_schedule_release_days(row[col_idx]):
                    if day not in release_days:
                        release_days.append(day)

            for day in release_days:
                try:
                    release_date = pd.Timestamp(year=release_year, month=month, day=day).normalize()
                except ValueError:
                    continue
                period_end_date = period_end_from_release_date(release_date).normalize()
                if period_end_date not in period_set:
                    continue
                if period_end_date in schedule_map:
                    continue
                schedule_map[period_end_date] = (release_date, release_time, source_title, source_url)
    return schedule_map


async def resolve_pmi_releases(
    client: HttpClient,
    periods: List[pd.Timestamp],
) -> tuple[List[CalendarRecord], List[UnresolvedRecord]]:
    ordered_periods = sorted({pd.Timestamp(item).normalize() for item in periods})
    schedule_map = await collect_pmi_schedule_map(client, ordered_periods)
    article_index = await collect_pmi_article_index(client)
    records: List[CalendarRecord] = []
    unresolved: List[UnresolvedRecord] = []

    for period_end_date in ordered_periods:
        scheduled = schedule_map.get(period_end_date)
        if scheduled is not None:
            release_date, release_time, source_title, source_url = scheduled
            records.append(
                CalendarRecord(
                    indicator_code="pmi",
                    period_end_date=str(period_end_date.date()),
                    release_date=str(release_date.date()),
                    release_time=None if release_time is None else f"{release_date.date()} {release_time}:00",
                    source_name="stats.gov.cn",
                    source_title=source_title,
                    source_url=source_url,
                    query_text=f"{release_date.year}年国家统计局主要统计信息发布日程表",
                    match_method="stats_schedule_calendar",
                    search_rank=None,
                )
            )
            continue

        article = article_index.get(period_end_date)
        if article is not None:
            source_title, source_url = article
            try:
                page_title, raw_release = await fetch_pmi_page(client, source_url)
            except Exception:
                raw_release = None
                page_title = source_title
            release_ts = parse_cn_date(raw_release)
            if release_ts is not None:
                release_time = None
                if release_ts.hour or release_ts.minute or release_ts.second:
                    release_time = release_ts.isoformat(sep=" ")
                records.append(
                    CalendarRecord(
                        indicator_code="pmi",
                        period_end_date=str(period_end_date.date()),
                        release_date=str(release_ts.normalize().date()),
                        release_time=release_time,
                        source_name="stats.gov.cn",
                        source_title=page_title or source_title,
                        source_url=source_url,
                        query_text=source_title,
                        match_method="stats_listing_article_date",
                        search_rank=None,
                    )
                )
                continue

        manual_seed = PMI_MANUAL_SEEDS.get(str(period_end_date.date()))
        if manual_seed is not None:
            records.append(
                CalendarRecord(
                    indicator_code="pmi",
                    period_end_date=str(period_end_date.date()),
                    release_date=manual_seed["release_date"],
                    release_time=manual_seed["release_time"],
                    source_name="stats.gov.cn_manual_seed",
                    source_title="PMI manual seed around 2012 warmup boundary",
                    source_url="manual_seed://stats.gov.cn/pmi",
                    query_text=f"{period_end_date.year}年{period_end_date.month}月中国采购经理指数运行情况",
                    match_method="manual_seed_adjacent_release_pattern",
                    search_rank=None,
                )
            )
            continue

        unresolved.append(
            UnresolvedRecord(
                indicator_code="pmi",
                period_end_date=str(period_end_date.date()),
                attempted_queries=f"{period_end_date.year}年{period_end_date.month}月中国采购经理指数运行情况",
                reason="nbs_schedule_or_listing_missing_period",
            )
        )
    return records, unresolved


async def resolve_indicator_releases(
    client: HttpClient,
    indicator_code: str,
    periods: List[pd.Timestamp],
) -> tuple[List[CalendarRecord], List[UnresolvedRecord]]:
    ordered_periods = sorted({pd.Timestamp(item).normalize() for item in periods})
    if indicator_code == "pmi":
        return await resolve_pmi_releases(client, ordered_periods)

    records: List[CalendarRecord] = []
    unresolved: List[UnresolvedRecord] = []
    for period_end_date in ordered_periods:
        queries = money_queries(period_end_date) if indicator_code == "money" else credit_queries(period_end_date)
        record = await resolve_pbc_release(client, indicator_code, period_end_date, queries)
        if record is None:
            proxy_queries = credit_queries(period_end_date) if indicator_code == "money" else money_queries(period_end_date)
            record = await resolve_pbc_joint_release_proxy(client, indicator_code, period_end_date, proxy_queries)
        if record is not None:
            records.append(record)
            continue
        unresolved.append(
            UnresolvedRecord(
                indicator_code=indicator_code,
                period_end_date=str(period_end_date.date()),
                attempted_queries=" | ".join(queries),
                reason="no_verified_pbc_result",
            )
        )
    return records, unresolved


def score_pbc_result(
    indicator_code: str,
    period_end_date: pd.Timestamp,
    result: SearchResult,
    query_norms: Iterable[str],
) -> int:
    title_norm = normalize_text(result.title)
    normalized_queries = list(query_norms)
    score = 0
    if title_norm in normalized_queries:
        score += 120
    elif any(norm in title_norm or title_norm in norm for norm in normalized_queries):
        score += 80

    label_norms = [normalize_text(label) for label in pbc_period_labels(period_end_date)]
    if any(label_norm in title_norm for label_norm in label_norms):
        score += 30

    if indicator_code == "money":
        if "金融统计数据报告" in title_norm:
            score += 30
        if "社会融资规模" in title_norm:
            score -= 40
    else:
        if "社会融资规模" in title_norm:
            score += 30
        if "金融统计数据报告" in title_norm:
            score -= 40
        if "增量" in title_norm:
            score += 20
        if "存量" in title_norm:
            score -= 12
        if period_end_date.year <= 2014 and "增量" not in title_norm and "存量" not in title_norm:
            score += 10

    if "www.pbc.gov.cn/diaochatongjisi" in result.url:
        score += 15
    elif "www.pbc.gov.cn/goutongjiaoliu" in result.url:
        score += 10
    elif "www.pbc.gov.cn" in result.url:
        score += 5
    else:
        score -= 25

    if result.search_date is not None:
        delta_days = (result.search_date.normalize() - period_end_date.normalize()).days
        if 1 <= delta_days <= 40:
            score += 8
        elif delta_days < 0 or delta_days > 120:
            score -= 10
    return score


async def search_pbc(client: HttpClient, query: str) -> List[SearchResult]:
    html = await client.get_text(
        PBC_SEARCH_URL,
        params={"q": query, "pageId": PBC_PAGE_ID, "pNo": "1", "sr": "score desc"},
    )
    return extract_pbc_search_results(html)


async def fetch_pbc_page(client: HttpClient, url: str) -> tuple[str, Optional[str], Optional[str]]:
    html = await client.get_text(url)
    title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    title = strip_tags(title_match.group(1)) if title_match else ""
    flat = " ".join(html.split())
    pub_time_match = re.search(
        r"文章来源：\s*(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?)",
        flat,
    )
    pub_date_match = re.search(
        r'<meta[^>]+name="PubDate"[^>]+content="([^"]+)"',
        html,
        flags=re.IGNORECASE,
    )
    pub_time = pub_time_match.group(1).strip() if pub_time_match else None
    pub_date = pub_date_match.group(1).strip() if pub_date_match else None
    return title, pub_time, pub_date


async def resolve_pbc_release(
    client: HttpClient,
    indicator_code: str,
    period_end_date: pd.Timestamp,
    queries: List[str],
) -> Optional[CalendarRecord]:
    candidates: List[tuple[int, SearchResult, str]] = []
    for query in queries:
        results = await search_pbc(client, query)
        query_norms = build_query_norms(indicator_code, period_end_date, query)
        for result in results[:8]:
            score = score_pbc_result(indicator_code, period_end_date, result, query_norms)
            candidates.append((score, result, query))
        if any(score >= 150 for score, _, _ in candidates):
            break

    candidates.sort(
        key=lambda item: (
            item[0],
            -item[1].rank,
            1 if "www.pbc.gov.cn/diaochatongjisi" in item[1].url else 0,
        ),
        reverse=True,
    )
    seen_urls: set[str] = set()
    for score, result, query in candidates[:12]:
        if score < 80 or result.url in seen_urls:
            continue
        seen_urls.add(result.url)
        try:
            source_title, pub_time, pub_date = await fetch_pbc_page(client, result.url)
        except Exception:
            continue
        chosen_title = source_title or result.title
        chosen_title_norm = normalize_text(chosen_title)
        expected_norms = build_query_norms(indicator_code, period_end_date, query)
        if not any(norm in chosen_title_norm or chosen_title_norm in norm for norm in expected_norms):
            continue
        if indicator_code == "credit":
            if period_end_date.year > 2014 and "增量" not in chosen_title_norm:
                continue
            if "存量" in chosen_title_norm and "增量" not in chosen_title_norm:
                continue
        if indicator_code == "money" and "金融统计数据报告" not in chosen_title_norm:
            continue

        release_time = None
        release_date = None
        if pub_time:
            release_time = pd.to_datetime(pub_time)
            release_date = release_time.normalize()
        elif pub_date:
            release_date = pd.to_datetime(pub_date).normalize()
        elif result.search_date is not None:
            release_date = result.search_date.normalize()
        if release_date is None or pd.isna(release_date):
            continue

        return CalendarRecord(
            indicator_code=indicator_code,
            period_end_date=str(period_end_date.date()),
            release_date=str(release_date.date()),
            release_time=None if release_time is None else release_time.isoformat(sep=" "),
            source_name="pbc.gov.cn",
            source_title=chosen_title,
            source_url=result.url,
            query_text=query,
            match_method="pbc_search_page_pubdate",
            search_rank=result.rank,
        )
    return None


async def resolve_pbc_joint_release_proxy(
    client: HttpClient,
    indicator_code: str,
    period_end_date: pd.Timestamp,
    queries: List[str],
) -> Optional[CalendarRecord]:
    label_norms = [normalize_text(label) for label in pbc_period_labels(period_end_date)]
    candidates: List[tuple[int, SearchResult, str]] = []
    for query in queries:
        results = await search_pbc(client, query)
        for result in results[:10]:
            title_norm = normalize_text(result.title)
            score = 0
            if any(label_norm in title_norm for label_norm in label_norms):
                score += 60
            if "www.pbc.gov.cn/diaochatongjisi" in result.url:
                score += 15
            elif "www.pbc.gov.cn/goutongjiaoliu" in result.url:
                score += 10
            if indicator_code == "money" and "社会融资规模" in title_norm:
                score += 25
            if indicator_code == "credit" and "金融统计数据报告" in title_norm:
                score += 25
            if result.search_date is not None:
                delta_days = (result.search_date.normalize() - period_end_date.normalize()).days
                if 1 <= delta_days <= 40:
                    score += 8
            candidates.append((score, result, query))
        if any(score >= 90 for score, _, _ in candidates):
            break

    candidates.sort(key=lambda item: (item[0], -item[1].rank), reverse=True)
    seen_urls: set[str] = set()
    for score, result, query in candidates[:10]:
        if score < 70 or result.url in seen_urls:
            continue
        seen_urls.add(result.url)
        try:
            source_title, pub_time, pub_date = await fetch_pbc_page(client, result.url)
        except Exception:
            continue
        chosen_title = source_title or result.title
        chosen_title_norm = normalize_text(chosen_title)
        if not any(label_norm in chosen_title_norm for label_norm in label_norms):
            continue
        if indicator_code == "money" and "社会融资规模" not in chosen_title_norm:
            continue
        if indicator_code == "credit" and "金融统计数据报告" not in chosen_title_norm:
            continue

        release_time = None
        release_date = None
        if pub_time:
            release_time = pd.to_datetime(pub_time)
            release_date = release_time.normalize()
        elif pub_date:
            release_date = pd.to_datetime(pub_date).normalize()
        elif result.search_date is not None:
            release_date = result.search_date.normalize()
        if release_date is None or pd.isna(release_date):
            continue

        proxy_from = "credit" if indicator_code == "money" else "money"
        return CalendarRecord(
            indicator_code=indicator_code,
            period_end_date=str(period_end_date.date()),
            release_date=str(release_date.date()),
            release_time=None if release_time is None else release_time.isoformat(sep=" "),
            source_name="pbc.gov.cn",
            source_title=chosen_title,
            source_url=result.url,
            query_text=query,
            match_method=f"pbc_joint_release_proxy_from_{proxy_from}",
            search_rank=result.rank,
        )
    return None


@task_register()
class MacroReleaseCalendarTask(FetcherTask):
    domain = "macro"
    name = "macro_release_calendar"
    description = "抓取 PMI/社融/货币真实历史发布日期日历（PIT）"
    table_name = "macro_release_calendar"
    data_source = "akshare"
    primary_keys = ["indicator_code", "period_end_date"]
    date_column = "period_end_date"
    default_start_date = "20121130"
    update_type = "smart"
    single_batch = True
    smart_lookback_days = 90

    default_concurrent_limit = 1
    default_max_retries = 1
    default_retry_delay = 3
    default_request_sleep = 0.10
    default_indicator_concurrency = 4

    schema_def = {
        "indicator_code": {"type": "VARCHAR(16)", "constraints": "NOT NULL"},
        "period_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "release_date": {"type": "DATE", "constraints": "NOT NULL"},
        "release_time": {"type": "TIMESTAMP"},
        "source_name": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
        "source_title": {"type": "TEXT", "constraints": "NOT NULL"},
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
        "query_text": {"type": "TEXT"},
        "match_method": {"type": "VARCHAR(128)", "constraints": "NOT NULL"},
        "search_rank": {"type": "INTEGER"},
    }

    indexes = [
        {"name": "idx_macro_release_calendar_period", "columns": "indicator_code, period_end_date"},
        {"name": "idx_macro_release_calendar_release_date", "columns": "release_date"},
        {"name": "idx_macro_release_calendar_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["indicator_code"].notna(), "indicator_code 不能为空"),
        (lambda df: df["period_end_date"].notna(), "period_end_date 不能为空"),
        (lambda df: df["release_date"].notna(), "release_date 不能为空"),
        (lambda df: df["source_name"].notna(), "source_name 不能为空"),
        (lambda df: df["source_title"].notna(), "source_title 不能为空"),
        (lambda df: df["source_url"].notna(), "source_url 不能为空"),
        (
            lambda df: df["indicator_code"].isin(["pmi", "money", "credit"]),
            "indicator_code 必须为 pmi/money/credit",
        ),
    ]

    def _apply_config(self, task_config: Dict) -> None:
        super()._apply_config(task_config)
        self.request_sleep = float(task_config.get("request_sleep", self.default_request_sleep))
        self.indicator_concurrency = int(
            task_config.get("indicator_concurrency", self.default_indicator_concurrency)
        )

    def supports_incremental_update(self) -> bool:
        """宏观发布日期日历任务支持智能增量更新。"""
        return True

    def get_incremental_skip_reason(self) -> str:
        return ""

    async def get_batch_list(self, **kwargs) -> List[Dict[str, Any]]:
        return [
            {
                "start_date": kwargs.get("start_date", self.default_start_date),
                "end_date": kwargs.get("end_date", datetime.now().strftime("%Y%m%d")),
            }
        ]

    async def prepare_params(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        return batch.copy()

    async def _get_existing_periods(self) -> set[tuple[str, str]]:
        """查询目标表中已入库的 (indicator_code, period_end_date) 集合。"""
        target_table = self.get_full_table_name()
        try:
            if not await self.db.table_exists(self):
                return set()
            rows = await self.db.fetch(
                f'SELECT indicator_code, period_end_date FROM {target_table}'
            )
            return {
                (row["indicator_code"], str(pd.Timestamp(row["period_end_date"]).date()))
                for row in rows
                if row["indicator_code"] and row["period_end_date"]
            }
        except Exception as exc:
            self.logger.warning(f"查询已入库月份失败，将处理全量: {exc}")
            return set()

    async def _load_periods(self, start_date: str, end_date: str) -> Dict[str, List[pd.Timestamp]]:
        start_ts = pd.to_datetime(start_date).normalize()
        end_ts = pd.to_datetime(end_date).normalize()
        sources = {
            "pmi": "tushare.macro_pmi",
            "money": "tushare.macro_cn_m",
            "credit": "tushare.macro_sf_month",
        }
        missing_tables: List[str] = []
        for table_name in sources.values():
            if not await self.db.table_exists(table_name):
                missing_tables.append(table_name)
        if missing_tables:
            raise RuntimeError(
                f"缺少源表: {', '.join(missing_tables)}，请先运行对应宏观抓取任务"
            )

        # 增量模式下排除已入库月份
        existing_periods: set[tuple[str, str]] = set()
        if self.update_type != "full":
            existing_periods = await self._get_existing_periods()
            if existing_periods:
                self.logger.info(
                    f"任务 {self.name}: 已入库 {len(existing_periods)} 条记录，增量模式将跳过这些月份"
                )

        periods_by_indicator: Dict[str, List[pd.Timestamp]] = {}
        for indicator_code, table_name in sources.items():
            rows = await self.db.fetch(
                f"""
                SELECT month_end_date
                FROM {table_name}
                WHERE month_end_date >= $1
                  AND month_end_date <= $2
                ORDER BY month_end_date
                """,
                start_ts.date(),
                end_ts.date(),
            )
            all_periods = [
                pd.Timestamp(row["month_end_date"]).normalize()
                for row in rows
                if row["month_end_date"] is not None
            ]
            if existing_periods:
                filtered = [
                    p for p in all_periods
                    if (indicator_code, str(p.date())) not in existing_periods
                ]
                skipped = len(all_periods) - len(filtered)
                if skipped > 0:
                    self.logger.info(
                        f"任务 {self.name}: {indicator_code} 跳过 {skipped} 个已入库月份，"
                        f"剩余 {len(filtered)} 个待处理"
                    )
                periods_by_indicator[indicator_code] = filtered
            else:
                periods_by_indicator[indicator_code] = all_periods
        return periods_by_indicator

    async def _resolve_single_pbc_period(
        self,
        client: HttpClient,
        indicator_code: str,
        period_end_date: pd.Timestamp,
    ) -> tuple[Optional[CalendarRecord], Optional[UnresolvedRecord]]:
        queries = money_queries(period_end_date) if indicator_code == "money" else credit_queries(period_end_date)
        record = await resolve_pbc_release(client, indicator_code, period_end_date, queries)
        if record is None:
            proxy_queries = credit_queries(period_end_date) if indicator_code == "money" else money_queries(period_end_date)
            record = await resolve_pbc_joint_release_proxy(client, indicator_code, period_end_date, proxy_queries)
        if record is not None:
            return record, None
        return (
            None,
            UnresolvedRecord(
                indicator_code=indicator_code,
                period_end_date=str(period_end_date.date()),
                attempted_queries=" | ".join(queries),
                reason="no_verified_pbc_result",
            ),
        )

    async def _resolve_indicator_with_progress(
        self,
        client: HttpClient,
        indicator_code: str,
        periods: List[pd.Timestamp],
        stop_event: Optional[asyncio.Event] = None,
    ) -> tuple[List[CalendarRecord], List[UnresolvedRecord]]:
        ordered_periods = sorted({pd.Timestamp(item).normalize() for item in periods})
        total = len(ordered_periods)
        if total == 0:
            self.logger.info(f"任务 {self.name}: {indicator_code} 无可处理月份，跳过")
            return [], []

        self.logger.info(
            f"任务 {self.name}: 开始解析 {indicator_code} 发布日历，共 {total} 个月份"
        )

        if indicator_code == "pmi":
            records, unresolved = await resolve_pmi_releases(client, ordered_periods)
            self.logger.info(
                f"任务 {self.name}: 完成 {indicator_code} 发布日历，resolved={len(records)} unresolved={len(unresolved)}"
            )
            return records, unresolved

        semaphore = asyncio.Semaphore(max(self.indicator_concurrency, 1))
        completed = 0
        records: List[CalendarRecord] = []
        unresolved: List[UnresolvedRecord] = []

        async def worker(period_end_date: pd.Timestamp) -> tuple[Optional[CalendarRecord], Optional[UnresolvedRecord]]:
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError
            async with semaphore:
                return await self._resolve_single_pbc_period(client, indicator_code, period_end_date)

        tasks = [asyncio.create_task(worker(period_end_date)) for period_end_date in ordered_periods]
        try:
            for future in asyncio.as_completed(tasks):
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError
                record, miss = await future
                if record is not None:
                    records.append(record)
                if miss is not None:
                    unresolved.append(miss)
                completed += 1
                if completed == 1 or completed == total or completed % 10 == 0:
                    self.logger.info(
                        f"任务 {self.name}: {indicator_code} 进度 {completed}/{total} "
                        f"(resolved={len(records)}, unresolved={len(unresolved)})"
                    )
        except Exception:
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

        records.sort(key=lambda item: (item.indicator_code, item.period_end_date))
        unresolved.sort(key=lambda item: (item.indicator_code, item.period_end_date))
        self.logger.info(
            f"任务 {self.name}: 完成 {indicator_code} 发布日历，resolved={len(records)} unresolved={len(unresolved)}"
        )
        return records, unresolved

    async def fetch_batch(
        self,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError

        start_date = str(params["start_date"])
        end_date = str(params["end_date"])
        self.logger.info(
            f"任务 {self.name}: 加载宏观源表月份范围 start={start_date} end={end_date}"
        )
        periods_by_indicator = await self._load_periods(start_date, end_date)

        total_periods = sum(len(v) for v in periods_by_indicator.values())
        self.logger.info(
            f"任务 {self.name}: 月份载入完成 pmi={len(periods_by_indicator.get('pmi', []))} "
            f"money={len(periods_by_indicator.get('money', []))} "
            f"credit={len(periods_by_indicator.get('credit', []))} "
            f"(总待处理: {total_periods})"
        )

        if total_periods == 0:
            self.logger.info(f"任务 {self.name}: 无待处理月份，数据已是最新")
            return pd.DataFrame(columns=list(self.schema_def.keys()))

        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError

        records: List[CalendarRecord] = []
        unresolved: List[UnresolvedRecord] = []

        async with HttpClient(request_sleep=self.request_sleep) as client:
            for indicator_code in ["pmi", "money", "credit"]:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError
                indicator_records, indicator_unresolved = await self._resolve_indicator_with_progress(
                    client,
                    indicator_code,
                    periods_by_indicator.get(indicator_code, []),
                    stop_event=stop_event,
                )
                records.extend(indicator_records)
                unresolved.extend(indicator_unresolved)

        if unresolved:
            preview = "; ".join(
                f"{item.indicator_code}:{item.period_end_date}:{item.reason}"
                for item in unresolved[:8]
            )
            if len(unresolved) > 8:
                preview = f"{preview}; ..."
            self.logger.warning(
                f"任务 {self.name}: {len(unresolved)} 个月份未解析（将入库已解析部分）。样例: {preview}"
            )

        if not records:
            self.logger.warning(
                f"任务 {self.name}: 所有 {total_periods} 个月份均未解析成功，无数据可入库"
            )
            return pd.DataFrame(columns=list(self.schema_def.keys()))

        self.logger.info(
            f"任务 {self.name}: 解析完成，resolved={len(records)} unresolved={len(unresolved)}"
        )
        frame = pd.DataFrame([asdict(record) for record in records])
        return frame.sort_values(["indicator_code", "period_end_date"]).reset_index(drop=True)

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if data is None or data.empty:
            return data
        df = data.copy()
        for column in ["period_end_date", "release_date"]:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column]).dt.date
        if "release_time" in df.columns:
            df["release_time"] = pd.to_datetime(df["release_time"], errors="coerce")
        if "search_rank" in df.columns:
            df["search_rank"] = pd.to_numeric(df["search_rank"], errors="coerce").astype("Int64")
        df = df.sort_values(["indicator_code", "period_end_date"]).reset_index(drop=True)
        return df


__all__ = [
    "CalendarRecord",
    "HtmlCell",
    "MacroReleaseCalendarTask",
    "SearchResult",
    "UnresolvedRecord",
    "collect_pmi_article_index",
    "collect_pmi_schedule_map",
    "expand_table_grid",
    "extract_links",
    "extract_pbc_search_results",
    "fetch_pmi_page",
    "is_pmi_schedule_title",
    "month_to_end_date",
    "period_end_from_release_date",
    "resolve_pmi_releases",
]
