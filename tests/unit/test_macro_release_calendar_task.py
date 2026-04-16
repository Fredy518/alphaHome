#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import pytest

from alphahome.fetchers.tasks.macro.macro_release_calendar import (
    CalendarRecord,
    MacroReleaseCalendarTask,
    UnresolvedRecord,
    expand_table_grid,
    extract_links,
    extract_pbc_search_results,
    is_pmi_schedule_title,
    month_to_end_date,
    period_end_from_release_date,
    resolve_nbs_schedule_page,
    HtmlCell,
)


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return []

    async def table_exists(self, table_name):
        return True


def test_macro_release_calendar_task_attributes():
    task = MacroReleaseCalendarTask(db_connection=_MockDB())
    assert task.name == "macro_release_calendar"
    assert task.table_name == "macro_release_calendar"
    assert task.data_source == "akshare"
    assert task.primary_keys == ["indicator_code", "period_end_date"]
    assert task.supports_incremental_update() is True
    assert task.get_incremental_skip_reason() == ""


def test_month_to_end_date():
    assert month_to_end_date("202402") == pd.Timestamp("2024-02-29")
    assert month_to_end_date("202411") == pd.Timestamp("2024-11-30")
    assert month_to_end_date("2024-11") is None


def test_period_end_from_release_date():
    assert period_end_from_release_date(pd.Timestamp("2024-03-31")) == pd.Timestamp("2024-03-31")
    assert period_end_from_release_date(pd.Timestamp("2024-04-01")) == pd.Timestamp("2024-03-31")
    assert period_end_from_release_date(pd.Timestamp("2024-01-01")) == pd.Timestamp("2023-12-31")


def test_is_pmi_schedule_title():
    assert is_pmi_schedule_title("中国制造业采购经理指数月度报告")
    assert is_pmi_schedule_title("中国采购经理指数月度报告")
    assert is_pmi_schedule_title("采购经理指数月度报告（含综合PMI产出指数）")
    assert not is_pmi_schedule_title("居民消费价格指数月度报告")


def test_expand_table_grid_handles_rowspan_and_colspan():
    rows = [
        [HtmlCell("指标", rowspan=2), HtmlCell("1月", colspan=2)],
        [HtmlCell("上旬"), HtmlCell("下旬")],
        [HtmlCell("采购经理指数"), HtmlCell("1/09:00"), HtmlCell("31/09:00")],
    ]
    grid = expand_table_grid(rows)
    assert grid == [
        ["指标", "1月", "1月"],
        ["指标", "上旬", "下旬"],
        ["采购经理指数", "1/09:00", "31/09:00"],
    ]


def test_extract_links_parses_anchor_text_and_href():
    html = """
    <html><body>
      <a href="/a1.html"> 2024年1月中国采购经理指数运行情况 </a>
      <a href="https://example.com/a2.html">其他链接</a>
    </body></html>
    """
    links = extract_links(html)
    assert links == [
        ("/a1.html", "2024年1月中国采购经理指数运行情况"),
        ("https://example.com/a2.html", "其他链接"),
    ]


def test_extract_pbc_search_results_parses_search_blocks():
    html = """
    <div class="searchMod">
      <h3><a href="https://www.pbc.gov.cn/a1.html">2024年3月金融统计数据报告</a></h3>
      <p class="txtCon">摘要内容A</p>
      <p class="dates"><span>发布日期</span><span>2024-04-12</span></p>
    </div>
    <div class="searchMod">
      <h3><a href="https://www.pbc.gov.cn/a2.html">2024年3月社会融资规模增量统计数据报告</a></h3>
      <p class="txtCon">摘要内容B</p>
      <p class="dates"><span>发布时间</span><span>2024-04-12</span></p>
    </div>
    """
    results = extract_pbc_search_results(html)
    assert len(results) == 2
    assert results[0].rank == 1
    assert results[0].title == "2024年3月金融统计数据报告"
    assert results[0].url == "https://www.pbc.gov.cn/a1.html"
    assert results[0].search_date == pd.Timestamp("2024-04-12")
    assert results[1].rank == 2
    assert results[1].title == "2024年3月社会融资规模增量统计数据报告"


def test_resolve_nbs_schedule_page_uses_known_official_mapping():
    title, url = resolve_nbs_schedule_page(2019)
    assert title == "2019年国家统计局主要统计信息发布日程表"
    assert url.startswith("https://www.stats.gov.cn/")
    assert resolve_nbs_schedule_page(2026) is None


@pytest.mark.asyncio
async def test_fetch_batch_returns_sorted_dataframe(monkeypatch):
    task = MacroReleaseCalendarTask(db_connection=_MockDB())

    async def fake_load_periods(start_date, end_date):
        return {
            "pmi": [pd.Timestamp("2024-01-31")],
            "money": [pd.Timestamp("2024-01-31")],
            "credit": [pd.Timestamp("2024-01-31")],
        }

    class _DummyHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def fake_resolve_indicator_with_progress(client, indicator_code, periods, stop_event=None):
        return (
            [
                CalendarRecord(
                    indicator_code=indicator_code,
                    period_end_date="2024-01-31",
                    release_date="2024-02-01",
                    release_time=None,
                    source_name="test",
                    source_title=f"{indicator_code} title",
                    source_url=f"https://example.com/{indicator_code}",
                    query_text=indicator_code,
                    match_method="unit_test",
                    search_rank=1,
                )
            ],
            [],
        )

    monkeypatch.setattr(task, "_load_periods", fake_load_periods)
    monkeypatch.setattr("alphahome.fetchers.tasks.macro.macro_release_calendar.HttpClient", _DummyHttpClient)
    monkeypatch.setattr(task, "_resolve_indicator_with_progress", fake_resolve_indicator_with_progress)

    df = await task.fetch_batch({"start_date": "20240101", "end_date": "20240228"})
    assert list(df["indicator_code"]) == ["credit", "money", "pmi"]
    assert list(df["release_date"].astype(str)) == ["2024-02-01", "2024-02-01", "2024-02-01"]


@pytest.mark.asyncio
async def test_fetch_batch_returns_empty_when_all_unresolved(monkeypatch):
    task = MacroReleaseCalendarTask(db_connection=_MockDB())

    async def fake_load_periods(start_date, end_date):
        return {"pmi": [pd.Timestamp("2024-01-31")], "money": [], "credit": []}

    class _DummyHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def fake_resolve_indicator_with_progress(client, indicator_code, periods, stop_event=None):
        if indicator_code == "pmi":
            return (
                [],
                [
                    UnresolvedRecord(
                        indicator_code="pmi",
                        period_end_date="2024-01-31",
                        attempted_queries="2024年1月中国采购经理指数运行情况",
                        reason="unit_test_unresolved",
                    )
                ],
            )
        return ([], [])

    monkeypatch.setattr(task, "_load_periods", fake_load_periods)
    monkeypatch.setattr("alphahome.fetchers.tasks.macro.macro_release_calendar.HttpClient", _DummyHttpClient)
    monkeypatch.setattr(task, "_resolve_indicator_with_progress", fake_resolve_indicator_with_progress)

    df = await task.fetch_batch({"start_date": "20240101", "end_date": "20240228"})
    assert df.empty
    assert list(df.columns) == list(task.schema_def.keys())


@pytest.mark.asyncio
async def test_fetch_batch_partial_resolve_saves_resolved(monkeypatch):
    """部分月份未解析时，已解析的记录仍应正常返回。"""
    task = MacroReleaseCalendarTask(db_connection=_MockDB())

    async def fake_load_periods(start_date, end_date):
        return {
            "pmi": [pd.Timestamp("2024-01-31"), pd.Timestamp("2024-02-29")],
            "money": [],
            "credit": [],
        }

    class _DummyHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def fake_resolve_indicator_with_progress(client, indicator_code, periods, stop_event=None):
        if indicator_code == "pmi" and periods:
            return (
                [
                    CalendarRecord(
                        indicator_code="pmi",
                        period_end_date="2024-01-31",
                        release_date="2024-02-01",
                        release_time=None,
                        source_name="test",
                        source_title="pmi title",
                        source_url="https://example.com/pmi",
                        query_text="pmi",
                        match_method="unit_test",
                        search_rank=1,
                    )
                ],
                [
                    UnresolvedRecord(
                        indicator_code="pmi",
                        period_end_date="2024-02-29",
                        attempted_queries="test",
                        reason="unit_test_unresolved",
                    )
                ],
            )
        return ([], [])

    monkeypatch.setattr(task, "_load_periods", fake_load_periods)
    monkeypatch.setattr("alphahome.fetchers.tasks.macro.macro_release_calendar.HttpClient", _DummyHttpClient)
    monkeypatch.setattr(task, "_resolve_indicator_with_progress", fake_resolve_indicator_with_progress)

    df = await task.fetch_batch({"start_date": "20240101", "end_date": "20240331"})
    assert len(df) == 1
    assert df.iloc[0]["indicator_code"] == "pmi"
    assert df.iloc[0]["period_end_date"] == "2024-01-31"


@pytest.mark.asyncio
async def test_fetch_batch_no_periods_returns_empty(monkeypatch):
    """所有月份已入库时，返回空 DataFrame 而非失败。"""
    task = MacroReleaseCalendarTask(db_connection=_MockDB())

    async def fake_load_periods(start_date, end_date):
        return {"pmi": [], "money": [], "credit": []}

    monkeypatch.setattr(task, "_load_periods", fake_load_periods)

    df = await task.fetch_batch({"start_date": "20240101", "end_date": "20240228"})
    assert df.empty
