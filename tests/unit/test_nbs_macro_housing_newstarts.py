from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks.macro.nbs_macro_housing_newstarts import (
    NBSHousingReport,
    NBSMacroHousingNewStartsTask,
    _ensure_complete_derivations,
    collect_nbs_housing_article_index,
    normalize_nbs_housing_reports,
    parse_nbs_housing_period,
    parse_nbs_housing_report,
)


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return []

    async def table_exists(self, target):
        return False


def _report(period: str, cumulative: float, yoy: float) -> NBSHousingReport:
    ts = pd.Timestamp(period)
    return NBSHousingReport(
        period_end_date=ts,
        release_date=ts + pd.Timedelta(days=15),
        release_time=None,
        cumulative_area_10k_sqm=cumulative,
        cumulative_yoy_reported=yoy,
        completed_cumulative_area_10k_sqm=cumulative / 2,
        completed_cumulative_yoy_reported=yoy - 1,
        sold_cumulative_area_10k_sqm=cumulative * 2,
        sold_cumulative_yoy_reported=yoy + 1,
        source_name="www.stats.gov.cn",
        source_title="test",
        source_url="https://www.stats.gov.cn/test",
        match_method="unit_test",
        source_hash="b" * 64,
    )


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("2026年1—7月份全国房地产市场基本情况", "2026-07-31"),
        ("2025年上半年全国房地产市场基本情况", "2025-06-30"),
        ("2021年1-2月份全国房地产开发和销售情况", "2021-02-28"),
        ("2022年全国房地产开发投资下降10.0%", "2022-12-31"),
        ("2020年1—12月份全国房地产开发投资和销售情况", "2020-12-31"),
    ],
)
def test_parse_nbs_housing_period_variants(title, expected):
    assert parse_nbs_housing_period(title) == pd.Timestamp(expected)


def test_parse_official_table_and_precise_release_time():
    html = """
    <html><head>
      <meta name="PubDate" content="2026/08/17 15:00" />
      <title>2026年1—7月份全国房地产市场基本情况</title>
    </head><body>
      <table><tr><th>指标</th><th>绝对量</th><th>同比增长（%）</th></tr>
      <tr><td>房屋新开工面积（万平方米）</td><td>26700</td><td>-24.0</td></tr>
      <tr><td>房屋竣工面积（万平方米）</td><td>11000</td><td>-15.0</td></tr>
      <tr><td>商品房销售面积（万平方米）</td><td>50000</td><td>-8.0</td></tr>
      </table>
    </body></html>
    """
    report = parse_nbs_housing_report(
        html, "https://www.stats.gov.cn/sj/zxfb/202608/example.html"
    )
    assert report.period_end_date == pd.Timestamp("2026-07-31")
    assert report.release_date == pd.Timestamp("2026-08-17")
    assert report.release_time == pd.Timestamp("2026-08-17 15:00")
    assert report.cumulative_area_10k_sqm == 26700
    assert report.cumulative_yoy_reported == -24.0
    assert report.completed_cumulative_area_10k_sqm == 11000
    assert report.completed_cumulative_yoy_reported == -15.0
    assert report.sold_cumulative_area_10k_sqm == 50000
    assert report.sold_cumulative_yoy_reported == -8.0
    assert report.match_method == "official_table"


def test_parse_information_disclosure_page_narrative_and_date_only():
    html = """
    <html><head><title>国家统计局信息公开</title></head><body>
      <div>成文日期 2021年09月15日</div>
      <h1>2021年1—8月份全国房地产开发投资增长10.9%</h1>
      <p>房屋新开工面积 135502 万平方米，下降 3.2%。</p>
      <p>房屋竣工面积 46739 万平方米，增长 26.0%。</p>
      <p>商品房销售面积 114193 万平方米，增长 15.9%。</p>
    </body></html>
    """
    report = parse_nbs_housing_report(
        html,
        "https://www.stats.gov.cn/xxgk/sjfb/zxfb2020/202109/example.html",
    )
    assert report.period_end_date == pd.Timestamp("2021-08-31")
    assert report.release_date == pd.Timestamp("2021-09-15")
    assert report.release_time is None
    assert report.cumulative_area_10k_sqm == 135502
    assert report.cumulative_yoy_reported == -3.2
    assert report.completed_cumulative_area_10k_sqm == 46739
    assert report.completed_cumulative_yoy_reported == 26.0
    assert report.sold_cumulative_area_10k_sqm == 114193
    assert report.sold_cumulative_yoy_reported == 15.9
    assert report.match_method == "official_narrative"


def test_normalize_cumulative_area_and_derive_same_bucket_yoy():
    reports = [
        _report("2021-02-28", 17037, 64.3),
        _report("2021-03-31", 36163, 28.2),
        _report("2022-02-28", 14967, -12.2),
        _report("2022-03-31", 29838, -17.5),
    ]
    frame = normalize_nbs_housing_reports(reports)
    assert frame["monthly_area_10k_sqm"].tolist() == [
        17037,
        19126,
        14967,
        14871,
    ]
    assert frame.iloc[0]["monthly_bucket"] == "jan_feb_combined"
    assert frame.iloc[1]["monthly_bucket"] == "calendar_month"
    assert frame.iloc[2]["monthly_yoy_derived"] == pytest.approx(
        (14967 / 17037 - 1) * 100
    )
    assert frame.iloc[3]["monthly_yoy_derived"] == pytest.approx(
        (14871 / 19126 - 1) * 100
    )
    assert frame.iloc[3]["completed_monthly_yoy_derived"] == pytest.approx(
        (14871 / 19126 - 1) * 100
    )
    assert frame.iloc[3]["sold_monthly_yoy_derived"] == pytest.approx(
        (14871 / 19126 - 1) * 100
    )


def test_incomplete_cumulative_chain_fails_closed():
    frame = normalize_nbs_housing_reports(
        [
            _report("2026-05-31", 50000, -10),
            _report("2026-07-31", 70000, -12),
        ]
    )

    with pytest.raises(RuntimeError, match="连续月份缺口"):
        _ensure_complete_derivations(
            frame,
            output_start=pd.Timestamp("2026-01-01"),
            output_end=pd.Timestamp("2026-12-31"),
        )


def test_yoy_completeness_applies_only_to_requested_output_window():
    frame = normalize_nbs_housing_reports(
        [
            _report("2025-02-28", 17000, -10),
            _report("2025-03-31", 35000, -11),
            _report("2026-02-28", 15000, -12),
            _report("2026-03-31", 30000, -13),
        ]
    )

    _ensure_complete_derivations(
        frame,
        output_start=pd.Timestamp("2026-01-01"),
        output_end=pd.Timestamp("2026-12-31"),
    )

    with pytest.raises(RuntimeError, match="月同比派生链不完整"):
        _ensure_complete_derivations(
            frame,
            output_start=pd.Timestamp("2025-01-01"),
            output_end=pd.Timestamp("2025-12-31"),
        )


@pytest.mark.asyncio
async def test_listing_collector_discovers_official_report_and_deduplicates_links():
    class _DummyClient:
        async def get_text(self, url):
            return """
            <a href="/sj/zxfb/202608/t20260817_1.html">
              2026年1—7月份全国房地产市场基本情况
            </a>
            <a href="/sj/zxfb/202608/t20260817_1.html">
              2026年1—7月份全国房地产市场基本情况
            </a>
            """

    result = await collect_nbs_housing_article_index(
        _DummyClient(),
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-08-31"),
        max_pages=1,
    )
    assert result == {
        pd.Timestamp("2026-07-31"): (
            "2026年1—7月份全国房地产市场基本情况",
            "https://www.stats.gov.cn/sj/zxfb/202608/t20260817_1.html",
        )
    }


@pytest.mark.asyncio
async def test_fetch_batch_fails_closed_on_article_error(monkeypatch):
    period = pd.Timestamp("2026-07-31")

    class _DummyHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get_text(self, url):
            raise RuntimeError("temporary source failure")

    async def fake_collect(*args, **kwargs):
        return {
            period: (
                "2026年1—7月份全国房地产市场基本情况",
                "https://www.stats.gov.cn/sj/zxfb/202608/example.html",
            )
        }

    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.nbs_macro_housing_newstarts.HttpClient",
        _DummyHttpClient,
    )
    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.nbs_macro_housing_newstarts.collect_nbs_housing_article_index",
        fake_collect,
    )
    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.nbs_macro_housing_newstarts._expected_published_periods",
        lambda *args, **kwargs: {period},
    )
    task = NBSMacroHousingNewStartsTask(db_connection=_MockDB())

    with pytest.raises(RuntimeError, match="拒绝保存部分结果"):
        await task.fetch_batch({"start_date": "20260701", "end_date": "20260731"})


def test_tasks_are_registered_for_normal_gui_smart_execution():
    assert "pbc_macro_mlt_loan" in UnifiedTaskFactory._task_registry
    assert "nbs_macro_housing_newstarts" in UnifiedTaskFactory._task_registry

    task = NBSMacroHousingNewStartsTask(db_connection=_MockDB())
    assert task.data_source == "nbs"
    assert task.date_column == "period_end_date"
    assert task.supports_incremental_update() is True
    assert task.default_start_date == "20210228"
    assert date(2021, 2, 28) == pd.Timestamp(task.default_start_date).date()
