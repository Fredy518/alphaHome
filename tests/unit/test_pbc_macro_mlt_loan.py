from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from alphahome.fetchers.tasks.macro.macro_release_calendar import (
    CalendarRecord,
    money_queries,
)
from alphahome.fetchers.tasks.macro.pbc_macro_mlt_loan import (
    PBCMacroMLTLoanTask,
    PBCMLTReport,
    normalize_pbc_mlt_reports,
    parse_pbc_mlt_report,
)


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return []

    async def table_exists(self, target):
        return False


def _calendar(period: str, release: str = "2025-04-13") -> CalendarRecord:
    return CalendarRecord(
        indicator_code="money",
        period_end_date=period,
        release_date=release,
        release_time=f"{release} 17:00:00",
        source_name="pbc.gov.cn",
        source_title="金融统计数据报告",
        source_url="https://www.pbc.gov.cn/report.html",
        query_text="金融统计数据报告",
        match_method="unit_test",
        search_rank=1,
    )


def _report(
    period: str,
    basis: str,
    household: float,
    enterprise: float,
) -> PBCMLTReport:
    ts = pd.Timestamp(period)
    return PBCMLTReport(
        period_end_date=ts,
        release_date=ts + pd.Timedelta(days=12),
        release_time=None,
        report_basis=basis,
        household_mlt_reported_100m_cny=household,
        enterprise_mlt_reported_100m_cny=enterprise,
        source_title="test",
        source_url="https://www.pbc.gov.cn/test",
        query_text="test",
        match_method="unit_test",
        search_rank=1,
        source_hash="a" * 64,
    )


def test_parse_monthly_report_reproduces_legacy_total_and_negative_amount():
    html = """
    <html><body>
      <p>二、11月份人民币贷款增加5229亿元</p>
      <p>分部门看，住户贷款增加2575亿元，其中短期贷款增加1060亿元，
      中长期贷款增加1515亿元；非金融企业及其他部门贷款增加2103亿元，
      其中短期贷款增加123亿元，中长期贷款减少31亿元，票据融资增加。</p>
    </body></html>
    """
    report = parse_pbc_mlt_report(
        html, pd.Timestamp("2012-11-30"), _calendar("2012-11-30", "2012-12-11")
    )
    assert report.report_basis == "monthly"
    assert report.household_mlt_reported_100m_cny == 1515
    assert report.enterprise_mlt_reported_100m_cny == -31

    frame = normalize_pbc_mlt_reports([report])
    assert frame.iloc[0]["total_mlt_monthly_100m_cny"] == 1484
    assert frame.iloc[0]["normalization_status"] == "direct_monthly"


def test_parse_ytd_report_converts_trillion_yuan_to_100m_cny():
    html = """
    <html><body>
      <p>二、一季度人民币贷款增加9.78万亿元</p>
      <p>分部门看，住户贷款增加1.04万亿元，其中，短期贷款增加1603亿元，
      中长期贷款增加8832亿元；企（事）业单位贷款增加8.66万亿元，其中，
      短期贷款增加3.51万亿元，中长期贷款增加5.58万亿元，票据融资减少。</p>
    </body></html>
    """
    report = parse_pbc_mlt_report(
        html, pd.Timestamp("2025-03-31"), _calendar("2025-03-31")
    )
    assert report.report_basis == "ytd"
    assert report.household_mlt_reported_100m_cny == 8832
    assert report.enterprise_mlt_reported_100m_cny == 55800


def test_parse_ytd_report_recognizes_first_two_months_wording():
    html = """
    <p>二、前两个月人民币贷款增加6.14万亿元</p>
    <p>前两个月人民币贷款增加6.14万亿元。分部门看，住户贷款增加547亿元，
    其中，短期贷款减少3238亿元，中长期贷款增加3785亿元；
    企（事）业单位贷款增加5.82万亿元，其中，短期贷款增加2.07万亿元，
    中长期贷款增加4万亿元，票据融资减少3456亿元。</p>
    """
    report = parse_pbc_mlt_report(
        html,
        pd.Timestamp("2025-02-28"),
        _calendar("2025-02-28", "2025-03-14"),
    )
    assert report.report_basis == "ytd"


def test_parse_monthly_components_use_nearest_anchor_after_ytd_heading():
    html = """
    <p>二、前七个月人民币贷款增加16.08万亿元</p>
    <p>前七个月人民币贷款增加16.08万亿元。
    7月份人民币贷款增加3459亿元。分部门看，
    住户贷款减少2007亿元，其中，短期贷款减少1335亿元，
    中长期贷款减少672亿元；企（事）业单位贷款增加2378亿元，
    其中，短期贷款减少3785亿元，中长期贷款增加2712亿元。</p>
    """
    report = parse_pbc_mlt_report(
        html,
        pd.Timestamp("2023-07-31"),
        _calendar("2023-07-31", "2023-08-11"),
    )
    assert report.report_basis == "monthly"
    assert report.household_mlt_reported_100m_cny == -672
    assert report.enterprise_mlt_reported_100m_cny == 2712


def test_normalize_mixed_monthly_and_ytd_reports_within_calendar_year():
    reports = [
        _report("2025-01-31", "monthly", 4935, 34600),
        _report("2025-02-28", "ytd", 3785, 40000),
        _report("2025-03-31", "ytd", 8832, 55800),
    ]
    frame = normalize_pbc_mlt_reports(reports)
    assert frame["household_mlt_monthly_100m_cny"].tolist() == [
        4935,
        -1150,
        5047,
    ]
    assert frame["enterprise_mlt_monthly_100m_cny"].tolist() == [
        34600,
        5400,
        15800,
    ]
    assert frame["total_mlt_monthly_100m_cny"].tolist() == [
        39535,
        4250,
        20847,
    ]
    assert frame["normalization_status"].tolist() == [
        "direct_monthly",
        "derived_from_ytd",
        "derived_from_ytd",
    ]


def test_quarter_end_money_queries_try_both_quarter_and_month_labels():
    queries = money_queries(pd.Timestamp("2025-03-31"))
    assert queries[0] == "2025年一季度金融统计数据报告"
    assert "2025年3月金融统计数据报告" in queries
    assert "2025年3月份金融统计数据报告" in queries


def test_task_is_a_normal_smart_incremental_task():
    task = PBCMacroMLTLoanTask(db_connection=_MockDB())
    assert task.name == "pbc_macro_mlt_loan"
    assert task.data_source == "pbc"
    assert task.date_column == "period_end_date"
    assert task.supports_incremental_update() is True


@pytest.mark.asyncio
async def test_fetch_batch_uses_january_anchor_then_filters_requested_window(
    monkeypatch,
):
    calls = []

    class _DummyHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def fake_fetch(client, period):
        calls.append(period)
        values = {
            1: (4935, 34600),
            2: (3785, 40000),
            3: (8832, 55800),
        }
        household, enterprise = values[period.month]
        return _report(
            str(period.date()),
            "monthly" if period.month == 1 else "ytd",
            household,
            enterprise,
        )

    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.pbc_macro_mlt_loan.HttpClient",
        _DummyHttpClient,
    )
    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.pbc_macro_mlt_loan.fetch_pbc_mlt_report",
        fake_fetch,
    )
    task = PBCMacroMLTLoanTask(db_connection=_MockDB())
    frame = await task.fetch_batch({"start_date": "20250301", "end_date": "20250331"})
    assert [item.date() for item in calls] == [
        date(2025, 1, 31),
        date(2025, 2, 28),
        date(2025, 3, 31),
    ]
    assert frame["period_end_date"].tolist() == [date(2025, 3, 31)]
    assert frame.iloc[0]["total_mlt_monthly_100m_cny"] == 20847


@pytest.mark.asyncio
async def test_fetch_batch_fails_closed_on_unexpected_historical_gap(monkeypatch):
    class _DummyHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def fake_fetch(client, period):
        if period.month == 2:
            return None
        return _report(
            str(period.date()),
            "monthly" if period.month == 1 else "ytd",
            100 * period.month,
            1000 * period.month,
        )

    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.pbc_macro_mlt_loan.HttpClient",
        _DummyHttpClient,
    )
    monkeypatch.setattr(
        "alphahome.fetchers.tasks.macro.pbc_macro_mlt_loan.fetch_pbc_mlt_report",
        fake_fetch,
    )
    task = PBCMacroMLTLoanTask(db_connection=_MockDB())

    with pytest.raises(RuntimeError, match="拒绝保存部分结果"):
        await task.fetch_batch({"start_date": "20250301", "end_date": "20250331"})
