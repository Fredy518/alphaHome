#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""FRED 宏观数据采集任务单元测试。

测试策略（对齐 test_akshare_macro_tasks.py 风格）：
- 不触网：用 _FakeFredAPI 替换真实 FredAPI，直接返回构造的 CSV 解析结果。
- _MockDB 占位数据库。
- 内联构造原始 DataFrame，验证 process_data 的列重命名、类型转换、日期规整、去重。
- FredAPI 单独测 CSV 解析（monkeypatch requests）。
"""

from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.sources.fred.fred_api import FredAPI, FredAPIError
from alphahome.fetchers.sources.yahoo.yahoo_api import YahooAPI
from alphahome.fetchers.tasks.macro.fred_macro_credit_spread import (
    FredMacroCreditSpreadTask,
)
from alphahome.fetchers.tasks.macro.fred_macro_dxy import FredMacroDxyTask
from alphahome.fetchers.tasks.macro.fred_macro_fed_balance import (
    FredMacroFedBalanceTask,
)
from alphahome.fetchers.tasks.macro.fred_macro_fed_rate import FredMacroFedRateTask
from alphahome.fetchers.tasks.macro.fred_macro_sofr import FredMacroSofrTask
from alphahome.fetchers.tasks.macro.fred_macro_sofr_term import FredMacroSofrTermTask
from alphahome.fetchers.tasks.macro.fred_macro_ted import FredMacroTedTask
from alphahome.fetchers.tasks.macro.fred_macro_treasury_yield import (
    FredMacroTreasuryYieldTask,
)
from alphahome.fetchers.tasks.macro.fred_macro_us_short_rate import (
    FredMacroUsShortRateTask,
)
from alphahome.fetchers.tasks.macro.fred_macro_vix import FredMacroVixTask


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return []

    async def table_exists(self, target):
        return False


class _FakeFredAPI:
    """替身 FredAPI：按 series_id 返回预设的 DataFrame（已为 CSV 解析后形态）。"""

    def __init__(self, data: Dict[str, pd.DataFrame]):
        self.data = data
        self.calls: List[str] = []

    async def fetch_series(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        stop_event=None,
    ) -> Optional[pd.DataFrame]:
        self.calls.append(series_id)
        df = self.data.get(series_id)
        return None if df is None else df.copy()


def _make_task(task_cls, fake_api, update_type=UpdateTypes.FULL, **kwargs):
    return task_cls(db_connection=_MockDB(), api=fake_api, update_type=update_type, **kwargs)


def _run_fred_process(task, df: pd.DataFrame) -> pd.DataFrame:
    """直接调用 process_data（跳过 fetch_batch 的网络层）。"""
    return task.process_data(df)


# --------------------------------------------------------------------------
# FredAPI CSV 解析测试
# --------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


def test_fred_api_parses_csv_and_handles_dot_as_nan(monkeypatch):
    csv_text = (
        "observation_date,DFEDTARU\n"
        "2024-01-01,3.75\n"
        "2024-01-02,.\n"
        "2024-01-03,4.00\n"
    )
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured["params"] = params
        return _FakeResponse(csv_text, 200)

    monkeypatch.setattr("alphahome.fetchers.sources.fred.fred_api.requests.get", fake_get)

    api = FredAPI(request_interval=0, max_retries=1)
    import asyncio

    df = asyncio.run(api.fetch_series("DFEDTARU", start_date="20240101", end_date="20240103"))

    assert captured["params"] == {
        "id": "DFEDTARU",
        "cosd": "2024-01-01",
        "coed": "2024-01-03",
    }
    assert list(df.columns) == ["observation_date", "DFEDTARU"]
    assert len(df) == 3
    assert df.iloc[0]["DFEDTARU"] == 3.75
    assert pd.isna(df.iloc[1]["DFEDTARU"])  # "." → NaN
    assert df.iloc[2]["DFEDTARU"] == 4.00


def test_fred_api_raises_on_4xx(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        return _FakeResponse("Bad Request", 400)

    monkeypatch.setattr("alphahome.fetchers.sources.fred.fred_api.requests.get", fake_get)

    api = FredAPI(request_interval=0, max_retries=1)
    import asyncio

    with pytest.raises(FredAPIError):
        asyncio.run(api.fetch_series("BAD_SERIES"))


# --------------------------------------------------------------------------
# 单序列 FRED 任务 process_data 测试
# --------------------------------------------------------------------------

def test_dxy_attributes():
    assert FredMacroDxyTask.name == "fred_macro_dxy"
    assert FredMacroDxyTask.table_name == "macro_dxy"
    assert FredMacroDxyTask.primary_keys == ["date"]
    assert FredMacroDxyTask.series_ids == ["DTWEXBGS"]
    assert FredMacroDxyTask.data_source == "fred"


def test_dxy_process_data_converts_types_and_deduplicates():
    task = _make_task(FredMacroDxyTask, _FakeFredAPI({}))
    # process_data 接收已重命名列（fetch_batch 负责序列 ID → 目标列名重命名）
    raw = pd.DataFrame(
        [
            {"date": "2024-04-01", "dxy_close": "119.50"},
            {"date": "2024-04-01", "dxy_close": "119.60"},  # 去重保留最后
            {"date": "2024-04-02", "dxy_close": "120.10"},
        ]
    )
    processed = _run_fred_process(task, raw)

    assert len(processed) == 2
    assert processed["date"].tolist() == [date(2024, 4, 1), date(2024, 4, 2)]
    assert processed["dxy_close"].tolist() == [119.60, 120.10]


def test_vix_process_data_drops_null_dates():
    task = _make_task(FredMacroVixTask, _FakeFredAPI({}))
    raw = pd.DataFrame(
        [
            {"date": "2024-04-01", "vix_close": 18.44},
            {"date": "invalid", "vix_close": 20.00},  # 日期解析失败，丢弃
            # 真实流程中 "." 已由 FredAPI 的 CSV 解析转为 NaN（pd.read_csv na_values=["."]）
            {"date": "2024-04-02", "vix_close": float("nan")},
        ]
    )
    processed = _run_fred_process(task, raw)

    assert len(processed) == 2
    assert processed["date"].tolist() == [date(2024, 4, 1), date(2024, 4, 2)]
    assert processed["vix_close"].iloc[0] == 18.44
    assert pd.isna(processed["vix_close"].iloc[1])


def test_sofr_attributes_and_process_data():
    assert FredMacroSofrTask.series_ids == ["SOFR"]
    task = _make_task(FredMacroSofrTask, _FakeFredAPI({}))
    raw = pd.DataFrame(
        [{"date": "2024-04-01", "sofr": "3.63"}]
    )
    processed = _run_fred_process(task, raw)
    assert processed["date"].tolist() == [date(2024, 4, 1)]
    assert processed["sofr"].tolist() == [3.63]


def test_ted_attributes_marked_discontinued():
    assert FredMacroTedTask.series_ids == ["TEDRATE"]
    # 文档/注释应体现序列已停用
    assert "2022" in FredMacroTedTask.schema_def["ted_spread"]["comment"]


# --------------------------------------------------------------------------
# 多序列合并任务（fed_rate）fetch_batch 测试
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fed_rate_fetch_batch_merges_three_series_on_date():
    fake_api = _FakeFredAPI(
        {
            "DFEDTARU": pd.DataFrame(
                [
                    {"observation_date": "2024-04-01", "DFEDTARU": "3.75"},
                    {"observation_date": "2024-04-02", "DFEDTARU": "3.75"},
                ]
            ),
            "DFEDTARL": pd.DataFrame(
                [
                    {"observation_date": "2024-04-01", "DFEDTARL": "3.50"},
                    {"observation_date": "2024-04-02", "DFEDTARL": "3.50"},
                ]
            ),
            # DFF 缺少 04-02，外连接后该日 effective_rate 为 NaN
            "DFF": pd.DataFrame(
                [{"observation_date": "2024-04-01", "DFF": "3.63"}]
            ),
        }
    )
    task = _make_task(FredMacroFedRateTask, fake_api, update_type=UpdateTypes.FULL)

    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240402"})

    # 三序列均被请求
    assert set(fake_api.calls) == {"DFEDTARU", "DFEDTARL", "DFF"}
    assert len(merged) == 2
    # fetch_batch 仅做合并与列重命名，日期与数值仍为字符串（process_data 才做类型转换）
    assert merged["date"].tolist() == ["2024-04-01", "2024-04-02"]
    assert merged["target_upper"].tolist() == ["3.75", "3.75"]
    assert merged["target_lower"].tolist() == ["3.50", "3.50"]
    assert merged["effective_rate"].iloc[0] == "3.63"
    assert pd.isna(merged["effective_rate"].iloc[1])  # DFF 缺失日


@pytest.mark.asyncio
async def test_fed_rate_fetch_batch_returns_none_when_all_series_empty():
    fake_api = _FakeFredAPI({})  # 无数据
    task = _make_task(FredMacroFedRateTask, fake_api, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240402"})
    assert merged is None


# --------------------------------------------------------------------------
# MANUAL 窗口过滤测试
# --------------------------------------------------------------------------

def test_dxy_process_data_applies_manual_window():
    task = _make_task(
        FredMacroDxyTask,
        _FakeFredAPI({}),
        update_type=UpdateTypes.MANUAL,
        start_date="2024-04-02",
        end_date="2024-04-03",
    )
    raw = pd.DataFrame(
        [
            {"date": "2024-04-01", "dxy_close": "119.50"},
            {"date": "2024-04-02", "dxy_close": "120.10"},
            {"date": "2024-04-03", "dxy_close": "120.50"},
            {"date": "2024-04-04", "dxy_close": "121.00"},
        ]
    )
    # 模拟 FetcherTask 已设置生效窗口（MANUAL 模式下 _effective_* 来自 start/end_date）
    task._effective_start_date = "20240402"
    task._effective_end_date = "20240403"

    processed = _run_fred_process(task, raw)

    assert processed["date"].tolist() == [date(2024, 4, 2), date(2024, 4, 3)]


# --------------------------------------------------------------------------
# Yahoo v8 fallback 测试
# --------------------------------------------------------------------------

def test_yahoo_api_parses_v8_json(monkeypatch):
    """YahooAPI 正确解析 v8 chart JSON：timestamp + indicators.quote.close。"""
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {"symbol": "^VIX"},
                    "timestamp": [1718942400, 1719115200, 1719201600],
                    "indicators": {
                        "quote": [
                            {"open": [18.0, None, 18.5], "close": [18.44, None, 18.2],
                             "high": [18.6, None, 18.7], "low": [18.1, None, 18.0]}
                        ]
                    },
                }
            ]
        }
    }

    class _FakeResp:
        status_code = 200
        def json(self):
            return payload

    captured = {}

    def fake_yahoo_get(url, params=None, headers=None, timeout=None):
        captured["params"] = params
        return _FakeResp()

    monkeypatch.setattr(
        "alphahome.fetchers.sources.yahoo.yahoo_api.requests.get",
        fake_yahoo_get,
    )

    api = YahooAPI()
    import asyncio
    df = asyncio.run(api.fetch_close("^VIX", start_date="20240620", end_date="20240624"))

    # 第二个点 close=None 应被丢弃
    assert len(df) == 2
    assert list(df.columns) == ["observation_date", "close"]
    assert df["close"].tolist() == [18.44, 18.2]
    assert captured["params"]["period2"] == int(
        datetime(2024, 6, 25, tzinfo=timezone.utc).timestamp()
    )


def test_yahoo_api_raises_on_4xx(monkeypatch):
    class _FakeResp:
        status_code = 404
        text = "Not Found"
    monkeypatch.setattr(
        "alphahome.fetchers.sources.yahoo.yahoo_api.requests.get",
        lambda url, params=None, headers=None, timeout=None: _FakeResp(),
    )
    api = YahooAPI()
    import asyncio
    from alphahome.fetchers.sources.yahoo.yahoo_api import YahooAPIError
    with pytest.raises(YahooAPIError):
        asyncio.run(api.fetch_close("BADSYM"))


class _FailingFredAPI(_FakeFredAPI):
    """FRED 总是抛异常的替身，用于触发 Yahoo fallback。"""

    async def fetch_series(self, series_id, start_date=None, end_date=None, stop_event=None):
        self.calls.append(series_id)
        raise FredAPIError(f"FRED {series_id} unreachable (simulated)")


@pytest.mark.asyncio
async def test_dxy_falls_back_to_yahoo_when_fred_fails(monkeypatch):
    """FRED 不可达时，DXY 任务自动 fallback 至 Yahoo v8 取 DX-Y.NYB。"""
    fake_fred = _FailingFredAPI({})

    async def fake_yahoo_close(self, symbol, start_date=None, end_date=None, stop_event=None):
        assert symbol == "DX-Y.NYB"
        return pd.DataFrame(
            [
                {"observation_date": "2024-04-01", "close": 105.80},
                {"observation_date": "2024-04-02", "close": 105.47},
            ]
        )

    monkeypatch.setattr(YahooAPI, "fetch_close", fake_yahoo_close)

    task = _make_task(FredMacroDxyTask, fake_fred, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240402"})

    # FRED 被调用（并失败），Yahoo fallback 接管
    assert fake_fred.calls == ["DTWEXBGS"]
    assert merged is not None
    assert merged["date"].tolist() == ["2024-04-01", "2024-04-02"]
    # Yahoo 的 close 列被重命名为 DTWEXBGS，再由 column_mapping 映射为 dxy_close
    assert "dxy_close" in merged.columns
    assert merged["dxy_close"].tolist() == [105.80, 105.47]


@pytest.mark.asyncio
async def test_vix_falls_back_to_yahoo(monkeypatch):
    """VIX 任务 FRED 失败时 fallback 至 ^VIX。"""
    fake_fred = _FailingFredAPI({})

    async def fake_yahoo_close(self, symbol, start_date=None, end_date=None, stop_event=None):
        assert symbol == "^VIX"
        return pd.DataFrame(
            [{"observation_date": "2024-04-01", "close": 18.44}]
        )

    monkeypatch.setattr(YahooAPI, "fetch_close", fake_yahoo_close)

    task = _make_task(FredMacroVixTask, fake_fred, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240401"})

    assert merged is not None
    assert merged["vix_close"].tolist() == [18.44]


@pytest.mark.asyncio
async def test_fed_rate_has_no_yahoo_fallback():
    """fed_rate 任务未声明 yahoo_fallback，FRED 失败时应显式失败。"""
    fake_fred = _FailingFredAPI({})
    task = _make_task(FredMacroFedRateTask, fake_fred, update_type=UpdateTypes.FULL)
    with pytest.raises(FredAPIError):
        await task.fetch_batch({"start_date": "20240401", "end_date": "20240402"})
    assert fake_fred.calls == ["DFEDTARU"]


# --------------------------------------------------------------------------
# SOFR 体系任务测试
# --------------------------------------------------------------------------

def test_sofr_term_attributes():
    assert FredMacroSofrTermTask.name == "fred_macro_sofr_term"
    assert FredMacroSofrTermTask.table_name == "macro_sofr_term"
    assert FredMacroSofrTermTask.series_ids == [
        "SOFR30DAYAVG",
        "SOFR90DAYAVG",
        "SOFR180DAYAVG",
    ]
    assert FredMacroSofrTermTask.data_source == "fred"


@pytest.mark.asyncio
async def test_sofr_term_fetch_batch_merges_three_term_series():
    fake_api = _FakeFredAPI(
        {
            "SOFR30DAYAVG": pd.DataFrame(
                [
                    {"observation_date": "2024-04-01", "SOFR30DAYAVG": "3.61"},
                    {"observation_date": "2024-04-02", "SOFR30DAYAVG": "3.62"},
                ]
            ),
            "SOFR90DAYAVG": pd.DataFrame(
                [
                    {"observation_date": "2024-04-01", "SOFR90DAYAVG": "3.63"},
                    # 04-02 缺失
                ]
            ),
            "SOFR180DAYAVG": pd.DataFrame(
                [
                    {"observation_date": "2024-04-01", "SOFR180DAYAVG": "3.67"},
                    {"observation_date": "2024-04-02", "SOFR180DAYAVG": "3.68"},
                ]
            ),
        }
    )
    task = _make_task(FredMacroSofrTermTask, fake_api, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240402"})

    assert set(fake_api.calls) == {"SOFR30DAYAVG", "SOFR90DAYAVG", "SOFR180DAYAVG"}
    assert len(merged) == 2
    assert merged["date"].tolist() == ["2024-04-01", "2024-04-02"]
    assert merged["sofr_30d"].tolist() == ["3.61", "3.62"]
    # 04-02 的 sofr_90d 因 SOFR90DAYAVG 缺失应为 NaN
    assert merged["sofr_90d"].iloc[0] == "3.63"
    assert pd.isna(merged["sofr_90d"].iloc[1])


def test_us_short_rate_attributes():
    assert FredMacroUsShortRateTask.name == "fred_macro_us_short_rate"
    assert FredMacroUsShortRateTask.table_name == "macro_us_short_rate"
    assert FredMacroUsShortRateTask.series_ids == ["IORB", "OBFR", "RRPONTSYAWARD"]
    # iorb 列注释应体现 SOFR-IORB 利差替代 TED 的用途
    assert "SOFR-IORB" in FredMacroUsShortRateTask.schema_def["iorb"]["comment"]


@pytest.mark.asyncio
async def test_us_short_rate_fetch_batch_merges_with_mismatched_starts():
    # 三序列起点不一（ON RRP 2013、OBFR 2016、IORB 2021），外连接后早期日期部分列为 NaN
    fake_api = _FakeFredAPI(
        {
            "RRPONTSYAWARD": pd.DataFrame(
                [{"observation_date": "2015-01-01", "RRPONTSYAWARD": "0.25"}]
            ),
            "OBFR": pd.DataFrame(
                [{"observation_date": "2015-01-01", "OBFR": "0.12"}]
            ),
            # IORB 2015 年尚无数据
        }
    )
    task = _make_task(FredMacroUsShortRateTask, fake_api, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20150101", "end_date": "20150102"})

    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["on_rrp"] == "0.25"
    assert row["obfr"] == "0.12"
    assert pd.isna(row["iorb"])  # IORB 2015 年无数据


def test_treasury_yield_attributes():
    assert FredMacroTreasuryYieldTask.name == "fred_macro_treasury_yield"
    assert FredMacroTreasuryYieldTask.table_name == "macro_treasury_yield"
    assert FredMacroTreasuryYieldTask.series_ids == ["DGS1MO", "DGS3MO"]
    # yield_3m 注释应体现 TED 替代用途
    assert "SOFR-国债" in FredMacroTreasuryYieldTask.schema_def["yield_3m"]["comment"]


@pytest.mark.asyncio
async def test_treasury_yield_fetch_batch_merges_two_tenors():
    fake_api = _FakeFredAPI(
        {
            "DGS1MO": pd.DataFrame(
                [{"observation_date": "2024-04-01", "DGS1MO": "3.68"}]
            ),
            "DGS3MO": pd.DataFrame(
                [
                    {"observation_date": "2024-04-01", "DGS3MO": "3.83"},
                    {"observation_date": "2024-04-02", "DGS3MO": "3.85"},
                ]
            ),
        }
    )
    task = _make_task(FredMacroTreasuryYieldTask, fake_api, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240402"})

    assert len(merged) == 2
    assert merged["yield_1m"].iloc[0] == "3.68"
    assert pd.isna(merged["yield_1m"].iloc[1])  # DGS1MO 04-02 缺失
    assert merged["yield_3m"].tolist() == ["3.83", "3.85"]


# --------------------------------------------------------------------------
# 第二批：美联储资产负债表 + 信用利差
# --------------------------------------------------------------------------

def test_fed_balance_attributes():
    assert FredMacroFedBalanceTask.name == "fred_macro_fed_balance"
    assert FredMacroFedBalanceTask.table_name == "macro_fed_balance"
    assert FredMacroFedBalanceTask.series_ids == ["WALCL"]
    assert FredMacroFedBalanceTask.data_source == "fred"
    assert FredMacroFedBalanceTask.column_mapping == {"WALCL": "total_assets"}


@pytest.mark.asyncio
async def test_fed_balance_fetch_batch_single_series():
    fake_api = _FakeFredAPI(
        {
            "WALCL": pd.DataFrame(
                [
                    {"observation_date": "2024-04-03", "WALCL": "7400000"},
                    {"observation_date": "2024-04-10", "WALCL": "7380000"},
                ]
            )
        }
    )
    task = _make_task(FredMacroFedBalanceTask, fake_api, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240403", "end_date": "20240410"})

    assert fake_api.calls == ["WALCL"]
    assert merged["date"].tolist() == ["2024-04-03", "2024-04-10"]
    assert merged["total_assets"].tolist() == ["7400000", "7380000"]


def test_credit_spread_attributes():
    assert FredMacroCreditSpreadTask.name == "fred_macro_credit_spread"
    assert FredMacroCreditSpreadTask.table_name == "macro_credit_spread"
    assert FredMacroCreditSpreadTask.series_ids == ["BAAFF"]
    assert FredMacroCreditSpreadTask.column_mapping == {"BAAFF": "credit_spread"}


@pytest.mark.asyncio
async def test_credit_spread_fetch_batch():
    fake_api = _FakeFredAPI(
        {
            "BAAFF": pd.DataFrame(
                [{"observation_date": "2024-04-01", "BAAFF": "2.36"}]
            )
        }
    )
    task = _make_task(FredMacroCreditSpreadTask, fake_api, update_type=UpdateTypes.FULL)
    merged = await task.fetch_batch({"start_date": "20240401", "end_date": "20240401"})

    assert merged["date"].tolist() == ["2024-04-01"]
    assert merged["credit_spread"].tolist() == ["2.36"]
