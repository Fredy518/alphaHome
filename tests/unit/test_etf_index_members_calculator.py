from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.etf_index_members_calculator import (
    ETFIndexMembersCalculator,
)
from alphahome.pit.pit_etf_index_members_manager import (
    PITETFIndexMembersMonthlyManager,
)


def _official(weight_date: str = "2026-01-30") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "index_code": ["IDX"] * 5,
            "index_name": ["测试指数"] * 5,
            "weight_trade_date": [weight_date] * 5,
            "ts_code": [f"00000{i}.SZ" for i in range(1, 6)],
            "raw_weight": [20.0] * 5,
        }
    )


def _holding_rows(
    *, ann_date: str, end_date: str, start: int, count: int, total_weight: float
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "index_code": ["IDX"] * count,
            "index_name": ["测试指数"] * count,
            "etf_code": ["510000.SH"] * count,
            "ann_date": [ann_date] * count,
            "end_date": [end_date] * count,
            "ts_code": [f"{start + i:06d}.SZ" for i in range(count)],
            "raw_weight": [total_weight / count] * count,
        }
    )


def test_official_weight_has_priority_over_etf_holding_proxy():
    holdings = _holding_rows(
        ann_date="2026-01-20",
        end_date="2025-12-31",
        start=100,
        count=10,
        total_weight=70.0,
    )

    result = ETFIndexMembersCalculator().calculate(
        _official(), holdings, ["2026-01-31"], ["IDX"]
    )

    assert set(result["weight_basis"]) == {"official_index_weight"}
    assert set(result["weight_source"]) == {"rawdata.index_weight"}
    assert not result["is_fallback"].any()
    assert result["weight"].sum() == 1.0


def test_holding_fallback_combines_visible_top_ten_and_residual_disclosure():
    top_ten = _holding_rows(
        ann_date="2026-01-20",
        end_date="2025-12-31",
        start=100,
        count=10,
        total_weight=60.0,
    )
    residual = _holding_rows(
        ann_date="2026-03-31",
        end_date="2025-12-31",
        start=200,
        count=10,
        total_weight=40.0,
    )
    holdings = pd.concat([top_ten, residual], ignore_index=True)
    calculator = ETFIndexMembersCalculator()

    result = calculator.calculate(
        pd.DataFrame(), holdings, ["2026-01-31", "2026-04-30"], ["IDX"]
    )

    january = result.loc[result["obs_date"].eq(pd.Timestamp("2026-01-31"))]
    april = result.loc[result["obs_date"].eq(pd.Timestamp("2026-04-30"))]
    assert january["source_quality"].unique().tolist() == ["partial"]
    assert january["source_member_count"].unique().tolist() == [10]
    assert january["source_weight_sum"].unique().tolist() == [60.0]
    assert january["source_available_date"].max() == pd.Timestamp("2026-01-20")
    assert april["source_quality"].unique().tolist() == ["high"]
    assert april["source_member_count"].unique().tolist() == [20]
    assert april["source_weight_sum"].unique().tolist() == [100.0]
    assert april["source_available_date"].max() == pd.Timestamp("2026-03-31")
    assert april["weight"].sum() == pytest.approx(1.0)


def test_holding_fallback_normalizes_zero_padded_hk_aliases():
    rows = []
    for number in range(20, 30):
        for symbol in (f"{number:05d}.HK", f"{number}.HK"):
            rows.append(
                {
                    "index_code": "IDX",
                    "index_name": "测试指数",
                    "etf_code": "526030.SH",
                    "ann_date": "2026-06-23",
                    "end_date": "2026-06-18",
                    "ts_code": symbol,
                    "raw_weight": 7.2,
                }
            )
    holdings = pd.DataFrame(rows)

    result = ETFIndexMembersCalculator().calculate(
        pd.DataFrame(), holdings, ["2026-08-31"], ["IDX"]
    )

    assert len(result) == 10
    assert result["source_member_count"].unique().tolist() == [10]
    assert result["source_weight_sum"].unique().tolist() == [pytest.approx(72.0)]
    assert result["source_coverage_rate"].unique().tolist() == [pytest.approx(0.72)]
    assert result["source_quality"].unique().tolist() == ["partial"]
    assert result["is_eligible"].all()
    assert result["weight"].sum() == pytest.approx(1.0)


@pytest.mark.parametrize("value", [None, pd.NA, pd.NaT, float("nan")])
def test_member_code_normalization_treats_missing_scalars_as_empty(value):
    assert ETFIndexMembersCalculator.normalize_member_code(value) == ""


def test_future_holding_announcement_is_not_visible():
    holdings = _holding_rows(
        ann_date="2026-03-31",
        end_date="2025-12-31",
        start=100,
        count=10,
        total_weight=60.0,
    )

    result = ETFIndexMembersCalculator().calculate(
        pd.DataFrame(), holdings, ["2026-01-31"], ["IDX"]
    )

    assert result.empty
    assert ETFIndexMembersCalculator().METHOD_VERSION


def test_stale_holding_is_retained_as_ineligible_evidence():
    holdings = _holding_rows(
        ann_date="2025-01-20",
        end_date="2024-12-31",
        start=100,
        count=20,
        total_weight=100.0,
    )

    result = ETFIndexMembersCalculator().calculate(
        pd.DataFrame(), holdings, ["2026-01-31"], ["IDX"]
    )

    assert set(result["source_quality"]) == {"high"}
    assert not result["is_eligible"].any()
    assert all("holding_stale_over_limit" in row for row in result["quality_reasons"])


def test_manager_load_sources_accepts_empty_query_without_columns():
    official = _official().rename(columns={"weight_trade_date": "weight_trade_date"})

    class FakeContext:
        def __init__(self):
            self.calls = 0

        def query_dataframe(self, _sql, _params):
            self.calls += 1
            if self.calls == 1:
                return official.copy()
            return pd.DataFrame()

    manager = PITETFIndexMembersMonthlyManager()
    manager.context = FakeContext()
    manager._load_index_names = lambda _codes: {"IDX": "测试指数"}

    sources = manager._load_sources([pd.Timestamp("2026-01-31").date()], ["IDX"])

    assert sources["official_weights"]["index_name"].tolist() == ["测试指数"] * 5
    assert sources["fund_holdings"].empty
    assert set(
        [
            "index_code",
            "index_name",
            "etf_code",
            "ann_date",
            "end_date",
            "ts_code",
            "raw_weight",
        ]
    ).issubset(sources["fund_holdings"].columns)
