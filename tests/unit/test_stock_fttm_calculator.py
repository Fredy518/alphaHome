from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.stock_fttm_calculator import StockFTTMCalculator


def _rows(
    report_date: str,
    fy1: float | None,
    fy2: float | None,
    *,
    ts_code: str = "000001.SZ",
    org_name: str = "机构A",
    author_name: str = "作者A",
    eps1: float | None = None,
    eps2: float | None = None,
) -> pd.DataFrame:
    year = pd.Timestamp(report_date).year
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "org_name": org_name,
                "author_name": author_name,
                "report_date": report_date,
                "quarter": f"{year}Q4",
                "np": fy1,
                "eps": eps1,
                "total_share": 100.0,
            },
            {
                "ts_code": ts_code,
                "org_name": org_name,
                "author_name": author_name,
                "report_date": report_date,
                "quarter": f"{year + 1}Q4",
                "np": fy2,
                "eps": eps2,
                "total_share": 100.0,
            },
        ]
    )


@pytest.mark.parametrize(
    ("report_date", "expected_weights", "expected"),
    [
        ("2024-02-20", (1.0, 0.0), 100.0),
        ("2024-05-20", (0.75, 0.25), 125.0),
        ("2024-08-20", (0.5, 0.5), 150.0),
        ("2024-12-20", (0.25, 0.75), 175.0),
    ],
)
def test_report_quarter_anchors_weights(report_date, expected_weights, expected):
    result = StockFTTMCalculator().calculate(
        _rows(report_date, 100.0, 200.0), [pd.Timestamp(report_date) + pd.offsets.MonthEnd(0)]
    )

    row = result.iloc[0]
    assert (row.fy1_weight, row.fy2_weight) == expected_weights
    assert row.fttm_np == pytest.approx(expected)


@pytest.mark.parametrize(
    ("fy1", "fy2", "status", "expected"),
    [
        (100.0, None, "fy1_only", 100.0),
        (None, 200.0, "fy2_only", 200.0),
    ],
)
def test_single_year_fallback_repeats_available_value(fy1, fy2, status, expected):
    result = StockFTTMCalculator().calculate(
        _rows("2024-05-20", fy1, fy2), ["2024-05-31"]
    )

    row = result.iloc[0]
    assert row.estimate_pair_status == status
    assert bool(row.is_single_year_fallback)
    assert row.fy1_np_used == expected
    assert row.fy2_np_used == expected
    assert row.fttm_np == expected


def test_both_missing_produces_no_event_and_negative_zero_are_retained():
    calculator = StockFTTMCalculator()
    missing = calculator.calculate(_rows("2024-05-20", None, None), ["2024-05-31"])
    values = pd.concat(
        [
            _rows("2024-05-20", -100.0, 0.0, ts_code="000001.SZ"),
            _rows("2024-05-20", 0.0, 0.0, ts_code="000002.SZ"),
        ],
        ignore_index=True,
    )
    retained = calculator.calculate(values, ["2024-05-31"])

    assert missing.empty
    assert len(retained) == 2
    assert (retained.fttm_np < 0).any()
    assert (retained.fttm_np == 0).any()


def test_np_precedes_eps_and_eps_uses_only_exact_day_share():
    forecasts = _rows(
        "2024-05-20", 100.0, None, eps1=999.0, eps2=2.0
    ).drop(columns="total_share")
    shares = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-05-19", "total_share": 999.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-05-20", "total_share": 100.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-05-21", "total_share": 888.0},
        ]
    )

    result = StockFTTMCalculator().calculate(forecasts, ["2024-05-31"], shares)
    row = result.iloc[0]

    assert row.fy1_np_used == 100.0
    assert row.fy1_value_source == "np"
    assert row.fy2_np_used == 200.0
    assert row.fy2_value_source == "eps_share"
    assert row.fttm_np == pytest.approx(125.0)


def test_eps_does_not_previous_or_future_fill_share():
    forecasts = _rows("2024-05-20", None, None, eps1=1.0, eps2=2.0).drop(
        columns="total_share"
    )
    shares = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-05-19", "total_share": 100.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-05-21", "total_share": 100.0},
        ]
    )

    assert StockFTTMCalculator().calculate(forecasts, ["2024-05-31"], shares).empty


def test_pairing_never_crosses_report_event_and_year_anchor_does_not_roll():
    event_a = _rows("2024-12-20", 100.0, None, author_name="甲")
    event_b = _rows("2024-12-21", None, 200.0, author_name="乙")
    result = StockFTTMCalculator().calculate(
        pd.concat([event_a, event_b], ignore_index=True), ["2025-01-31"]
    )

    # The later event is selected, but remains FY2-only. It must not borrow
    # FY1 from the prior event or roll its years because obs_date is in 2025.
    row = result.iloc[0]
    assert row.selected_report_date == pd.Timestamp("2024-12-21")
    assert row.estimate_pair_status == "fy2_only"
    assert row.fy1_year == 2024
    assert row.fy2_year == 2025
    assert row.fttm_np == 200.0


def test_visibility_window_is_left_open_right_closed():
    forecasts = pd.concat(
        [
            _rows("2024-07-31", 100.0, 100.0, author_name="边界"),
            _rows("2024-08-01", 200.0, 200.0, author_name="窗口内"),
            _rows("2025-02-01", 300.0, 300.0, author_name="未来"),
        ],
        ignore_index=True,
    )

    result = StockFTTMCalculator().calculate(forecasts, ["2025-01-31"])

    assert len(result) == 1
    assert result.iloc[0].selected_author_name == "窗口内"
    assert result.iloc[0].selected_report_date == pd.Timestamp("2024-08-01")


def test_same_date_author_tie_and_duplicate_input_are_deterministic():
    rows = pd.concat(
        [
            _rows("2024-05-20", 100.0, 200.0, author_name="乙"),
            _rows("2024-05-20", 110.0, 210.0, author_name="甲"),
        ],
        ignore_index=True,
    )
    rows = pd.concat([rows, rows.iloc[[0]]], ignore_index=True)

    first = StockFTTMCalculator().calculate(rows.sample(frac=1, random_state=1), ["2024-05-31"])
    second = StockFTTMCalculator().calculate(rows.sample(frac=1, random_state=2), ["2024-05-31"])

    assert first.iloc[0].selected_author_name == min(["甲", "乙"])
    pd.testing.assert_frame_equal(first, second)
