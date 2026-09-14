import json

import pandas as pd
import pytest

from fundpos.constants import ASSETS
from fundpos.errors import ProtocolError
from fundpos.storage import atomic_json, code_fingerprint
from fundpos.validation import (
    ValidationState,
    choose_dates,
    evaluate_predictions,
    promotion_decision,
    validation_calculation_dates,
)


def test_signed_error_cannot_cancel_and_missing_label_is_not_zero():
    rows = []
    for code, weight in [("A", 0.1), ("B", 0.3), ("C", 0.8)]:
        rows.append(
            {
                "fund_code": code,
                "valuation_date": "2023-06-30",
                "status": "ok",
                **dict.fromkeys(ASSETS[2:], 0),
                "801030.SI": weight,
                "stock_weight": weight,
            }
        )
    predictions = pd.DataFrame(rows)
    labels = predictions.iloc[:2].drop(columns="status").copy()
    labels["801030.SI"] = 0.2
    labels["stock_weight"] = 0.2
    metric, errors = evaluate_predictions(predictions, labels)
    assert metric["industry_l1_mean"] == pytest.approx(0.1)
    assert metric["stock_mae"] == pytest.approx(0.1)
    assert metric["label_count"] == 2 and len(errors) == 2


def test_promotion_requires_all_gates():
    base = dict(
        status="evaluated",
        industry_l1_mean=0.3,
        stock_mae=0.05,
        hk_mae=0.02,
        industry_l1_p90=0.5,
        coverage=0.99,
    )
    candidate = dict(base, industry_l1_mean=0.25)
    assert promotion_decision(base, candidate)["promoted"]
    assert not promotion_decision(base, dict(candidate, coverage=0.98))["promoted"]
    assert not promotion_decision(base, dict(candidate, hk_mae=0.021))["promoted"]
    assert not promotion_decision(base, dict(candidate, scope="conditional_diagnostic"))["promoted"]


def test_holdout_requires_frozen_selection_and_opens_once(settings):
    state = ValidationState(settings)
    with pytest.raises(ProtocolError, match="selection"):
        state.assert_open_allowed("final")
    atomic_json(
        state.path,
        {
            "selection_frozen": "prior01_smooth0",
            "protocol_hash": state.protocol_hash,
            "code_hash": code_fingerprint(settings.root),
            "selection_assessment": {"all_selection_gates_pass": True},
        },
    )
    state.mark_final_opened("input-hash")
    assert json.loads(state.path.read_text())["final_input_hash"] == "input-hash"
    with pytest.raises(ProtocolError, match="already opened"):
        state.mark_final_opened("second-input")
    with pytest.raises(ProtocolError, match="already opened"):
        state.assert_open_allowed("development")


def test_holdout_rejects_failed_selection_gates(settings):
    state = ValidationState(settings)
    atomic_json(
        state.path,
        {
            "selection_frozen": "prior01_smooth0",
            "protocol_hash": state.protocol_hash,
            "code_hash": code_fingerprint(settings.root),
            "selection_assessment": {"all_selection_gates_pass": False},
        },
    )
    with pytest.raises(ProtocolError, match="gates"):
        state.assert_open_allowed("final")


def test_weekly_date_is_last_exchange_day_not_always_friday():
    calendar = pd.to_datetime(
        ["2023-09-25", "2023-09-26", "2023-09-27", "2023-09-28", "2023-10-09", "2023-10-10"]
    )
    result = choose_dates(calendar, "2023-09-25", "2023-10-10", "weekly")
    assert result.tolist() == [pd.Timestamp("2023-09-28"), pd.Timestamp("2023-10-10")]


def test_validation_label_cadence_uses_only_real_label_trading_dates():
    calendar = pd.to_datetime(["2023-06-29", "2023-06-30", "2023-07-03", "2023-09-28"])
    result = validation_calculation_dates(
        calendar,
        "2023-01-01",
        "2023-12-31",
        ["2023-06-30", "2023-07-01", "2023-09-28"],
        cadence="prior_truth_label_date",
    )
    assert result.tolist() == [pd.Timestamp("2023-06-30"), pd.Timestamp("2023-09-28")]


def test_validation_rejects_unknown_cadence():
    with pytest.raises(ProtocolError, match="cadence"):
        validation_calculation_dates(
            pd.to_datetime(["2023-06-30"]),
            "2023-01-01",
            "2023-12-31",
            ["2023-06-30"],
            cadence="monthly",
        )
