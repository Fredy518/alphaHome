from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest

from alphahome.pit.industry_evidence import (
    AMBIGUOUS_QUALITY, AMBIGUOUS_REASON, CLASSIFICATION_COLUMNS,
    select_latest_classification,
)
from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager


def record(label="A", start="2017-01-03", end=None, **changes):
    return {
        "ts_code": "X", "in_date": start, "out_date": end,
        **{f"industry_level{i}": label + str(i) for i in (1, 2, 3)},
        **{f"industry_code{i}": label + str(i) for i in (1, 2, 3)},
        **changes,
    }


@pytest.mark.parametrize("reverse", [False, True])
def test_conflict_does_not_select_an_older_or_first_payload(reverse):
    rows = [record("OLD", "2016-01-01"), record("A", end="2019-12-01"), record("B")]
    result = select_latest_classification(pd.DataFrame(rows[::-1] if reverse else rows), "2019-11-30")
    row = result.iloc[0]
    assert row["data_quality"] == AMBIGUOUS_QUALITY
    assert row["classification_ambiguous"]
    assert row[list(CLASSIFICATION_COLUMNS)].isna().all()
    assert row["in_date"] == "2017-01-03"
    assert len(row["data_quality"]) <= 20
    unique = select_latest_classification(pd.DataFrame(rows), "2019-12-01").iloc[0]
    assert unique["industry_code1"] == "B1" and unique["data_quality"] == "normal"


@pytest.mark.parametrize("reverse", [False, True])
def test_identical_payload_duplicates_keep_a_whole_original_row(reverse):
    rows = [record(end="2027-01-01", industry_level3=None), record(industry_level3=None)]
    row = select_latest_classification(pd.DataFrame(rows[::-1] if reverse else rows), "2026-09-30").iloc[0]
    assert row["data_quality"] == "normal" and not row["classification_ambiguous"]
    assert pd.isna(row["industry_level3"]) and pd.isna(row["out_date"])


def test_latest_null_field_is_not_filled_from_an_older_classification():
    rows = [record("LATEST", "2018-01-01", industry_level2=None, industry_code2=None), record("OLD", "2017-01-03")]
    row = select_latest_classification(pd.DataFrame(rows), "2018-01-31").iloc[0]
    assert row["industry_code1"] == "LATEST1"
    assert pd.isna(row["industry_level2"]) and pd.isna(row["industry_code2"])


def test_future_and_expired_records_are_bounded_before_selection():
    frame = pd.DataFrame([record(start="2018-01-01", end="2018-02-01")])
    assert select_latest_classification(frame, "2017-12-31").empty
    assert select_latest_classification(frame, "2018-02-01").empty
    assert len(select_latest_classification(frame, "2018-02-01", active_only=False)) == 1


@pytest.mark.parametrize("changes", [{"in_date": None}, {"out_date": "invalid"}, {"ts_code": None}])
def test_invalid_retained_dates_or_keys_are_rejected(changes):
    with pytest.raises(ValueError, match="industry_evidence_invalid"):
        select_latest_classification(pd.DataFrame([record(**changes)]), "2026-09-30")


@pytest.mark.parametrize("source", ["sw", "ci"])
@pytest.mark.parametrize("reverse", [False, True])
def test_pit_generator_keeps_unknown_key_with_conservative_handling(source, reverse):
    rows = [record("A"), record("B")]
    frame = pd.DataFrame(rows[::-1] if reverse else rows)
    frame = frame.rename(columns={f"industry_level{i}": f"l{i}_name" for i in (1,2,3)} | {f"industry_code{i}": f"l{i}_code" for i in (1,2,3)})
    manager = PITIndustryClassificationManager.__new__(PITIndustryClassificationManager)
    manager.context = Mock(query_dataframe=Mock(return_value=frame))
    manager.logger = Mock()
    row = manager._generate_industry_snapshot(source, date(2019, 11, 30))[0]
    assert (row["ts_code"], row["obs_date"], row["data_source"]) == ("X", date(2019,11,30), source)
    assert all(row[column] is None for column in CLASSIFICATION_COLUMNS)
    assert row["data_quality"] == AMBIGUOUS_QUALITY
    assert row["requires_special_gpa_handling"] and row["gpa_calculation_method"] == "null"
    assert row["special_handling_reason"] == AMBIGUOUS_REASON
