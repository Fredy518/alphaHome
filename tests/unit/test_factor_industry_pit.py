import logging
from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest

from alphahome.factors.core.data_repository import IndustryDataUnavailable, PFactorDataRepository
from alphahome.factors.core.p_factor_calculator import PFactorCalculator


AS_OF = "2002-04-05"
CODE = "000045.SZ"


def member(code=CODE, in_date="2002-04-04", industry="纺织服饰", out_date=None):
    return {
        "ts_code": code, "in_date": in_date, "out_date": out_date,
        "industry_level1": industry, "industry_level2": industry, "industry_level3": industry,
    }


def pit(code=CODE, obs_date="2002-04-04", **overrides):
    return {
        "ts_code": code, "obs_date": obs_date, "data_source": "sw",
        "industry_level1": "纺织服饰", "requires_special_gpa_handling": False,
        "gpa_calculation_method": "standard", **overrides,
    }


class IndustryContext:
    db_manager = object()

    def __init__(self, members=(), ci_members=(), optimized=(), helper_error=None):
        self.members, self.ci_members = members, ci_members
        self.optimized, self.helper_error = optimized, helper_error
        self.queries = []

    def query_dataframe(self, sql, params):
        self.queries.append((sql, params))
        if "get_industry_classification_batch_pit_optimized" in sql:
            if self.helper_error:
                raise self.helper_error
            return pd.DataFrame(self.optimized)
        if "FROM tushare.index_swmember" in sql:
            source_rows = self.members
        else:
            assert "FROM tushare.index_cimember" in sql
            source_rows = self.ci_members
        rows = [row for row in source_rows if row["ts_code"] in params[0]]
        # Deliberately support the old unbounded query: its future result must
        # never be reached, which reproduces AH-002 through the public reader.
        if "in_date <= %s" in sql:
            rows = [row for row in rows if row["in_date"] <= params[1]]
        if "out_date IS NULL OR out_date > %s" in sql:
            rows = [row for row in rows if row["out_date"] is None or row["out_date"] > params[2]]
        frame = pd.DataFrame(sorted(rows, key=lambda row: row["in_date"], reverse=True))
        if not frame.empty and "DISTINCT ON (ts_code)" in sql:
            frame = frame.drop_duplicates("ts_code")
        return frame


def reader(context):
    return PFactorDataRepository(context, logging.getLogger("test_industry_pit"))


@pytest.mark.parametrize("source_date", ["2002-04-04", AS_OF])
def test_member_on_or_before_asof_keeps_real_source_date(source_date):
    context = IndustryContext([member(in_date=source_date)])
    result = reader(context).industry_classification([CODE], AS_OF)
    assert result.iloc[0]["obs_date"] == source_date
    assert result.iloc[0]["in_date"] == source_date
    assert result.iloc[0]["source_table"] == "tushare.index_swmember"
    assert result.iloc[0]["source_method"] == "sw_active"


@pytest.mark.parametrize("members", [[], [member(in_date="2002-04-06")], [member(in_date="2011-10-10")]])
def test_missing_history_never_uses_future_first_membership(members):
    context = IndustryContext(members)
    with pytest.raises(IndustryDataUnavailable, match="industry_history_missing"):
        reader(context).industry_classification([CODE], AS_OF)
    member_queries = [sql for sql, _ in context.queries if "index_swmember" in sql]
    assert len(member_queries) == 2
    assert all("in_date <= %s" in sql for sql in member_queries)


def test_latest_past_member_remains_available_with_provenance():
    context = IndustryContext([
        member(in_date="2001-01-01", out_date="2001-02-01"),
        member(in_date="2002-01-01", out_date="2002-02-01"),
    ])
    result = reader(context).industry_classification([CODE], AS_OF)
    assert result.iloc[0]["obs_date"] == "2002-01-01"
    assert result.iloc[0]["source_method"] == "sw_past"


def test_dated_ci_membership_fills_gap_without_backdating_sw():
    context = IndustryContext(
        members=[member(in_date="2002-04-06")],
        ci_members=[member(in_date="2002-04-04", industry="汽车")],
    )
    result = reader(context).industry_classification([CODE], AS_OF)
    assert result.iloc[0]["obs_date"] == "2002-04-04"
    assert result.iloc[0]["source_table"] == "tushare.index_cimember"
    assert result.iloc[0]["source_method"] == "ci_active"
    assert not result.iloc[0]["requires_special_gpa_handling"]


def test_future_ci_membership_is_not_used():
    context = IndustryContext(ci_members=[member(in_date="2002-04-06")])
    with pytest.raises(IndustryDataUnavailable, match="industry_history_missing"):
        reader(context).industry_classification([CODE], AS_OF)


@pytest.mark.parametrize("source_date", ["2002-04-04", date(2002, 4, 5)])
def test_optimized_pit_preserves_observation_date_without_member_query(source_date):
    context = IndustryContext(optimized=[pit(obs_date=source_date)])
    result = reader(context).industry_classification([CODE], AS_OF)
    assert result.iloc[0]["obs_date"] == source_date
    assert result.iloc[0]["source_method"] == "pit_latest"
    assert len(context.queries) == 1


@pytest.mark.parametrize("source_date", ["2002-04-06", "2011-10-10", None, "invalid"])
def test_invalid_optimized_dates_fail_closed(source_date):
    context = IndustryContext(optimized=[pit(obs_date=source_date)])
    with pytest.raises(IndustryDataUnavailable, match="industry_date_invalid"):
        reader(context).industry_classification([CODE], AS_OF)


def test_partial_pit_fills_only_missing_codes_from_legal_history():
    context = IndustryContext(
        [member("B", in_date="2002-04-03")], optimized=[pit()],
    )
    result = reader(context).industry_classification([CODE, "B"], AS_OF).set_index("ts_code")
    assert result.loc[CODE, "obs_date"] == "2002-04-04"
    assert result.loc["B", "obs_date"] == "2002-04-03"
    assert context.queries[1][1][0] == ["B"]


def test_partial_coverage_blocks_the_complete_calculation_before_save(monkeypatch):
    context = IndustryContext(optimized=[pit()])
    calculator = PFactorCalculator(context=context)
    monkeypatch.setattr(calculator, "_get_mvp_precomputed_indicators_pit", lambda *_: pd.DataFrame({"ts_code": [CODE, "B"]}))
    save = Mock()
    monkeypatch.setattr(calculator, "_save_p_factors_mvp", save)
    with pytest.raises(IndustryDataUnavailable, match="industry_history_missing:1:B"):
        calculator.calculate_p_factors_pit(AS_OF, [CODE, "B"])
    save.assert_not_called()


def test_optional_helper_failure_can_use_verified_historical_membership():
    context = IndustryContext([member()], helper_error=RuntimeError("helper absent"))
    assert len(reader(context).industry_classification([CODE], AS_OF)) == 1


@pytest.mark.parametrize("source", ["sw_active", "sw_past"])
def test_member_result_is_rechecked_even_if_query_returns_future_row(source):
    context = Mock()
    future = pd.DataFrame([member(in_date="2011-10-10")])
    context.query_dataframe.side_effect = [future] if source == "sw_active" else [pd.DataFrame(), future]
    with pytest.raises(IndustryDataUnavailable, match=f"industry_date_invalid:{source}"):
        reader(context)._fallback_industry([CODE], AS_OF)


def test_financial_gpa_handling_is_unchanged_for_valid_history():
    context = IndustryContext([member(industry="银行"), member("B")])
    calculator = PFactorCalculator(context=context)
    industry = calculator.data_repository.industry_classification([CODE, "B"], AS_OF)
    frame = pd.DataFrame({"ts_code": [CODE, "B"], "gpa_ttm": [9999.0, 10.0]})
    result = calculator._apply_industry_special_handling(frame, AS_OF, industry)
    assert pd.isna(result.loc[result["ts_code"] == CODE, "gpa_ttm"]).all()
    assert result.loc[result["ts_code"] == "B", "gpa_ttm"].iloc[0] == 10.0


@pytest.mark.parametrize("optimized, reason", [
    ([pit(), pit()], "industry_duplicate_rows"),
    ([pit(industry_level1=None)], "industry_classification_unknown"),
    ([pit(gpa_calculation_method=None)], "industry_handling_unknown"),
    ([pit(code="outside")], "industry_universe_invalid"),
])
def test_unknown_or_ambiguous_industry_does_not_reach_math(optimized, reason):
    with pytest.raises(IndustryDataUnavailable, match=reason):
        reader(IndustryContext(optimized=optimized)).industry_classification([CODE], AS_OF)


def test_empty_universe_needs_no_industry_read():
    context = IndustryContext()
    assert reader(context).industry_classification([], AS_OF).empty
    assert context.queries == []
