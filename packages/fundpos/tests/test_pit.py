import pandas as pd
import pytest

from fundpos.errors import DataUnavailable
from fundpos.pit import (
    eligible_universe,
    last_full_holdings,
    map_membership,
    recover_holding_announcements,
)


def test_security_level_announcements_do_not_promote_all_holdings():
    h = pd.DataFrame(
        {
            "fund_code": ["F"] * 2,
            "report_date": ["2023-06-30"] * 2,
            "ann_date": [None, None],
            "security_code": ["A", "B"],
            "market_value": [100, 200],
            "weight": [0.1, 0.2],
            "full_report_verified": [True, True],
        }
    )
    disclosed = h.copy()
    disclosed["ann_date"] = ["2023-07-20", "2023-08-30"]
    recovered = recover_holding_announcements(h, disclosed)
    assert recovered.ann_date.tolist() == [pd.Timestamp("2023-07-20"), pd.Timestamp("2023-08-30")]
    with pytest.raises(DataUnavailable, match="NO_VERIFIED_FULL_HOLDINGS"):
        last_full_holdings(recovered, "F", "2023-08-01")
    assert len(last_full_holdings(recovered, "F", "2023-08-30")) == 2
    disclosed.loc[1, "market_value"] = 999
    assert pd.isna(recover_holding_announcements(h, disclosed).iloc[1].ann_date)


def test_first_classification_is_not_retrofilled():
    h = pd.DataFrame({"security_code": ["A.SZ"], "weight": [0.2]})
    m = pd.DataFrame(
        {
            "security_code": ["A.SZ", "A.SZ"],
            "industry": ["801030.SI", "801080.SI"],
            "in_date": ["2022-01-01", "2023-01-01"],
            "out_date": ["2023-01-01", None],
        }
    )
    with pytest.raises(DataUnavailable, match="UNMAPPED_HOLDINGS"):
        map_membership(h, m, "2021-12-31")
    assert map_membership(h, m, "2022-12-31").industry.iloc[0] == "801030.SI"
    assert map_membership(h, m, "2023-01-01").industry.iloc[0] == "801080.SI"


def test_share_dedup_keeps_new_c_class_aum(bundle):
    f = bundle.frames["funds"]
    f.loc[f.fund_code == "DEMOC001.OF", "found_date"] = pd.Timestamp("2023-09-01")
    u = eligible_universe(f, bundle["classification"], "2023-09-29")
    assert len(u) == 3
    assert len(u.loc[u.master_code == "DEMO001.OF", "share_codes"].iloc[0]) == 2
    old = eligible_universe(f, bundle["classification"], "2023-08-31")
    assert len(old.loc[old.master_code == "DEMO001.OF", "share_codes"].iloc[0]) == 1


def test_missing_historical_category_is_not_current_pool_backfill(bundle):
    classification = bundle["classification"]
    classification["in_date"] = pd.Timestamp("2024-01-01")
    assert eligible_universe(bundle["funds"], classification, "2023-09-29").empty
