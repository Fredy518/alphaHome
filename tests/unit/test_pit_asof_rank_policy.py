import pytest

from alphahome.features.recipes.mv.pit_asof_rank_sql import ah_premium_sql, fund_holdings_sql


@pytest.mark.parametrize('minimum', [None, True, 1, 0, -1, 2.5, '60'])
def test_policy_must_supply_valid_warmup(minimum):
    with pytest.raises(ValueError):
        fund_holdings_sql(min_history_periods=minimum)
    with pytest.raises(ValueError):
        ah_premium_sql(history_interval='1 year', min_observations=minimum)


@pytest.mark.parametrize('interval', [None, '', '0 years', "1 year'; DROP TABLE x;--", '-1 day'])
def test_history_interval_is_explicit_and_cannot_inject_sql(interval):
    with pytest.raises(ValueError):
        ah_premium_sql(history_interval=interval, min_observations=2)
