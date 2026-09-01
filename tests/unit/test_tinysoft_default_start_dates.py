import re

from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks import discover_tasks

discover_tasks(force_reload=True)


RECOMMENDED_FULL_STARTS = {
    "tinysoft_bond_basic_ext": "19900101",
    "tinysoft_cbond_price_chg": "19900101",
    "tinysoft_fund_abs_holding_detail": "19980101",
    "tinysoft_fund_asset_alloc": "19980101",
    "tinysoft_fund_basic_ext": "19900101",
    "tinysoft_fund_bond_alloc": "19980101",
    "tinysoft_fund_bond_holding_detail": "19980101",
    "tinysoft_fund_broker_seat": "19980101",
    "tinysoft_fund_cbond_holding_detail": "19980101",
    "tinysoft_fund_classification_info": "19000101",
    "tinysoft_fund_classification_member": "19000101",
    "tinysoft_fund_financial_quarterly_ext": "19980101",
    "tinysoft_fund_fof_holding_detail": "20170101",
    "tinysoft_fund_holder_structure": "19980101",
    "tinysoft_fund_industry_alloc": "19980101",
    "tinysoft_fund_manager_ext": "19900101",
    "tinysoft_fund_minute": "20240101",
    "tinysoft_fund_stock_holding_detail": "19980101",
    "tinysoft_fund_stock_trade_summary": "19980101",
    "tinysoft_fund_top_holder": "19980101",
    "tinysoft_future_basic_ext": "19950417",
    "tinysoft_future_product_mapping_ext": "19950417",
    "tinysoft_index_basic_ext": "19900101",
    "tinysoft_index_member_versioned": "20000101",
    "tinysoft_index_minute": "20240101",
    "tinysoft_market_calendar_multi": "19901219",
    "tinysoft_option_basic_daily_ext": "20150209",
    "tinysoft_stock_basic_ext": "19901219",
    "tinysoft_stock_fina_pit_ext": "20000101",
    "tinysoft_stock_holder_change_ext": "20050101",
    "tinysoft_stock_hsgt_daily": "20141117",
    "tinysoft_stock_hsgt_hold": "20141117",
    "tinysoft_stock_hsgt_short_balance": "20141117",
    "tinysoft_stock_hsgt_top10": "20141117",
    "tinysoft_stock_industry_versioned": "20000101",
    "tinysoft_stock_lending_balance": "20130228",
    "tinysoft_stock_lending_summary": "20130228",
    "tinysoft_stock_lending_trade": "20130228",
    "tinysoft_stock_margin": "20100331",
    "tinysoft_stock_margindetail": "20100331",
    "tinysoft_stock_minute": "20240101",
    "tinysoft_stock_pledge_balance": "20130624",
    "tinysoft_stock_pledge_detail": "20130624",
    "tinysoft_stock_pledge_rate": "20130624",
    "tinysoft_stock_pledge_summary": "20130624",
    "tinysoft_stock_public_trade_info": "20050101",
    "tinysoft_stock_repurchase_ext": "20050101",
    "tinysoft_stock_suspend": "19901219",
    "tinysoft_stock_unlock_schedule": "20050101",
}


def _registered_tinysoft_tasks():
    return {
        name: cls
        for name, cls in UnifiedTaskFactory._task_registry.items()
        if name.startswith("tinysoft_") or getattr(cls, "data_source", None) == "tinysoft"
    }


def test_all_registered_tinysoft_tasks_have_reviewed_full_start_dates():
    tasks = _registered_tinysoft_tasks()
    assert tasks, "Tinysoft task registry is empty; discover_tasks() did not populate tasks"
    missing = sorted(set(tasks) - set(RECOMMENDED_FULL_STARTS))

    assert not missing, f"Tinysoft tasks missing FULL start-date review: {missing}"


def test_tinysoft_full_start_dates_are_not_later_than_reviewed_floor():
    tasks = _registered_tinysoft_tasks()

    for name, cls in sorted(tasks.items()):
        start = getattr(cls, "default_start_date", None)
        reviewed_floor = RECOMMENDED_FULL_STARTS[name]

        assert isinstance(start, str) and re.fullmatch(r"\d{8}", start), (
            f"{name} default_start_date must be YYYYMMDD, got {start!r}"
        )
        assert start <= reviewed_floor, (
            f"{name} default_start_date={start} is later than reviewed FULL floor {reviewed_floor}"
        )
