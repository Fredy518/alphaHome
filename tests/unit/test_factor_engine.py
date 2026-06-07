import pytest

from alphahome.factors.pipelines import (
    FactorEngine,
    FactorEngineConfig,
    allocate_contiguous_balanced,
    generate_friday_dates,
    generate_quarter_range,
    parse_quarter,
    shard_items,
)


class MockFactorCalculator:
    def __init__(self, success_count=2):
        self.success_count = success_count
        self.calls = []

    def _get_trading_stock_codes(self, calc_date):
        self.calls.append(("stocks", calc_date))
        return ["000001.SZ", "600000.SH"]

    def _filter_missing_dates(self, dates):
        return [value for value in dates if value.endswith("12")]

    def calculate_p_factors_pit(self, calc_date, stock_codes):
        self.calls.append(("p", calc_date, tuple(stock_codes)))
        return {"success_count": self.success_count, "failed_count": 0}

    def calculate_g_factors_pit(self, calc_date, stock_codes):
        self.calls.append(("g", calc_date, tuple(stock_codes)))
        return {"success_count": self.success_count, "failed_count": 0}

    def calculate_p_factors_batch_pit(self, start_date, end_date, mode):
        self.calls.append(("p_batch", start_date, end_date, mode))
        return {"success_count": self.success_count, "failed_count": 0}

    def calculate_g_factors_batch_pit(self, start_date, end_date, mode):
        self.calls.append(("g_batch", start_date, end_date, mode))
        return {"success_count": self.success_count, "failed_count": 0}


def test_generate_friday_dates_matches_legacy_weekly_logic():
    assert generate_friday_dates("2024-01-01", "2024-01-20") == [
        "2024-01-05",
        "2024-01-12",
        "2024-01-19",
    ]
    assert generate_friday_dates("2024-01-06", "2024-01-11") == []


def test_quarter_parsing_and_range_expansion():
    quarter = parse_quarter("2025Q3")
    assert quarter.year == 2025
    assert quarter.quarter == 3
    assert quarter.date_range == ("2025-07-01", "2025-09-30")
    assert [q.label for q in generate_quarter_range("2024Q4", "2025Q2")] == [
        "2024Q4",
        "2025Q1",
        "2025Q2",
    ]


def test_quarter_validation_rejects_bad_values():
    with pytest.raises(ValueError):
        parse_quarter("2025Q5")
    with pytest.raises(ValueError):
        generate_quarter_range("2025Q2", "2025Q1")


def test_contiguous_worker_allocation_matches_script_semantics():
    assert allocate_contiguous_balanced([2020, 2021, 2022, 2023, 2024], 2) == [
        [2020, 2021, 2022],
        [2023, 2024],
    ]
    assert shard_items(["a", "b", "c"], 1, 2) == ["c"]
    with pytest.raises(ValueError):
        shard_items(["a"], 2, 2)


def test_config_validation_rejects_mutually_exclusive_selectors():
    with pytest.raises(ValueError):
        FactorEngine(
            FactorEngineConfig(dates=["2024-01-05"], start_year=2024, end_year=2024)
        )

    with pytest.raises(ValueError):
        FactorEngine(FactorEngineConfig(start_quarter="2024Q1"))

    with pytest.raises(ValueError):
        FactorEngine(FactorEngineConfig(missing_mode="batch_missing"))


def test_resolve_dates_shards_explicit_dates():
    engine = FactorEngine(
        FactorEngineConfig(
            factor_types=("p",),
            dates=["2024-01-05", "2024-01-12", "2024-01-19"],
            worker_id=0,
            total_workers=2,
        ),
        p_calculator=MockFactorCalculator(),
    )
    assert engine.resolve_dates() == ["2024-01-05", "2024-01-12"]


def test_filter_missing_dates_uses_provider():
    def existing_provider(factor_type, dates):
        assert factor_type == "p"
        return ["2024-01-05"]

    engine = FactorEngine(
        FactorEngineConfig(factor_types=("p",), dates=["2024-01-05"]),
        p_calculator=MockFactorCalculator(),
        existing_date_provider=existing_provider,
    )
    assert engine.filter_missing_dates("p", ["2024-01-05", "2024-01-12"]) == ["2024-01-12"]


def test_batch_missing_dates_use_provider_union_by_factor():
    existing = {
        "p": {"2024-01-01"},
        "g": {"2024-01-01", "2024-01-02"},
    }

    def existing_provider(factor_type, dates):
        return existing[factor_type]

    engine = FactorEngine(
        FactorEngineConfig(
            factor_types=("p", "g"),
            start_date="2024-01-01",
            end_date="2024-01-03",
            missing_mode="batch_missing",
        ),
        p_calculator=MockFactorCalculator(),
        g_calculator=MockFactorCalculator(),
        existing_date_provider=existing_provider,
    )
    assert engine.resolve_dates() == ["2024-01-02", "2024-01-03"]


def test_recent_missing_dates_are_deterministic_with_today():
    def existing_provider(factor_type, dates):
        return dates[:-1]

    engine = FactorEngine(
        FactorEngineConfig(
            factor_types=("p",),
            missing_mode="recent_missing",
            months_back=1,
            today="2024-02-01",
        ),
        p_calculator=MockFactorCalculator(),
        existing_date_provider=existing_provider,
    )
    assert engine.resolve_dates() == ["2024-02-01"]


def test_run_executes_specific_dates_with_mock_calculators():
    p_calculator = MockFactorCalculator(success_count=2)
    g_calculator = MockFactorCalculator(success_count=3)
    engine = FactorEngine(
        FactorEngineConfig(
            factor_types=("p", "g"),
            dates=["2024-01-05"],
        ),
        p_calculator=p_calculator,
        g_calculator=g_calculator,
    )

    result = engine.run()

    assert result["work_item_count"] == 2
    assert result["success_count"] == 5
    assert ("p", "2024-01-05", ("000001.SZ", "600000.SH")) in p_calculator.calls
    assert ("g", "2024-01-05", ("000001.SZ", "600000.SH")) in g_calculator.calls


def test_run_executes_year_ranges_as_batch_items():
    p_calculator = MockFactorCalculator(success_count=10)
    engine = FactorEngine(
        FactorEngineConfig(
            factor_types=("p",),
            start_year=2020,
            end_year=2021,
            worker_id=1,
            total_workers=2,
        ),
        p_calculator=p_calculator,
    )

    result = engine.run()

    assert result["work_item_count"] == 1
    assert result["success_count"] == 10
    assert p_calculator.calls == [("p_batch", "2021-01-01", "2021-12-31", "backfill")]
