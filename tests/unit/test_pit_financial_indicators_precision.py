import pandas as pd

from alphahome.pit.calculators.financial_indicators_calculator import FinancialIndicatorsCalculator


class FakePITContext:
    db_manager = object()


def test_financial_indicator_cleaning_rounds_numeric_columns_to_four_decimals():
    calculator = FinancialIndicatorsCalculator(FakePITContext())
    data = pd.DataFrame(
        {
            "gpa_ttm": [100.0],
            "roe_excl_ttm": [6.030433],
            "roa_excl_ttm": [0.487888],
            "revenue_yoy_growth": [1.417621],
        }
    )

    cleaned = calculator._clean_indicators_data(data, list(data.columns))

    assert cleaned.loc[0, "roe_excl_ttm"] == 6.0304
    assert cleaned.loc[0, "roa_excl_ttm"] == 0.4879
    assert cleaned.loc[0, "revenue_yoy_growth"] == 1.4176
