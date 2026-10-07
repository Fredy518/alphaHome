"""Reject internally inconsistent prices without inventing corrected values."""
import pandas as pd


class PriceDataQualityError(ValueError):
    pass


def validate_ohlc(frame: pd.DataFrame) -> None:
    """Check all four prices, even when a caller requests close only.

    Missing/nonpositive prices and relationships beyond the documented 1e-5
    relative rounding allowance are unavailable, not zero-price observations.
    Raw records remain untouched. The exception exposes only a count.
    """
    columns = ['open', 'high', 'low', 'close']
    if frame.empty:
        return
    if not set(columns).issubset(frame.columns):
        raise PriceDataQualityError('ohlc_validation_columns_missing')
    prices=frame[columns].apply(pd.to_numeric, errors='coerce')
    maximum=prices.abs().max(axis=1)
    allowance=maximum * 1e-5
    invalid=(prices.isna().any(axis=1) | prices.isin([float('inf'),-float('inf')]).any(axis=1) | prices.le(0).any(axis=1)
             | ((prices['low']-prices['high']) > allowance)
             | ((prices['open']-prices['high']) > allowance)
             | ((prices['close']-prices['high']) > allowance)
             | ((prices['low']-prices['open']) > allowance)
             | ((prices['low']-prices['close']) > allowance))
    if invalid.any():
        raise PriceDataQualityError(f'ohlc_unavailable_rows:{int(invalid.sum())}')
