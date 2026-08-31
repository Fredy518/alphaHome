"""Manager for A-share proxy FAPI/expected-ROE rows."""

from __future__ import annotations

from .calculators.etf_index_a_share_proxy_fapi_calculator import (
    ETFIndexAShareProxyFAPICalculator,
)
from .calculators.etf_index_a_share_proxy_members_calculator import (
    ETFIndexAShareProxyMembersCalculator,
)
from .pit_etf_index_fapi_manager import PITETFIndexFAPIMonthlyManager


class PITETFIndexAShareProxyFAPIMonthlyManager(PITETFIndexFAPIMonthlyManager):
    """Refresh FAPI facts for explicitly labelled A-share proxy members."""

    MEMBER_METHOD_VERSION = ETFIndexAShareProxyMembersCalculator.METHOD_VERSION
    PIT_SEMANTICS = (
        "labelled A-share proxy PIT members at t; adjacent-month common "
        "stock-broker FTTM rows; current-month book equity; relative to CSI800"
    )
    WEIGHT_LIMIT = (
        "signals cover only the A-share subset of a cross-market index; full-index "
        "ETF returns remain a separate implementation series"
    )

    def __init__(self) -> None:
        super().__init__()
        self.calculator = ETFIndexAShareProxyFAPICalculator()


__all__ = ["PITETFIndexAShareProxyFAPIMonthlyManager"]
