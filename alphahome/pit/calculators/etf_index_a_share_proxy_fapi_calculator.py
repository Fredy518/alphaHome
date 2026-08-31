"""FAPI/expected-ROE calculator for labelled A-share proxy universes."""

from __future__ import annotations

import pandas as pd

from .etf_index_fapi_calculator import ETFIndexFAPICalculator


class ETFIndexAShareProxyFAPICalculator(ETFIndexFAPICalculator):
    """Run the ETF-index calculation without losing proxy-scope metadata."""

    METHOD_VERSION = "source_adapted_etf_index_csi800_a_share_proxy_v1"
    PROXY_MEMBER_COLUMNS = [
        "constituent_scope",
        "is_proxy",
        "scope_weight_rate",
    ]
    MEMBER_META_COLUMNS = [
        *ETFIndexFAPICalculator.MEMBER_META_COLUMNS,
        *PROXY_MEMBER_COLUMNS,
    ]
    _MEMBER_META_END = ETFIndexFAPICalculator.INDEX_OUTPUT_COLUMNS.index(
        "benchmark_code"
    )
    INDEX_OUTPUT_COLUMNS = [
        *ETFIndexFAPICalculator.INDEX_OUTPUT_COLUMNS[:_MEMBER_META_END],
        "member_constituent_scope",
        "member_is_proxy",
        "member_scope_weight_rate",
        *ETFIndexFAPICalculator.INDEX_OUTPUT_COLUMNS[_MEMBER_META_END:],
    ]

    def _adapt_members(
        self, members: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        prepared, member_meta = super()._adapt_members(members)
        member_meta = member_meta.rename(
            columns={
                "constituent_scope": "member_constituent_scope",
                "is_proxy": "member_is_proxy",
                "scope_weight_rate": "member_scope_weight_rate",
            }
        )
        if (
            not member_meta.empty
            and not member_meta["member_is_proxy"].fillna(False).all()
        ):
            raise ValueError("A股子样本FAPI任务收到了未标记的代理成分")
        return prepared, member_meta


__all__ = ["ETFIndexAShareProxyFAPICalculator"]
