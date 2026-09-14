import numpy as np
import pandas as pd
import pytest

from fundpos.errors import DataUnavailable
from fundpos.reconstruction import market_cap_bridge, reconstruct_portfolio


def test_legacy_denominator_bug_is_fixed():
    old = pd.DataFrame(
        {
            "security_code": ["A", "B"],
            "group": ["C", "C"],
            "industry": ["801030.SI"] * 2,
            "weight": [0.1, 0.1],
        }
    )
    heavy = old.iloc[:1].copy()
    heavy["weight"] = 0.12
    allocation = pd.DataFrame({"group": ["C"], "weight": [0.2]})
    corrected = reconstruct_portfolio(
        old, heavy, allocation, pd.DataFrame(columns=["group", "weight", "industry"])
    )
    assert corrected.set_index("security_code").at["B", "weight"] == pytest.approx(0.08)
    assert corrected.weight.sum() == pytest.approx(0.2)
    assert 0.12 + 0.08 * 0.1 / 0.2 == pytest.approx(0.16)  # independent old-path calculation


def test_manufacturing_bridge_keeps_multiple_sw_industries():
    empty = pd.DataFrame(columns=["security_code", "group", "industry", "weight"])
    alloc = pd.DataFrame({"group": ["C"], "weight": [0.2]})
    bridge = market_cap_bridge(
        pd.DataFrame(
            {
                "security_code": ["A", "B"],
                "group": ["C", "C"],
                "industry": ["801030.SI", "801080.SI"],
                "float_mv": [30, 70],
            }
        )
    )
    result = reconstruct_portfolio(empty, empty, alloc, bridge)
    assert result.weight.tolist() == pytest.approx([0.06, 0.14])
    assert result.is_proxy.all()
    with pytest.raises(DataUnavailable, match="NO_PROXY_BRIDGE"):
        reconstruct_portfolio(empty, empty, alloc, bridge.iloc[:0])


def test_cap_applies_to_issuer_and_residual_is_not_lost():
    heavy = pd.DataFrame(
        {
            "security_code": [f"H{i}" for i in range(10)],
            "group": ["C"] * 10,
            "industry": ["801030.SI"] * 10,
            "weight": [0.01] * 10,
            "issuer_code": [f"H{i}" for i in range(10)],
        }
    )
    previous = pd.DataFrame(
        {
            "security_code": ["X.A", "X.H"],
            "issuer_code": ["X", "X"],
            "group": ["C", "C"],
            "industry": ["801030.SI", "hk"],
            "weight": [0.1, 0.1],
        }
    )
    allocation = pd.DataFrame({"group": ["C"], "weight": [0.2]})
    bridge = pd.DataFrame({"group": ["C"], "industry": ["801030.SI"], "weight": [1.0]})
    out = reconstruct_portfolio(previous, heavy, allocation, bridge)
    assert out.loc[out.security_code.isin(["X.A", "X.H"]), "weight"].sum() == pytest.approx(0.01)
    assert out.loc[out.is_proxy, "weight"].sum() == pytest.approx(0.09)
    assert np.isclose(out.weight.sum(), 0.2, atol=1e-6)


def test_inconsistent_disclosure_is_visible():
    heavy = pd.DataFrame(
        {"security_code": ["A"], "group": ["C"], "industry": ["801030.SI"], "weight": [0.25]}
    )
    with pytest.raises(DataUnavailable, match="HEAVY_EXCEEDS_DISCLOSURE"):
        reconstruct_portfolio(
            heavy.iloc[:0], heavy, pd.DataFrame({"group": ["C"], "weight": [0.2]}), pd.DataFrame()
        )
