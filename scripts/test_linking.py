#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test script for multi-period attribution linking.

Demonstrates the Carino/Menchero linking algorithms.
"""

from alphahome.barra.linking import (
    link_carino,
    link_menchero,
    link_simple_compound,
    MultiPeriodLinker,
)


def main():
    print("=" * 60)
    print("Multi-Period Attribution Linking Demo")
    print("=" * 60)
    
    # Simulate 5 days of single-period attributions
    period_returns = [0.012, -0.008, 0.015, 0.003, -0.005]  # Daily active returns
    period_contributions = [
        {"size": 0.004, "value": 0.003, "momentum": 0.002, "specific": 0.003},
        {"size": -0.002, "value": -0.003, "momentum": 0.001, "specific": -0.004},
        {"size": 0.005, "value": 0.004, "momentum": 0.003, "specific": 0.003},
        {"size": 0.001, "value": 0.001, "momentum": 0.000, "specific": 0.001},
        {"size": -0.001, "value": -0.002, "momentum": -0.001, "specific": -0.001},
    ]
    
    print("\n--- Input Data ---")
    print(f"Period returns: {period_returns}")
    print(f"Sum of returns (arithmetic): {sum(period_returns):.4f}")
    
    # Geometric total return
    import numpy as np
    geo_total = np.prod([1 + r for r in period_returns]) - 1
    print(f"Geometric total return: {geo_total:.4f}")
    
    print("\n--- Method 1: Carino Linking ---")
    carino_result = link_carino(period_returns, period_contributions)
    carino_sum = sum(carino_result.values())
    print(f"Linked contributions: {carino_result}")
    print(f"Sum of linked contributions: {carino_sum:.6f}")
    print(f"Difference from geo return: {abs(geo_total - carino_sum):.6e}")
    
    print("\n--- Method 2: Menchero Linking ---")
    menchero_result = link_menchero(period_returns, period_contributions)
    menchero_sum = sum(menchero_result.values())
    print(f"Linked contributions: {menchero_result}")
    print(f"Sum of linked contributions: {menchero_sum:.6f}")
    print(f"Difference from geo return: {abs(geo_total - menchero_sum):.6e}")
    
    print("\n--- Method 3: Simple Additive (no compounding) ---")
    simple_result = link_simple_compound(period_contributions)
    simple_sum = sum(simple_result.values())
    print(f"Summed contributions: {simple_result}")
    print(f"Sum: {simple_sum:.6f}")
    print(f"(Note: Simple method doesn't account for compounding)")
    
    print("\n--- Using MultiPeriodLinker Class ---")
    linker = MultiPeriodLinker(method="carino")
    for i, (r, c) in enumerate(zip(period_returns, period_contributions), 1):
        linker.add_period(return_=r, contributions=c, date=f"Day {i}")
    
    result = linker.get_linked()
    print(f"Total return: {result['total_return']:.4f}")
    print(f"N periods: {result['n_periods']}")
    print(f"Linked contributions: {result['linked_contributions']}")
    print(f"Recon error: {result['recon_error']:.6e}")
    
    print("\n--- Period-by-period DataFrame ---")
    df = linker.to_dataframe()
    print(df)
    
    print("\n" + "=" * 60)
    print("✅ Multi-period linking demo completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
