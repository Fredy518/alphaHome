#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test script for Barra risk model estimation.

Demonstrates factor covariance and specific variance estimation.
"""

import asyncio
import pandas as pd

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import create_async_manager
from alphahome.barra.risk_model import (
    RiskModel,
    RiskModelConfig,
    estimate_factor_covariance,
    estimate_specific_variance,
)


async def main():
    print("=" * 60)
    print("Barra Risk Model Estimation Demo")
    print("=" * 60)
    
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("No database URL configured")

    db = create_async_manager(db_url)
    await db.connect()
    
    try:
        # Fetch historical factor returns
        print("\n--- Fetching factor returns ---")
        fr_sql = """
            SELECT *
            FROM barra.factor_returns_daily
            ORDER BY trade_date
        """
        fr_rows = await db.fetch(fr_sql)
        fr_df = pd.DataFrame([dict(r) for r in fr_rows])
        print(f"Factor returns: {len(fr_df)} days")
        
        if len(fr_df) < 5:
            print("Not enough factor returns data for risk model estimation")
            return
        
        # Fetch specific returns
        print("\n--- Fetching specific returns ---")
        sr_sql = """
            SELECT trade_date, ticker, specific_return
            FROM barra.specific_returns_daily
        """
        sr_rows = await db.fetch(sr_sql)
        sr_df = pd.DataFrame([dict(r) for r in sr_rows])
        print(f"Specific returns: {len(sr_df)} stock-days")
        
        # Configure risk model with shorter window (we have limited data)
        n_days = len(fr_df)
        config = RiskModelConfig(
            cov_window=min(252, n_days),
            half_life=min(126, n_days // 2) if n_days > 10 else None,
            min_observations=min(30, n_days),
            newey_west_lags=2,
            specific_var_shrinkage=0.2,
        )
        print(f"\nConfig: window={config.cov_window}, half_life={config.half_life}, min_obs={config.min_observations}")
        
        # Estimate factor covariance
        print("\n--- Factor Covariance Estimation ---")
        try:
            factor_cov, cov_diag = estimate_factor_covariance(fr_df, config)
            print(f"Diagnostics: {cov_diag}")
            
            # Show factor volatilities
            import numpy as np
            vols = pd.Series(np.sqrt(np.diag(factor_cov.values)), index=factor_cov.index)
            print("\nAnnualized Factor Volatilities:")
            for f, v in vols.head(10).items():
                print(f"  {f}: {v:.2%}")
            if len(vols) > 10:
                print(f"  ... and {len(vols) - 10} more factors")
            
            # Show top correlations
            print("\nTop Factor Correlations (excluding diagonal):")
            corr = factor_cov.values / np.outer(vols.values, vols.values)
            np.fill_diagonal(corr, 0)
            corr_df = pd.DataFrame(corr, index=factor_cov.index, columns=factor_cov.columns)
            
            # Flatten and sort
            corr_pairs = []
            for i, f1 in enumerate(corr_df.index):
                for j, f2 in enumerate(corr_df.columns):
                    if i < j:
                        corr_pairs.append((f1, f2, corr_df.iloc[i, j]))
            corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
            for f1, f2, c in corr_pairs[:5]:
                print(f"  {f1} vs {f2}: {c:.3f}")
                
        except ValueError as e:
            print(f"Factor covariance estimation failed: {e}")
        
        # Estimate specific variance
        print("\n--- Specific Variance Estimation ---")
        try:
            spec_var, spec_diag = estimate_specific_variance(sr_df, config)
            print(f"Diagnostics: {spec_diag}")
            
            # Show distribution
            print(f"\nSpecific Volatility Distribution (annualized):")
            spec_vol = np.sqrt(spec_var["specific_var"])
            print(f"  Mean:   {spec_vol.mean():.2%}")
            print(f"  Median: {spec_vol.median():.2%}")
            print(f"  Min:    {spec_vol.min():.2%}")
            print(f"  Max:    {spec_vol.max():.2%}")
            
        except ValueError as e:
            print(f"Specific variance estimation failed: {e}")
        
        # Use RiskModel class
        print("\n--- Using RiskModel Class ---")
        model = RiskModel(config=config)
        try:
            model.fit(fr_df, sr_df)
            print("✅ Risk model fitted successfully!")
            
            factor_vol = model.get_factor_volatility()
            print(f"\nFactor count: {len(factor_vol)}")
            print(f"Specific variance stocks: {len(model.specific_var)}")
            
        except Exception as e:
            print(f"RiskModel fitting failed: {e}")
        
    finally:
        await db.close()
    
    print("\n" + "=" * 60)
    print("✅ Risk model demo completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
