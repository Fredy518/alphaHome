"""Barra processor tasks.

Tasks are added incrementally:
- exposures (X_{t-1})
- factor returns (WLS, sum-to-zero)
- portfolio attribution (single-period)
- multi-period attribution (Carino/Menchero linking)

Schema/table creation is handled by scripts/initialize_barra_schema.py.
"""

# Import task modules so @task_register decorators execute on import.
from .barra_exposures_daily import BarraExposuresDailyTask  # noqa: F401
from .barra_exposures_full import BarraExposuresFullTask  # noqa: F401  # Post-MVP Full Barra
from .barra_factor_returns_daily import BarraFactorReturnsDailyTask  # noqa: F401
from .barra_portfolio_attribution_daily import BarraPortfolioAttributionDailyTask  # noqa: F401
from .barra_multi_period_attribution import BarraMultiPeriodAttributionTask  # noqa: F401
from .barra_risk_model_daily import BarraRiskModelDailyTask  # noqa: F401
