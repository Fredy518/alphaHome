"""Registered P and G factor tasks."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.factors.base import FactorTask, FactorTaskContract
from alphahome.factors.core import GFactorCalculator, PFactorCalculator


@task_register()
class PFactorTask(FactorTask):
    name = "factor_p"
    table_name = "p_factor"
    description = "P因子周五PIT快照"
    contract = FactorTaskContract(
        task_name=name,
        domain="quality",
        source_tables=(
            "pit.pit_financial_indicators",
            "pit.pit_industry_classification",
        ),
        output_table="factors.p_factor",
        calc_date_key="calc_date",
        primary_keys=("ts_code", "calc_date"),
        dependencies=(),
        readiness_dependencies=(
            "pit_financial_indicators",
            "pit_industry_classification",
        ),
        supported_modes=("smart", "manual", "full", "audit"),
        calculator_class=PFactorCalculator,
        formula_version="v2.0",
        eligibility_policy="latest_eligible_pit_financial_indicator",
        audit_denominator="latest_eligible_pit_financial_indicator",
    )


@task_register()
class GFactorTask(FactorTask):
    name = "factor_g"
    table_name = "g_factor"
    description = "G因子周五成长快照"
    contract = FactorTaskContract(
        task_name=name,
        domain="growth",
        source_tables=("factors.p_factor",),
        output_table="factors.g_factor",
        calc_date_key="calc_date",
        primary_keys=("ts_code", "calc_date"),
        dependencies=("factor_p",),
        readiness_dependencies=(),
        supported_modes=("smart", "manual", "full", "audit"),
        calculator_class=GFactorCalculator,
        formula_version="v1.1",
        eligibility_policy="same_date_p_with_existing_history_threshold",
        audit_denominator="same_date_p_snapshot",
        history_lookback_days=730,
    )


__all__ = ["GFactorTask", "PFactorTask"]
