"""AlphaHome 内部的 ETF 候选池月度维护入口。

本模块只负责编排候选池自身的月度资格、只读计划和原子执行。原始数据采集
与 ETF 产品事实刷新由 AlphaHome 的更新域先行完成；候选池执行时重新生成
计划并验证事实新鲜度，任何门禁失败都不会写入新快照。
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any

import psycopg2

from alphahome.curation.deepseek_candidate_client import (
    DEFAULT_MODEL,
    DeepSeekCandidateClient,
)
from alphahome.curation.etf_candidate_ai_automation import (
    CandidateAutomationError,
    build_candidate_automation_plan,
    execute_candidate_automation,
    get_existing_month_result,
)


MONTHLY_TASK_NAME = "etf_candidate_ai_monthly"
DEFAULT_NOT_BEFORE_DAY = 5
DEFAULT_BATCH_SIZE = 12
DEFAULT_MAX_NEW_PRODUCTS = 50


def _validate_not_before_day(value: int) -> None:
    if not 1 <= value <= 28:
        raise ValueError("not_before_day must be between 1 and 28")


def _model_name(model_requested: str | None) -> str:
    return model_requested or os.environ.get("DEEPSEEK_MODEL") or DEFAULT_MODEL


def _base_result(
    *,
    run_date: date,
    not_before_day: int,
    model_requested: str,
) -> dict[str, Any]:
    return {
        "task_name": MONTHLY_TASK_NAME,
        "run_date": run_date.isoformat(),
        "run_month": run_date.replace(day=1).isoformat(),
        "not_before_day": not_before_day,
        "model_requested": model_requested,
        "capital_authority": False,
        "order_authority": False,
    }


def build_candidate_monthly_maintenance_plan(
    database_url: str,
    *,
    run_date: date,
    model_requested: str | None = None,
    not_before_day: int = DEFAULT_NOT_BEFORE_DAY,
    reconfirm_all: bool = False,
    max_new_products: int = DEFAULT_MAX_NEW_PRODUCTS,
) -> dict[str, Any]:
    """生成供 GUI/CLI 展示的只读月度计划，不刷新数据、不调用模型。"""

    _validate_not_before_day(not_before_day)
    model = _model_name(model_requested)
    base = _base_result(
        run_date=run_date,
        not_before_day=not_before_day,
        model_requested=model,
    )
    if run_date.day < not_before_day:
        return {
            **base,
            "status": "deferred_before_monthly_window",
            "executable_now": False,
            "api_key_available": bool(os.environ.get("DEEPSEEK_API_KEY")),
        }

    connection = psycopg2.connect(database_url)
    try:
        existing = get_existing_month_result(connection, base["run_month"])
        connection.rollback()
        if existing is not None:
            return {
                **base,
                **existing,
                "executable_now": False,
                "api_key_available": bool(os.environ.get("DEEPSEEK_API_KEY")),
            }

        plan = build_candidate_automation_plan(
            connection,
            model_requested=model,
            run_date=run_date,
            reconfirm_all=reconfirm_all,
            max_new_products=max_new_products,
        )
        connection.rollback()
        summary = plan.summary()
        api_key_available = bool(os.environ.get("DEEPSEEK_API_KEY"))
        missing_api_key = bool(plan.target_items) and not api_key_available
        if missing_api_key:
            status = "blocked_missing_api_key"
        elif plan.executable:
            status = "ready"
        else:
            # GUI 的上游采集与 Features 尚未运行时，当前事实可能暂时陈旧。
            # 保留为可调度状态，实际执行会在上游完成后重新生成并验证计划。
            status = "ready_after_upstream_refresh"
        return {
            **base,
            **summary,
            "status": status,
            "executable_now": plan.executable and not missing_api_key,
            "api_key_available": api_key_available,
        }
    finally:
        connection.close()


def execute_candidate_monthly_maintenance(
    database_url: str,
    *,
    run_date: date,
    model_requested: str | None = None,
    not_before_day: int = DEFAULT_NOT_BEFORE_DAY,
    reconfirm_all: bool = False,
    max_new_products: int = DEFAULT_MAX_NEW_PRODUCTS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    expected_plan_hash: str | None = None,
) -> dict[str, Any]:
    """在上游更新完成后执行月度维护；每个自然月最多成功一次。"""

    _validate_not_before_day(not_before_day)
    model = _model_name(model_requested)
    base = _base_result(
        run_date=run_date,
        not_before_day=not_before_day,
        model_requested=model,
    )
    if run_date.day < not_before_day:
        return {
            **base,
            "status": "deferred_before_monthly_window",
        }

    connection = psycopg2.connect(database_url)
    try:
        existing = get_existing_month_result(connection, base["run_month"])
        connection.rollback()
        if existing is not None:
            return {**base, **existing}

        plan = build_candidate_automation_plan(
            connection,
            model_requested=model,
            run_date=run_date,
            reconfirm_all=reconfirm_all,
            max_new_products=max_new_products,
        )
        connection.rollback()
        if not plan.executable:
            raise CandidateAutomationError(
                "ETF candidate monthly guards failed after upstream refresh: "
                + str(plan.plan_payload["guards"])
            )
        if expected_plan_hash is not None and expected_plan_hash != plan.plan_hash:
            raise CandidateAutomationError(
                "expected plan hash does not match current read-only plan"
            )

        client = DeepSeekCandidateClient(model=model) if plan.target_items else None
        result = execute_candidate_automation(
            connection,
            plan,
            client=client,
            expected_plan_hash=plan.plan_hash,
            batch_size=batch_size,
        )
        result.update(
            {
                "task_name": MONTHLY_TASK_NAME,
                "current_candidate_count": plan.plan_payload[
                    "current_candidate_count"
                ],
                "new_product_count": plan.plan_payload["new_product_count"],
                "llm_target_count": plan.plan_payload["llm_target_count"],
                "capital_authority": False,
                "order_authority": False,
            }
        )
        return result
    finally:
        connection.close()


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_MAX_NEW_PRODUCTS",
    "DEFAULT_NOT_BEFORE_DAY",
    "MONTHLY_TASK_NAME",
    "build_candidate_monthly_maintenance_plan",
    "execute_candidate_monthly_maintenance",
]
