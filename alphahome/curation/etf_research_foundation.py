"""ETF research-foundation orchestration shared by scripts and the GUI.

The service keeps manual candidate identity separate from AlphaHome-computed
facts and always preserves the no-capital/no-orders boundary.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable

import psycopg2

from ..features.recipes.mv.fund.etf_product_facts_current import (
    ETFProductFactsCurrentMV,
)
from ..features.recipes.mv.index.etf_exposure_technical_current_universe_daily import (
    ETFExposureTechnicalCurrentUniverseDailyMV,
)
from ..features.recipes.mv.index.index_direct_valuation_daily import (
    IndexDirectValuationDailyMV,
)
from ..features.recipes.mv.industry.industry_earnings_observation_monthly import (
    IndustryEarningsObservationMonthlyMV,
)
from ..features.storage.database_init import FeaturesDatabaseInit
from .etf_candidate_master import (
    ensure_candidate_data_coverage_view,
    load_candidate_master_snapshot,
    read_and_validate_payload,
)

ProgressCallback = Callable[[dict[str, Any]], None]

PRE_IMPORT_RECIPES = (
    ETFProductFactsCurrentMV,
    IndexDirectValuationDailyMV,
    IndustryEarningsObservationMonthlyMV,
)
POST_IMPORT_RECIPES = (ETFExposureTechnicalCurrentUniverseDailyMV,)

FACT_OBJECTS = (
    {
        "key": "etf_product_facts",
        "label": "ETF产品事实",
        "relation": "features.mv_etf_product_facts_current",
        "date_column": "as_of_date",
        "boundary": "当前快照，非PIT历史",
    },
    {
        "key": "index_direct_valuation",
        "label": "指数直接估值",
        "relation": "features.mv_index_direct_valuation_daily",
        "date_column": "trade_date",
        "boundary": "仅数据商直接口径，不静默重构",
    },
    {
        "key": "etf_exposure_technical",
        "label": "候选指数技术原子",
        "relation": "features.mv_etf_exposure_technical_current_universe_daily",
        "date_column": "trade_date",
        "boundary": "当前候选宇宙，不宣称无幸存者偏差",
    },
    {
        "key": "industry_earnings",
        "label": "行业盈利与预期原子",
        "relation": "features.mv_industry_earnings_observation_monthly",
        "date_column": "obs_date",
        "boundary": "PIT月频原子，不含事后合成分数",
    },
)

RELATION_KEYS = {
    "refresh_log": "features.mv_refresh_log",
    "candidate_batch": "fund_pool_on.etf_candidate_master_latest_batch",
    "candidate_current": "fund_pool_on.etf_candidate_master_current_enriched",
    "candidate_ai_run": "fund_pool_on.etf_candidate_ai_run",
    "index_coverage": "fund_pool_on.etf_candidate_index_coverage_current",
    **{item["key"]: item["relation"] for item in FACT_OBJECTS},
}


def _emit_progress(
    callback: ProgressCallback | None,
    *,
    stage: str,
    message: str,
    status: str = "running",
    **details: Any,
) -> None:
    if callback is None:
        return
    try:
        callback(
            {
                "stage": stage,
                "message": message,
                "status": status,
                **details,
            }
        )
    except Exception:
        # Progress reporting must never change the database outcome.
        return


def _record_to_dict(record: Any) -> dict[str, Any]:
    return dict(record) if record is not None else {}


async def _ensure_and_refresh(
    db_manager: Any,
    recipe_class: type,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    recipe = recipe_class(db_manager=db_manager, schema="features")
    _emit_progress(
        progress_callback,
        stage=recipe.name,
        message=f"正在刷新 {recipe.description}",
    )
    existed = await recipe.exists()
    if not existed:
        await recipe.create(if_not_exists=False)
    result = await recipe.refresh()
    if result.get("status") != "success":
        raise RuntimeError(
            f"refresh failed for {recipe.full_name}: "
            f"{result.get('error_message') or result}"
        )
    summary = {
        "name": recipe.full_name,
        "created": not existed,
        "status": result["status"],
        "row_count": result.get("row_count"),
        "duration_seconds": result.get("duration_seconds"),
        "refresh_time": result.get("refresh_time"),
    }
    _emit_progress(
        progress_callback,
        stage=recipe.name,
        message=f"{recipe.description} 刷新完成",
        status="success",
        row_count=summary["row_count"],
    )
    return summary


def _load_snapshot(
    database_url: str,
    payload: dict[str, Any],
    *,
    verify_source_file: bool,
) -> dict[str, Any]:
    connection = psycopg2.connect(database_url)
    try:
        return load_candidate_master_snapshot(
            connection,
            payload,
            verify_source_file=verify_source_file,
        )
    finally:
        connection.close()


def _ensure_coverage_view(database_url: str) -> None:
    connection = psycopg2.connect(database_url)
    try:
        if not ensure_candidate_data_coverage_view(connection):
            raise RuntimeError(
                "candidate index coverage view prerequisites are missing"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


async def get_etf_research_foundation_status(db_manager: Any) -> dict[str, Any]:
    """Return a GUI-facing status snapshot for all managed objects."""

    relation_sql = "SELECT " + ", ".join(
        f"(to_regclass('{relation}') IS NOT NULL) AS {key}"
        for key, relation in RELATION_KEYS.items()
    )
    relation_rows = await db_manager.fetch(relation_sql)
    relations = _record_to_dict(relation_rows[0]) if relation_rows else {}

    refresh_names = [item["relation"].rsplit(".", 1)[1] for item in FACT_OBJECTS]
    log_rows = []
    if relations.get("refresh_log"):
        log_rows = await db_manager.fetch(
            """
            SELECT DISTINCT ON (view_name)
                view_name,
                finished_at AT TIME ZONE 'Asia/Shanghai' AS finished_at,
                row_count,
                success
            FROM features.mv_refresh_log
            WHERE schema_name = 'features'
              AND view_name = ANY($1::text[])
            ORDER BY view_name, finished_at DESC
            """,
            refresh_names,
        )
    refresh_logs = {row["view_name"]: _record_to_dict(row) for row in log_rows or []}

    facts: list[dict[str, Any]] = []
    watermarks: dict[str, Any] = {}
    for item in FACT_OBJECTS:
        exists = bool(relations.get(item["key"]))
        watermark = None
        if exists:
            rows = await db_manager.fetch(
                f"SELECT MAX({item['date_column']}) AS watermark "
                f"FROM {item['relation']}"
            )
            watermark = rows[0]["watermark"] if rows else None
        view_name = item["relation"].rsplit(".", 1)[1]
        refresh_log = refresh_logs.get(view_name, {})
        facts.append(
            {
                "key": item["key"],
                "label": item["label"],
                "relation": item["relation"],
                "exists": exists,
                "row_count": refresh_log.get("row_count") if exists else 0,
                "last_refresh": refresh_log.get("finished_at") if exists else None,
                "watermark": watermark,
                "boundary": item["boundary"],
            }
        )
        watermarks[item["key"]] = watermark

    candidate_batch: dict[str, Any] = {}
    candidate_current: dict[str, Any] = {}
    candidate_ai_run: dict[str, Any] = {}
    index_coverage: dict[str, Any] = {}
    if relations.get("candidate_batch"):
        rows = await db_manager.fetch(
            """
            SELECT
                snapshot_id,
                source_file_name,
                source_file_sha256,
                workbook_generated_on,
                product_facts_as_of,
                loaded_at AT TIME ZONE 'Asia/Shanghai' AS loaded_at,
                row_count,
                exposure_count,
                capital_authority,
                order_authority
            FROM fund_pool_on.etf_candidate_master_latest_batch
            """
        )
        candidate_batch = _record_to_dict(rows[0]) if rows else {}
    if relations.get("candidate_current"):
        rows = await db_manager.fetch(
            """
            SELECT
                COUNT(*)::integer AS row_count,
                COUNT(DISTINCT exposure_id)::integer AS exposure_count,
                COUNT(*) FILTER (WHERE candidate_status = '正式候选')::integer
                    AS formal_candidate_count,
                COUNT(*) FILTER (WHERE candidate_status = '条件候选')::integer
                    AS conditional_candidate_count,
                COUNT(*) FILTER (WHERE candidate_status = '观察')::integer
                    AS watch_count,
                COUNT(*) FILTER (
                    WHERE confirmation_status = 'AI_CONFIRMED'
                )::integer AS ai_confirmed_count,
                COUNT(*) FILTER (
                    WHERE confirmation_status = 'AI_REVIEW_REQUIRED'
                )::integer AS ai_review_required_count,
                COUNT(*) FILTER (
                    WHERE confirmation_status = 'HUMAN_CONFIRMED'
                )::integer AS human_confirmed_count,
                COUNT(*) FILTER (
                    WHERE confirmation_status = 'LEGACY_IMPORTED'
                )::integer AS legacy_imported_count,
                COUNT(*) FILTER (WHERE live_facts_as_of IS NOT NULL)::integer
                    AS live_product_fact_count,
                COUNT(*) FILTER (WHERE live_core_facts_complete)::integer
                    AS live_complete_count,
                COUNT(*) FILTER (
                    WHERE live_product_auxiliary_state IS DISTINCT FROM
                          snapshot_product_auxiliary_state
                )::integer AS live_auxiliary_state_change_count,
                MAX(live_facts_as_of) AS live_facts_as_of
            FROM fund_pool_on.etf_candidate_master_current_enriched
            """
        )
        candidate_current = _record_to_dict(rows[0]) if rows else {}
    if relations.get("candidate_ai_run"):
        rows = await db_manager.fetch(
            """
            SELECT
                ai_run_id,
                run_month,
                facts_as_of,
                model_requested,
                prompt_version,
                status,
                decision_count,
                output_snapshot_id,
                finished_at AT TIME ZONE 'Asia/Shanghai' AS finished_at
            FROM fund_pool_on.etf_candidate_ai_run
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        candidate_ai_run = _record_to_dict(rows[0]) if rows else {}
    if relations.get("index_coverage"):
        rows = await db_manager.fetch(
            """
            SELECT
                COUNT(*)::integer AS tracking_index_count,
                COUNT(*) FILTER (WHERE technical_available)::integer
                    AS technical_index_count,
                COUNT(*) FILTER (WHERE direct_valuation_available)::integer
                    AS direct_valuation_index_count,
                MAX(technical_latest_date) AS technical_latest_date,
                MAX(valuation_latest_date) AS direct_valuation_latest_date
            FROM fund_pool_on.etf_candidate_index_coverage_current
            """
        )
        index_coverage = _record_to_dict(rows[0]) if rows else {}

    return {
        "status": "success",
        "relations": relations,
        "facts": facts,
        "candidate_batch": candidate_batch,
        "candidate_current": candidate_current,
        "candidate_ai_run": candidate_ai_run,
        "index_coverage": index_coverage,
        "watermarks": watermarks,
        "authority": {
            "capital_authority": False,
            "order_authority": False,
        },
    }


async def update_etf_research_foundation(
    db_manager: Any,
    database_url: str,
    candidate_snapshot: str | Path,
    *,
    verify_source_file: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Run the ordered, idempotent ETF research-foundation update."""

    snapshot_path = Path(candidate_snapshot)
    _emit_progress(
        progress_callback,
        stage="validate",
        message="正在校验候选母表标准快照和来源哈希",
    )
    payload = await asyncio.to_thread(
        read_and_validate_payload,
        snapshot_path,
        verify_source_file=verify_source_file,
    )

    initializer = FeaturesDatabaseInit(db_manager=db_manager, schema="features")
    await initializer.ensure_initialized()

    refreshes: list[dict[str, Any]] = []
    for recipe_class in PRE_IMPORT_RECIPES:
        refreshes.append(
            await _ensure_and_refresh(db_manager, recipe_class, progress_callback)
        )

    _emit_progress(
        progress_callback,
        stage="candidate_import",
        message="正在幂等载入候选母表版本",
    )
    candidate_load = await asyncio.to_thread(
        _load_snapshot,
        database_url,
        payload,
        verify_source_file=verify_source_file,
    )
    _emit_progress(
        progress_callback,
        stage="candidate_import",
        message="候选母表版本载入完成",
        status="success",
        row_count=candidate_load.get("row_count"),
    )

    for recipe_class in POST_IMPORT_RECIPES:
        refreshes.append(
            await _ensure_and_refresh(db_manager, recipe_class, progress_callback)
        )

    await asyncio.to_thread(_ensure_coverage_view, database_url)
    foundation_status = await get_etf_research_foundation_status(db_manager)
    _emit_progress(
        progress_callback,
        stage="complete",
        message="ETF研究底座维护完成",
        status="success",
    )
    return {
        "status": "success",
        "snapshot_id": payload["snapshot_id"],
        "authority": foundation_status["authority"],
        "refreshes": refreshes,
        "candidate_load": candidate_load,
        "candidate_batch": foundation_status["candidate_batch"],
        "candidate_current": foundation_status["candidate_current"],
        "index_coverage": foundation_status["index_coverage"],
        "watermarks": foundation_status["watermarks"],
        "facts": foundation_status["facts"],
    }


__all__ = [
    "FACT_OBJECTS",
    "POST_IMPORT_RECIPES",
    "PRE_IMPORT_RECIPES",
    "get_etf_research_foundation_status",
    "update_etf_research_foundation",
]
