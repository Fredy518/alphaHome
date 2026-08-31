"""Audit and coverage utilities for PIT tasks."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from alphahome.common.logging_utils import get_logger
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.pit.base.pit_task import PITTaskContract

logger = get_logger(__name__)


def _quote_identifier(value: str) -> str:
    return f'"{str(value).replace(chr(34), chr(34) * 2)}"'


def _split_relation(relation: str) -> Tuple[str, str]:
    if "." not in relation:
        return "public", relation
    schema, table = relation.split(".", 1)
    return schema, table


def _qualified(relation: str) -> str:
    schema, table = _split_relation(relation)
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


class PITAuditService:
    """Read-only PIT coverage checks plus audit snapshot persistence."""

    def __init__(self, db_manager: Any):
        self.db = db_manager
        self._listed_stock_count: Optional[int] = None
        self._listed_stock_count_lock = asyncio.Lock()

    async def list_pit_tasks(self) -> List[Dict[str, Any]]:
        task_classes = self._pit_task_classes()
        task_specs: List[Tuple[str, Any, PITTaskContract]] = []
        for task_name in sorted(task_classes):
            task_class = task_classes[task_name]
            contract = getattr(task_class, "contract", None)
            if not isinstance(contract, PITTaskContract):
                continue
            task_specs.append((task_name, task_class, contract))

        async def load_task(
            task_name: str,
            task_class: Any,
            contract: PITTaskContract,
        ) -> Dict[str, Any]:
            info = contract.to_dict()
            info.update(
                {
                    "name": task_name,
                    "description": getattr(task_class, "description", ""),
                    "selected": False,
                }
            )
            try:
                stats = await self._table_stats(contract)
                info.update(
                    {
                        "latest_date": stats.get("latest_pit_time"),
                        "row_count": stats.get("row_count", 0),
                        "coverage_rate": stats.get("coverage_rate"),
                        "gap_count": stats.get("gap_count"),
                        "live_status": stats.get("status", "unknown"),
                    }
                )
            except Exception as exc:
                logger.warning("获取PIT实时表状态失败: %s: %s", task_name, exc)
                info.update(
                    {
                        "latest_date": None,
                        "row_count": 0,
                        "coverage_rate": None,
                        "gap_count": None,
                        "live_status": "error",
                    }
                )

            audit_snapshot: Optional[Dict[str, Any]] = None
            try:
                audit_snapshot = await self._latest_snapshot_for_task(task_name)
            except Exception as exc:
                logger.warning("获取PIT最近审计快照失败: %s: %s", task_name, exc)
            info.update(
                audit_snapshot
                or {
                    "last_audit_time": None,
                    "audited_latest_date": None,
                    "audited_row_count": None,
                    "audited_coverage_rate": None,
                    "audited_gap_count": None,
                    "audit_status": None,
                }
            )

            execution: Optional[Dict[str, Any]] = None
            try:
                execution = await self._latest_task_execution_for_task(task_name)
            except Exception as exc:
                logger.warning("获取PIT最近执行记录失败: %s: %s", task_name, exc)
            info.update(
                execution
                or {
                    "last_execution_time": None,
                    "last_execution_status": None,
                    "last_execution_details": None,
                }
            )

            # Compatibility aliases for callers that still consume the old keys.
            info["recent_status"] = info.get("last_execution_status") or info.get("live_status")
            info["last_run_time"] = info.get("last_execution_time")
            return info

        return list(
            await asyncio.gather(
                *(
                    load_task(task_name, task_class, contract)
                    for task_name, task_class, contract in task_specs
                )
            )
        )

    async def audit_all(self, persist: bool = True) -> List[Dict[str, Any]]:
        task_names = sorted(self._pit_task_classes().keys())
        results = []
        for task_name in task_names:
            results.append(await self.audit_task(task_name, persist=persist))
        return results

    async def audit_task(self, task_name: str, persist: bool = True) -> Dict[str, Any]:
        task_classes = self._pit_task_classes()
        task_class = task_classes.get(task_name)
        if task_class is None:
            return {"task_name": task_name, "status": "error", "error": "未注册的PIT任务"}

        contract = getattr(task_class, "contract", None)
        if not isinstance(contract, PITTaskContract):
            return {"task_name": task_name, "status": "error", "error": "任务缺少PIT contract"}

        stats = await self._table_stats(contract)
        raw_gap = await self._raw_gap_summary(contract, stats.get("coverage_period"))
        domain_details = await self._domain_audit_details(
            contract, stats.get("coverage_period")
        )
        details = {
            "contract": contract.to_dict(),
            "coverage_period": stats.get("coverage_period"),
            "coverage_count": stats.get("coverage_count"),
            "listed_stock_count": stats.get("listed_stock_count"),
            "denominator_name": stats.get("denominator_name"),
            "denominator_count": stats.get("denominator_count"),
            "raw_vs_pit": raw_gap,
            "domain_metrics": domain_details,
        }
        status = stats.get("status", "unknown")
        if status == "healthy" and contract.domain == "industry_fttm":
            valued_count = sum(
                int(row.get("valued_industries") or 0)
                for row in domain_details.get("levels", [])
            )
            if valued_count == 0:
                status = "structure_only"
        if status == "healthy" and contract.domain == "industry_fapi":
            valued_count = sum(
                int(row.get("valued_industries") or 0)
                for row in domain_details.get("levels", [])
            )
            if valued_count == 0:
                status = "structure_only"
        if status == "healthy" and contract.domain == "index_fttm":
            if int(domain_details.get("valued_universes") or 0) == 0:
                status = "structure_only"
        result = {
            "task_name": task_name,
            "output_table": contract.output_table,
            "latest_pit_time": stats.get("latest_pit_time"),
            "row_count": stats.get("row_count", 0),
            "coverage_rate": stats.get("coverage_rate"),
            "gap_count": stats.get("gap_count"),
            "status": status,
            "details": details,
        }
        if persist:
            await self._persist_audit_snapshot(result)
        return result

    async def get_coverage_matrix(self, limit: int = 8) -> Dict[str, Any]:
        tasks = await self._contracts()
        financial_tasks = [item for item in tasks if item[1].domain == "financials"]
        periods = await self._latest_financial_periods([contract for _, contract in financial_tasks], limit)
        listed_count = await self._current_listed_count()
        rows: List[Dict[str, Any]] = []

        for task_name, contract in financial_tasks:
            if not await self._relation_exists(contract.output_table):
                for period in periods:
                    rows.append(
                        {
                            "task_name": task_name,
                            "output_table": contract.output_table,
                            "report_period": period,
                            "coverage_count": 0,
                            "listed_stock_count": listed_count,
                            "coverage_rate": 0.0 if listed_count else None,
                            "gap_count": listed_count if listed_count else None,
                        }
                    )
                continue
            relation = _qualified(contract.output_table)
            listed_join = await self._listed_join_sql("t")
            for period in periods:
                record = await self.db.fetch_one(
                    f"""
                    SELECT COUNT(DISTINCT t.ts_code)::bigint AS coverage_count
                    FROM {relation} t
                    {listed_join}
                    WHERE t.end_date = $1
                    """,
                    period,
                )
                coverage_count = int(record["coverage_count"] or 0) if record else 0
                rows.append(
                    {
                        "task_name": task_name,
                        "output_table": contract.output_table,
                        "report_period": period,
                        "coverage_count": coverage_count,
                        "listed_stock_count": listed_count,
                        "coverage_rate": round(coverage_count / listed_count, 6) if listed_count else None,
                        "gap_count": max(listed_count - coverage_count, 0) if listed_count else None,
                    }
                )
        return {"listed_stock_count": listed_count, "periods": periods, "rows": rows}

    async def diagnose_stock(self, ts_code: str, period_limit: int = 24) -> Dict[str, Any]:
        ts_code = (ts_code or "").strip().upper()
        if not ts_code:
            return {"status": "error", "error": "缺少股票代码", "ts_code": ts_code}

        results: List[Dict[str, Any]] = []
        for task_name, contract in await self._contracts():
            task_result: Dict[str, Any] = {
                "task_name": task_name,
                "output_table": contract.output_table,
                "domain": contract.domain,
                "pit_time_key": contract.pit_time_key,
            }
            if not await self._relation_exists(contract.output_table):
                task_result["status"] = "missing_table"
                results.append(task_result)
                continue

            if contract.domain == "financials":
                pit_periods = await self._periods_for_stock(contract.output_table, ts_code, "end_date", period_limit)
                raw_periods = await self._raw_periods_for_stock(contract.source_tables, ts_code, period_limit)
                expected_periods = await self._expected_financial_periods_for_stock(ts_code, period_limit)
                missing_expected_periods = sorted(set(expected_periods) - set(pit_periods), reverse=True)
                task_result.update(
                    {
                        "status": "ok",
                        "expected_periods": expected_periods,
                        "pit_periods": pit_periods,
                        "raw_periods": raw_periods,
                        "raw_missing_in_pit": sorted(set(raw_periods) - set(pit_periods), reverse=True),
                        "pit_not_in_raw": sorted(set(pit_periods) - set(raw_periods), reverse=True),
                        "missing_expected_periods": missing_expected_periods,
                        "gap_diagnosis": await self._diagnose_financial_gap_reasons(
                            contract,
                            ts_code,
                            missing_expected_periods[:period_limit],
                        ),
                        "latest_rows": await self._latest_rows_for_stock(contract, ts_code),
                    }
                )
            else:
                obs_dates = await self._periods_for_stock(contract.output_table, ts_code, contract.pit_time_key, period_limit)
                task_result.update(
                    {
                        "status": "ok",
                        "pit_periods": obs_dates,
                        "latest_rows": await self._latest_rows_for_stock(contract, ts_code),
                    }
                )
            results.append(task_result)

        return {"status": "success", "ts_code": ts_code, "tasks": results}

    async def _contracts(self) -> List[Tuple[str, PITTaskContract]]:
        task_classes = self._pit_task_classes()
        contracts = []
        for task_name, task_class in sorted(task_classes.items()):
            contract = getattr(task_class, "contract", None)
            if isinstance(contract, PITTaskContract):
                contracts.append((task_name, contract))
        return contracts

    @staticmethod
    def _pit_task_classes() -> Dict[str, Any]:
        """Return registered PIT task classes without requiring a GUI bootstrap."""
        from alphahome.pit.tasks import discover_tasks

        discover_tasks()
        try:
            return UnifiedTaskFactory.get_tasks_by_type("pit")
        except RuntimeError as exc:
            if "尚未初始化" not in str(exc):
                raise
            registry = getattr(UnifiedTaskFactory, "_task_registry", {})
            return {
                name: task_class
                for name, task_class in registry.items()
                if getattr(task_class, "task_type", None) == "pit"
            }

    async def _table_stats(self, contract: PITTaskContract) -> Dict[str, Any]:
        if not await self._relation_exists(contract.output_table):
            denominator_count = await self._denominator_count(contract, None)
            return {
                "status": "missing_table",
                "row_count": 0,
                "latest_pit_time": None,
                "coverage_period": None,
                "coverage_count": 0,
                "listed_stock_count": denominator_count,
                "denominator_name": contract.audit_denominator,
                "denominator_count": denominator_count,
                "coverage_rate": 0.0 if denominator_count else None,
                "gap_count": denominator_count if denominator_count else None,
            }

        relation = _qualified(contract.output_table)
        columns = await self._get_columns(contract.output_table)
        time_key = contract.pit_time_key if contract.pit_time_key in columns else None
        latest_expr = f"MAX({_quote_identifier(time_key)})::date" if time_key else "NULL::date"
        row = await self.db.fetch_one(
            f"""
            SELECT COUNT(*)::bigint AS row_count,
                   {latest_expr} AS latest_pit_time
            FROM {relation}
            """
        )
        row_count = int(row["row_count"] or 0) if row else 0
        latest_pit_time = row["latest_pit_time"] if row else None

        coverage_key = "end_date" if contract.domain == "financials" and "end_date" in columns else contract.pit_time_key
        preliminary_denominator = (
            await self._current_listed_count()
            if contract.audit_denominator == "current_listed_stocks"
            else 0
        )
        coverage = await self._coverage_for_latest(
            contract,
            coverage_key,
            min_coverage_count=(
                max(1, int(preliminary_denominator * 0.05))
                if contract.domain == "financials" and preliminary_denominator
                else 1
            ),
        )
        coverage_count = int(coverage.get("coverage_count") or 0)
        coverage_period = coverage.get("coverage_period")
        denominator_count = await self._denominator_count(contract, coverage_period)
        coverage_rate = (
            round(coverage_count / denominator_count, 6) if denominator_count else None
        )
        gap_count = (
            max(denominator_count - coverage_count, 0) if denominator_count else None
        )
        status = "empty" if row_count == 0 else "healthy"
        return {
            "status": status,
            "row_count": row_count,
            "latest_pit_time": latest_pit_time,
            "coverage_period": coverage_period,
            "coverage_count": coverage_count,
            # Compatibility key used by the existing GUI.  New consumers should
            # use denominator_name/count because an industry entity is not a stock.
            "listed_stock_count": denominator_count,
            "denominator_name": contract.audit_denominator,
            "denominator_count": denominator_count,
            "coverage_rate": coverage_rate,
            "gap_count": gap_count,
        }

    async def _coverage_for_latest(
        self,
        contract: PITTaskContract,
        coverage_key: str,
        min_coverage_count: int = 1,
    ) -> Dict[str, Any]:
        output_table = contract.output_table
        columns = await self._get_columns(output_table)
        if coverage_key not in columns:
            return {"coverage_period": None, "coverage_count": 0}
        relation = _qualified(output_table)
        key = _quote_identifier(coverage_key)
        entity_keys = tuple(contract.audit_entity_keys or ("ts_code",))
        available_entity_keys = [value for value in entity_keys if value in columns]
        if len(available_entity_keys) != len(entity_keys):
            return {"coverage_period": None, "coverage_count": 0}
        if len(available_entity_keys) == 1:
            distinct_expression = f"t.{_quote_identifier(available_entity_keys[0])}"
        else:
            distinct_expression = "(" + ", ".join(
                f"t.{_quote_identifier(value)}" for value in available_entity_keys
            ) + ")"
        entity_not_null = " AND ".join(
            f"t.{_quote_identifier(value)} IS NOT NULL" for value in available_entity_keys
        )
        listed_join = ""
        extra_filter = ""
        if contract.audit_denominator == "current_listed_stocks" and "ts_code" in available_entity_keys:
            listed_join = await self._listed_join_sql("t")
        elif contract.audit_denominator == "pit_time_active_stocks" and "ts_code" in available_entity_keys:
            extra_filter = f"""
                AND EXISTS (
                    SELECT 1
                    FROM rawdata.stock_basic active
                    WHERE active.ts_code = t.ts_code
                      AND active.list_date <= t.{key}
                      AND (active.delist_date IS NULL OR active.delist_date > t.{key})
                      AND active.exchange IN ('SSE', 'SZSE', 'BSE')
                      AND active.curr_type = 'CNY'
                )
            """
        threshold = max(1, int(min_coverage_count or 1))
        before_period: Any = None
        fallback: Optional[Dict[str, Any]] = None

        # Count only one indexed candidate period at a time.  The previous SQL
        # grouped every historical row on each GUI refresh, which made list
        # loading scale with the full PIT history instead of the latest cross-section.
        while True:
            period_filter = "" if before_period is None else f"AND t.{key} < $1"
            period_args: tuple[Any, ...] = () if before_period is None else (before_period,)
            period_row = await self.db.fetch_one(
                f"""
                SELECT /* pit_coverage_candidate_period */
                       MAX(t.{key})::date AS coverage_period
                FROM {relation} t
                WHERE t.{key} IS NOT NULL
                  {period_filter}
                """,
                *period_args,
            )
            coverage_period = period_row["coverage_period"] if period_row else None
            if coverage_period is None:
                break

            count_row = await self.db.fetch_one(
                f"""
                SELECT /* pit_coverage_count */
                       COUNT(DISTINCT {distinct_expression})::bigint AS coverage_count
                FROM {relation} t
                {listed_join}
                WHERE t.{key} = $1
                  AND {entity_not_null}
                  {extra_filter}
                """,
                coverage_period,
            )
            coverage_count = int(count_row["coverage_count"] or 0) if count_row else 0
            candidate = {
                "coverage_period": coverage_period,
                "coverage_count": coverage_count,
            }
            if coverage_count > 0 and fallback is None:
                fallback = candidate
            if coverage_count >= threshold:
                return candidate
            before_period = coverage_period

        return fallback or {"coverage_period": None, "coverage_count": 0}

    async def _denominator_count(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> int:
        denominator = contract.audit_denominator or "current_listed_stocks"
        if denominator == "current_listed_stocks":
            return await self._current_listed_count()
        if denominator == "pit_time_active_stocks":
            if not coverage_period or not await self._relation_exists("rawdata.stock_basic"):
                return 0
            row = await self.db.fetch_one(
                """
                SELECT COUNT(DISTINCT ts_code)::bigint AS cnt
                FROM rawdata.stock_basic
                WHERE list_date <= $1
                  AND (delist_date IS NULL OR delist_date > $1)
                  AND exchange IN ('SSE', 'SZSE', 'BSE')
                  AND curr_type = 'CNY'
                """,
                coverage_period,
            )
            return int(row["cnt"] or 0) if row else 0
        if denominator == "pit_time_structural_industries":
            if not coverage_period or not await self._relation_exists(
                "pit.pit_industry_classification"
            ):
                return 0
            row = await self.db.fetch_one(
                """
                WITH structural AS (
                    SELECT data_source AS classification_source,
                           'L1'::text AS industry_level,
                           industry_code1 AS industry_code,
                           'total_mv'::text AS weight_basis
                    FROM pit.pit_industry_classification
                    WHERE obs_date = $1
                      AND data_source = 'sw'
                      AND industry_code1 IS NOT NULL
                    UNION
                    SELECT data_source AS classification_source,
                           'L2'::text AS industry_level,
                           industry_code2 AS industry_code,
                           'total_mv'::text AS weight_basis
                    FROM pit.pit_industry_classification
                    WHERE obs_date = $1
                      AND data_source = 'sw'
                      AND industry_code2 IS NOT NULL
                )
                SELECT COUNT(*)::bigint AS cnt FROM structural
                """,
                coverage_period,
            )
            return int(row["cnt"] or 0) if row else 0
        if denominator == "configured_index_fttm_universes":
            if not coverage_period:
                return 0
            from alphahome.pit.pit_index_fttm_manager import configured_universe_count

            return configured_universe_count(coverage_period)
        if denominator == "annual_report_events_at_ann_date":
            if not coverage_period or not await self._relation_exists(
                "pit.pit_income_quarterly"
            ):
                return 0
            income_columns = await self._get_columns("pit.pit_income_quarterly")
            if "n_income_attr_p_ytd" not in income_columns:
                return 0
            row = await self.db.fetch_one(
                """
                WITH first_annual_report AS (
                    SELECT ts_code, end_date, MIN(ann_date)::date AS ann_date
                    FROM pit.pit_income_quarterly
                    WHERE data_source = 'report'
                      AND EXTRACT(MONTH FROM end_date) = 12
                      AND EXTRACT(DAY FROM end_date) = 31
                    GROUP BY ts_code, end_date
                )
                SELECT COUNT(*)::bigint AS cnt
                FROM first_annual_report
                WHERE ann_date = $1
                """,
                coverage_period,
            )
            return int(row["cnt"] or 0) if row else 0
        logger.warning("未知PIT审计分母: %s", denominator)
        return 0

    async def _domain_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        if not coverage_period or not await self._relation_exists(contract.output_table):
            return {}
        if contract.domain == "stock_fttm":
            return await self._stock_fttm_audit_details(contract, coverage_period)
        if contract.domain == "industry_fttm":
            return await self._industry_fttm_audit_details(contract, coverage_period)
        if contract.domain == "industry_fapi":
            return await self._industry_fapi_audit_details(contract, coverage_period)
        if contract.domain == "index_fttm":
            return await self._index_fttm_audit_details(contract, coverage_period)
        if contract.domain == "stock_consensus_fy":
            return await self._stock_consensus_fy_audit_details(
                contract, coverage_period
            )
        if contract.domain == "earnings_surprise":
            return await self._earnings_surprise_audit_details(
                contract, coverage_period
            )
        return {}

    async def _stock_consensus_fy_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        relation = _qualified(contract.output_table)
        row = await self.db.fetch_one(
            f"""
            SELECT COUNT(*)::bigint AS row_count,
                   COUNT(DISTINCT ts_code)::bigint AS stock_count,
                   COUNT(*) FILTER (WHERE is_eligible)::bigint AS eligible_count,
                   COUNT(*) FILTER (WHERE source_max_report_date > obs_date)::bigint
                       AS late_source_count,
                   COUNT(*) FILTER (
                       WHERE org_count < np_org_count OR org_count < eps_org_count
                   )::bigint AS org_denominator_mismatch_count,
                   COUNT(*) FILTER (
                       WHERE revision_revised_org_count_1m <>
                             revision_up_org_count_1m + revision_down_org_count_1m
                          OR revision_revised_org_count_3m <>
                             revision_up_org_count_3m + revision_down_org_count_3m
                   )::bigint AS revision_denominator_mismatch_count
            FROM {relation}
            WHERE obs_date = $1
            """,
            coverage_period,
        )
        if not row:
            return {}
        result = {key: int(value or 0) for key, value in dict(row).items()}
        total = result.get("row_count", 0)
        result["eligible_rate"] = (
            round(result["eligible_count"] / total, 6) if total else None
        )
        return result

    async def _earnings_surprise_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        relation = _qualified(contract.output_table)
        row = await self.db.fetch_one(
            f"""
            SELECT COUNT(*)::bigint AS event_count,
                   COUNT(*) FILTER (WHERE consensus_obs_date IS NOT NULL)::bigint
                       AS matched_consensus_count,
                   COUNT(*) FILTER (WHERE is_eligible)::bigint AS eligible_count,
                   COUNT(*) FILTER (
                       WHERE consensus_obs_date >= ann_date
                   )::bigint AS non_prior_consensus_count,
                   COUNT(*) FILTER (
                       WHERE actual_np_yuan IS NOT NULL
                         AND actual_np_10k IS DISTINCT FROM actual_np_yuan / 10000.0
                   )::bigint AS unit_conversion_mismatch_count
            FROM {relation}
            WHERE ann_date = $1
            """,
            coverage_period,
        )
        if not row:
            return {}
        result = {key: int(value or 0) for key, value in dict(row).items()}
        total = result.get("event_count", 0)
        result["consensus_match_rate"] = (
            round(result["matched_consensus_count"] / total, 6) if total else None
        )
        result["eligible_rate"] = (
            round(result["eligible_count"] / total, 6) if total else None
        )
        return result

    async def _stock_fttm_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        row = await self.db.fetch_one(
            f"""
            SELECT
                COUNT(*)::bigint AS row_count,
                COUNT(DISTINCT ts_code)::bigint AS stock_count,
                COUNT(DISTINCT org_name)::bigint AS org_count,
                COUNT(*) FILTER (WHERE estimate_pair_status = 'both')::bigint AS both_count,
                COUNT(*) FILTER (WHERE is_single_year_fallback)::bigint AS fallback_count,
                COUNT(*) FILTER (
                    WHERE fy1_value_source = 'eps_share' OR fy2_value_source = 'eps_share'
                )::bigint AS eps_fallback_count,
                COUNT(*) FILTER (
                    WHERE fy1_value_source = 'np' OR fy2_value_source = 'np'
                )::bigint AS np_path_count,
                COUNT(*) FILTER (WHERE source_max_report_date > obs_date)::bigint AS late_source_count,
                COUNT(*) FILTER (WHERE fttm_np < 0)::bigint AS negative_count,
                COUNT(*) FILTER (WHERE fttm_np = 0)::bigint AS zero_count,
                COUNT(*) FILTER (WHERE fttm_np IS NULL)::bigint AS null_count,
                (
                    SELECT COUNT(*) FROM (
                        SELECT ts_code, org_name, obs_date
                        FROM {_qualified(contract.output_table)}
                        WHERE obs_date = $1
                        GROUP BY ts_code, org_name, obs_date
                        HAVING COUNT(*) > 1
                    ) duplicate_keys
                )::bigint AS duplicate_key_count
            FROM {_qualified(contract.output_table)}
            WHERE obs_date = $1
            """,
            coverage_period,
        )
        if not row:
            return {}
        result = {key: int(value or 0) for key, value in dict(row).items()}
        total = result.get("row_count", 0)
        result["both_pair_rate"] = (
            round(result["both_count"] / total, 6) if total else None
        )
        result["single_year_fallback_rate"] = (
            round(result["fallback_count"] / total, 6) if total else None
        )
        return result

    async def _industry_fttm_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        relation = _qualified(contract.output_table)
        rows = await self.db.fetch(
            f"""
            SELECT
                industry_level,
                COUNT(*)::bigint AS structural_industries,
                COUNT(*) FILTER (WHERE industry_fttm_np IS NOT NULL)::bigint AS valued_industries,
                COUNT(*) FILTER (WHERE is_eligible)::bigint AS eligible_industries,
                COUNT(*) FILTER (WHERE source_max_report_date > obs_date)::bigint AS late_source_count,
                COUNT(*) FILTER (
                    WHERE diffusion_up < 0 OR diffusion_up > 1
                )::bigint AS diffusion_out_of_range_count,
                COUNT(*) FILTER (
                    WHERE matched_org_count <> up_org_count + down_or_flat_org_count
                )::bigint AS diffusion_denominator_mismatch_count,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY covered_mv_rate)
                    AS p25_covered_mv_rate,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY covered_mv_rate)
                    AS median_covered_mv_rate,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY org_count)
                    AS median_org_count,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY matched_org_count)
                    AS median_matched_org_count,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY median_org_mv_coverage)
                    AS median_of_org_mv_coverage,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY p25_org_mv_coverage)
                    AS p25_of_org_mv_coverage
            FROM {relation}
            WHERE obs_date = $1
            GROUP BY industry_level
            ORDER BY industry_level
            """,
            coverage_period,
        )
        levels = [_jsonable(dict(row)) for row in rows or []]
        reason_rows = await self.db.fetch(
            f"""
            SELECT q.reason, COUNT(*)::bigint AS count
            FROM {relation} t
            CROSS JOIN LATERAL jsonb_array_elements_text(t.quality_reasons) AS q(reason)
            WHERE t.obs_date = $1
            GROUP BY q.reason
            ORDER BY q.reason
            """,
            coverage_period,
        )
        consistency = await self.db.fetch_one(
            f"""
            SELECT COUNT(*)::bigint AS inconsistent_code_name_groups
            FROM (
                SELECT classification_source, industry_level, industry_code
                FROM {relation}
                WHERE obs_date = $1
                GROUP BY classification_source, industry_level, industry_code
                HAVING COUNT(DISTINCT industry_name) > 1
            ) conflicts
            """,
            coverage_period,
        )
        return {
            "levels": levels,
            "quality_reason_counts": {
                row["reason"]: int(row["count"] or 0) for row in reason_rows or []
            },
            "inconsistent_code_name_groups": int(
                consistency["inconsistent_code_name_groups"] or 0
            )
            if consistency
            else 0,
        }

    async def _industry_fapi_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        relation = _qualified(contract.output_table)
        rows = await self.db.fetch(
            f"""
            SELECT
                industry_level,
                COUNT(*)::bigint AS structural_industries,
                COUNT(*) FILTER (WHERE fapi_spread_equal IS NOT NULL)::bigint
                    AS valued_industries,
                COUNT(*) FILTER (WHERE is_eligible)::bigint AS eligible_industries,
                COUNT(*) FILTER (WHERE is_ratio_eligible)::bigint
                    AS ratio_eligible_industries,
                COUNT(*) FILTER (
                    WHERE source_max_report_date > obs_date
                       OR previous_source_max_report_date >= obs_date
                )::bigint AS late_source_count,
                COUNT(*) FILTER (
                    WHERE fapi_spread_equal NOT BETWEEN 0 AND 1
                       OR fapi_spread_weighted NOT BETWEEN 0 AND 1
                       OR fapi_ratio_equal NOT BETWEEN 0 AND 1
                       OR fapi_ratio_weighted NOT BETWEEN 0 AND 1
                )::bigint AS fapi_out_of_range_count,
                COUNT(*) FILTER (
                    WHERE matched_org_count <>
                          spread_up_org_count + spread_down_or_flat_org_count
                )::bigint AS spread_denominator_mismatch_count,
                COUNT(*) FILTER (
                    WHERE ratio_matched_org_count <>
                          ratio_up_org_count + ratio_down_or_flat_org_count
                )::bigint AS ratio_denominator_mismatch_count,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY matched_org_count)
                    AS median_matched_org_count,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY median_common_stock_count)
                    AS median_common_stock_count,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY average_report_age_days)
                    AS median_average_report_age_days
            FROM {relation}
            WHERE obs_date = $1
            GROUP BY industry_level
            ORDER BY industry_level
            """,
            coverage_period,
        )
        levels = [_jsonable(dict(row)) for row in rows or []]
        reason_rows = await self.db.fetch(
            f"""
            SELECT q.reason, COUNT(*)::bigint AS count
            FROM {relation} t
            CROSS JOIN LATERAL jsonb_array_elements_text(t.quality_reasons)
                AS q(reason)
            WHERE t.obs_date = $1
            GROUP BY q.reason
            ORDER BY q.reason
            """,
            coverage_period,
        )
        consistency = await self.db.fetch_one(
            f"""
            SELECT COUNT(*)::bigint AS inconsistent_code_name_groups
            FROM (
                SELECT classification_source, industry_level, industry_code
                FROM {relation}
                WHERE obs_date = $1
                GROUP BY classification_source, industry_level, industry_code
                HAVING COUNT(DISTINCT industry_name) > 1
            ) conflicts
            """,
            coverage_period,
        )
        return {
            "levels": levels,
            "quality_reason_counts": {
                row["reason"]: int(row["count"] or 0) for row in reason_rows or []
            },
            "inconsistent_code_name_groups": int(
                consistency["inconsistent_code_name_groups"] or 0
            )
            if consistency
            else 0,
        }

    async def _index_fttm_audit_details(
        self, contract: PITTaskContract, coverage_period: Any
    ) -> Dict[str, Any]:
        relation = _qualified(contract.output_table)
        row = await self.db.fetch_one(
            f"""
            SELECT
                COUNT(*)::bigint AS configured_universes,
                COUNT(*) FILTER (WHERE index_fttm_np IS NOT NULL)::bigint
                    AS valued_universes,
                COUNT(*) FILTER (WHERE is_eligible)::bigint AS eligible_universes,
                COUNT(*) FILTER (WHERE source_max_report_date > obs_date)::bigint
                    AS late_source_count,
                COUNT(*) FILTER (WHERE weight_trade_date > obs_date)::bigint
                    AS future_weight_count,
                COUNT(*) FILTER (
                    WHERE (weight_basis = 'official_weight' AND weight_staleness_days > 65)
                       OR (weight_basis = 'total_mv' AND weight_staleness_days > 31)
                )::bigint AS stale_weight_count,
                COUNT(*) FILTER (
                    WHERE diffusion_up < 0 OR diffusion_up > 1
                )::bigint AS diffusion_out_of_range_count,
                COUNT(*) FILTER (
                    WHERE matched_org_count <> up_org_count + down_or_flat_org_count
                )::bigint AS diffusion_denominator_mismatch_count,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY covered_weight_rate)
                    AS p25_covered_weight_rate,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY covered_weight_rate)
                    AS median_covered_weight_rate,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY org_count)
                    AS median_org_count,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY matched_org_count)
                    AS median_matched_org_count
            FROM {relation}
            WHERE obs_date = $1
            """,
            coverage_period,
        )
        if not row:
            return {}
        result = _jsonable(dict(row))
        reason_rows = await self.db.fetch(
            f"""
            SELECT q.reason, COUNT(*)::bigint AS count
            FROM {relation} t
            CROSS JOIN LATERAL jsonb_array_elements_text(t.quality_reasons) AS q(reason)
            WHERE t.obs_date = $1
            GROUP BY q.reason
            ORDER BY q.reason
            """,
            coverage_period,
        )
        result["quality_reason_counts"] = {
            item["reason"]: int(item["count"] or 0) for item in reason_rows or []
        }
        return result

    async def _raw_gap_summary(self, contract: PITTaskContract, coverage_period: Any) -> Dict[str, Any]:
        if not coverage_period or contract.domain != "financials":
            return {}

        source = None
        for candidate in contract.source_tables:
            if await self._relation_exists(candidate):
                source = candidate
                break
        if not source:
            return {"status": "no_source_relation"}

        source_cols = await self._get_columns(source)
        pit_cols = await self._get_columns(contract.output_table)
        if "end_date" not in source_cols or "ts_code" not in source_cols or "end_date" not in pit_cols:
            return {"status": "unsupported_columns", "source_table": source}

        source_relation = _qualified(source)
        pit_relation = _qualified(contract.output_table)
        raw_listed_join = await self._listed_join_sql("s")
        pit_listed_join = await self._listed_join_sql("p")
        source_filter = ""
        pit_filter = ""
        if "data_source" in source_cols and "data_source" in pit_cols:
            # Derived accounting indicators intentionally exclude forecast rows.
            # Compare like-for-like standard accounting sources only.
            source_filter = "AND s.data_source IN ('report', 'express')"
            pit_filter = "AND p.data_source IN ('report', 'express')"

            if contract.task_name == "pit_financial_indicators":
                # The indicator calculator deliberately skips unconverted
                # RPT_ORIG income rows and requires an as-of balance sheet with
                # non-null total assets for the same report period.  Its audit
                # source universe must use the same eligibility contract;
                # otherwise expected non-output rows are reported as leaks.
                if "conversion_status" in source_cols:
                    source_filter += (
                        "\nAND COALESCE(s.conversion_status, '') <> 'RPT_ORIG'"
                    )

                balance_source = next(
                    (
                        candidate
                        for candidate in contract.source_tables
                        if candidate != source and "balance" in candidate.lower()
                    ),
                    None,
                )
                if balance_source and await self._relation_exists(balance_source):
                    balance_cols = await self._get_columns(balance_source)
                    required_balance_cols = {
                        "ts_code",
                        "end_date",
                        "ann_date",
                        "data_source",
                        "tot_assets",
                    }
                    if required_balance_cols.issubset(balance_cols):
                        balance_relation = _qualified(balance_source)
                        source_filter += f"""
AND EXISTS (
    SELECT 1
    FROM {balance_relation} b
    WHERE b.ts_code = s.ts_code
      AND b.end_date = s.end_date
      AND b.ann_date <= s.ann_date
      AND b.data_source IN ('report', 'express')
      AND b.tot_assets IS NOT NULL
)"""
        elif "data_source" in pit_cols:
            # A dedicated Tushare statement table represents formal reports;
            # do not inflate the PIT side with express/forecast entities.
            pit_filter = "AND p.data_source = 'report'"
        row = await self.db.fetch_one(
            f"""
            WITH raw_codes AS (
                SELECT DISTINCT s.ts_code
                FROM {source_relation} s
                {raw_listed_join}
                WHERE s.end_date = $1
                  AND s.ts_code IS NOT NULL
                  {source_filter}
            ),
            pit_codes AS (
                SELECT DISTINCT p.ts_code
                FROM {pit_relation} p
                {pit_listed_join}
                WHERE p.end_date = $1
                  AND p.ts_code IS NOT NULL
                  {pit_filter}
            )
            SELECT
                (SELECT COUNT(*) FROM raw_codes)::bigint AS raw_count,
                (SELECT COUNT(*) FROM pit_codes)::bigint AS pit_count,
                (SELECT COUNT(*) FROM raw_codes r LEFT JOIN pit_codes p USING (ts_code) WHERE p.ts_code IS NULL)::bigint AS raw_missing_in_pit
            """,
            coverage_period,
        )
        if not row:
            return {"status": "no_rows", "source_table": source}
        return {
            "status": "ok",
            "source_table": source,
            "period": coverage_period,
            "raw_count": int(row["raw_count"] or 0),
            "pit_count": int(row["pit_count"] or 0),
            "raw_missing_in_pit": int(row["raw_missing_in_pit"] or 0),
        }

    async def _persist_audit_snapshot(self, audit_result: Dict[str, Any]) -> None:
        await self._ensure_audit_snapshot_table()
        await self.db.execute(
            """
            INSERT INTO pit.pit_audit_snapshot (
                task_name,
                output_table,
                latest_pit_time,
                row_count,
                coverage_rate,
                gap_count,
                status,
                details_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            """,
            audit_result.get("task_name"),
            audit_result.get("output_table"),
            audit_result.get("latest_pit_time"),
            int(audit_result.get("row_count") or 0),
            audit_result.get("coverage_rate"),
            audit_result.get("gap_count"),
            audit_result.get("status"),
            json.dumps(_jsonable(audit_result.get("details") or {}), ensure_ascii=False),
        )

    async def _ensure_audit_snapshot_table(self) -> None:
        await self.db.execute(
            """
            CREATE SCHEMA IF NOT EXISTS pit;
            CREATE TABLE IF NOT EXISTS pit.pit_audit_snapshot (
                id BIGSERIAL PRIMARY KEY,
                snapshot_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                task_name VARCHAR(128) NOT NULL,
                output_table VARCHAR(256) NOT NULL,
                latest_pit_time DATE,
                row_count BIGINT NOT NULL DEFAULT 0,
                coverage_rate NUMERIC(12, 6),
                gap_count BIGINT,
                status VARCHAR(32) NOT NULL,
                details_json JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            CREATE INDEX IF NOT EXISTS idx_pit_audit_snapshot_task_time
            ON pit.pit_audit_snapshot (task_name, snapshot_time DESC);
            """
        )

    async def _latest_snapshot_for_task(self, task_name: str) -> Optional[Dict[str, Any]]:
        if not await self._relation_exists("pit.pit_audit_snapshot"):
            return None
        row = await self.db.fetch_one(
            """
            SELECT snapshot_time,
                   latest_pit_time,
                   row_count,
                   coverage_rate,
                   gap_count,
                   status
            FROM pit.pit_audit_snapshot
            WHERE task_name = $1
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            task_name,
        )
        if not row:
            return None
        return {
            "last_audit_time": row["snapshot_time"],
            "audited_latest_date": row["latest_pit_time"],
            "audited_row_count": int(row["row_count"] or 0),
            "audited_coverage_rate": (
                float(row["coverage_rate"]) if row["coverage_rate"] is not None else None
            ),
            "audited_gap_count": int(row["gap_count"]) if row["gap_count"] is not None else None,
            "audit_status": row["status"],
        }

    async def _latest_task_execution_for_task(self, task_name: str) -> Optional[Dict[str, Any]]:
        if not await self._relation_exists("public.task_status"):
            return None
        row = await self.db.fetch_one(
            """
            SELECT status,
                   update_time,
                   details
            FROM public.task_status
            WHERE task_name = $1
            ORDER BY update_time DESC
            LIMIT 1
            """,
            task_name,
        )
        if not row:
            return None
        return {
            "last_execution_time": row["update_time"],
            "last_execution_status": row["status"],
            "last_execution_details": row["details"],
        }

    async def _latest_financial_periods(self, contracts: Sequence[PITTaskContract], limit: int) -> List[Any]:
        periods: set[Any] = set()
        for contract in contracts:
            if not await self._relation_exists(contract.output_table):
                continue
            columns = await self._get_columns(contract.output_table)
            if "end_date" not in columns:
                continue
            rows = await self.db.fetch(
                f"""
                SELECT DISTINCT end_date
                FROM {_qualified(contract.output_table)}
                WHERE end_date IS NOT NULL
                ORDER BY end_date DESC
                LIMIT $1
                """,
                limit,
            )
            periods.update(row["end_date"] for row in rows or [])
        return sorted(periods, reverse=True)[:limit]

    async def _periods_for_stock(self, relation_name: str, ts_code: str, period_column: str, limit: int) -> List[Any]:
        columns = await self._get_columns(relation_name)
        if "ts_code" not in columns or period_column not in columns:
            return []
        rows = await self.db.fetch(
            f"""
            SELECT DISTINCT {_quote_identifier(period_column)}::date AS period
            FROM {_qualified(relation_name)}
            WHERE ts_code = $1
              AND {_quote_identifier(period_column)} IS NOT NULL
            ORDER BY period DESC
            LIMIT $2
            """,
            ts_code,
            limit,
        )
        return [row["period"] for row in rows or []]

    async def _expected_financial_periods_for_stock(self, ts_code: str, limit: int) -> List[Any]:
        """Build a cross-statement report-period baseline for stock diagnostics."""
        preferred_relations = (
            "pit.pit_income_quarterly",
            "pit.pit_balance_quarterly",
            "pit.pit_cashflow_quarterly",
        )
        periods: set[Any] = set()
        for relation in preferred_relations:
            if not await self._relation_exists(relation):
                continue
            columns = await self._get_columns(relation)
            if "ts_code" not in columns or "end_date" not in columns:
                continue
            rows = await self.db.fetch(
                f"""
                SELECT DISTINCT end_date::date AS period
                FROM {_qualified(relation)}
                WHERE ts_code = $1
                  AND end_date IS NOT NULL
                ORDER BY period DESC
                LIMIT $2
                """,
                ts_code,
                limit,
            )
            periods.update(row["period"] for row in rows or [])
        return sorted(periods, reverse=True)[:limit]

    async def _raw_periods_for_stock(self, source_tables: Sequence[str], ts_code: str, limit: int) -> List[Any]:
        periods: set[Any] = set()
        for source in source_tables:
            if not await self._relation_exists(source):
                continue
            columns = await self._get_columns(source)
            if "ts_code" not in columns or "end_date" not in columns:
                continue
            rows = await self.db.fetch(
                f"""
                SELECT DISTINCT end_date::date AS period
                FROM {_qualified(source)}
                WHERE ts_code = $1
                  AND end_date IS NOT NULL
                ORDER BY period DESC
                LIMIT $2
                """,
                ts_code,
                limit,
            )
            periods.update(row["period"] for row in rows or [])
        return sorted(periods, reverse=True)[:limit]

    async def _diagnose_financial_gap_reasons(
        self,
        contract: PITTaskContract,
        ts_code: str,
        periods: Sequence[Any],
    ) -> List[Dict[str, Any]]:
        diagnoses: List[Dict[str, Any]] = []
        if not periods:
            return diagnoses

        output_columns = await self._get_columns(contract.output_table)
        output_fields = self._business_value_fields(output_columns)

        for period in periods:
            source_checks = []
            has_source_relation = False
            has_raw_rows = False
            has_eligible_rows = False
            has_valid_rows = False

            for source in self._source_probe_relations(contract.source_tables):
                if not await self._relation_exists(source):
                    source_checks.append({"source_table": source, "status": "missing_relation"})
                    continue

                has_source_relation = True
                columns = await self._get_columns(source)
                if "ts_code" not in columns or "end_date" not in columns:
                    source_checks.append({"source_table": source, "status": "unsupported_columns"})
                    continue

                raw_rows = await self._count_source_rows(source, ts_code, period)
                eligible_rows = await self._count_source_rows(
                    source,
                    ts_code,
                    period,
                    self._source_eligibility_filter(contract, columns),
                )
                common_fields = sorted(output_fields & self._business_value_fields(columns))
                valid_rows = await self._count_source_rows(
                    source,
                    ts_code,
                    period,
                    self._source_eligibility_filter(contract, columns),
                    common_fields,
                )

                has_raw_rows = has_raw_rows or raw_rows > 0
                has_eligible_rows = has_eligible_rows or eligible_rows > 0
                has_valid_rows = has_valid_rows or valid_rows > 0
                source_checks.append(
                    {
                        "source_table": source,
                        "status": "ok",
                        "raw_rows": raw_rows,
                        "eligible_rows": eligible_rows,
                        "valid_rows": valid_rows,
                        "checked_value_fields": common_fields,
                    }
                )

            if not has_source_relation:
                reason = "source_relation_missing"
            elif not has_raw_rows:
                reason = "source_missing"
            elif not has_eligible_rows:
                reason = "source_not_eligible"
            elif not has_valid_rows:
                reason = "source_empty_after_field_filter"
            else:
                reason = "pit_build_gap"

            diagnoses.append(
                {
                    "period": period,
                    "reason": reason,
                    "source_checks": source_checks,
                }
            )

        return diagnoses

    def _source_probe_relations(self, source_tables: Sequence[str]) -> List[str]:
        probes: List[str] = []
        for source in source_tables:
            if source not in probes:
                probes.append(source)
            schema, table = _split_relation(source)
            if schema in {"tushare", "akshare", "tinysoft", "excel"}:
                raw_relation = f"rawdata.{table}"
                if raw_relation not in probes:
                    probes.append(raw_relation)
        return probes

    @staticmethod
    def _business_value_fields(columns: Iterable[str]) -> set[str]:
        excluded = {
            "id",
            "ts_code",
            "symbol",
            "end_date",
            "ann_date",
            "f_ann_date",
            "obs_date",
            "calc_date",
            "data_source",
            "report_type",
            "comp_type",
            "year",
            "quarter",
            "created_at",
            "updated_at",
            "update_time",
        }
        return {column for column in columns if column not in excluded}

    @staticmethod
    def _source_eligibility_filter(contract: PITTaskContract, columns: Iterable[str]) -> str:
        column_set = set(columns)
        if contract.task_name == "pit_cashflow_quarterly" and "report_type" in column_set:
            return "AND (report_type = 1 OR report_type IS NULL)"
        return ""

    async def _count_source_rows(
        self,
        relation_name: str,
        ts_code: str,
        period: Any,
        extra_filter: str = "",
        any_non_null_fields: Optional[Sequence[str]] = None,
    ) -> int:
        value_filter = ""
        if any_non_null_fields:
            value_filter = "AND (" + " OR ".join(f"{_quote_identifier(field)} IS NOT NULL" for field in any_non_null_fields) + ")"
        row = await self.db.fetch_one(
            f"""
            SELECT COUNT(*)::bigint AS cnt
            FROM {_qualified(relation_name)}
            WHERE ts_code = $1
              AND end_date = $2
              {extra_filter}
              {value_filter}
            """,
            ts_code,
            period,
        )
        return int(row["cnt"] or 0) if row else 0

    async def _latest_rows_for_stock(self, contract: PITTaskContract, ts_code: str, limit: int = 10) -> List[Dict[str, Any]]:
        columns = await self._get_columns(contract.output_table)
        if "ts_code" not in columns:
            return []
        preferred = ["ts_code", "end_date", "ann_date", "obs_date", "data_source"]
        select_cols = [col for col in preferred if col in columns]
        if not select_cols:
            return []
        order_col = contract.pit_time_key if contract.pit_time_key in columns else select_cols[-1]
        rows = await self.db.fetch(
            f"""
            SELECT {', '.join(_quote_identifier(col) for col in select_cols)}
            FROM {_qualified(contract.output_table)}
            WHERE ts_code = $1
            ORDER BY {_quote_identifier(order_col)} DESC
            LIMIT $2
            """,
            ts_code,
            limit,
        )
        return [dict(row) for row in rows or []]

    async def _current_listed_count(self) -> int:
        if self._listed_stock_count is not None:
            return self._listed_stock_count
        async with self._listed_stock_count_lock:
            if self._listed_stock_count is not None:
                return self._listed_stock_count
            try:
                if not await self._relation_exists("rawdata.stock_basic"):
                    self._listed_stock_count = 0
                    return 0
                columns = await self._get_columns("rawdata.stock_basic")
                if "list_status" in columns:
                    row = await self.db.fetch_one(
                        "SELECT COUNT(DISTINCT ts_code)::bigint AS cnt FROM rawdata.stock_basic WHERE list_status = 'L'"
                    )
                else:
                    row = await self.db.fetch_one(
                        "SELECT COUNT(DISTINCT ts_code)::bigint AS cnt FROM rawdata.stock_basic"
                    )
                self._listed_stock_count = int(row["cnt"] or 0) if row else 0
                return self._listed_stock_count
            except Exception as exc:
                logger.warning("查询当前上市股票数失败: %s", exc)
                self._listed_stock_count = 0
                return 0

    async def _listed_join_sql(self, target_alias: str) -> str:
        """Return a SQL join clause limiting target rows to currently listed A shares."""
        try:
            if not await self._relation_exists("rawdata.stock_basic"):
                return ""
            columns = await self._get_columns("rawdata.stock_basic")
            if "ts_code" not in columns:
                return ""
            status_filter = "WHERE list_status = 'L'" if "list_status" in columns else ""
            return (
                "JOIN ("
                f"SELECT DISTINCT ts_code FROM rawdata.stock_basic {status_filter}"
                f") listed ON listed.ts_code = {target_alias}.ts_code"
            )
        except Exception as exc:
            logger.warning("构造当前上市股票过滤条件失败: %s", exc)
            return ""

    async def _relation_exists(self, relation_name: str) -> bool:
        schema, table = _split_relation(relation_name)
        row = await self.db.fetch_one(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1
                  AND table_name = $2
                  AND table_type IN ('BASE TABLE', 'VIEW')
            ) AS exists
            """,
            schema,
            table,
        )
        return bool(row and row["exists"])

    async def _get_columns(self, relation_name: str) -> set[str]:
        schema, table = _split_relation(relation_name)
        rows = await self.db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
            """,
            schema,
            table,
        )
        return {row["column_name"] for row in rows or []}


__all__ = ["PITAuditService"]
