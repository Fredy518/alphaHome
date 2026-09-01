#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PIT cashflow statement manager."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .base.pit_config import PITConfig
from .base.pit_table_manager import PITTableManager
from .financial_code_utils import normalize_tushare_financial_ts_codes


class PITCashflowQuarterlyManager(PITTableManager):
    """Build and maintain pit.pit_cashflow_quarterly from formal cashflow reports."""

    def __init__(self):
        super().__init__("pit_cashflow_quarterly")
        self.tushare_table = self.table_config["tushare_table"]
        self.key_fields = self.table_config["key_fields"]
        self.data_fields = self.table_config["data_fields"]

    def full_backfill(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        self.logger.info("开始PIT现金流量表历史全量回填")
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)
        batch_size = batch_size or self.batch_size

        try:
            self._ensure_table_exists()
            raw = self._fetch_tushare_data(start_date, end_date)
            if raw.empty:
                self._record_execution_stats(raw_count=0, processed_count=0, result=None)
                return {"backfilled_records": 0, "message": "无数据需要回填"}
            processed = self._preprocess_data(raw)
            result = self._batch_upsert_to_pit(processed, batch_size)
            self._record_execution_stats(len(raw), len(processed), result)
            self.logger.info(
                "PIT现金流量表历史全量回填完成: raw=%s, processed=%s, inserted=%s, updated=%s, errors=%s",
                len(raw),
                len(processed),
                result["inserted"],
                result["updated"],
                result["errors"],
            )
            return {
                "backfilled_records": result["inserted"] + result["updated"],
                "inserted_records": result["inserted"],
                "updated_records": result["updated"],
                "error_records": result["errors"],
                "message": f"成功回填 {result['inserted'] + result['updated']} 条记录",
            }
        except Exception as exc:
            self.logger.error("现金流量表历史回填失败: %s", exc, exc_info=True)
            return {"backfilled_records": 0, "error": str(exc), "message": "历史回填失败"}

    def incremental_update(
        self,
        days: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        self.logger.info("开始PIT现金流量表增量更新")
        days = days or PITConfig.DEFAULT_DATE_RANGES["incremental_days"]
        batch_size = batch_size or self.batch_size
        start_date, end_date = self.resolve_incremental_date_range(
            days,
            (
                (
                    f"{PITConfig.TUSHARE_SCHEMA}.{self.tushare_table}",
                    ("f_ann_date", "ann_date"),
                    "update_time",
                ),
            ),
        )

        try:
            self._ensure_table_exists()
            raw = self._fetch_tushare_data(start_date, end_date)
            if raw.empty:
                self._record_execution_stats(raw_count=0, processed_count=0, result=None)
                return {"updated_records": 0, "message": "无数据需要更新"}
            processed = self._preprocess_data(raw)
            result = self._batch_upsert_to_pit(processed, batch_size)
            self._record_execution_stats(len(raw), len(processed), result)
            self.logger.info(
                "PIT现金流量表增量更新完成: raw=%s, processed=%s, inserted=%s, updated=%s, errors=%s",
                len(raw),
                len(processed),
                result["inserted"],
                result["updated"],
                result["errors"],
            )
            return {
                "updated_records": result["inserted"] + result["updated"],
                "inserted_records": result["inserted"],
                "updated_existing_records": result["updated"],
                "error_records": result["errors"],
                "message": f"成功更新 {result['inserted'] + result['updated']} 条记录",
            }
        except Exception as exc:
            self.logger.error("现金流量表增量更新失败: %s", exc, exc_info=True)
            return {"updated_records": 0, "error": str(exc), "message": "增量更新失败"}

    def single_backfill(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: Optional[int] = None,
        do_validate: bool = True,
    ) -> Dict[str, Any]:
        if not ts_code:
            return {"backfilled_records": 0, "error": "缺少 ts_code", "message": "必须提供 ts_code"}
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)
        batch_size = batch_size or self.batch_size

        try:
            self._ensure_table_exists()
            raw = self._fetch_tushare_data(start_date, end_date, ts_code=ts_code)
            if raw.empty:
                self._record_execution_stats(raw_count=0, processed_count=0, result=None)
                return {"ts_code": ts_code, "backfilled_records": 0, "message": "无数据需要回填"}
            processed = self._preprocess_data(raw)
            result = self._batch_upsert_to_pit(processed, batch_size)
            self._record_execution_stats(len(raw), len(processed), result)
            self.logger.info(
                "PIT现金流量表单股回填完成: ts_code=%s, raw=%s, processed=%s, inserted=%s, updated=%s, errors=%s",
                ts_code,
                len(raw),
                len(processed),
                result["inserted"],
                result["updated"],
                result["errors"],
            )
            out = {
                "ts_code": ts_code,
                "backfilled_records": result["inserted"] + result["updated"],
                "inserted_records": result["inserted"],
                "updated_records": result["updated"],
                "error_records": result["errors"],
                "message": f"单股回填完成，共 {result['inserted'] + result['updated']} 条",
            }
            if do_validate:
                out["validation"] = self._validate_single_stock(ts_code, start_date, end_date)
            return out
        except Exception as exc:
            self.logger.error("现金流量表单股回填失败: %s", exc, exc_info=True)
            return {
                "ts_code": ts_code,
                "backfilled_records": 0,
                "error": str(exc),
                "message": "单股回填失败",
            }

    def _fetch_tushare_data(
        self,
        start_date: str,
        end_date: str,
        ts_code: Optional[str] = None,
    ) -> pd.DataFrame:
        source_cols = self._get_table_columns(PITConfig.TUSHARE_SCHEMA, self.tushare_table)
        select_parts = [
            "ts_code",
            "end_date",
            "COALESCE(f_ann_date, ann_date) AS ann_date",
        ]
        for field in self.data_fields:
            if field in source_cols:
                select_parts.append(field)
            else:
                select_parts.append(f"NULL::numeric AS {field}")

        report_filter = "AND (report_type = 1 OR report_type IS NULL)" if "report_type" in source_cols else ""
        sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {PITConfig.TUSHARE_SCHEMA}.{self.tushare_table}
        WHERE COALESCE(f_ann_date, ann_date) >= %s
          AND COALESCE(f_ann_date, ann_date) <= %s
          AND ts_code IS NOT NULL
          AND end_date IS NOT NULL
          AND COALESCE(f_ann_date, ann_date) IS NOT NULL
          {report_filter}
        """
        params: List[Any] = [start_date, end_date]
        if ts_code:
            sql += " AND ts_code = %s"
            params.append(ts_code)
        sql += " ORDER BY ts_code, end_date, ann_date"
        df = self.context.query_dataframe(sql, tuple(params))
        return df if df is not None else pd.DataFrame()

    def _record_execution_stats(
        self,
        raw_count: int,
        processed_count: int,
        result: Optional[Dict[str, int]],
    ) -> None:
        inserted = int((result or {}).get("inserted", 0))
        updated = int((result or {}).get("updated", 0))
        errors = int((result or {}).get("errors", 0))
        skipped = max(int(raw_count) - int(processed_count), 0)
        self.stats["processed_records"] += int(processed_count)
        self.stats["success_records"] += inserted + updated
        self.stats["error_records"] += errors
        self.stats["skipped_records"] += skipped

    def _preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if data is None or data.empty:
            return pd.DataFrame()

        work = normalize_tushare_financial_ts_codes(data, self.logger)
        work["data_source"] = "report"
        work["end_date"] = pd.to_datetime(work["end_date"], errors="coerce").dt.date
        work["ann_date"] = pd.to_datetime(work["ann_date"], errors="coerce").dt.date

        end_date_dt = pd.to_datetime(work["end_date"], errors="coerce")
        work["year"] = end_date_dt.dt.year
        work["quarter"] = end_date_dt.dt.month.map(
            {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}
        )

        for field in self.data_fields:
            if field not in work.columns:
                work[field] = None
            if field in work.columns:
                work[field] = pd.to_numeric(work[field], errors="coerce")

        before = len(work)
        work = work.dropna(subset=["ts_code", "end_date", "ann_date"])
        if len(work) != before:
            self.logger.warning("移除了 %s 条关键字段为空的现金流记录", before - len(work))

        if self.data_fields:
            work = work[work[self.data_fields].notna().any(axis=1)]

        work = work.sort_values(["ts_code", "end_date", "ann_date"])
        work = work.drop_duplicates(subset=["ts_code", "end_date", "ann_date", "data_source"], keep="last")
        return work

    def _batch_upsert_to_pit(self, data: pd.DataFrame, batch_size: int) -> Dict[str, int]:
        if data is None or data.empty:
            return {"inserted": 0, "updated": 0, "errors": 0}

        pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
        extra_candidates = ["year", "quarter"]
        extras = [field for field in extra_candidates if field in pit_cols and field in data.columns]
        all_fields = self.key_fields + self.data_fields + extras + ["data_source"]
        field_list = ", ".join(all_fields)
        placeholder_list = ", ".join([f"%({field})s" for field in all_fields])
        update_fields = [field for field in all_fields if field not in ["ts_code", "end_date", "ann_date"]]
        update_list = ", ".join([f"{field} = EXCLUDED.{field}" for field in update_fields])
        upsert_sql = f"""
        INSERT INTO {PITConfig.PIT_SCHEMA}.{self.table_name} ({field_list})
        VALUES ({placeholder_list})
        ON CONFLICT (ts_code, end_date, ann_date, data_source) DO UPDATE SET
            {update_list},
            updated_at = CURRENT_TIMESTAMP
        """

        inserted = 0
        updated = 0
        errors = 0
        batch_size = max(int(batch_size or self.batch_size), 1)
        for start in range(0, len(data), batch_size):
            batch = data.iloc[start : start + batch_size]
            for _, row in batch.iterrows():
                params = self._sanitize_params({field: row.get(field) for field in all_fields})
                try:
                    exists = self.context.query_dataframe(
                        f"""
                        SELECT 1
                        FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
                        WHERE ts_code = %s
                          AND end_date = %s
                          AND ann_date = %s
                          AND data_source = %s
                        """,
                        (params["ts_code"], params["end_date"], params["ann_date"], params["data_source"]),
                    )
                    self.context.db_manager.execute_sync(upsert_sql, params)
                    if exists is not None and not exists.empty:
                        updated += 1
                    else:
                        inserted += 1
                except Exception as exc:
                    errors += 1
                    self.logger.error(
                        "现金流UPSERT失败 %s-%s-%s: %s",
                        params.get("ts_code"),
                        params.get("end_date"),
                        params.get("ann_date"),
                        exc,
                    )
        return {"inserted": inserted, "updated": updated, "errors": errors}

    def _validate_single_stock(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        core_fields = [field for field in self.data_fields if field in self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)]
        select_cols = ["ts_code", "end_date", "ann_date", "data_source"] + core_fields
        sql = (
            f"SELECT {', '.join(select_cols)} FROM {PITConfig.PIT_SCHEMA}.{self.table_name} "
            "WHERE ts_code=%s AND ann_date >= %s AND ann_date <= %s "
            "ORDER BY ann_date, end_date"
        )
        df = self.context.query_dataframe(sql, (ts_code, start_date, end_date))
        if df is None or df.empty:
            return {"ts_code": ts_code, "range": [start_date, end_date], "rows": 0}
        all_null = int(df[core_fields].isna().all(axis=1).sum()) if core_fields else 0
        key_null = int(df[["ts_code", "end_date", "ann_date"]].isna().any(axis=1).sum())
        return {
            "ts_code": ts_code,
            "range": [start_date, end_date],
            "rows": int(len(df)),
            "all_core_null_rows": all_null,
            "key_field_null_rows": key_null,
            "status": "passed" if all_null == 0 and key_null == 0 else "warning",
        }

    def _get_table_columns(self, schema: str, table: str) -> set[str]:
        sql = "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s"
        try:
            df = self.context.query_dataframe(sql, (schema, table))
            return set(df["column_name"].tolist()) if df is not None and not df.empty else set()
        except Exception:
            return set()

    @staticmethod
    def _sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
        cleaned: Dict[str, Any] = {}
        for key, value in params.items():
            try:
                cleaned[key] = None if pd.isna(value) else value
            except Exception:
                cleaned[key] = value
        return cleaned


def main() -> int:
    parser = argparse.ArgumentParser(description="PIT现金流量表管理器")
    parser.add_argument("--mode", choices=["full-backfill", "incremental", "single-backfill"], help="执行模式")
    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="增量更新天数")
    parser.add_argument("--batch-size", type=int, help="批次大小")
    parser.add_argument("--ts-code", help="指定单股 ts_code")
    parser.add_argument("--status", action="store_true", help="显示表状态")
    parser.add_argument("--validate", action="store_true", help="验证数据完整性")
    args = parser.parse_args()

    try:
        from alphahome.common.logging_utils import setup_logging

        setup_logging(
            log_level="INFO",
            log_to_file=True,
            log_dir="logs",
            log_filename=f"pit_cashflow_quarterly_{datetime.now().strftime('%Y%m%d')}.log",
        )
    except Exception:
        pass

    with PITCashflowQuarterlyManager() as manager:
        if args.status:
            print(manager.get_table_status())
            return 0
        if args.validate and not args.mode:
            print(manager.validate_data_integrity())
            return 0
        if args.mode == "full-backfill":
            result = manager.full_backfill(args.start_date, args.end_date, args.batch_size)
        elif args.mode == "incremental":
            result = manager.incremental_update(args.days, args.batch_size)
        elif args.mode == "single-backfill":
            result = manager.single_backfill(args.ts_code, args.start_date, args.end_date, args.batch_size, args.validate)
        else:
            parser.error("必须指定 --mode，或使用 --status/--validate")
        print(result)
        return 0 if "error" not in result else 1


if __name__ == "__main__":
    sys.exit(main())
