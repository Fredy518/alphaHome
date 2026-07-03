import logging

import pandas as pd

from alphahome.pit.pit_cashflow_quarterly_manager import PITCashflowQuarterlyManager


def test_pit_cashflow_preprocess_adds_report_source_and_deduplicates():
    manager = PITCashflowQuarterlyManager()
    manager.logger = logging.getLogger("test_pit_cashflow")
    raw = pd.DataFrame(
        {
            "ts_code": ["002549.SZ", "002549.SZ"],
            "end_date": ["2025-12-31", "2025-12-31"],
            "ann_date": ["2026-04-10", "2026-04-10"],
            "net_profit": [1.0, 2.0],
            "n_cashflow_act": [10.0, 20.0],
        }
    )

    processed = manager._preprocess_data(raw)

    assert len(processed) == 1
    assert processed.iloc[0]["data_source"] == "report"
    assert processed.iloc[0]["net_profit"] == 2.0
    assert processed.iloc[0]["year"] == 2025
    assert processed.iloc[0]["quarter"] == 4


def test_pit_cashflow_upsert_uses_source_aware_primary_key():
    manager = PITCashflowQuarterlyManager()
    manager.logger = logging.getLogger("test_pit_cashflow")

    class _FakeDB:
        def __init__(self):
            self.executed = []

        def execute_sync(self, query, params=None):
            self.executed.append((query, params))
            return 1

    class _FakeContext:
        def __init__(self):
            self.db_manager = _FakeDB()

        def query_dataframe(self, query, params=None):
            if "information_schema.columns" in query:
                return pd.DataFrame(
                    {
                        "column_name": [
                            "ts_code",
                            "end_date",
                            "ann_date",
                            "data_source",
                            "net_profit",
                            "n_cashflow_act",
                            "year",
                            "quarter",
                        ]
                    }
                )
            return pd.DataFrame()

    manager.context = _FakeContext()
    data = pd.DataFrame(
        {
            "ts_code": ["002549.SZ"],
            "end_date": [pd.Timestamp("2025-12-31").date()],
            "ann_date": [pd.Timestamp("2026-04-10").date()],
            "data_source": ["report"],
            "net_profit": [2.0],
            "n_cashflow_act": [20.0],
            "year": [2025],
            "quarter": [4],
        }
    )

    result = manager._batch_upsert_to_pit(data, batch_size=100)

    assert result == {"inserted": 1, "updated": 0, "errors": 0}
    executed_sql, params = manager.context.db_manager.executed[0]
    assert "ON CONFLICT (ts_code, end_date, ann_date, data_source)" in executed_sql
    assert params["data_source"] == "report"


def test_pit_cashflow_records_execution_stats():
    manager = PITCashflowQuarterlyManager()

    manager._record_execution_stats(
        raw_count=5,
        processed_count=4,
        result={"inserted": 2, "updated": 1, "errors": 1},
    )

    assert manager.stats["processed_records"] == 4
    assert manager.stats["success_records"] == 3
    assert manager.stats["error_records"] == 1
    assert manager.stats["skipped_records"] == 1
