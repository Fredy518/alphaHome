import pandas as pd
import pytest

from alphahome.fetchers.tasks.stock.tinysoft_stock_fina_pit_ext import (
    TinySoftStockFinaPitExtTask,
)


class _DummyApi:
    async def call_dataframe(self, *args, **kwargs):
        return pd.DataFrame()


class _FakeDB:
    def __init__(self, rows=None):
        self._rows = rows or []

    async def get_column_names(self, target):
        return ["ts_code", "list_status"]

    async def fetch(self, query, *args, **kwargs):
        return self._rows


class _MultiSymbolApi:
    async def call_dataframe(self, func, table_id, **kwargs):
        stock = kwargs.get("stock")
        assert func == "infoarray"
        assert table_id == 42
        return pd.DataFrame(
            {
                "截止日": [20241231],
                "公布日": [20250320],
                "每股收益(摊薄)": ["2.15"],
                "每股净资产": [22.1],
                "StockID": [stock],
            }
        )


def _make_task(task_config=None, db=None, api=None):
    return TinySoftStockFinaPitExtTask(
        db_connection=db if db is not None else object(),
        api=api or _DummyApi(),
        tinysoft_config={},
        task_config=task_config or {},
    )


def test_process_data_melts_metric_rows():
    task = _make_task()
    raw = pd.DataFrame(
        {
            "截止日": [20241231],
            "公布日": [20250320],
            "每股收益(摊薄)": ["2.15"],
            "每股净资产": [22.1],
            "StockID": ["SZ000001"],
            "finance_source": ["report_42_main"],
            "source_table_id": [42],
            "metric_defs": [
                [
                    {"metric_name": "eps_diluted", "field_id": 42002, "field_name": "每股收益(摊薄)"},
                    {"metric_name": "bps", "field_id": 42006, "field_name": "每股净资产"},
                ]
            ],
        }
    )
    processed = task.process_data(raw, start_date="20250301", end_date="20250331")
    assert not processed.empty
    assert len(processed) == 2
    assert set(processed["metric_name"]) == {"eps_diluted", "bps"}
    pe_row = processed[processed["metric_name"] == "eps_diluted"].iloc[0]
    assert pe_row["metric_value"] == pytest.approx(2.15)
    assert pe_row["metric_expr"] == "report(42002,20241231)"
    assert str(pe_row["trade_date"]) == "2025-03-20"


def test_process_data_filters_empty_metrics_by_default():
    task = _make_task(task_config={"include_empty_metrics": False})
    raw = pd.DataFrame(
        {
            "截止日": [20241231],
            "公布日": [20250320],
            "每股收益(摊薄)": [None],
            "StockID": ["SZ000001"],
            "finance_source": ["report_42_main"],
            "source_table_id": [42],
            "metric_defs": [[{"metric_name": "eps_diluted", "field_id": 42002, "field_name": "每股收益(摊薄)"}]],
        }
    )
    processed = task.process_data(raw, start_date="20250301", end_date="20250331")
    assert processed.empty


@pytest.mark.asyncio
async def test_get_batch_list_with_metric_profiles():
    task = _make_task(
        db=_FakeDB(rows=[{"ts_code": "000001.SZ"}, {"ts_code": "600000.SH"}]),
        task_config={"symbol_batch_size": 2},
    )
    batches = await task.get_batch_list(
        start_date="20260301",
        end_date="20260302",
        metric_profiles=[
            {
                "finance_source": "report_42_main",
                "table_id": 42,
                "metric_defs": [
                    {"metric_name": "eps_diluted", "field_id": 42002, "field_name": "每股收益(摊薄)"},
                    {"metric_name": "bps", "field_id": 42006, "field_name": "每股净资产"},
                ],
            }
        ],
    )
    assert len(batches) == 1
    assert batches[0]["finance_source"] == "report_42_main"
    assert batches[0]["table_id"] == 42
    assert len(batches[0]["metric_defs"]) == 2
    assert len(batches[0]["symbol_pairs"]) == 2


@pytest.mark.asyncio
async def test_fetch_batch_supports_symbol_pairs():
    task = _make_task(
        db=_FakeDB(),
        api=_MultiSymbolApi(),
        task_config={"skip_failed_symbols": False},
    )
    df = await task.fetch_batch(
        {
            "finance_source": "report_42_main",
            "table_id": 42,
            "metric_defs": [{"metric_name": "eps_diluted", "field_id": 42002, "field_name": "每股收益(摊薄)"}],
            "symbol_pairs": [
                {"ts_code": "000001.SZ", "stock": "SZ000001"},
                {"ts_code": "600000.SH", "stock": "SH600000"},
            ],
            "service": "",
            "timeout_ms": 45000,
        }
    )
    assert df is not None
    assert len(df) == 2
    assert set(df["StockID"]) == {"SZ000001", "SH600000"}
