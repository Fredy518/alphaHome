import pytest

from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.sources.tushare import batch_utils
from alphahome.fetchers.tasks.stock.tushare_stock_kplconcept import (
    TushareStockKplConceptTask,
)


def test_kplconcept_task_remains_registered():
    assert (
        UnifiedTaskFactory._task_registry["tushare_stock_kplconcept"]
        is TushareStockKplConceptTask
    )


@pytest.mark.asyncio
async def test_kplconcept_task_builds_trade_day_batches(monkeypatch):
    captured = {}

    async def fake_generate_trade_day_batches(**kwargs):
        captured.update(kwargs)
        return [
            {
                "start_date": "20260623",
                "end_date": "20260623",
                "fields": kwargs["additional_params"]["fields"],
            }
        ]

    monkeypatch.setattr(
        batch_utils,
        "generate_trade_day_batches",
        fake_generate_trade_day_batches,
    )
    task = TushareStockKplConceptTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )

    batches = await task.get_batch_list(start_date="20260623", end_date="20260625")

    assert task.api_name == "kpl_concept"
    assert batches == [
        {
            "start_date": "20260623",
            "end_date": "20260623",
            "fields": "trade_date,ts_code,name,z_t_num,up_num",
        }
    ]
    assert captured["start_date"] == "20260623"
    assert captured["end_date"] == "20260625"
    assert captured["batch_size"] == 1


@pytest.mark.asyncio
async def test_kplconcept_prepare_params_uses_trade_date_only():
    task = TushareStockKplConceptTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )

    params = await task.prepare_params(
        {
            "start_date": "20260828",
            "end_date": "20260828",
            "fields": "trade_date,ts_code,name,z_t_num,up_num",
        }
    )

    assert params == {
        "trade_date": "20260828",
        "fields": "trade_date,ts_code,name,z_t_num,up_num",
    }


@pytest.mark.asyncio
async def test_kplconcept_prepare_params_rejects_multi_day_range():
    task = TushareStockKplConceptTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )

    with pytest.raises(ValueError, match="只支持单日批次"):
        await task.prepare_params(
            {"start_date": "20260827", "end_date": "20260828"}
        )
