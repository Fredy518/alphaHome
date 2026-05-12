import pandas as pd
import pytest

from alphahome.fetchers.base.fetcher_task import FetcherTask


class _FailingBatchFetcherTask(FetcherTask):
    name = "failing_batch_fetcher"
    table_name = "failing_batch_fetcher"

    async def get_batch_list(self, **kwargs):
        return []

    async def prepare_params(self, batch):
        return {"batch": batch}

    async def fetch_batch(self, params, stop_event=None):
        if params["batch"] == "bad":
            raise RuntimeError("batch boom")
        return pd.DataFrame({"batch": [params["batch"]]})


@pytest.mark.asyncio
async def test_execute_batches_raises_when_any_batch_exhausts_retries():
    task = _FailingBatchFetcherTask(
        db_connection=object(),
        task_config={"concurrent_limit": 2, "max_retries": 2, "retry_delay": 0},
    )

    with pytest.raises(RuntimeError, match="1/2 batches failed"):
        await task._execute_batches(["ok", "bad"])

