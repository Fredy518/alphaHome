import asyncio
import threading

import pytest

from alphahome.common.async_worker import run_owned_worker


async def test_async_cancellation_joins_worker_and_preserves_committed_result():
    started, may_close, closed = threading.Event(), threading.Event(), threading.Event()
    results = []
    def worker(cancellation):
        started.set()
        assert cancellation.wait(5)
        assert may_close.wait(5)
        closed.set()
        return {"status": "cancelled", "committed_rows": 3}
    task = asyncio.create_task(run_owned_worker(worker, on_cancelled_result=results.append))
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    task.cancel()
    may_close.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert closed.is_set()
    assert results == [{"status": "cancelled", "committed_rows": 3}]
