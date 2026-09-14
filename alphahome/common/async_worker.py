"""Join synchronous domain workers before propagating async cancellation."""

import asyncio
import threading


async def run_owned_worker(function, *, on_cancelled_result=None):
    cancellation = threading.Event()
    worker = asyncio.create_task(asyncio.to_thread(function, cancellation))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        cancellation.set()
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not worker.cancelled() and worker.exception() is None and on_cancelled_result:
            on_cancelled_result(worker.result())
        raise
