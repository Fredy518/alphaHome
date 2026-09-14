from alphahome.features.storage.refresh import MaterializedViewRefresh
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def test_materialized_view_refresh_has_is_materialized_view_helper():
    # Guardrail: prevent AttributeError in runtime refresh path
    assert hasattr(MaterializedViewRefresh, "_is_materialized_view")


@pytest.mark.parametrize("error", [PermissionError("denied"), TimeoutError("timeout"), RuntimeError("execution failed")])
async def test_execution_errors_never_fallback(error):
    executor = MaterializedViewRefresh(SimpleNamespace())
    executor._is_materialized_view = AsyncMock(return_value=True)
    executor._concurrent_capability = AsyncMock(return_value=None)
    executor._execute = AsyncMock(side_effect=error)
    with pytest.raises(type(error)):
        await executor._execute_refresh("example", "features", "concurrent", allow_blocking_fallback=True)
    assert executor._execute.await_count == 1
    assert "CONCURRENTLY" in executor._execute.call_args.args[0]


async def test_unknown_row_count_is_not_reported_as_zero():
    executor = MaterializedViewRefresh(SimpleNamespace())
    executor._fetch_val = AsyncMock(side_effect=TimeoutError())
    assert await executor._get_row_count("example", "features") is None
