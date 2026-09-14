import logging
from datetime import date
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.tasks.stock.tushare_stock_dcmember import (
    TushareStockDcMemberTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_kplmember import (
    TushareStockKplMemberTask,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_type",
    [TushareStockDcMemberTask, TushareStockKplMemberTask],
)
async def test_smart_monthly_member_marks_current_incomplete_month_as_expected(task_type):
    task = object.__new__(task_type)
    task.logger = logging.getLogger(task_type.name)
    task.get_latest_date_for_task = AsyncMock(return_value=date(2026, 8, 31))

    batches = await task.get_batch_list(
        update_type=UpdateTypes.SMART,
        start_date="20260901",
        end_date="20260915",
    )

    assert batches == []
    assert "没有新的完整月末批次" in task._smart_skip_reason
    assert "2026-08-31" in task._smart_skip_reason
