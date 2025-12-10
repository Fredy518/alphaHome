#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OperationPipeline 防御性测试
"""

import pandas as pd
import pytest

from alphahome.processors.operations.base_operation import Operation, OperationPipeline


class _EchoOperation(Operation):
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return data.copy()


class _NoneOperation(Operation):
    async def apply(self, data: pd.DataFrame, **kwargs):
        return None


@pytest.mark.asyncio
async def test_pipeline_raises_on_none_result_by_default():
    pipeline = OperationPipeline()
    pipeline.add_operation(_EchoOperation())
    pipeline.add_operation(_NoneOperation())

    df = pd.DataFrame({"a": [1, 2, 3]})

    with pytest.raises(ValueError):
        await pipeline.apply(df)


@pytest.mark.asyncio
async def test_pipeline_continues_when_stop_on_error_false():
    pipeline = OperationPipeline(config={"stop_on_error": False})
    pipeline.add_operation(_NoneOperation())

    df = pd.DataFrame({"a": [1, 2, 3]})
    result = await pipeline.apply(df)

    assert isinstance(result, pd.DataFrame)
    assert result.empty

