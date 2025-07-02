#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基础层组件测试

测试BaseProcessor, DataProcessor, BlockProcessor等基础组件。
"""

import pytest
import pandas as pd
import numpy as np
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

# 导入被测试的组件
from alphahome.processors.base import (
    BaseProcessor,
    DataProcessor, 
    BlockProcessor,
    BlockProcessorMixin
)


class ConcreteBaseProcessor(BaseProcessor):
    """具体的BaseProcessor实现，用于测试"""

    async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """简单的处理逻辑：添加一个标记列"""
        result = data.copy()
        result["base_processed"] = True
        return result


class TestBaseProcessor:
    """测试BaseProcessor基类"""

    def test_init(self):
        """测试初始化"""
        processor = ConcreteBaseProcessor(name="test_processor")

        assert processor.name == "test_processor"
        assert processor.config == {}
        assert processor._process_count == 0
        assert processor.logger is not None

    def test_init_with_config(self):
        """测试带配置的初始化"""
        config = {"param1": "value1", "param2": 42}
        processor = ConcreteBaseProcessor(name="test", config=config)

        assert processor.config == config
        assert processor.config["param1"] == "value1"
        assert processor.config["param2"] == 42

    @pytest.mark.asyncio
    async def test_execute_success(self, sample_stock_data):
        """测试成功执行"""
        processor = ConcreteBaseProcessor(name="test")

        result = await processor.execute(sample_stock_data)

        assert result["status"] == "success"
        assert "data" in result
        assert "metadata" in result

        # 验证处理结果
        processed_data = result["data"]
        assert len(processed_data) == len(sample_stock_data)
        assert "base_processed" in processed_data.columns
        assert processed_data["base_processed"].all()

    def test_validate_input_empty_dataframe(self):
        """测试空DataFrame验证"""
        processor = ConcreteBaseProcessor(name="test")
        empty_df = pd.DataFrame()

        # 空DataFrame应该通过验证（由子类决定如何处理）
        processor._validate_input_data(empty_df)

    def test_validate_input_valid_dataframe(self):
        """测试有效DataFrame验证"""
        processor = ConcreteBaseProcessor(name="test")
        valid_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        processor._validate_input_data(valid_df)

    def test_validate_input_invalid_type(self):
        """测试无效输入类型"""
        processor = ConcreteBaseProcessor(name="test")

        with pytest.raises(TypeError):
            processor._validate_input_data("not a dataframe")

        with pytest.raises(TypeError):
            processor._validate_input_data(None)

    def test_reset_stats(self):
        """测试统计重置"""
        processor = ConcreteBaseProcessor(name="test")

        # 模拟一些统计数据
        processor._process_count = 5
        processor._total_time = 10.5

        processor.reset()

        assert processor._process_count == 0
        assert processor._total_time == 0.0
        assert processor._last_process_time is None


class ConcreteDataProcessor(DataProcessor):
    """具体的DataProcessor实现，用于测试"""
    
    async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """简单的处理逻辑：添加一个新列"""
        result = data.copy()
        result["processed"] = True
        result["process_time"] = datetime.now()
        return result


class TestDataProcessor:
    """测试DataProcessor类"""
    
    def test_init(self):
        """测试初始化"""
        processor = ConcreteDataProcessor(name="data_processor")
        
        assert processor.name == "data_processor"
        assert hasattr(processor, 'process')
    
    @pytest.mark.asyncio
    async def test_execute_success(self, sample_stock_data):
        """测试成功执行"""
        processor = ConcreteDataProcessor(name="test")
        
        result = await processor.execute(sample_stock_data)
        
        assert result["status"] == "success"
        assert "data" in result
        assert "metadata" in result
        
        # 验证处理结果
        processed_data = result["data"]
        assert len(processed_data) == len(sample_stock_data)
        assert "processed" in processed_data.columns
        assert processed_data["processed"].all()
    
    @pytest.mark.asyncio
    async def test_execute_empty_data(self):
        """测试空数据处理"""
        processor = ConcreteDataProcessor(name="test")
        empty_df = pd.DataFrame()
        
        result = await processor.execute(empty_df)
        
        assert result["status"] == "success"
        assert result["data"].empty
    
    @pytest.mark.asyncio
    async def test_execute_with_preprocessing(self):
        """测试带预处理的执行"""
        # 创建真正有重复行的数据
        dirty_data_with_duplicates = pd.DataFrame({
            'id': [1, 2, 3, 1, 2],  # 重复的行
            'price': [10.0, 11.0, 12.0, 10.0, 11.0],  # 完全相同的值
            'volume': [1000, 1100, 1200, 1000, 1100],  # 完全相同的值
        })

        class PreprocessingDataProcessor(DataProcessor):
            async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
                result = data.copy()
                result["cleaned"] = True
                return result

        # 使用配置启用去重
        processor = PreprocessingDataProcessor(
            name="test",
            config={"drop_duplicates": True}
        )

        result = await processor.execute(dirty_data_with_duplicates)

        assert result["status"] == "success"
        processed_data = result["data"]

        # 验证重复行被移除（通过配置的drop_duplicates）
        assert len(processed_data) < len(dirty_data_with_duplicates)
        assert "cleaned" in processed_data.columns
        assert len(processed_data) == 3  # 应该只剩3行
    
    @pytest.mark.asyncio
    async def test_execute_with_postprocessing(self, sample_stock_data):
        """测试带后处理的执行"""
        class PostprocessingDataProcessor(DataProcessor):
            async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
                result = data.copy()
                result["processed"] = True
                return result

            async def _post_process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
                # 先调用父类的后处理
                result = await super()._post_process(data, **kwargs)
                # 添加汇总信息
                result["row_count"] = len(result)
                return result

        processor = PostprocessingDataProcessor(name="test")

        result = await processor.execute(sample_stock_data)

        assert result["status"] == "success"
        processed_data = result["data"]

        assert "processed" in processed_data.columns
        assert "row_count" in processed_data.columns
        assert processed_data["row_count"].iloc[0] == len(sample_stock_data)
    
    @pytest.mark.asyncio
    async def test_execute_error_handling(self):
        """测试错误处理"""
        class ErrorDataProcessor(DataProcessor):
            async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
                raise ValueError("测试错误")
        
        processor = ErrorDataProcessor(name="test")
        
        result = await processor.execute(pd.DataFrame({"col": [1, 2, 3]}))
        
        assert result["status"] == "error"
        assert "error" in result
        assert "测试错误" in result["error"]
    
    def test_stats_tracking(self):
        """测试统计跟踪"""
        processor = ConcreteDataProcessor(name="test")

        # 初始统计
        assert processor._process_count == 0
        assert processor._total_time == 0.0

        # 执行后统计会在execute方法中更新
        # 这里主要测试统计结构的正确性


class ConcreteBlockProcessorMixin(BlockProcessorMixin):
    """具体的BlockProcessorMixin实现，用于测试"""

    def get_data_blocks(self, data: pd.DataFrame):
        """获取数据块"""
        return self._split_data_into_blocks(data)

    async def process_block(self, block: pd.DataFrame, block_id: int, **kwargs) -> pd.DataFrame:
        """处理单个数据块"""
        result = block.copy()
        result["block_processed"] = True
        result["block_id"] = block_id
        return result


class TestBlockProcessorMixin:
    """测试BlockProcessorMixin"""

    def test_init(self):
        """测试初始化"""
        mixin = ConcreteBlockProcessorMixin()

        assert mixin.block_size == 10000  # 默认值
        assert mixin.overlap_size == 0
        assert mixin.parallel_blocks is False

    def test_init_with_config(self):
        """测试带配置的初始化"""
        config = {
            "block_size": 5000,
            "overlap_size": 100,
            "parallel_blocks": True
        }
        mixin = ConcreteBlockProcessorMixin(config=config)

        assert mixin.block_size == 5000
        assert mixin.overlap_size == 100
        assert mixin.parallel_blocks is True
    
    def test_split_data_into_blocks(self, sample_stock_data):
        """测试数据分块"""
        mixin = ConcreteBlockProcessorMixin(config={"block_size": 30})

        blocks = mixin._split_data_into_blocks(sample_stock_data)

        # 100行数据，每块30行，应该有4块
        assert len(blocks) == 4

        # 前3块应该有30行
        for i in range(3):
            assert len(blocks[i]) == 30

        # 最后一块应该有10行
        assert len(blocks[3]) == 10

    def test_split_data_with_overlap(self, sample_stock_data):
        """测试带重叠的数据分块"""
        mixin = ConcreteBlockProcessorMixin(config={
            "block_size": 30,
            "overlap_size": 5
        })

        blocks = mixin._split_data_into_blocks(sample_stock_data)

        # 验证重叠
        if len(blocks) > 1:
            # 第二块的前5行应该与第一块的后5行相同
            overlap_data1 = blocks[0].tail(5)
            overlap_data2 = blocks[1].head(5)

            pd.testing.assert_frame_equal(
                overlap_data1.reset_index(drop=True),
                overlap_data2.reset_index(drop=True)
            )

    def test_combine_block_results(self):
        """测试块结果合并"""
        mixin = ConcreteBlockProcessorMixin()

        # 创建模拟的块结果
        block_results = [
            pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}),
            pd.DataFrame({"col1": [3, 4], "col2": ["c", "d"]}),
            pd.DataFrame({"col1": [5, 6], "col2": ["e", "f"]})
        ]

        combined = mixin._combine_block_results(block_results)

        assert len(combined) == 6
        assert list(combined["col1"]) == [1, 2, 3, 4, 5, 6]
        assert list(combined["col2"]) == ["a", "b", "c", "d", "e", "f"]


class ConcreteBlockProcessor(BlockProcessor):
    """具体的BlockProcessor实现，用于测试"""

    async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """简单的处理逻辑"""
        result = data.copy()
        result["block_processed"] = True
        result["block_id"] = kwargs.get("block_id", 0)
        return result

    def get_data_blocks(self, data: pd.DataFrame):
        """获取数据块"""
        return self._split_data_into_blocks(data)

    async def process_block(self, block: pd.DataFrame, block_id: int, **kwargs) -> pd.DataFrame:
        """处理单个数据块"""
        result = block.copy()
        result["block_processed"] = True
        result["block_id"] = block_id
        return result


class TestBlockProcessor:
    """测试BlockProcessor类"""
    
    def test_init(self):
        """测试初始化"""
        processor = ConcreteBlockProcessor(name="block_processor")
        
        assert processor.name == "block_processor"
        assert hasattr(processor, 'process')
        assert hasattr(processor, 'run_all_blocks')
    
    @pytest.mark.asyncio
    async def test_execute_with_blocks(self, sample_stock_data):
        """测试分块执行"""
        processor = ConcreteBlockProcessor(
            name="test",
            config={"block_size": 25}
        )
        
        result = await processor.execute(sample_stock_data)
        
        assert result["status"] == "success"
        processed_data = result["data"]
        
        # 验证所有数据都被处理
        assert len(processed_data) == len(sample_stock_data)
        assert "block_processed" in processed_data.columns
        assert processed_data["block_processed"].all()
    
    @pytest.mark.asyncio
    async def test_run_all_blocks_sequential(self, sample_stock_data):
        """测试顺序分块处理"""
        processor = ConcreteBlockProcessor(
            name="test",
            config={
                "block_size": 30,
                "parallel_blocks": False
            }
        )
        
        result = await processor.run_all_blocks(sample_stock_data)
        
        assert len(result) == len(sample_stock_data)
        assert "block_processed" in result.columns
        assert result["block_processed"].all()
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_run_all_blocks_parallel(self, sample_stock_data):
        """测试并行分块处理"""
        processor = ConcreteBlockProcessor(
            name="test",
            config={
                "block_size": 25,
                "parallel_blocks": True
            }
        )
        
        result = await processor.run_all_blocks(sample_stock_data)
        
        assert len(result) == len(sample_stock_data)
        assert "block_processed" in result.columns
        assert result["block_processed"].all()
    
    @pytest.mark.asyncio
    async def test_empty_data_handling(self):
        """测试空数据处理"""
        processor = ConcreteBlockProcessor(name="test")
        empty_df = pd.DataFrame()
        
        result = await processor.execute(empty_df)
        
        assert result["status"] == "success"
        assert result["data"].empty
