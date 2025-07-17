"""
Unit tests for the bt_extensions execution engine components.
"""

import pytest
import pandas as pd
import backtrader as bt
from unittest.mock import MagicMock, patch

from alphahome.bt_extensions.execution.batch_loader import BatchDataLoader
from alphahome.bt_extensions.execution.parallel_runner import ParallelBacktestRunner
from alphahome.bt_extensions.utils.exceptions import BatchLoadingError


@pytest.fixture
def mock_data_tool():
    """Fixture for a mocked AlphaDataTool."""
    return MagicMock()


class TestBatchLoader:
    """Tests for the BatchLoader component."""

    def test_load_all_stocks_success(self, mock_data_tool):
        """
        Test that load_all_stocks successfully returns data from the data tool.
        """
        # Arrange
        expected_df = pd.DataFrame({'close': [100, 101, 102]})
        mock_data_tool.get_stock_data_batch.return_value = expected_df
        
        loader = BatchDataLoader(alpha_data_tool=mock_data_tool)
        stock_list = ['AAPL', 'GOOG']
        
        # Act
        result_df = loader.load_stocks_data(stock_list, start_date='2023-01-01', end_date='2023-01-31')
        
        # Assert
        assert result_df.equals(expected_df)
        mock_data_tool.get_stock_data_batch.assert_called_once_with(
            symbols=stock_list,
            start_date='2023-01-01',
            end_date='2023-01-31',
            use_cache=True
        )

    def test_load_all_stocks_raises_custom_error_on_failure(self, mock_data_tool):
        """
        Test that load_all_stocks raises a BatchLoadingError when the data tool fails.
        """
        # Arrange
        mock_data_tool.get_stock_data_batch.side_effect = Exception("Database connection failed")
        loader = BatchDataLoader(alpha_data_tool=mock_data_tool)
        
        # Act & Assert
        with pytest.raises(BatchLoadingError, match="批量数据加载过程中发生错误"):
            loader.load_stocks_data(['AAPL'], start_date='2023-01-01', end_date='2023-01-31')


# --- Test Helpers for ParallelRunner ---

class SimpleTestStrategy(bt.Strategy):
    """A simple strategy that does nothing but can be run."""
    def __init__(self):
        self.log(f'Strategy created on data: {self.data.p.name}')
    
    def next(self):
        pass

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

class FailingStrategy(bt.Strategy):
    """A strategy that is designed to fail."""
    def next(self):
        raise ValueError("Intentional strategy failure")

@pytest.fixture
def sample_dataframe():
    """Fixture for a sample pandas DataFrame for backtesting."""
    dates = pd.to_datetime(pd.date_range(start='2023-01-01', periods=10))
    data = {
        'open': [100 + i for i in range(10)],
        'high': [105 + i for i in range(10)],
        'low': [95 + i for i in range(10)],
        'close': [102 + i for i in range(10)],
        'volume': [1000 + i*10 for i in range(10)],
        'openinterest': [0] * 10
    }
    df = pd.DataFrame(data, index=dates)
    return df

class TestParallelRunner:
    """Tests for the ParallelBacktestRunner component."""

    @patch('alphahome.bt_extensions.execution.parallel_runner.ProcessPoolExecutor')
    def test_run_single_strategy_success(self, mock_executor, sample_dataframe):
        """Test running a single, successful strategy."""
        # Arrange
        # Mock the future object
        mock_future = MagicMock()
        mock_future.result.return_value = {"TEST": {"final_value": 105000}}
        
        # Configure the executor mock
        mock_executor.return_value.__enter__.return_value.submit.return_value = mock_future
        mock_executor.return_value.__enter__.return_value.as_completed.return_value = [mock_future]

        runner = ParallelBacktestRunner(max_workers=1)
        strategies = (SimpleTestStrategy, {})
        
        # Act
        results = runner.run_parallel_backtests(
            stock_codes=['TEST'],
            strategy_class=SimpleTestStrategy,
            strategy_params={},
            start_date='2023-01-01',
            end_date='2023-01-10'
        )
        
        # Assert
        assert 'TEST' in results['results']
        assert results['results']['TEST']['final_value'] == 105000
        assert results['summary']['successful_stocks'] == 1

    @patch('alphahome.bt_extensions.execution.parallel_runner.ProcessPoolExecutor')
    def test_run_with_failing_strategy(self, mock_executor, caplog):
        """Test that the runner handles a failing strategy gracefully."""
        # Arrange
        mock_future = MagicMock()
        mock_future.result.side_effect = ValueError("Intentional batch failure")
        
        mock_executor.return_value.__enter__.return_value.submit.return_value = mock_future
        mock_executor.return_value.__enter__.return_value.as_completed.return_value = [mock_future]

        runner = ParallelBacktestRunner(max_workers=1)
        
        # Act
        results = runner.run_parallel_backtests(
            stock_codes=['FAIL_TEST'],
            strategy_class=FailingStrategy,
            strategy_params={},
            start_date='2023-01-01',
            end_date='2023-01-10'
        )
        
        # Assert
        assert len(results['failed_batches']) == 1
        assert "批次 1 失败" in caplog.text
        assert "Intentional batch failure" in caplog.text

    @patch('alphahome.bt_extensions.execution.parallel_runner.ProcessPoolExecutor')
    def test_run_multiple_strategies(self, mock_executor, sample_dataframe):
        """Test running multiple strategies concurrently."""
        # Arrange
        # This test is more complex. We'll simplify by focusing on task submission
        # and assuming successful execution of batches.
        mock_future_1 = MagicMock()
        mock_future_1.result.return_value = {"TEST_A": {"final_value": 101}}
        mock_future_2 = MagicMock()
        mock_future_2.result.return_value = {"TEST_B": {"final_value": 102}}
        
        mock_executor.return_value.__enter__.return_value.submit.side_effect = [mock_future_1, mock_future_2]
        mock_executor.return_value.__enter__.return_value.as_completed.return_value = [mock_future_1, mock_future_2]

        runner = ParallelBacktestRunner(max_workers=2, batch_size=1)
        
        # Act
        results = runner.run_parallel_backtests(
            stock_codes=['TEST_A', 'TEST_B'],
            strategy_class=SimpleTestStrategy,
            strategy_params={},
            start_date='2023-01-01',
            end_date='2023-01-10'
        )

        # Assert
        assert results['summary']['successful_stocks'] == 2
        assert mock_executor.return_value.__enter__.return_value.submit.call_count == 2
        assert 'TEST_A' in results['results']
        assert 'TEST_B' in results['results'] 