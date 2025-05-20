import pytest
import pytest_asyncio
import pandas as pd
from pandas.testing import assert_frame_equal
import datetime
from unittest.mock import patch, AsyncMock, MagicMock
import asyncpg # For type hints and mocking asyncpg.Record

# Module to test
from alphahome.fetchers.tools import calendar

# Helper to create asyncpg.Record-like mocks
def _create_mock_record(data: dict, columns: list) -> dict:
    # record = MagicMock(spec=asyncpg.Record)
    # record._data = {col: data.get(col) for col in columns}
    # record.get = lambda key, default=None: record._data.get(key, default)
    # record.__getitem__ = lambda key: record._data[key]
    # # Make it behave like a dict for pd.DataFrame constructor if needed
    # record.keys = lambda: record._data.keys()
    # record.values = lambda: record._data.values()
    # record.items = lambda: record._data.items()
    # return record
    # Simplification: return a dictionary directly, as DataFrame constructor handles list of dicts well.
    # Ensure all specified columns are present, even if None, to match Record behavior more closely.
    return {col: data.get(col) for col in columns}

@pytest_asyncio.fixture(autouse=True)
async def mock_db_pool_and_config(monkeypatch):
    """Mocks DB config loading and asyncpg.create_pool for all tests."""
    # Mock _load_db_config
    mock_load_config = MagicMock(return_value={"url": "postgresql://mockuser:mockpass@mockhost/mockdb"})
    monkeypatch.setattr(calendar, '_load_db_config', mock_load_config)

    # Mock asyncpg.create_pool
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    # Mock the connection acquisition context manager
    mock_connection = AsyncMock(spec=asyncpg.Connection)
    mock_pool.acquire.return_value.__aenter__.return_value = mock_connection 
    # Ensure __aexit__ is also an AsyncMock or a MagicMock that can be awaited
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None) 

    mock_create_pool = AsyncMock(return_value=mock_pool)
    monkeypatch.setattr(calendar.asyncpg, 'create_pool', mock_create_pool)
    
    # Store the mock connection on the mock_pool so tests can access its 'fetch' mock
    mock_pool.mock_connection = mock_connection 

    # Reset calendar module's global state before each test
    calendar._DB_POOL = None 
    calendar._TRADE_CAL_CACHE.clear()

    yield mock_pool # Tests can use this to access mock_pool.mock_connection.fetch

    # Cleanup (though _DB_POOL is reset at start, good practice)
    calendar._DB_POOL = None
    calendar._TRADE_CAL_CACHE.clear()


# --- Tests for get_trade_cal --- 
@pytest.mark.asyncio
async def test_get_trade_cal_success(mock_db_pool_and_config):
    mock_conn = mock_db_pool_and_config.mock_connection
    mock_records = [
        _create_mock_record({'exchange': 'SSE', 'cal_date': datetime.date(2023, 1, 3), 'is_open': 1, 'pretrade_date': datetime.date(2023, 1, 2)}, ['exchange', 'cal_date', 'is_open', 'pretrade_date']),
        _create_mock_record({'exchange': 'SSE', 'cal_date': datetime.date(2023, 1, 4), 'is_open': 1, 'pretrade_date': datetime.date(2023, 1, 3)}, ['exchange', 'cal_date', 'is_open', 'pretrade_date'])
    ]
    mock_conn.fetch = AsyncMock(return_value=mock_records)

    df = await calendar.get_trade_cal(start_date='20230101', end_date='20230105', exchange='SSE')

    mock_conn.fetch.assert_awaited_once()
    assert not df.empty
    assert len(df) == 2
    assert df['cal_date'].tolist() == ['20230103', '20230104']
    assert df['is_open'].tolist() == [1, 1]
    assert df['exchange'].tolist() == ['SSE', 'SSE']
    assert df['pretrade_date'].tolist() == ['20230102', '20230103']
    assert pd.api.types.is_integer_dtype(df['is_open']) # Should be Int64

@pytest.mark.asyncio
async def test_get_trade_cal_hkex_mapping(mock_db_pool_and_config):
    mock_conn = mock_db_pool_and_config.mock_connection
    mock_conn.fetch = AsyncMock(return_value=[])
    
    await calendar.get_trade_cal(start_date='20230101', end_date='20230105', exchange='HK')
    args, _ = mock_conn.fetch.call_args
    assert args[1] == 'HKEX' # Second arg to fetch is db_exchange_code

    await calendar.get_trade_cal(start_date='20230101', end_date='20230105', exchange='hkex')
    args, _ = mock_conn.fetch.call_args
    assert args[1] == 'HKEX'

@pytest.mark.asyncio
async def test_get_trade_cal_empty_result(mock_db_pool_and_config):
    mock_conn = mock_db_pool_and_config.mock_connection
    mock_conn.fetch = AsyncMock(return_value=[])
    df = await calendar.get_trade_cal(start_date='20230101', end_date='20230105')
    assert df.empty

@pytest.mark.asyncio
async def test_get_trade_cal_db_error(mock_db_pool_and_config):
    mock_conn = mock_db_pool_and_config.mock_connection
    mock_conn.fetch = AsyncMock(side_effect=Exception("DB error"))
    df = await calendar.get_trade_cal(start_date='20230101', end_date='20230105')
    assert df.empty

@pytest.mark.asyncio
async def test_get_trade_cal_pool_unavailable(monkeypatch, mock_db_pool_and_config):
    # Ensure _get_db_pool returns None *after* initial setup by mock_db_pool_and_config
    # To do this, we re-patch _get_db_pool within this test
    mock_get_pool = AsyncMock(return_value=None)
    monkeypatch.setattr(calendar, '_get_db_pool', mock_get_pool)
    # Clear cache to force _get_db_pool call by get_trade_cal
    calendar._TRADE_CAL_CACHE.clear()

    df = await calendar.get_trade_cal(start_date='20230101', end_date='20230105')
    assert df.empty
    mock_get_pool.assert_awaited_once() # Verify our specific mock was called

@pytest.mark.asyncio
async def test_get_trade_cal_caching(mock_db_pool_and_config):
    mock_conn = mock_db_pool_and_config.mock_connection
    mock_records = [
        _create_mock_record({'exchange': 'SSE', 'cal_date': datetime.date(2023, 1, 3), 'is_open': 1, 'pretrade_date': None}, ['exchange', 'cal_date', 'is_open', 'pretrade_date'])
    ]
    mock_conn.fetch = AsyncMock(return_value=mock_records)

    await calendar.get_trade_cal(start_date='20230101', end_date='20230105', exchange='SSE') # First call, populates cache
    await calendar.get_trade_cal(start_date='20230101', end_date='20230105', exchange='SSE') # Second call, should use cache

    mock_conn.fetch.assert_awaited_once() # fetch should only be called once
    assert len(calendar._TRADE_CAL_CACHE) == 1

# --- Tests for is_trade_day --- 
@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal') # Mock get_trade_cal for these tests
async def test_is_trade_day_true(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230103'], 'is_open': [1]
    })
    assert await calendar.is_trade_day('20230103') is True

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_is_trade_day_false_not_open(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230103'], 'is_open': [0]
    })
    assert await calendar.is_trade_day('20230103') is False

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_is_trade_day_false_not_in_calendar(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230104'], 'is_open': [1] # Different date
    })
    assert await calendar.is_trade_day('20230103') is False

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_is_trade_day_empty_calendar(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame()
    assert await calendar.is_trade_day('20230103') is False

# --- Tests for get_last_trade_day --- (Simplified, more comprehensive tests would mock get_trade_cal responses)
@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_last_trade_day_simple(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230101', '20230102', '20230103', '20230104'],
        'is_open': [0, 1, 1, 1] # 02,03,04 are trade days
    })
    assert await calendar.get_last_trade_day('20230104', n=1) == '20230103'
    assert await calendar.get_last_trade_day('20230104', n=2) == '20230102'
    assert await calendar.get_last_trade_day('20230103', n=1) == '20230102'
    # Test with date as datetime object
    assert await calendar.get_last_trade_day(datetime.date(2023,1,4), n=1) == '20230103'

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_last_trade_day_on_non_trade_day(mock_get_trade_cal):
    # Assumes get_trade_cal is called with end_date = date_str. 
    # The mocked df contains days up to and including date_str.
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230102', '20230103', '20230105'], # 04 is missing (non-trade)
        'is_open': [1, 1, 1]
    })
    # get_last_trade_day for 20230104 (non-trade day) should be 20230103
    # The logic inside get_last_trade_day filters for cal_date <= date_str and is_open == 1
    # So, for date='20230104', it considers ['20230102', '20230103']
    assert await calendar.get_last_trade_day('20230104', n=1) == '20230103'

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_last_trade_day_no_history(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230103'], 'is_open': [1]
    })
    assert await calendar.get_last_trade_day('20230103', n=2) is None # Not enough history

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_last_trade_day_invalid_date_str(mock_get_trade_cal):
    assert await calendar.get_last_trade_day('invalid-date') is None
    mock_get_trade_cal.assert_not_called() # Should fail before calling get_trade_cal

# --- Tests for get_next_trade_day --- (Simplified)
@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_next_trade_day_simple(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230103', '20230104', '20230105', '20230106'],
        'is_open': [1, 1, 0, 1] # 03,04,06 are trade days
    })
    assert await calendar.get_next_trade_day('20230103', n=1) == '20230104'
    assert await calendar.get_next_trade_day('20230103', n=2) == '20230106' 
    assert await calendar.get_next_trade_day('20230104', n=1) == '20230106'

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_next_trade_day_on_non_trade_day(mock_get_trade_cal):
    # For date='20230105' (non-trade), next is '20230106'
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230103', '20230104', '20230106', '20230109'], 
        'is_open': [1,1,1,1]
    })
    # get_trade_cal will be called with start_date='20230105'
    # The logic filters for cal_date >= start_date_str and is_open == 1
    # So relevant days are ['20230106', '20230109']
    assert await calendar.get_next_trade_day('20230105', n=1) == '20230106' 
    assert await calendar.get_next_trade_day('20230105', n=2) == '20230109'

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_next_trade_day_no_future(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230103'], 'is_open': [1]
    })
    assert await calendar.get_next_trade_day('20230103', n=2) is None

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_next_trade_day_invalid_date_str(mock_get_trade_cal):
    assert await calendar.get_next_trade_day('invalid-date') is None
    mock_get_trade_cal.assert_not_called()

# --- Tests for get_trade_days_between ---
@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_trade_days_between_normal_range(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230102', '20230103', '20230104', '20230105'],
        'is_open': [0, 1, 1, 0] # 03, 04 are trade days
    })
    result = await calendar.get_trade_days_between('20230101', '20230106')
    assert result == ['20230103', '20230104']

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_trade_days_between_empty_result(mock_get_trade_cal):
    mock_get_trade_cal.return_value = pd.DataFrame({
        'cal_date': ['20230102', '20230105'], 'is_open': [0, 0]
    })
    assert await calendar.get_trade_days_between('20230101', '20230106') == []

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_trade_days_between_start_after_end(mock_get_trade_cal):
    assert await calendar.get_trade_days_between('20230106', '20230101') == []
    mock_get_trade_cal.assert_not_called() # Should return early

@pytest.mark.asyncio
@patch('alphahome.fetchers.tools.calendar.get_trade_cal')
async def test_get_trade_days_between_invalid_dates(mock_get_trade_cal):
    assert await calendar.get_trade_days_between('invalid', '20230101') == []
    mock_get_trade_cal.assert_not_called()
    assert await calendar.get_trade_days_between('20230101', 'invalid') == []
    mock_get_trade_cal.assert_not_called() 