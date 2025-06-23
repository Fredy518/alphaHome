# tests/unit/test_common/test_planning/test_batch_planner.py
import pytest
from datetime import datetime
from alphahome.common.planning.batch_planner import (
    BatchPlanner,
    Source,
    Partition,
    Map,
)


@pytest.fixture
def sample_stock_codes():
    return ["000001.SZ", "600519.SH", "300750.SZ", "000002.SZ", "000003.SZ"]


@pytest.fixture
def sample_dates():
    # Dates covering multiple months and quarters
    return [
        "20230115", "20230120",
        "20230330", "20230331",
        "20230401",
        "20230630",
        "20230701",
    ]

# --- Test Source Strategies ---

def test_source_from_list(sample_stock_codes):
    source_func = Source.from_list(sample_stock_codes)
    assert source_func() == sample_stock_codes

async def test_source_from_callable(sample_stock_codes):
    def get_codes():
        return sample_stock_codes
    
    source_func = Source.from_callable(get_codes)
    assert source_func() == sample_stock_codes

async def test_source_from_async_callable(sample_stock_codes):
    async def get_codes_async():
        return sample_stock_codes

    source_func = Source.from_callable(get_codes_async)
    assert await source_func() == sample_stock_codes


# --- Test Partition Strategies ---

def test_partition_by_size_even(sample_stock_codes):
    partitioner = Partition.by_size(1)
    result = partitioner(sample_stock_codes)
    assert result == [["000001.SZ"], ["600519.SH"], ["300750.SZ"], ["000002.SZ"], ["000003.SZ"]]
    assert len(result) == 5

def test_partition_by_size_uneven(sample_stock_codes):
    partitioner = Partition.by_size(2)
    result = partitioner(sample_stock_codes)
    assert result == [["000001.SZ", "600519.SH"], ["300750.SZ", "000002.SZ"], ["000003.SZ"]]
    assert len(result) == 3

def test_partition_by_size_larger_than_list(sample_stock_codes):
    partitioner = Partition.by_size(10)
    result = partitioner(sample_stock_codes)
    assert result == [["000001.SZ", "600519.SH", "300750.SZ", "000002.SZ", "000003.SZ"]]
    assert len(result) == 1

def test_partition_by_size_empty_list():
    partitioner = Partition.by_size(2)
    assert partitioner([]) == []

def test_partition_by_size_invalid():
    with pytest.raises(ValueError):
        Partition.by_size(0)
    with pytest.raises(ValueError):
        Partition.by_size(-1)

def test_partition_by_month(sample_dates):
    partitioner = Partition.by_month()
    result = partitioner(sample_dates)
    assert result == [
        ["20230115", "20230120"],
        ["20230330", "20230331"],
        ["20230401"],
        ["20230630"],
        ["20230701"],
    ]

def test_partition_by_quarter(sample_dates):
    partitioner = Partition.by_quarter()
    result = partitioner(sample_dates)
    assert len(result) == 3  # Expect 3 groups: Q1, Q2, Q3
    assert result[0] == ["20230115", "20230120", "20230330", "20230331"] # All Q1 dates
    assert result[1] == ["20230401", "20230630"] # All Q2 dates
    assert result[2] == ["20230701"] # All Q3 dates

    # Also test with datetime objects
    dt_objects = [datetime.strptime(d, "%Y%m%d") for d in sample_dates]
    dt_result = partitioner(dt_objects)
    assert len(dt_result) == 3
    assert dt_result[0] == [datetime(2023, 1, 15), datetime(2023, 1, 20), datetime(2023, 3, 30), datetime(2023, 3, 31)]


# --- Test Map Strategies ---

def test_map_to_dict():
    mapper = Map.to_dict("ts_code")
    assert mapper(["000001.SZ"]) == {"ts_code": "000001.SZ"}

def test_map_to_dict_invalid_size():
    mapper = Map.to_dict("ts_code")
    with pytest.raises(ValueError):
        mapper([])
    with pytest.raises(ValueError):
        mapper(["000001.SZ", "600519.SH"])

def test_map_to_date_range():
    mapper = Map.to_date_range("start_date", "end_date")
    batch = ["20230101", "20230102", "20230103"]
    assert mapper(batch) == {"start_date": "20230101", "end_date": "20230103"}
    assert mapper(["20230101"]) == {"start_date": "20230101", "end_date": "20230101"}

def test_map_to_date_range_empty():
    mapper = Map.to_date_range("start", "end")
    with pytest.raises(ValueError):
        mapper([])

def test_map_with_custom_func():
    def custom_mapper(batch):
        return {"codes": batch, "count": len(batch)}
    
    mapper = Map.with_custom_func(custom_mapper)
    batch = ["a", "b"]
    assert mapper(batch) == {"codes": ["a", "b"], "count": 2}


# --- Test BatchPlanner Integration ---

@pytest.mark.asyncio
async def test_planner_stock_codes(sample_stock_codes):
    """Simulates generating batches by stock code."""
    planner = BatchPlanner(
        source=Source.from_list(sample_stock_codes),
        partition_strategy=Partition.by_size(1),
        map_strategy=Map.to_dict("ts_code"),
    )
    result = await planner.generate()
    expected = [
        {"ts_code": "000001.SZ"},
        {"ts_code": "600519.SH"},
        {"ts_code": "300750.SZ"},
        {"ts_code": "000002.SZ"},
        {"ts_code": "000003.SZ"},
    ]
    assert result == expected

@pytest.mark.asyncio
async def test_planner_date_range(sample_dates):
    """Simulates generating batches by a date range of 2 days."""
    planner = BatchPlanner(
        source=Source.from_list(sample_dates),
        partition_strategy=Partition.by_size(2),
        map_strategy=Map.to_date_range(start_field="begin", end_field="stop"),
    )
    result = await planner.generate()
    expected = [
        {"begin": "20230115", "stop": "20230120"},
        {"begin": "20230330", "stop": "20230331"},
        {"begin": "20230401", "stop": "20230630"},
        {"begin": "20230701", "stop": "20230701"},
    ]
    assert result == expected


@pytest.mark.asyncio
async def test_planner_with_callable_source(sample_stock_codes):
    """Tests using a callable as the source."""
    async def get_codes_async():
        return sample_stock_codes

    planner = BatchPlanner(
        source=Source.from_callable(get_codes_async),
        partition_strategy=Partition.by_size(3),
        map_strategy=Map.with_custom_func(lambda b: {"my_batch": b}),
    )
    result = await planner.generate()
    expected = [
        {"my_batch": ["000001.SZ", "600519.SH", "300750.SZ"]},
        {"my_batch": ["000002.SZ", "000003.SZ"]},
    ]
    assert result == expected


@pytest.mark.asyncio
async def test_planner_with_additional_params(sample_stock_codes):
    """Tests the 'additional_params' feature."""
    planner = BatchPlanner(
        source=Source.from_list(sample_stock_codes),
        partition_strategy=Partition.by_size(1),
        map_strategy=Map.to_dict("ts_code"),
    )
    
    # Pass additional_params to the generate method
    fixed_params = {"report_type": "1", "limit": 1000}
    result = await planner.generate(additional_params=fixed_params)

    expected = [
        {"ts_code": "000001.SZ", "report_type": "1", "limit": 1000},
        {"ts_code": "600519.SH", "report_type": "1", "limit": 1000},
        {"ts_code": "300750.SZ", "report_type": "1", "limit": 1000},
        {"ts_code": "000002.SZ", "report_type": "1", "limit": 1000},
        {"ts_code": "000003.SZ", "report_type": "1", "limit": 1000},
    ]
    assert result == expected

@pytest.mark.asyncio
async def test_planner_empty_source():
    """Tests that an empty source produces an empty list."""
    planner = BatchPlanner(
        source=Source.from_list([]),
        partition_strategy=Partition.by_size(1),
        map_strategy=Map.to_dict("ts_code"),
    )
    result = await planner.generate()
    assert result == [] 