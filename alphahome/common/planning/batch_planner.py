# alphahome/common/planning/batch_planner.py
"""
A flexible and composable batch planning system.
This module provides a declarative way to define how a large task
should be broken down into smaller batches for execution.
"""
import asyncio
import inspect
from datetime import datetime
from itertools import groupby
from typing import (Any, Awaitable, Callable, Dict, Iterable, List, TypeVar,
                    Union)

T = TypeVar("T")
K = TypeVar("K")
BatchResult = Dict[str, Any]
SourceData = Union[Iterable[T], Awaitable[Iterable[T]]]
PartitionStrategy = Callable[[Iterable[T]], List[List[T]]]
MapStrategy = Callable[[List[T]], BatchResult]


class Source:
    """Provides strategies for defining the source of elements to be batched."""

    @staticmethod
    def from_list(data: Iterable[T]) -> Callable[[], Iterable[T]]:
        """
        Creates a source from an existing iterable (e.g., a list).

        Args:
            data: The iterable data.

        Returns:
            A callable that returns the data.
        """
        return lambda: data

    @staticmethod
    def from_callable(
        func: Callable[..., SourceData]
    ) -> Callable[..., SourceData]:
        """
        Creates a source from a callable (function or method).
        The callable will be invoked during the planning phase to fetch the data.

        Args:
            func: A synchronous or asynchronous callable that returns an iterable.

        Returns:
            The original callable.
        """
        return func


class Partition:
    """Provides strategies for partitioning a list of elements into batches."""

    @staticmethod
    def by_size(size: int) -> PartitionStrategy:
        """
        Partitions the list into fixed-size chunks.

        Args:
            size: The maximum size of each chunk.

        Returns:
            A partition strategy function.
        """
        if size <= 0:
            raise ValueError("Partition size must be positive.")

        def partitioner(data: Iterable[T]) -> List[List[T]]:
            if not isinstance(data, list):
                data = list(data)
            return [data[i : i + size] for i in range(0, len(data), size)]

        return partitioner

    @staticmethod
    def by_month() -> PartitionStrategy:
        """
        Partitions a list of date-like objects by month.
        Assumes elements are strings in 'YYYYMMDD' format or datetime objects.
        """
        def partitioner(data: Iterable[Union[str, datetime]]) -> List[List[Any]]:
            def get_year_month(date_item: Union[str, datetime]) -> str:
                if isinstance(date_item, str):
                    return date_item[:6]
                return date_item.strftime("%Y%m")

            return [
                list(group)
                for _, group in groupby(sorted(data), key=get_year_month)
            ]

        return partitioner

    @staticmethod
    def by_quarter() -> PartitionStrategy:
        """
        Partitions a list of date-like objects by quarter.
        Assumes elements are strings in 'YYYYMMDD' format or datetime objects.
        """
        def partitioner(data: Iterable[Union[str, datetime]]) -> List[List[Any]]:
            def get_year_quarter(date_item: Union[str, datetime]) -> str:
                if isinstance(date_item, str):
                    dt = datetime.strptime(date_item, "%Y%m%d")
                else:
                    dt = date_item
                quarter = (dt.month - 1) // 3 + 1
                return f"{dt.year}Q{quarter}"

            return [
                list(group)
                for _, group in groupby(sorted(data), key=get_year_quarter)
            ]

        return partitioner


class Map:
    """Provides strategies for mapping a partitioned batch to a result dictionary."""

    @staticmethod
    def to_dict(field_name: str) -> MapStrategy:
        """
        Maps a single-element batch to a dictionary.
        Raises ValueError if the batch does not contain exactly one element.

        Args:
            field_name: The key for the dictionary.

        Returns:
            A map strategy function.
        """
        def mapper(batch: List[T]) -> BatchResult:
            if len(batch) != 1:
                raise ValueError(
                    f"Map.to_dict strategy expects a batch of size 1, but got {len(batch)}."
                )
            return {field_name: batch[0]}

        return mapper

    @staticmethod
    def to_date_range(start_field: str, end_field: str) -> MapStrategy:
        """
        Maps a batch of dates to a start and end date dictionary.

        Args:
            start_field: The key for the start date.
            end_field: The key for the end date.

        Returns:
            A map strategy function.
        """
        def mapper(batch: List[T]) -> BatchResult:
            if not batch:
                raise ValueError("Cannot map an empty batch to a date range.")
            return {start_field: batch[0], end_field: batch[-1]}

        return mapper

    @staticmethod
    def with_custom_func(func: Callable[[List[T]], BatchResult]) -> MapStrategy:
        """
        Uses a custom function to map a batch to a dictionary.

        Args:
            func: The function to apply to each batch.

        Returns:
            The custom map strategy function.
        """
        return func


class BatchPlanner:
    """
    A planner that generates batch parameters based on composable strategies.
    """

    def __init__(
        self,
        source: Callable[..., SourceData],
        partition_strategy: PartitionStrategy,
        map_strategy: MapStrategy,
    ):
        """
        Initializes the BatchPlanner.

        Args:
            source: A callable that provides the data to be partitioned.
                    This can be a simple lambda or an async method.
            partition_strategy: A function that partitions the data from the source.
            map_strategy: A function that maps each partition to a dictionary.
        """
        self.source = source
        self.partition_strategy = partition_strategy
        self.map_strategy = map_strategy

    async def generate(self, *args, **kwargs) -> List[BatchResult]:
        """
        Generates the list of batch dictionaries.

        Args:
            *args, **kwargs: Arguments to be passed to the source callable.
                             A special kwarg 'additional_params' can be provided
                             as a dict to be merged into every resulting batch.

        Returns:
            A list of dictionaries, where each dictionary represents the
            parameters for one batch.
        """
        additional_params = kwargs.pop("additional_params", {})
        
        # Await the source if it's a coroutine function
        if inspect.iscoroutinefunction(self.source):
            elements = await self.source(*args, **kwargs)
        else:
            elements = self.source(*args, **kwargs)

        if not elements:
            return []

        partitions = self.partition_strategy(elements)
        
        final_batches = [self.map_strategy(p) for p in partitions if p]

        if additional_params:
            for batch in final_batches:
                batch.update(additional_params)

        return final_batches 