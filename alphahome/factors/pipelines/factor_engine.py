"""Config-driven P/G factor execution engine."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from alphahome.factors.core import GFactorCalculator, PFactorCalculator


VALID_FACTOR_TYPES = {"p", "g"}
VALID_MODES = {"auto", "incremental", "backfill"}
VALID_MISSING_MODES = {"none", "batch_missing", "recent_missing"}


@dataclass(frozen=True, order=True)
class Quarter:
    year: int
    quarter: int

    @property
    def label(self) -> str:
        return f"{self.year}Q{self.quarter}"

    @property
    def date_range(self) -> Tuple[str, str]:
        if self.quarter == 1:
            return f"{self.year}-01-01", f"{self.year}-03-31"
        if self.quarter == 2:
            return f"{self.year}-04-01", f"{self.year}-06-30"
        if self.quarter == 3:
            return f"{self.year}-07-01", f"{self.year}-09-30"
        if self.quarter == 4:
            return f"{self.year}-10-01", f"{self.year}-12-31"
        raise ValueError(f"invalid quarter: {self.quarter}")


@dataclass(frozen=True)
class FactorWorkItem:
    factor_type: str
    start_date: str
    end_date: str
    mode: str = "backfill"
    calc_date: Optional[str] = None
    year: Optional[int] = None
    quarter: Optional[Quarter] = None


@dataclass
class FactorEngineConfig:
    factor_types: Sequence[str] = ("p", "g")
    dates: Optional[Sequence[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    quarter: Optional[Sequence[str]] = None
    start_quarter: Optional[str] = None
    end_quarter: Optional[str] = None
    worker_id: Optional[int] = None
    total_workers: Optional[int] = None
    workers: Optional[int] = None
    mode: str = "auto"
    missing_mode: str = "none"
    months_back: int = 3
    dry_run: bool = False
    log_level: str = "INFO"
    today: Optional[str] = None
    launch_delay: int = 2
    extra: Dict[str, Any] = field(default_factory=dict)


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid date format: {value}; expected YYYY-MM-DD") from exc


def validate_date(value: str) -> str:
    parse_date(value)
    return value


def parse_quarter(value: str) -> Quarter:
    match = re.fullmatch(r"(\d{4})[Qq]([1-4])", value.strip())
    if not match:
        raise ValueError(f"invalid quarter format: {value}; expected YYYYQN")
    return Quarter(int(match.group(1)), int(match.group(2)))


def generate_quarters_for_years(start_year: int, end_year: int) -> List[Quarter]:
    if start_year > end_year:
        raise ValueError("start_year must be <= end_year")
    return [Quarter(year, quarter) for year in range(start_year, end_year + 1) for quarter in range(1, 5)]


def generate_quarter_range(start_quarter: str, end_quarter: str) -> List[Quarter]:
    start = parse_quarter(start_quarter)
    end = parse_quarter(end_quarter)
    if start > end:
        raise ValueError("start_quarter must be <= end_quarter")

    quarters: List[Quarter] = []
    current = start
    while current <= end:
        quarters.append(current)
        next_quarter = current.quarter + 1
        next_year = current.year
        if next_quarter > 4:
            next_quarter = 1
            next_year += 1
        current = Quarter(next_year, next_quarter)
    return quarters


def generate_friday_dates(start_date: str, end_date: str) -> List[str]:
    start = parse_date(start_date)
    end = parse_date(end_date)
    if start > end:
        raise ValueError("start_date must be <= end_date")

    current = start
    while current.weekday() != 4 and current <= end:
        current += timedelta(days=1)

    dates: List[str] = []
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=7)
    return dates


def allocate_contiguous_balanced(items: Sequence[Any], workers: int) -> List[List[Any]]:
    if workers <= 0:
        raise ValueError("workers must be > 0")

    sorted_items = list(items)
    allocation: List[List[Any]] = [[] for _ in range(workers)]
    if not sorted_items:
        return allocation

    base = len(sorted_items) // workers
    extra = len(sorted_items) % workers
    index = 0
    for worker_id in range(workers):
        count = base + (1 if worker_id < extra else 0)
        allocation[worker_id] = sorted_items[index : index + count]
        index += count
    return allocation


def shard_items(items: Sequence[Any], worker_id: Optional[int], total_workers: Optional[int]) -> List[Any]:
    if worker_id is None and total_workers is None:
        return list(items)
    if worker_id is None or total_workers is None:
        raise ValueError("worker_id and total_workers must be provided together")
    if total_workers <= 0:
        raise ValueError("total_workers must be > 0")
    if worker_id < 0 or worker_id >= total_workers:
        raise ValueError("worker_id must be in [0, total_workers)")
    return allocate_contiguous_balanced(items, total_workers)[worker_id]


class FactorEngine:
    """Unified executor for P/G factor date, year, quarter, and missing modes."""

    def __init__(
        self,
        config: FactorEngineConfig,
        p_calculator: Any = None,
        g_calculator: Any = None,
        context: Any = None,
        db_manager: Any = None,
        existing_date_provider: Optional[Callable[[str, Sequence[str]], Iterable[str]]] = None,
    ):
        self.config = config
        self.context = context
        self.db_manager = db_manager or getattr(context, "db_manager", None)
        self.existing_date_provider = existing_date_provider
        self.logger = logging.getLogger("FactorEngine")
        self.logger.setLevel(getattr(logging, config.log_level, logging.INFO))
        self._p_calculator = p_calculator
        self._g_calculator = g_calculator
        self.validate_config()

    @property
    def p_calculator(self) -> Any:
        if self._p_calculator is None:
            self._p_calculator = PFactorCalculator(context=self.context, db_manager=self.db_manager)
        return self._p_calculator

    @property
    def g_calculator(self) -> Any:
        if self._g_calculator is None:
            self._g_calculator = GFactorCalculator(context=self.context, db_manager=self.db_manager)
        return self._g_calculator

    def validate_config(self) -> None:
        factor_types = [factor_type.lower() for factor_type in self.config.factor_types]
        invalid = sorted(set(factor_types) - VALID_FACTOR_TYPES)
        if invalid:
            raise ValueError(f"invalid factor_types: {invalid}")
        if self.config.mode not in VALID_MODES:
            raise ValueError(f"invalid mode: {self.config.mode}")
        if self.config.missing_mode not in VALID_MISSING_MODES:
            raise ValueError(f"invalid missing_mode: {self.config.missing_mode}")
        if self.config.months_back <= 0:
            raise ValueError("months_back must be > 0")
        if self.config.workers is not None and self.config.workers <= 0:
            raise ValueError("workers must be > 0")
        if self.config.worker_id is not None or self.config.total_workers is not None:
            shard_items([], self.config.worker_id, self.config.total_workers)

        selector_count = sum(
            [
                bool(self.config.dates),
                bool(self.config.start_date or self.config.end_date),
                bool(self.config.start_year is not None or self.config.end_year is not None),
                bool(self.config.quarter),
                bool(self.config.start_quarter or self.config.end_quarter),
            ]
        )
        if self.config.missing_mode == "recent_missing":
            if selector_count > 0:
                raise ValueError("recent_missing mode cannot be combined with explicit date selectors")
            return
        if self.config.missing_mode == "batch_missing":
            if not self.config.start_date or not self.config.end_date:
                raise ValueError("batch_missing mode requires both start_date and end_date")
            if bool(self.config.start_date) != bool(self.config.end_date):
                raise ValueError("batch_missing mode requires both start_date and end_date")
            validate_date(self.config.start_date)
            validate_date(self.config.end_date)
            return
        if selector_count != 1:
            raise ValueError("exactly one date selector must be provided")
        if bool(self.config.start_date) != bool(self.config.end_date):
            raise ValueError("start_date and end_date must be provided together")
        if bool(self.config.start_year is not None) != bool(self.config.end_year is not None):
            raise ValueError("start_year and end_year must be provided together")
        if bool(self.config.start_quarter) != bool(self.config.end_quarter):
            raise ValueError("start_quarter and end_quarter must be provided together")
        if self.config.quarter and (self.config.start_quarter or self.config.end_quarter):
            raise ValueError("quarter cannot be combined with start_quarter/end_quarter")

    def normalized_factor_types(self) -> List[str]:
        return [factor_type.lower() for factor_type in self.config.factor_types]

    def resolve_dates(self) -> List[str]:
        if self.config.missing_mode in {"batch_missing", "recent_missing"}:
            return self._resolve_missing_dates()

        if self.config.dates:
            dates = [validate_date(value) for value in self.config.dates]
            return shard_items(dates, self.config.worker_id, self.config.total_workers)

        if self.config.start_date and self.config.end_date:
            dates = generate_friday_dates(self.config.start_date, self.config.end_date)
            return shard_items(dates, self.config.worker_id, self.config.total_workers)

        ranges = self.resolve_date_ranges()
        dates: List[str] = []
        for start_date, end_date, _ in ranges:
            dates.extend(generate_friday_dates(start_date, end_date))
        return dates

    def resolve_date_ranges(self) -> List[Tuple[str, str, Dict[str, Any]]]:
        if self.config.start_year is not None and self.config.end_year is not None:
            years = list(range(self.config.start_year, self.config.end_year + 1))
            years = shard_items(years, self.config.worker_id, self.config.total_workers)
            return [(f"{year}-01-01", f"{year}-12-31", {"year": year}) for year in years]

        quarters = self.resolve_quarters()
        if quarters:
            return [
                (start_date, end_date, {"quarter": quarter})
                for quarter in quarters
                for start_date, end_date in [quarter.date_range]
            ]

        if self.config.start_date and self.config.end_date:
            return [(self.config.start_date, self.config.end_date, {})]

        if self.config.dates:
            dates = shard_items(
                [validate_date(value) for value in self.config.dates],
                self.config.worker_id,
                self.config.total_workers,
            )
            return [(value, value, {"calc_date": value}) for value in dates]

        return []

    def resolve_quarters(self) -> List[Quarter]:
        quarters: List[Quarter] = []
        if self.config.quarter:
            quarters = sorted(parse_quarter(value) for value in self.config.quarter)
        elif self.config.start_quarter and self.config.end_quarter:
            quarters = generate_quarter_range(self.config.start_quarter, self.config.end_quarter)
        elif self.config.start_year is not None and self.config.end_year is not None:
            quarters = generate_quarters_for_years(self.config.start_year, self.config.end_year)
        if not quarters:
            return []
        return shard_items(quarters, self.config.worker_id, self.config.total_workers)

    def filter_missing_dates(self, factor_type: str, dates: Sequence[str]) -> List[str]:
        factor_type = factor_type.lower()
        if not dates:
            return []
        if self.existing_date_provider is not None:
            existing = {
                self._coerce_date_string(value)
                for value in self.existing_date_provider(factor_type, list(dates))
            }
            return [value for value in dates if value not in existing]

        calculator = self._calculator_for_factor(factor_type)
        if hasattr(calculator, "_filter_missing_dates"):
            return list(calculator._filter_missing_dates(list(dates)))
        return list(dates)

    def resolve_work_items(self) -> List[FactorWorkItem]:
        mode = "backfill" if self.config.mode == "auto" else self.config.mode
        factor_types = self.normalized_factor_types()
        work_items: List[FactorWorkItem] = []

        if self.config.dates or self.config.missing_mode in {"batch_missing", "recent_missing"}:
            dates = self.resolve_dates()
            for factor_type in factor_types:
                factor_dates = dates
                if self.config.missing_mode == "none" and self.config.mode == "incremental":
                    factor_dates = self.filter_missing_dates(factor_type, dates)
                for calc_date in factor_dates:
                    work_items.append(
                        FactorWorkItem(
                            factor_type=factor_type,
                            start_date=calc_date,
                            end_date=calc_date,
                            mode=mode,
                            calc_date=calc_date,
                        )
                    )
            return work_items

        for start_date, end_date, meta in self.resolve_date_ranges():
            for factor_type in factor_types:
                work_items.append(
                    FactorWorkItem(
                        factor_type=factor_type,
                        start_date=start_date,
                        end_date=end_date,
                        mode=mode,
                        year=meta.get("year"),
                        quarter=meta.get("quarter"),
                        calc_date=meta.get("calc_date"),
                    )
                )
        return work_items

    def run(self) -> Dict[str, Any]:
        work_items = self.resolve_work_items()
        if self.config.dry_run:
            return {
                "dry_run": True,
                "work_item_count": len(work_items),
                "work_items": [item.__dict__.copy() for item in work_items],
            }

        started_at = time.time()
        results: List[Dict[str, Any]] = []
        totals = {"success_count": 0, "failed_count": 0}

        for item in work_items:
            result = self._run_work_item(item)
            results.append({"work_item": item, "result": result})
            totals["success_count"] += int(result.get("success_count", 0))
            totals["failed_count"] += int(result.get("failed_count", 0))

        return {
            "work_item_count": len(work_items),
            "success_count": totals["success_count"],
            "failed_count": totals["failed_count"],
            "total_time": time.time() - started_at,
            "details": results,
        }

    def run_worker(self) -> Dict[str, Any]:
        return self.run()

    def launch_workers(self, script_kind: str) -> int:
        raise NotImplementedError(
            "Worker launching is provided by compatibility scripts; use resolve_date_ranges/resolve_work_items for orchestration."
        )

    def _run_work_item(self, item: FactorWorkItem) -> Dict[str, Any]:
        calculator = self._calculator_for_factor(item.factor_type)
        if item.calc_date:
            stock_codes = calculator._get_trading_stock_codes(item.calc_date)
            if item.factor_type == "p":
                return calculator.calculate_p_factors_pit(item.calc_date, stock_codes)
            return calculator.calculate_g_factors_pit(item.calc_date, stock_codes)

        method_name = "calculate_p_factors_batch_pit" if item.factor_type == "p" else "calculate_g_factors_batch_pit"
        return getattr(calculator, method_name)(
            start_date=item.start_date,
            end_date=item.end_date,
            mode=item.mode,
        )

    def _calculator_for_factor(self, factor_type: str) -> Any:
        if factor_type == "p":
            return self.p_calculator
        if factor_type == "g":
            return self.g_calculator
        raise ValueError(f"unsupported factor_type: {factor_type}")

    def _resolve_missing_dates(self) -> List[str]:
        if self.config.missing_mode == "batch_missing":
            start_date = self.config.start_date
            end_date = self.config.end_date
        else:
            today = parse_date(self.config.today) if self.config.today else date.today()
            end_date = today.strftime("%Y-%m-%d")
            start_date = (today - timedelta(days=self.config.months_back * 30)).strftime("%Y-%m-%d")

        if not start_date or not end_date:
            raise ValueError("missing date range could not be resolved")
        dates = self._query_missing_dates(start_date, end_date)
        return shard_items(dates, self.config.worker_id, self.config.total_workers)

    def _query_missing_dates(self, start_date: str, end_date: str) -> List[str]:
        if self.existing_date_provider is not None:
            all_dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d").tolist()
            missing: set[str] = set()
            for factor_type in self.normalized_factor_types():
                missing.update(self.filter_missing_dates(factor_type, all_dates))
            return sorted(missing)

        if self.context is None:
            raise ValueError("context is required for missing date detection")

        query = """
        WITH date_range AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS calc_date
        ),
        p_factor_dates AS (
            SELECT DISTINCT calc_date FROM pgs_factors.p_factor
        ),
        g_factor_dates AS (
            SELECT DISTINCT calc_date FROM pgs_factors.g_factor
        )
        SELECT dr.calc_date
        FROM date_range dr
        LEFT JOIN p_factor_dates pfd ON dr.calc_date = pfd.calc_date
        LEFT JOIN g_factor_dates gfd ON dr.calc_date = gfd.calc_date
        WHERE dr.calc_date IS NOT NULL
          AND (pfd.calc_date IS NULL OR gfd.calc_date IS NULL)
        ORDER BY dr.calc_date
        """
        result = self.context.query_dataframe(query, (start_date, end_date))
        if result is None or result.empty or "calc_date" not in result.columns:
            return []
        return pd.to_datetime(result["calc_date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").tolist()

    @staticmethod
    def _coerce_date_string(value: Any) -> str:
        if isinstance(value, datetime):
            return value.date().strftime("%Y-%m-%d")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        return str(value)
