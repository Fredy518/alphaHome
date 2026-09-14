"""Small immutable contracts shared by domain coordinators, without an executor."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo


def canonical_json(value: Any) -> str:
    def encode(item):
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        raise TypeError(f"Unsupported plan value: {type(item).__name__}")
    return json.dumps(value, default=encode, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def target_fingerprint(connection_string: str) -> str:
    """Identify the explicit server/database/role without retaining credentials."""
    from psycopg2.extensions import parse_dsn

    try:
        parsed = parse_dsn(connection_string)
    except Exception:
        raise ValueError("Invalid explicit database target") from None
    if any(not parsed.get(key) for key in ("host", "port", "dbname", "user")) or parsed.get("service"):
        raise ValueError("Host, port, database and role must be explicit")
    return fingerprint({
        "host": parsed["host"].lower(), "port": str(parsed.get("port", "5432")),
        "database": parsed["dbname"], "role": parsed.get("user", ""),
        "hostaddr": parsed.get("hostaddr", ""),
    })


@dataclass(frozen=True)
class RunRequest:
    domain: str
    tasks: tuple[str, ...]
    mode: str
    target_fingerprint: str
    start_date: date | None = None
    end_date: date | None = None
    timezone: str = "Asia/Shanghai"

    def __post_init__(self):
        if self.domain not in {"fetchers", "pit", "factors", "features"}:
            raise ValueError("Unknown execution domain")
        if not self.tasks or any(not isinstance(name, str) or not name for name in self.tasks):
            raise ValueError("At least one named task is required")
        object.__setattr__(self, "tasks", tuple(sorted(set(self.tasks))))
        if not self.mode:
            raise ValueError("An explicit domain mode is required")
        if not re.fullmatch(r"[0-9a-f]{64}", self.target_fingerprint):
            raise ValueError("A database target fingerprint is required")
        ZoneInfo(self.timezone)
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")


@dataclass(frozen=True)
class RunUnit:
    task_name: str
    dates: tuple[date, ...] = ()
    dependencies: tuple[str, ...] = ()
    estimated_rows: int | None = None
    existing_rows_to_replace: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    parameters_json: str = "{}"

    def __post_init__(self):
        object.__setattr__(self, "dates", tuple(sorted(set(self.dates))))
        object.__setattr__(self, "dependencies", tuple(sorted(set(self.dependencies))))
        if any(value is not None and value < 0 for value in (self.estimated_rows, self.existing_rows_to_replace)):
            raise ValueError("Plan row counts cannot be negative")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("Invalid unit date range")
        object.__setattr__(self, "parameters_json", canonical_json(json.loads(self.parameters_json)))


@dataclass(frozen=True)
class RunPlan:
    request: RunRequest
    units: tuple[RunUnit, ...]
    effective_cutoff: date
    schema_fingerprint: str
    source_fingerprint: str
    config_fingerprint: str
    blockers: tuple[str, ...] = ()
    contract_version: str = "domain_run_v1"

    def __post_init__(self):
        object.__setattr__(self, "units", tuple(self.units))
        object.__setattr__(self, "blockers", tuple(sorted(set(self.blockers))))
        seen = set()
        for unit in self.units:
            if unit.task_name in seen or not set(unit.dependencies) <= seen:
                raise ValueError("Plan units must be unique and ordered after their dependencies")
            if any(value > self.effective_cutoff for value in unit.dates):
                raise ValueError("Planned date exceeds the frozen cutoff")
            seen.add(unit.task_name)
        if not set(self.request.tasks) <= seen and not self.blockers:
            raise ValueError("Plan omits requested tasks")

    @classmethod
    def build(cls, request, units, effective_cutoff, *, schema, sources, config, blockers=()):
        return cls(request, tuple(units), effective_cutoff, fingerprint(schema), fingerprint(sources), fingerprint(config), tuple(blockers))

    @property
    def plan_hash(self):
        return fingerprint(asdict(self))

    def to_dict(self):
        return {**json.loads(canonical_json(asdict(self))), "plan_hash": self.plan_hash}

    @classmethod
    def from_dict(cls, payload):
        data = dict(payload)
        expected_hash = data.pop("plan_hash", None)
        request = dict(data.pop("request"))
        for key in ("start_date", "end_date"):
            if request.get(key):
                request[key] = date.fromisoformat(request[key])
        units = []
        for item in data.pop("units"):
            unit = dict(item)
            unit["dates"] = tuple(date.fromisoformat(value) for value in unit.get("dates", ()))
            for key in ("start_date", "end_date"):
                if unit.get(key):
                    unit[key] = date.fromisoformat(unit[key])
            units.append(RunUnit(**unit))
        data["effective_cutoff"] = date.fromisoformat(data["effective_cutoff"])
        plan = cls(request=RunRequest(**request), units=tuple(units), **data)
        if expected_hash is not None and expected_hash != plan.plan_hash:
            raise ValueError("Serialized plan hash does not match its contents")
        return plan

    def require_matching(self, expected_hash: str) -> None:
        if expected_hash != self.plan_hash:
            raise RuntimeError("Execution plan changed; refresh the preview before executing")
        if self.blockers:
            raise RuntimeError("Execution plan is blocked: " + "; ".join(self.blockers))


@dataclass(frozen=True)
class SourceBoundary:
    """Observed snapshots and certified consumption remain distinct values."""

    target_fingerprint: str
    observed_at: datetime
    snapshot_xmin: int | None
    watermark_fingerprint: str
    isolation: str
    consumed: bool = False
    scope: tuple[str, ...] = ()
    change_detection: str = "timestamp_and_mvcc_insert_update"

    def __post_init__(self):
        object.__setattr__(self, "scope", tuple(self.scope))
        if self.observed_at.tzinfo is None:
            raise ValueError("Source observation time must include a timezone")
        if self.consumed and (self.isolation != "repeatable_read" or not self.scope or self.snapshot_xmin is None):
            raise ValueError("Consumption requires a consistent snapshot and a completed scope")


@dataclass(frozen=True)
class RunResult:
    status: str
    attempted_rows: int | None = None
    committed_rows: int | None = None
    error_count: int = 0
    run_id: str | None = None
    plan_hash: str | None = None
    error_code: str | None = None

    def __post_init__(self):
        if self.status not in {"success", "partial_success", "error", "cancelled", "blocked", "no_op", "expected_no_data", "dry_run"}:
            raise ValueError("Unknown terminal run status")
        if any(value is not None and value < 0 for value in (self.attempted_rows, self.committed_rows, self.error_count)):
            raise ValueError("Run counts cannot be negative")
        if self.status == "success" and (self.error_count or self.error_code):
            raise ValueError("A run with errors cannot be successful")
        if self.attempted_rows is not None and self.committed_rows is not None and self.committed_rows > self.attempted_rows:
            raise ValueError("Committed rows exceed attempted rows")

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)
