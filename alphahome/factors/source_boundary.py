"""Conservative factor watermarks: observed changes are not consumed changes."""

from datetime import datetime
from typing import Any, Mapping

from .date_policy import FACTOR_TIMEZONE


WATERMARK_CONTRACT = "snapshot_consumed_v1"


def _timestamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result if result.tzinfo is not None else result.replace(tzinfo=FACTOR_TIMEZONE)


def consumed_watermarks(task_result: Mapping[str, Any], planned: Mapping[str, Any]) -> dict:
    snapshot = task_result.get("source_snapshot") or {}
    outcomes = task_result.get("dates") or {}
    if (task_result.get("status") != "success" or not snapshot.get("consistent") or not outcomes
            or any(item.get("status") not in {"success", "expected_no_data"} for item in outcomes.values())):
        return {}
    actual = snapshot.get("watermarks") or {}
    # The planner captures this ceiling before querying dirty dates. Revisions
    # arriving later may be visible to a calculator but not to all planned dates.
    return {
        source: min((value, actual[source]), key=_timestamp)
        for source, value in planned.items() if value is not None and actual.get(source) is not None
    }
