"""Conservative factor watermarks: observed changes are not consumed changes."""

from datetime import datetime
from typing import Any, Mapping

from .date_policy import FACTOR_TIMEZONE


WATERMARK_CONTRACT = "snapshot_consumed_v2"
SNAPSHOT_XMIN_KEY = "_snapshot_xmin"
STOCK_MASTER_PROJECTION = "eligibility_projection_v1:"
STOCK_MASTER_PROJECTION_SQL = """
    SELECT md5(COALESCE(string_agg(
        jsonb_build_array(ts_code,list_status,list_date,delist_date)::text,
        E'\\n' ORDER BY ts_code), '')) FROM tushare.stock_basic
"""


def _timestamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result if result.tzinfo is not None else result.replace(tzinfo=FACTOR_TIMEZONE)


def consumed_watermarks(task_result: Mapping[str, Any], planned: Mapping[str, Any], planned_xmin=None) -> dict:
    snapshot = task_result.get("source_snapshot") or {}
    outcomes = task_result.get("dates") or {}
    if (task_result.get("status") != "success" or not snapshot.get("consistent") or not outcomes
            or any(item.get("status") not in {"success", "expected_no_data"} for item in outcomes.values())):
        return {}
    actual = snapshot.get("watermarks") or {}
    snapshot_xmin = snapshot.get("xmin")
    if not isinstance(planned_xmin, int) or not isinstance(snapshot_xmin, int):
        return {}
    # The planner captures this ceiling before querying dirty dates. Revisions
    # arriving later may be visible to a calculator but not to all planned dates.
    if set(planned) != set(actual):
        return {}
    result = {}
    for source, value in planned.items():
        if isinstance(value, str) and value.startswith(STOCK_MASTER_PROJECTION):
            if value != actual[source]:
                return {}  # Eligibility changed after planning; do not certify it.
            result[source] = value
        else:
            result[source] = min((value, actual[source]), key=_timestamp) if value is not None and actual[source] is not None else None
    result[SNAPSHOT_XMIN_KEY] = min(planned_xmin, snapshot_xmin)
    return result
