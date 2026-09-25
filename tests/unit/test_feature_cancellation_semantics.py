from alphahome.features.coordinator import _summarize_feature_results
import pytest

from alphahome.features.coordinator import build_feature_plan


def test_feature_cancellation_is_not_counted_as_failure():
    result = _summarize_feature_results(
        "plan-hash",
        3,
        {
            "completed": {"status": "success"},
            "cancelled_a": {"status": "cancelled"},
            "cancelled_b": {"status": "cancelled"},
        },
    )

    assert result["status"] == "cancelled"
    assert result["success_count"] == 1
    assert result["fail_count"] == 0
    assert result["cancelled_count"] == 2


def test_feature_cancellation_preserves_real_errors_separately():
    result = _summarize_feature_results(
        "plan-hash",
        3,
        {
            "completed": {"status": "success"},
            "failed": {"status": "error"},
            "cancelled": {"status": "cancelled"},
        },
    )

    assert result["status"] == "cancelled"
    assert result["fail_count"] == 1
    assert result["cancelled_count"] == 1


@pytest.mark.parametrize("timeout_ms", [0, 600001])
def test_feature_inspection_budget_is_bounded_before_database_access(timeout_ms):
    with pytest.raises(ValueError, match="1-600 seconds"):
        build_feature_plan(
            "postgresql://unused@127.0.0.1:55439/alphahome_test_unused",
            ["stock_daily_enriched"],
            inspection_timeout_ms=timeout_ms,
        )
