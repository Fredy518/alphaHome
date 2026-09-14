from alphahome.gui.handlers import factor_management_handler


def test_factor_selection_state_is_independent_and_contains_dependencies(monkeypatch):
    monkeypatch.setattr(
        factor_management_handler,
        "_full_factor_task_list",
        [
            {
                "name": "factor_g",
                "description": "G因子",
                "dependencies": ["factor_p"],
                "selected": True,
            }
        ],
    )

    assert factor_management_handler.get_selected_factor_tasks() == [
        {
            "task_name": "factor_g",
            "task_type": "factor",
            "description": "G因子",
            "data_source": "factors",
            "dependencies": ["factor_p"],
            "task_config": {"factor_expand_dependencies": False},
        }
    ]


def test_factor_detail_separates_execution_and_audit_times():
    text = factor_management_handler._format_task_detail(
        {
            "name": "factor_p",
            "actual_latest_date": "2026-09-11",
            "expected_latest_date": "2026-09-11",
            "last_execution_status": "success",
            "last_execution_time": "2026-09-12 08:00:00",
            "last_audit_time": "2026-09-12 08:30:00",
            "audited_latest_date": "2026-09-11",
        }
    )

    assert "最近执行" in text
    assert "时间: 2026-09-12 08:00:00" in text
    assert "最近审计" in text
    assert "时间: 2026-09-12 08:30:00" in text


def test_preflight_format_exposes_dependency_expansion_and_blockers():
    text = factor_management_handler._format_preflight(
        {
            "status": "blocked_source",
            "mode": "smart",
            "effective_cutoff_date": "2026-09-11",
            "task_names": ["factor_p", "factor_g"],
            "total_dates": 4,
            "message": "missing_relation:pit.pit_financial_indicators",
            "task_plans": [
                {
                    "task_name": "factor_p",
                    "dates": ["2026-09-04", "2026-09-11"],
                    "blockers": ["missing_relation:pit.pit_financial_indicators"],
                }
            ],
        }
    )

    assert "factor_p, factor_g" in text
    assert "missing_relation:pit.pit_financial_indicators" in text
