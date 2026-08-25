from datetime import date

from alphahome.gui.handlers import pit_management_handler
from alphahome.gui.handlers.pit_management_handler import _format_stock_diagnosis, _format_task_detail


def test_format_stock_diagnosis_includes_expected_gap_reason():
    diagnosis = {
        "status": "success",
        "ts_code": "002549.SZ",
        "tasks": [
            {
                "task_name": "pit_cashflow_quarterly",
                "status": "ok",
                "missing_expected_periods": [date(2022, 6, 30)],
                "gap_diagnosis": [
                    {
                        "period": date(2022, 6, 30),
                        "reason": "source_missing",
                        "source_checks": [
                            {
                                "source_table": "tushare.fina_cashflow",
                                "status": "ok",
                                "raw_rows": 0,
                                "eligible_rows": 0,
                                "valid_rows": 0,
                            },
                            {
                                "source_table": "rawdata.fina_cashflow",
                                "status": "ok",
                                "raw_rows": 0,
                                "eligible_rows": 0,
                                "valid_rows": 0,
                            },
                        ],
                    }
                ],
                "raw_missing_in_pit": [],
                "pit_periods": [date(2022, 9, 30), date(2022, 3, 31)],
            }
        ],
    }

    text = _format_stock_diagnosis(diagnosis)

    assert "三表预期缺口: 2022-06-30" in text
    assert "2022-06-30: 源表无该报告期" in text
    assert "tushare.fina_cashflow: raw=0, eligible=0, valid=0" in text
    assert "rawdata.fina_cashflow: raw=0, eligible=0, valid=0" in text


def test_selected_pit_tasks_include_dependencies(monkeypatch):
    monkeypatch.setattr(
        pit_management_handler,
        "_full_pit_task_list",
        [
            {
                "name": "pit_financial_indicators",
                "description": "PIT财务指标",
                "dependencies": ["pit_income_quarterly", "pit_balance_quarterly"],
                "selected": True,
            }
        ],
    )

    selected = pit_management_handler.get_selected_pit_tasks()

    assert selected == [
        {
            "task_name": "pit_financial_indicators",
            "task_type": "pit",
            "description": "PIT财务指标",
            "data_source": "pit",
            "dependencies": ["pit_income_quarterly", "pit_balance_quarterly"],
        }
    ]


def test_format_task_detail_separates_live_execution_and_audit_values():
    text = _format_task_detail(
        {
            "name": "pit_income_quarterly",
            "latest_date": date(2026, 8, 12),
            "row_count": 468456,
            "coverage_rate": 0.99,
            "gap_count": 10,
            "live_status": "healthy",
            "last_execution_status": "success",
            "last_execution_time": "2026-08-14 14:16:08",
            "last_execution_details": "处理完成 (行数: 152)",
            "audit_status": "healthy",
            "last_audit_time": "2026-07-03 13:39:29",
            "audited_latest_date": date(2026, 7, 2),
            "audited_row_count": 468304,
            "audited_coverage_rate": 0.98,
            "audited_gap_count": 20,
        }
    )

    assert "当前实时表状态" in text
    assert "最新日期: 2026-08-12" in text
    assert "执行时间: 2026-08-14 14:16:08" in text
    assert "执行详情: 处理完成 (行数: 152)" in text
    assert "最近审计快照" in text
    assert "审计时间: 2026-07-03 13:39:29" in text
    assert "审计时最新日期: 2026-07-02" in text
