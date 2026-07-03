from datetime import date

from alphahome.gui.handlers.pit_management_handler import _format_stock_diagnosis


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
