from datetime import datetime

import pytest
from openpyxl import Workbook

from alphahome.fetchers.tasks.macro.excel_macro_dr007_history import (
    CALCULATION_VERSION,
    SOURCE_SERIES_ID,
    load_dr007_workbook_cache,
)


def _write_workbook(path, *, series_id=SOURCE_SERIES_ID):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "DateRate"
    sheet.append(["单位", None, None, None, None, None, None, "%"])
    sheet.append(["指标名称", None, None, None, None, None, None, "DR007"])
    sheet.append(["频率", None, None, None, None, None, None, "日"])
    sheet.append(["指标ID", None, None, None, None, None, None, series_id])
    sheet.append([datetime(2022, 1, 7), None, None, None, None, None, None, 2.01])
    sheet.append([datetime(2022, 1, 8), None, None, None, None, None, None, 2.02])
    sheet.append([datetime(2022, 1, 9), None, None, None, None, None, None, None])
    sheet.append([datetime(2022, 1, 10), None, None, None, None, None, None, 1.98])
    workbook.save(path)


def test_load_dr007_workbook_cache_keeps_provenance(tmp_path):
    path = tmp_path / "macro.xlsx"
    _write_workbook(path)

    frame = load_dr007_workbook_cache(path, "2022-01-07", "2022-01-08")

    assert frame["trade_date"].astype(str).tolist() == ["2022-01-07", "2022-01-08"]
    assert frame["dr007_pct"].tolist() == [2.01, 2.02]
    assert frame["source_cell"].tolist() == ["H5", "H6"]
    assert frame["is_weekend"].tolist() == [False, True]
    assert frame["source_workbook_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert set(frame["calculation_version"]) == {CALCULATION_VERSION}


def test_load_dr007_workbook_cache_rejects_wrong_series_id(tmp_path):
    path = tmp_path / "macro.xlsx"
    _write_workbook(path, series_id="WRONG")

    with pytest.raises(ValueError, match="元数据不符"):
        load_dr007_workbook_cache(path)


def test_load_dr007_workbook_cache_checks_expected_hash(tmp_path):
    path = tmp_path / "macro.xlsx"
    _write_workbook(path)

    with pytest.raises(ValueError, match="哈希不匹配"):
        load_dr007_workbook_cache(path, expected_workbook_sha256="0" * 64)
