from __future__ import annotations

from tinydata.codes import normalize_codes, tinysoft_symbol_to_ts_code, ts_code_to_tinysoft_symbol


def test_code_conversion():
    assert ts_code_to_tinysoft_symbol("000001.SZ") == "SZ000001"
    assert ts_code_to_tinysoft_symbol("000001.OF") == "OF000001"
    assert tinysoft_symbol_to_ts_code("SH600000") == "600000.SH"


def test_normalize_codes_deduplicates_and_splits_strings():
    assert normalize_codes("000001.SZ, 600000.SH 000001.SZ") == ["SZ000001", "SH600000"]
