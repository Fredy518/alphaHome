from __future__ import annotations

import pytest

from tinydata.errors import TinyDataCodePoolError
from tinydata.infotable import build_infotable_query, build_where_clause, format_select_fields, format_stock_selector


def test_format_stock_selector_single_and_array():
    assert format_stock_selector(["000001.SZ"]) == "'SZ000001'"
    assert format_stock_selector(["000001.SZ", "600000.SH"]) == "array('SZ000001','SH600000')"


def test_build_where_clause_dates():
    clause = build_where_clause(start_date="20230101", end_date="20231231", date_field="截止日")
    assert clause == '["截止日"]>=20230101 and ["截止日"]<=20231231'


def test_format_select_fields_quotes_plain_names_with_parentheses():
    assert format_select_fields(["StockID", "占净值比例(%)"]) == '["StockID"], ["占净值比例(%)"]'


def test_build_infotable_query_with_codes_and_fields():
    query = build_infotable_query(
        349,
        codes=["012345.OF", "000001.OF"],
        fields=["截止日", "代码"],
        where_clause='["截止日"]>=20231231',
    )
    assert query == (
        "return select [\"截止日\"], [\"代码\"] from infotable 349 "
        "of array('OF012345','OF000001') where [\"截止日\"]>=20231231 end;"
    )


def test_build_infotable_query_full_table_requires_flag():
    with pytest.raises(TinyDataCodePoolError):
        build_infotable_query(10)
    assert build_infotable_query(10, allow_full_table=True) == "return select * from infotable 10 end;"
