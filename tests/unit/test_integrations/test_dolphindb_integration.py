import pandas as pd

from alphahome.integrations.dolphindb.manager import DolphinDBManager
from alphahome.integrations.dolphindb.schema import (
    build_drop_database_script,
    build_kline_5min_init_script,
)
from alphahome.integrations.dolphindb.hikyuu_5min_importer import (
    apply_incremental_filter,
    market_5min_h5_path,
    normalize_hikyuu_5min_records,
    ts_code_to_hikyuu_symbol,
)


def test_build_kline_5min_init_script_smoke():
    script = build_kline_5min_init_script(
        db_path="dfs://kline_5min",
        table_name="kline_5min",
        start_month=202001,
        end_month=202012,
        hash_buckets=10,
    )
    assert 'alphahome_init_kline_5min("dfs://kline_5min", "kline_5min", 202001, 202012, 10)' in script
    assert "createPartitionedTable" in script
    assert "`month`ts_code" in script
    assert "keepDuplicates" not in script


def test_build_drop_database_script_smoke():
    s = build_drop_database_script(db_path="dfs://kline_5min")
    assert 'dropDatabase("dfs://kline_5min")' in s


def test_build_kline_5min_init_script_validates_table_name():
    try:
        build_kline_5min_init_script(table_name="bad-name")
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_market_5min_h5_path():
    p = market_5min_h5_path("E:/stock", "sh")
    assert p.replace("\\", "/").endswith("/sh_5min.h5")


def test_ts_code_to_hikyuu_symbol():
    assert ts_code_to_hikyuu_symbol("000001.SZ") == "SZ000001"
    assert ts_code_to_hikyuu_symbol("600000.SH") == "SH600000"


def test_normalize_hikyuu_5min_records_basic():
    import numpy as np

    dtype = np.dtype(
        [
            ("closePrice", "<u4"),
            ("datetime", "<u8"),
            ("highPrice", "<u4"),
            ("lowPrice", "<u4"),
            ("openPrice", "<u4"),
            ("transAmount", "<u8"),
            ("transCount", "<u8"),
        ]
    )
    records = np.array(
        [
            (1050, 202401020935, 1100, 900, 1000, 10000, 200),
            (2050, 202401020940, 2100, 1900, 2000, 20000, 300),
        ],
        dtype=dtype,
    )

    out = normalize_hikyuu_5min_records(records, ts_code="000001.SZ", price_scale=1000.0, amount_scale=10.0)
    assert list(out.columns) == [
        "ts_code",
        "trade_time",
        "month",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
    ]
    assert out["ts_code"].nunique() == 1
    assert int(out["month"].iloc[0]) == 202401
    assert pd.api.types.is_datetime64_any_dtype(out["trade_time"])


def test_apply_incremental_filter_strictly_after():
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_time": [pd.Timestamp("2024-01-02 09:35:00"), pd.Timestamp("2024-01-02 09:40:00")],
            "month": [202401, 202401],
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "vol": [1, 1],
            "amount": [1.0, 1.0],
        }
    )
    out = apply_incremental_filter(df, after=pd.Timestamp("2024-01-02 09:35:00"))
    assert len(out) == 1
    assert out.iloc[0]["trade_time"] == pd.Timestamp("2024-01-02 09:40:00")


def test_dolphindb_manager_get_max_trade_time_parses_dataframe():
    class FakeSession:
        def connect(self, *args):
            return True

        def run(self, script):
            if script == "1":
                return 1
            if "max(trade_time)" in script:
                return pd.DataFrame({"max_tt": [pd.Timestamp("2024-01-02 09:35:00")]})
            return 0

        def close(self):
            return None

    mgr = DolphinDBManager(session_factory=FakeSession)
    mgr.connect()
    try:
        ts = mgr.get_max_trade_time(db_path="dfs://kline_5min", table_name="kline_5min", ts_code="000001.SZ")
        assert ts == pd.Timestamp("2024-01-02 09:35:00")
    finally:
        mgr.close()


def test_dolphindb_manager_append_dataframe_uses_upload_and_table_insert():
    calls = {"connect": [], "upload": [], "run": [], "close": 0}

    class FakeSession:
        def connect(self, *args):
            calls["connect"].append(args)
            return True

        def upload(self, obj):
            calls["upload"].append(obj)

        def run(self, script):
            calls["run"].append(script)
            if script == "1":
                return 1
            return 123

        def close(self):
            calls["close"] += 1

    mgr = DolphinDBManager(session_factory=FakeSession)
    assert mgr.connect() is True

    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_time": [pd.Timestamp("2024-01-02 09:35:00")],
            "month": [202401],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "vol": [100],
            "amount": [1000.0],
        }
    )
    inserted = mgr.append_dataframe(df, db_path="dfs://kline_5min", table_name="kline_5min")
    assert inserted == 123
    assert calls["upload"]
    assert any('loadTable("dfs://kline_5min", "kline_5min")' in s for s in calls["run"])
