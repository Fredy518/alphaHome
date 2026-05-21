from __future__ import annotations

import pandas as pd

from tinydata.cache import CacheManager, make_cache_key


def test_cache_key_is_stable_for_sorted_payload():
    left = make_cache_key("dataset", {"b": 2, "a": 1})
    right = make_cache_key("dataset", {"a": 1, "b": 2})
    assert left == right


def test_parquet_cache_roundtrip(tmp_path):
    manager = CacheManager(tmp_path)
    key = make_cache_key("sample", {"codes": ["SZ000001"]})
    df = pd.DataFrame({"a": [1], "b": ["x"]})

    path = manager.write("sample", key, df)
    assert path.exists()

    loaded = manager.read("sample", key)
    pd.testing.assert_frame_equal(loaded, df)
