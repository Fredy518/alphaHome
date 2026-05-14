import json
import os
import subprocess
import sys


def test_hikyuu_data_adapter_import_does_not_initialize_hikyuu():
    script = r"""
import json
import os
import sys

import psutil

import alphahome.providers.tools.hikyuu_data_adapter  # noqa: F401

hikyuu_data_dir = os.path.abspath(os.environ.get("HIKYUU_DATA_DIR", "E:/stock"))
open_files = []
try:
    open_files = [
        os.path.abspath(item.path)
        for item in psutil.Process().open_files()
    ]
except Exception:
    open_files = []

print(json.dumps({
    "hikyuu_modules": [
        name for name in sys.modules
        if name == "hikyuu" or name.startswith("hikyuu.")
    ],
    "hikyuu_data_files": [
        path for path in open_files
        if path.startswith(hikyuu_data_dir)
        and path.lower().endswith((".h5", ".hdf5", ".db"))
    ],
}, ensure_ascii=False))
"""

    env = os.environ.copy()
    env.setdefault("HIKYUU_DATA_DIR", "E:/stock")
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["hikyuu_modules"] == []
    assert payload["hikyuu_data_files"] == []
