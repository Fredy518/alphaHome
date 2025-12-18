"""
DolphinDB integration (lightweight, single-node).

Primary use-case in AlphaHome:
- Store/query 5-minute K-line data in DolphinDB (dfs partitioned table)
- Provide a compute-accelerator layer for technical factors
"""

from .manager import DolphinDBManager
from .hikyuu_5min_importer import HikyuuKline5MinImporter, Hikyuu5MinImporterConfig
from .schema import build_kline_5min_init_script

__all__ = [
    "DolphinDBManager",
    "HikyuuKline5MinImporter",
    "Hikyuu5MinImporterConfig",
    "build_kline_5min_init_script",
]

