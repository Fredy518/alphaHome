# 数据源模块

# AkShare 数据源
from .akshare import (
    AkShareAPI,
    AkShareAPIError,
    AkShareRateLimitError,
    AkShareTask,
    AkShareSingleBatchTask,
    AkShareDataTransformer,
)
from .tinysoft import (
    TinySoftAPI,
    TinySoftAPIError,
    TinySoftAuthError,
    TinySoftDependencyError,
    TinySoftTask,
)

__all__ = [
    # AkShare
    "AkShareAPI",
    "AkShareAPIError",
    "AkShareRateLimitError",
    "AkShareTask",
    "AkShareSingleBatchTask",
    "AkShareDataTransformer",
    # Tinysoft
    "TinySoftAPI",
    "TinySoftAPIError",
    "TinySoftAuthError",
    "TinySoftDependencyError",
    "TinySoftTask",
]
