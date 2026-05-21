"""Fetcher task package.

Concrete task modules register themselves via decorators when imported.  Importing
this package stays cheap; callers that need the full registry should call
``discover_tasks()`` explicitly.
"""

from __future__ import annotations

import importlib
import pkgutil

_DISCOVERED = False


def discover_tasks(*, force_reload: bool = False) -> None:
    """Import all concrete task modules to populate the task registry."""
    global _DISCOVERED
    if _DISCOVERED and not force_reload:
        return

    for module_info in pkgutil.walk_packages(__path__, prefix=f"{__name__}."):
        if module_info.ispkg:
            continue
        importlib.import_module(module_info.name)

    _DISCOVERED = True


__all__ = ["discover_tasks"]
