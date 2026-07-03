"""PIT task discovery package."""

from __future__ import annotations

import importlib
import pkgutil

_DISCOVERED = False


def discover_tasks(*, force_reload: bool = False) -> None:
    """Import concrete PIT task modules to populate UnifiedTaskFactory."""
    global _DISCOVERED
    if _DISCOVERED and not force_reload:
        return

    for module_info in pkgutil.walk_packages(__path__, prefix=f"{__name__}."):
        if module_info.ispkg:
            continue
        importlib.import_module(module_info.name)

    _DISCOVERED = True


__all__ = ["discover_tasks"]

