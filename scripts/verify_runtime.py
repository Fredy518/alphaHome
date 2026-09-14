"""Offline compilation and import gate for the interpreter used in production."""

import importlib
import json
from pathlib import Path
import subprocess
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    files = subprocess.check_output(["git", "ls-files", "-z", "alphahome", "scripts"], cwd=root).decode("utf-8").split("\0")
    paths = [path for path in files if path.endswith(".py") and "/archive/" not in path and "/__pycache__/" not in path]
    for path in paths:
        compile((root / path).read_text(encoding="utf-8-sig"), path, "exec")
    modules = ["alphahome.common.db_manager", "alphahome.pit.run_plan", "alphahome.factors.coordinator",
               "alphahome.features.coordinator", "alphahome.gui.services.pit_service",
               "alphahome.gui.services.factor_service", "alphahome.gui.services.feature_service"]
    for module in modules:
        importlib.import_module(module)
    if "alphahome.gui.main_window" in sys.modules:
        raise RuntimeError("Service imports unexpectedly load the GUI main window")
    # Explicit GUI entrypoint import is checked separately from service imports.
    importlib.import_module("alphahome.gui.main_window")
    print(json.dumps({"status": "success", "python": sys.version.split()[0], "executable": sys.executable,
                      "compiled_files": len(paths), "imports": modules, "gui_import": "success"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
