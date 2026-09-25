"""Resolve the local input file required by an Excel collection task."""

from pathlib import Path
from typing import Any


def missing_excel_input_reason(task: Any) -> str | None:
    """Return a skip reason only when a configured Excel input is absent."""
    if getattr(task, "data_source", None) != "excel":
        return None

    path = getattr(task, "workbook_path", None)
    if path is None:
        path = getattr(task, "excel_file_path", None)
    if path is None:
        return None

    input_path = Path(path)
    if not input_path.exists():
        return f"Excel 来源文件不存在，已跳过: {input_path}"
    return None
