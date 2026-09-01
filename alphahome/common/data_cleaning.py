"""Shared value cleaning used before database persistence."""

from typing import Any


def normalize_database_string(value: Any) -> Any:
    """Match the text normalization applied by the asyncpg COPY path.

    Keeping this function shared prevents primary-key deduplication from using
    different values than the database writer eventually persists.
    Non-string values are returned unchanged.
    """

    if not isinstance(value, str):
        return value

    cleaned = (
        value.replace("\x00", "").replace("\r", "").replace("\n", "").replace("\t", " ")
    )
    return cleaned if cleaned else None
