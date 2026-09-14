"""Evidence dimensions are separate from execution status and audit history."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AuditDimensions:
    structure: str = "unknown"
    dates: str = "unknown"
    source_consumption: str = "unverified"
    coverage: str = "unknown"
    eligibility: str = "unknown"

    @property
    def healthy(self):
        return all(value in {"ready", "complete", "current", "qualified", "expected_no_data"} for value in asdict(self).values())

    def to_dict(self):
        return asdict(self)
