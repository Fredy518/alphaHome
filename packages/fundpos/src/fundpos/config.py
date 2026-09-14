from __future__ import annotations

import copy
import hashlib
import json
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root: Path
    values: dict

    @classmethod
    def load(cls, path: str | Path = "config/default.toml") -> Settings:
        path = Path(path).resolve()
        with path.open("rb") as stream:
            values = tomllib.load(stream)
        root = path.parent.parent if path.parent.name == "config" else path.parent
        model = values["model"]
        if model["window"] < 35 or model["constraint_tolerance"] > 1e-6:
            raise ValueError("window must be >=35; constraint_tolerance must be <=1e-6")
        if model["prior_penalty"] < 0 or model["smooth_penalty"] < 0:
            raise ValueError("penalties must be nonnegative")
        return cls(root, values)

    def path(self, key: str) -> Path:
        return self.root / self.values["project"][key]

    def with_model(self, **updates) -> Settings:
        values = copy.deepcopy(self.values)
        values["model"].update(updates)
        return Settings(self.root, values)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.values, sort_keys=True).encode()).hexdigest()
