import copy
from pathlib import Path

import pytest

from fundpos.config import Settings
from fundpos.data import DataBundle
from fundpos.demo import synthetic_bundle


@pytest.fixture(scope="session")
def base_bundle():
    return synthetic_bundle("2022-01-03", "2023-09-29", n_funds=3)


@pytest.fixture
def bundle(base_bundle):
    original, _ = base_bundle
    return DataBundle(
        {name: frame.copy(deep=True) for name, frame in original.frames.items()},
        copy.deepcopy(original.provenance),
    )


@pytest.fixture
def settings(tmp_path):
    root = Path(__file__).resolve().parents[1]
    values = copy.deepcopy(Settings.load(root / "config/default.toml").values)
    values["project"].update(data_dir=str(tmp_path / "data"), output_dir=str(tmp_path / "outputs"))
    return Settings(root, values)
