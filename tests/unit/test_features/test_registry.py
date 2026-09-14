import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from alphahome.features import registry as module
from alphahome.features.registry import DuplicateRecipeError, FeatureRegistry, InvalidRecipeError


@pytest.fixture(autouse=True)
def isolated_registry():
    before, discovered = FeatureRegistry.get_all(), FeatureRegistry._discovered
    yield
    FeatureRegistry._recipes = before
    FeatureRegistry._discovered = discovered
    FeatureRegistry._pending = None
    FeatureRegistry._discovery_owner = None


def test_force_discovery_rebuilds_cached_modules_without_losing_recipes():
    FeatureRegistry.discover()
    before = FeatureRegistry.get_all()
    assert before
    for _ in range(3):
        FeatureRegistry.discover(force_reload=True)
        assert FeatureRegistry.get_all() == before
    FeatureRegistry.reset()
    FeatureRegistry.discover()
    assert FeatureRegistry.get_all() == before


def test_failed_module_import_preserves_last_complete_registry(monkeypatch):
    FeatureRegistry.discover()
    before = FeatureRegistry.get_all()
    failing = next(iter(before.values())).__module__
    original = module.importlib.import_module

    def import_module(name):
        if name == failing:
            raise ImportError("fixture import failure")
        return original(name)

    monkeypatch.setattr(module.importlib, "import_module", import_module)
    with pytest.raises(ImportError, match="fixture"):
        FeatureRegistry.discover(force_reload=True)
    assert FeatureRegistry.get_all() == before
    assert FeatureRegistry._discovered


@pytest.mark.parametrize("invalid", [False, True])
def test_invalid_or_duplicate_declarations_do_not_publish_partial_discovery(monkeypatch, invalid):
    FeatureRegistry.discover()
    before = FeatureRegistry.get_all()
    original = next(iter(before.values()))
    fake = type("InvalidFeature", (), {
        "__module__": original.__module__, "_feature_registered_recipe": True,
        "name": "invalid_fixture" if invalid else original.name,
        "description": "fixture", "source_tables": [] if invalid else ["rawdata.fixture"],
    })
    monkeypatch.setattr(sys.modules[original.__module__], "fixture_declaration", fake, raising=False)
    with pytest.raises(InvalidRecipeError if invalid else DuplicateRecipeError):
        FeatureRegistry.discover(force_reload=True)
    assert FeatureRegistry.get_all() == before


def test_external_registration_survives_force_discovery_and_duplicates_still_fail():
    extension = type("Extension", (), {"__module__": "extension.fixture", "name": "extension_fixture",
                                      "description": "fixture", "source_tables": ["rawdata.fixture"]})
    FeatureRegistry.register(extension)
    with pytest.raises(DuplicateRecipeError):
        FeatureRegistry.register(extension)
    FeatureRegistry.discover(force_reload=True)
    assert FeatureRegistry.get(extension.name) is extension


def test_discovery_does_not_hold_registry_lock_while_waiting_for_import(monkeypatch):
    FeatureRegistry.discover()
    before = FeatureRegistry.get_all()
    target = next(iter(before.values())).__module__
    entered, release = threading.Event(), threading.Event()
    original = module.importlib.import_module

    def import_module(name):
        if name == target:
            entered.set()
            assert release.wait(5)
        return original(name)

    extension = type("ConcurrentExtension", (), {"__module__": "extension.concurrent", "name": "concurrent_extension",
                                                "description": "fixture", "source_tables": ["rawdata.fixture"]})
    monkeypatch.setattr(module.importlib, "import_module", import_module)
    with ThreadPoolExecutor(2) as pool:
        discovery = pool.submit(FeatureRegistry.discover, True)
        assert entered.wait(3)
        registration = pool.submit(FeatureRegistry.register, extension)
        try:
            registration.result(timeout=2)
            assert FeatureRegistry.get_all() == {**before, extension.name: extension}
        finally:
            release.set()
        discovery.result(timeout=3)
    assert FeatureRegistry.get_all() == {**before, extension.name: extension}
