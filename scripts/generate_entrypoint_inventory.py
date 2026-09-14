"""Generate the task/recipe inventory without configuration or database initialization."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def inventory():
    from alphahome.fetchers.tasks import discover_tasks as fetch
    from alphahome.pit.tasks import discover_tasks as pit
    from alphahome.factors.tasks import discover_tasks as factors
    from alphahome.common.task_system import UnifiedTaskFactory
    from alphahome.features import FeatureRegistry
    fetch()
    pit()
    factors()
    tasks = []
    for name, cls in sorted(UnifiedTaskFactory._task_registry.items()):
        contract = getattr(cls, "contract", None)
        tasks.append({"name": name, "module": cls.__module__, "type": cls.task_type,
                      "table": getattr(cls, "table_name", None), "source": getattr(cls, "data_source", None),
                      "contract": contract.to_dict() if hasattr(contract, "to_dict") else None})
    features = [{"name": cls.name, "module": cls.__module__, "target": cls().full_name,
                 "sources": cls.source_tables, "strategies": cls.supported_strategies,
                 "primary_keys": getattr(cls, "primary_keys", ())}
                for cls in FeatureRegistry.discover()]
    return {"task_counts": dict(sorted(Counter(item["type"] for item in tasks).items())),
            "feature_count": len(features), "tasks": tasks, "features": features}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, help="Write JSON to this explicit file; otherwise stdout")
    args = parser.parse_args()
    content = json.dumps(inventory(), ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(content, encoding="utf-8")
    else:
        print(content, end="")


if __name__ == "__main__":
    main()
