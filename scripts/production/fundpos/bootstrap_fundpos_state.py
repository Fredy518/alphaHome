"""Initialize AlphaHome-owned fundpos state from the frozen package seed."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from alphahome.integrations.fundpos import bootstrap_fundpos_state


def main() -> int:
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-root", type=Path, default=root / "packages" / "fundpos")
    parser.add_argument("--state-root", type=Path, default=root / "logs" / "fundpos-engine")
    args = parser.parse_args()
    result = bootstrap_fundpos_state(args.engine_root, args.state_root)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
