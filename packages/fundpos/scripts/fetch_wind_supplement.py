"""Run with the local Python that already has WindPy installed, not an API key in source."""

import argparse
import json
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    from WindPy import w

    response = w.start(waitTime=10)
    if response.ErrorCode != 0:
        print(
            json.dumps(
                {"status": "unavailable", "stage": "start", "error_code": response.ErrorCode}
            )
        )
        return 2
    rows = []
    attempts = []
    for asset, ticker in [("bond", "CBA00601.CS"), ("hk", "930933.CSI")]:
        response = w.wsd(ticker, "close", args.start, args.end, "Days=Trading")
        attempts.append({"asset": asset, "ticker": ticker, "error_code": response.ErrorCode})
        if response.ErrorCode != 0:
            continue
        price = pd.Series(response.Data[0], index=pd.to_datetime(response.Times), dtype=float)
        returns = price.pct_change(fill_method=None)
        previous = pd.Series(price.index, index=price.index).shift(1)
        for day, ret in returns.items():
            if pd.notna(ret):
                rows.append(
                    {
                        "date": day,
                        "start_date": previous.loc[day],
                        "ann_date": day,
                        "asset": asset,
                        "return": ret,
                        "source": f"Wind.wsd:{ticker}:close",
                        "return_basis": "CNY_index_level_change",
                    }
                )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        pd.DataFrame(rows).to_parquet(output, index=False)
    print(
        json.dumps(
            {
                "status": "ok"
                if len(attempts) == 2 and all(x["error_code"] == 0 for x in attempts)
                else "partial",
                "rows": len(rows),
                "attempts": attempts,
            }
        )
    )
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
