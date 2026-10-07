#!/usr/bin/env python
"""Cache public fund profiles and issuer disclosure PDFs for classification evidence.

Reads a frozen inventory JSON; never writes AlphaDB. Public disclosure documents
remain evidence data, not instructions to the classification model.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader


def compact(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def fetch_profile(session: requests.Session, code: str) -> dict:
    url = f"https://fundf10.eastmoney.com/jbgk_{code}.html"
    response = session.get(url, timeout=35)
    response.raise_for_status()
    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "lxml")
    fields = {}
    for row in soup.select("table tr"):
        cells = row.find_all(["th", "td"], recursive=False)
        for i in range(0, len(cells) - 1, 2):
            key = cells[i].get_text(strip=True)
            if key in (
                "基金全称",
                "基金简称",
                "基金代码",
                "基金类型",
                "基金管理人",
                "业绩比较基准",
                "跟踪标的",
            ):
                fields[key] = compact(cells[i + 1].get_text(" ", strip=True))
    for heading in soup.select("h4.t"):
        name = heading.get_text(strip=True)
        if name in ("投资目标", "投资范围", "投资策略", "风险收益特征"):
            body = " ".join(
                p.get_text(" ", strip=True) for p in heading.parent.find_all("p")
            )
            fields[name] = compact(body)[: 4000 if name == "投资策略" else 2000]
    if code not in fields.get("基金代码", "") or not fields.get("基金全称"):
        raise ValueError("profile code/full name not verified")
    return {
        "url": url,
        "kind": "secondary_fund_profile",
        "fields": fields,
        "sha256": hashlib.sha256(response.content).hexdigest(),
    }


def fetch_disclosure(session: requests.Session, code: str, output: Path) -> dict:
    response = session.get(
        "https://api.fund.eastmoney.com/f10/JJGG",
        params={"fundcode": code, "pageIndex": 1, "pageSize": 80, "type": 1},
        timeout=35,
    )
    response.raise_for_status()
    announcements = response.json().get("Data") or []
    cutoff = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    candidates = [
        a
        for a in announcements
        if "资料概要" in a.get("TITLE", "")
        and (a.get("PUBLISHDATEDesc") or "9999") <= cutoff
    ]
    if not candidates:
        raise ValueError(
            "no current product-summary disclosure in announcement inventory"
        )
    # Some funds publish multiple share-class summaries on the same date. Try
    # up to three; a different share class may omit the exchange share code.
    for notice in candidates[:3]:
        url = f"https://pdf.dfcfw.com/pdf/H2_{notice['ID']}_1.pdf"
        response = session.get(url, timeout=45)
        response.raise_for_status()
        if not response.content.startswith(b"%PDF"):
            raise ValueError("disclosure endpoint returned an unavailable/non-PDF response")
        reader = PdfReader(io.BytesIO(response.content))
        text = compact(
            "\n".join(page.extract_text() or "" for page in reader.pages[:6])
        )
        if code not in text:
            continue
        (output / "pdfs" / f"{code}_{notice['ID']}.pdf").write_bytes(response.content)
        return {
            "url": url,
            "kind": "issuer_product_summary",
            "title": notice["TITLE"],
            "published_on": notice["PUBLISHDATEDesc"],
            "fund_code_verified": code,
            "text": text[:12000],
            "sha256": hashlib.sha256(response.content).hexdigest(),
        }
    raise ValueError(
        "product summary PDFs do not contain the requested exchange share code"
    )


def research(row: dict, output: Path, profiles_only: bool = False) -> dict:
    full_code = row["record"]["fund_code"]
    code = full_code.split(".")[0]
    cache = output / "products" / f"{full_code}.json"
    previous = json.loads(cache.read_text(encoding="utf-8")) if cache.exists() else {}
    if previous:
        if (previous.get("profile") or {}).get("fields", {}).get("投资范围") and (
            profiles_only or previous.get("disclosure")
        ):
            return previous
    result = {
        "fund_code": full_code,
        "researched_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        "errors": [],
    }
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0", "Referer": "https://fundf10.eastmoney.com/"}
    )
    steps = [("profile", lambda: fetch_profile(session, code))]
    if profiles_only:
        result["disclosure"] = previous.get("disclosure")
        result["errors"] = previous.get("errors", [])
    else:
        steps.append(("disclosure", lambda: fetch_disclosure(session, code, output)))
    for key, fn in steps:
        for attempt in range(2):
            try:
                result[key] = fn()
                break
            except Exception as exc:
                if attempt == 1:
                    # A temporary read failure must not discard previously
                    # fetched evidence and its original source hash.
                    result[key] = previous.get(key)
                    result["errors"].append(
                        f"{key}: {type(exc).__name__}: {str(exc)[:180]}"
                    )
                else:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    if status in (401, 403, 429, 514, 567) or "non-PDF response" in str(exc):
                        result[key] = previous.get(key)
                        result["errors"].append(f"{key}: {type(exc).__name__}: {str(exc)[:180]}")
                        break
                    time.sleep(0.5)
    cache.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument(
        "--profiles-only",
        action="store_true",
        help="do not retry an unavailable disclosure host",
    )
    args = parser.parse_args()
    for sub in ("products", "pdfs"):
        (args.output_dir / sub).mkdir(parents=True, exist_ok=True)
    rows = json.loads(args.inventory.read_text(encoding="utf-8"))["targets"]
    rows.sort(
        key=lambda r: (
            r.get("daily_status") not in ("PRIMARY", "BACKUP", "RESERVE"),
            r["record"]["fund_code"],
        )
    )
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(research, row, args.output_dir, args.profiles_only): row[
                "record"
            ]["fund_code"]
            for row in rows
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if len(results) % 20 == 0 or result["errors"]:
                print(
                    json.dumps(
                        {
                            "completed": len(results),
                            "total": len(rows),
                            "fund_code": result["fund_code"],
                            "profile": bool(result["profile"]),
                            "disclosure": bool(result["disclosure"]),
                            "errors": result["errors"],
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
    payload = {
        "contract": "exchange_fund_classification_web_evidence_v1",
        "products": {
            r["fund_code"]: r for r in sorted(results, key=lambda r: r["fund_code"])
        },
    }
    (args.output_dir / "web_evidence.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "total": len(results),
                "profiles": sum(bool(r["profile"]) for r in results),
                "disclosures": sum(bool(r["disclosure"]) for r in results),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
