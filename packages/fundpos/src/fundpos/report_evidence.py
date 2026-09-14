from __future__ import annotations

import hashlib
import json
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from .storage import atomic_json, atomic_parquet, file_hash

API = "https://api.fund.eastmoney.com/f10/JJGG"
PDF = "https://pdf.dfcfw.com/pdf/H2_{notice_id}_1.pdf"
HEADERS = {
    "Referer": "https://fundf10.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) fundpos-evidence/3",
}


def report_period_from_title(title: str):
    if any(word in title for word in ("摘要", "提示性公告", "更正", "修订")):
        return None
    year = re.search(r"(20\d{2})年", title)
    if not year:
        return None
    value = int(year.group(1))
    if "年度报告" in title:
        return pd.Timestamp(value, 12, 31)
    if "中期报告" in title or "半年度报告" in title:
        return pd.Timestamp(value, 6, 30)
    quarter = re.search(r"第([一二三四1234])季度报告", title)
    if not quarter:
        return None
    number = {"一": 1, "二": 2, "三": 3, "四": 4}.get(
        quarter.group(1), int(quarter.group(1)) if quarter.group(1).isdigit() else 0
    )
    return pd.Timestamp(value, number * 3, 1) + pd.offsets.MonthEnd(0)


def _request(url, *, cookie=None, timeout=30):
    headers = dict(HEADERS)
    if cookie:
        headers["Cookie"] = cookie
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(), response.headers.get("Content-Type", "")


def _challenge_cookie(body: bytes) -> str:
    text = body.decode("utf-8", "replace")
    block = re.search(r"var e=\{(.*?)\},t=0", text)
    ssid = re.search(r"\(t,(\d{6,})\)", text)
    if not block or not ssid or "EO_Bot_Ssid" not in text:
        raise ValueError("Unrecognized PDF challenge")
    status_parts = [int(value) for value in re.findall(r":(\d{6,})", block.group(1))]
    if not status_parts:
        raise ValueError("Missing challenge status components")
    return f"__tst_status={sum(status_parts)}#; EO_Bot_Ssid={ssid.group(1)}"


def download_report_pdf(notice_id: str, path: Path) -> dict:
    url = PDF.format(notice_id=notice_id)
    body, content_type = _request(url)
    if not body.startswith(b"%PDF"):
        body, content_type = _request(url, cookie=_challenge_cookie(body))
    if not body.startswith(b"%PDF"):
        raise ValueError(f"Document is not a PDF: {content_type}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(body)
    temporary.replace(path)
    return {
        "document_sha256": hashlib.sha256(body).hexdigest(),
        "document_bytes": len(body),
        "document_path": str(path.resolve()),
        "document_url": url,
    }


def fetch_notice_index(fund_code: str, *, notice_type=3) -> list[dict]:
    code = fund_code.split(".")[0]
    page, rows = 1, []
    while True:
        url = f"{API}?fundcode={code}&pageIndex={page}&pageSize=100&type={notice_type}"
        body, _ = _request(url)
        content = json.loads(body)
        data = content.get("Data") or []
        rows.extend(data)
        if not data or page * 100 >= int(content.get("TotalCount") or len(rows)):
            break
        page += 1
    return rows


def collect_report_evidence(
    pilot: pd.DataFrame,
    start,
    end,
    output: Path,
    *,
    workers=6,
) -> tuple[pd.DataFrame, dict]:
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    selected, index_errors = [], []

    def read_fund(row):
        try:
            return row, fetch_notice_index(row.fund_code), None
        except Exception as exc:
            return row, [], type(exc).__name__

    records = list(pilot.drop_duplicates("fund_code").itertuples(index=False))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(read_fund, row) for row in records]
        for future in as_completed(futures):
            fund, notices, error = future.result()
            if error:
                index_errors.append({"fund_code": fund.fund_code, "error": error})
                continue
            candidates = []
            for notice in notices:
                report_date = report_period_from_title(str(notice.get("TITLE") or ""))
                ann_date = pd.to_datetime(notice.get("PUBLISHDATE"), errors="coerce")
                if report_date is None or pd.isna(ann_date) or not start <= report_date <= end:
                    continue
                candidates.append(
                    {
                        "fund_code": fund.fund_code,
                        "master_code": fund.master_code,
                        "fund_name": fund.fund_name,
                        "selection_group": fund.selection_group,
                        "report_date": report_date,
                        "ann_date": ann_date.normalize(),
                        "notice_id": notice["ID"],
                        "title": notice["TITLE"],
                        "source_index_url": (
                            f"{API}?fundcode={fund.fund_code.split('.')[0]}&type=3"
                        ),
                    }
                )
            if candidates:
                frame = pd.DataFrame(candidates).sort_values(["report_date", "ann_date"])
                frame["revision_count"] = frame.groupby("report_date").report_date.transform("size")
                selected.extend(
                    frame.drop_duplicates("report_date", keep="first").to_dict("records")
                )
    evidence = pd.DataFrame(selected)
    document_root = output / "report_documents"

    def download(row):
        target = (
            document_root
            / row["fund_code"]
            / f"{pd.Timestamp(row['report_date']):%Y-%m-%d}_{row['notice_id']}.pdf"
        )
        try:
            detail = (
                {
                    "document_sha256": file_hash(target),
                    "document_bytes": target.stat().st_size,
                    "document_path": str(target.resolve()),
                    "document_url": PDF.format(notice_id=row["notice_id"]),
                }
                if target.exists() and target.read_bytes()[:4] == b"%PDF"
                else download_report_pdf(row["notice_id"], target)
            )
            return row | detail | {"verified": True, "download_error": None}
        except Exception as exc:
            return row | {
                "document_sha256": None,
                "document_bytes": None,
                "document_path": str(target.resolve()),
                "document_url": PDF.format(notice_id=row["notice_id"]),
                "verified": False,
                "download_error": type(exc).__name__,
            }

    downloaded = []
    if not evidence.empty:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(download, row) for row in evidence.to_dict("records")]
            for future in as_completed(futures):
                downloaded.append(future.result())
    batch_result = pd.DataFrame(downloaded)
    if not batch_result.empty:
        batch_result = batch_result.sort_values(
            ["selection_group", "master_code", "report_date"]
        )
    existing_path = output / "report_evidence.parquet"
    existing = pd.read_parquet(existing_path) if existing_path.exists() else pd.DataFrame()
    result = pd.concat([existing, batch_result], ignore_index=True)
    if not result.empty:
        result = (
            result.sort_values(["fund_code", "report_date", "retrieved_at"])
            if "retrieved_at" in result
            else result.sort_values(["fund_code", "report_date"])
        )
        result = result.drop_duplicates(["fund_code", "report_date"], keep="last")
        result = result.sort_values(["selection_group", "master_code", "report_date"])
    summary = {
        "status": (
            "complete"
            if len(batch_result) and batch_result.verified.all()
            else "partial"
        ),
        "funds_requested": len(records),
        "funds_with_index_error": len(index_errors),
        "reports_selected": len(batch_result),
        "reports_verified": (
            int(batch_result.verified.sum()) if len(batch_result) else 0
        ),
        "existing_rows_preserved": len(existing),
        "evidence_rows_total": len(result),
        "period_start": str(start.date()),
        "period_end": str(end.date()),
        "source": "Eastmoney fund announcement index and PDF mirror",
        "source_role": "third_party_document_mirror",
        "index_errors": index_errors,
        "retrieved_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
    }
    output.mkdir(parents=True, exist_ok=True)
    atomic_parquet(output / "report_evidence.parquet", result)
    atomic_json(output / "report_evidence_summary.json", summary)
    return result, summary


def register_report_evidence(supplements: Path, evidence_path: Path, summary: dict):
    manifest_path = supplements / "manifest.json"
    content = (
        json.loads(manifest_path.read_text(encoding="utf8"))
        if manifest_path.exists()
        else {"files": []}
    )
    entry = {
        "table": "report_evidence",
        "file": str(evidence_path.relative_to(supplements)).replace("\\", "/"),
        "sha256": file_hash(evidence_path),
        "keys": ["fund_code", "report_date"],
        "source": summary["source"],
        "verified_at": summary["retrieved_at"],
        "priority": "fill_gaps",
        "source_role": summary["source_role"],
        "period_start": summary["period_start"],
        "period_end": summary["period_end"],
    }
    content["files"] = [
        value for value in content.get("files", []) if value.get("table") != "report_evidence"
    ] + [entry]
    atomic_json(manifest_path, content)
