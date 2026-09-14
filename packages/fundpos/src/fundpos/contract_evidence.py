from __future__ import annotations

import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from .report_evidence import API, download_report_pdf, fetch_notice_index
from .storage import atomic_json, atomic_parquet, file_hash


def _pdf_pages(path: Path) -> list[str]:
    process = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", str(path), "-"],
        capture_output=True,
        timeout=120,
    )
    if process.returncode:
        raise ValueError("PDF_TEXT_EXTRACTION_FAILED")
    return process.stdout.decode("utf-8", "replace").split("\f")


def _normalize(text: str) -> str:
    return re.sub(r"\s+", "", text).replace("％", "%")


def _first_match(pages: list[str], patterns: list[str]):
    found = []
    for page_number, page in enumerate(pages, 1):
        text = _normalize(page)
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                start, end = max(0, match.start() - 80), min(len(text), match.end() + 80)
                found.append((match, page_number, text[start:end]))
    return found


_DENOMINATOR_PATTERN = (
    r"(?P<denominator>非现金基金资产|基金非现金资产|非现金资产|"
    r"基金固定收益类资产|固定收益类资产|债券资产|基金资产净值|基金净资产|基金资产)"
)

_STOCK_SUBJECT_PATTERN = (
    r"(?P<subject>固定收益类资产以外的其他资产|非固定收益类资产|"
    r"股票等非固定收益类资产|股票、权证等权益类资产|股票等权益类资产|"
    r"股票等权益类品种|股票、权证|权益类资产|股票资产)"
)


def _denominator_code(value: str) -> str:
    if value in {"基金资产净值", "基金净资产"}:
        return "fund_nav"
    if value in {"非现金基金资产", "基金非现金资产", "非现金资产"}:
        return "non_cash_fund_assets"
    if value in {"基金固定收益类资产", "固定收益类资产"}:
        return "fixed_income_assets"
    if value == "债券资产":
        return "bond_assets"
    return "fund_assets"


def _stock_scope(value: str) -> str:
    if "非固定收益" in value or "固定收益类资产以外" in value:
        return "non_fixed_income_assets"
    if "权益" in value or "权证" in value:
        return "equity_assets"
    return "stock_assets"


def _ratio_candidates(
    pages: list[str], patterns: list[str], *, stock_scope: bool = False
) -> list[dict]:
    candidates = []
    for match, page, snippet in _first_match(pages, patterns):
        values = match.groupdict()
        lower = values.get("lower")
        upper = values.get("upper") or values.get("ratio")
        if upper is None:
            continue
        subject = values.get("subject")
        candidate = {
            "lower": float(lower) / 100 if lower is not None else 0.0,
            "upper": float(upper) / 100,
            "denominator": _denominator_code(values["denominator"]),
            "page": page,
            "original_text": snippet,
        }
        if stock_scope and subject:
            candidate["scope"] = _stock_scope(subject)
        candidates.append(candidate)
    return candidates


def _select_unambiguous(candidates: list[dict]) -> tuple[dict | None, int]:
    unique = {
        (value["lower"], value["upper"], value["denominator"])
        for value in candidates
    }
    return (candidates[0] if len(unique) == 1 else None, len(unique))


def _lower_bound_candidates(pages: list[str], subject: str) -> list[dict]:
    patterns = [
        rf"(?:投资于)?(?P<subject>{subject})(?:的)?(?:投资)?比例(?:合计)?"
        rf"(?:不低于|不少于){_DENOMINATOR_PATTERN}(?:的)?"
        r"(?P<ratio>\d+(?:\.\d+)?)%",
        rf"(?:投资于)?(?P<subject>{subject})(?:投资)?占{_DENOMINATOR_PATTERN}"
        r"(?:的)?比例(?:不低于|不少于)(?P<ratio>\d+(?:\.\d+)?)%",
        rf"投资于(?P<subject>{subject})(?:的)?(?:比例)?(?:不低于|不少于)"
        rf"{_DENOMINATOR_PATTERN}(?:的)?(?P<ratio>\d+(?:\.\d+)?)%",
    ]
    candidates = _ratio_candidates(pages, patterns)
    for candidate in candidates:
        candidate["lower"] = candidate["upper"]
    return candidates


def _chinese_integer(value: str) -> int | None:
    if value.isdigit():
        return int(value)
    digits = {
        "一": 1,
        "二": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
        "十": 10,
    }
    if value in digits:
        return digits[value]
    if value.startswith("十") and value[1:] in digits:
        return 10 + digits[value[1:]]
    if value.endswith("十") and value[:-1] in digits:
        return digits[value[:-1]] * 10
    return None


def _passive_ratio_exception(pages: list[str]) -> tuple[int | None, int | None, str | None]:
    """Find the grace period for passive breaches of portfolio ratios."""
    candidates = []
    for page_number, page in enumerate(pages, 1):
        text = _normalize(page)
        for match in re.finditer(
            r"应当在(?P<days>\d+|[一二三四五六七八九十]+)个交易日内(?:进行)?调整",
            text,
        ):
            start = max(0, match.start() - 700)
            end = min(len(text), match.end() + 120)
            context = text[start:end]
            if (
                "基金规模变动" not in context
                or "不符合" not in context
                or not any(value in context for value in ("投资比例", "投资组合"))
            ):
                continue
            days = _chinese_integer(match.group("days"))
            if days is not None:
                candidates.append((days, page_number, context))
    unique_days = {value[0] for value in candidates}
    if len(unique_days) != 1:
        return None, None, None
    return candidates[0]


def _bond_lower_bound_candidates(pages: list[str]) -> list[dict]:
    category = (
        r"债券资产|债券类资产|固定收益类(?:证券|金融工具|资产|"
        r"\(.{0,400}?\)资产|（.{0,400}?）资产)"
    )
    patterns = [
        rf"(?:本基金)?(?:对|投资于)(?P<subject>债券|{category})(?:的)?"
        rf"(?:投资)?比例(?:合计)?(?:不低于|不少于){_DENOMINATOR_PATTERN}"
        r"(?:的)?(?P<ratio>\d+(?:\.\d+)?)%",
        rf"(?P<subject>{category})(?:的)?(?:投资)?比例(?:合计)?"
        rf"(?:不低于|不少于){_DENOMINATOR_PATTERN}(?:的)?"
        r"(?P<ratio>\d+(?:\.\d+)?)%",
        rf"(?P<subject>{category})(?:投资)?占{_DENOMINATOR_PATTERN}(?:的)?比例"
        r"(?:不低于|不少于)(?P<ratio>\d+(?:\.\d+)?)%",
    ]
    candidates = _ratio_candidates(pages, patterns)
    for candidate in candidates:
        candidate["lower"] = candidate["upper"]
    return candidates


def extract_contract_constraints_from_pages(pages: list[str]) -> dict:
    """Extract portfolio-level constraints without treating issuer caps as asset caps."""
    stock_patterns = [
        rf"{_STOCK_SUBJECT_PATTERN}(?:投资)?占{_DENOMINATOR_PATTERN}(?:的)?比例"
        r"(?:为|是)(?P<lower>\d+(?:\.\d+)?)%?[-—~至到]"
        r"(?P<upper>\d+(?:\.\d+)?)%",
        rf"{_STOCK_SUBJECT_PATTERN}(?:的)?(?:投资)?比例(?:合计)?"
        rf"(?:不超过|不高于){_DENOMINATOR_PATTERN}(?:的)?"
        r"(?P<upper>\d+(?:\.\d+)?)%",
        rf"(?:投资于)?{_STOCK_SUBJECT_PATTERN}(?:的)?(?:投资)?比例(?:合计)?"
        rf"(?:为|是){_DENOMINATOR_PATTERN}(?:的)?"
        r"(?P<lower>\d+(?:\.\d+)?)%?[-—~至到]"
        r"(?P<upper>\d+(?:\.\d+)?)%",
        rf"{_STOCK_SUBJECT_PATTERN}(?:投资)?占{_DENOMINATOR_PATTERN}(?:的)?比例"
        r"(?:合计)?(?:不超过|不高于)(?P<upper>\d+(?:\.\d+)?)%",
    ]
    stock_candidates = _ratio_candidates(pages, stock_patterns, stock_scope=True)
    selected_stock, stock_candidate_count = _select_unambiguous(stock_candidates)

    bond_candidates = _bond_lower_bound_candidates(pages)
    selected_bond, bond_candidate_count = _select_unambiguous(bond_candidates)

    cbond_subject = (
        r"(?:可转债|可转换(?:公司)?债券)"
        r"(?:\(含.{0,180}?\)|（含.{0,180}?）)?"
        r"(?:[和及]可交换(?:公司)?债券)?"
    )
    cbond_candidates = _lower_bound_candidates(pages, cbond_subject)
    selected_cbond, cbond_candidate_count = _select_unambiguous(cbond_candidates)
    (
        passive_exception_days,
        passive_exception_page,
        passive_exception_text,
    ) = _passive_ratio_exception(pages)

    full_text = _normalize("".join(pages))
    substantive = bool(
        len(full_text) >= 10_000
        and "基金合同" in full_text
        and "投资范围" in full_text
        and ("投资组合比例" in full_text or "组合限制" in full_text)
    )
    no_direct_match = re.search(
        r"(?:本基金)?(?:不直接(?:投资|从二级市场买入)|不从二级市场(?:直接)?买入)"
        r"股票(?:、权证)?(?:等权益类资产)?",
        full_text,
    )
    direct_stock_allowed = None if no_direct_match is None else False
    disposal_days = None
    disposal_text = None
    if no_direct_match:
        context = full_text[no_direct_match.start() : no_direct_match.start() + 500]
        disposal = re.search(
            r"(?:股票|权证).{0,260}?(?:不超过|(?:之日)?起(?:的)?)"
            r"(?P<days>\d+)个交易日(?:的时间)?内卖出",
            context,
        )
        if disposal:
            disposal_days = int(disposal.group("days"))
            disposal_text = context[: min(len(context), disposal.end() + 80)]

    fixed_income_primary = bool(
        (selected_bond and selected_bond["lower"] >= 0.5)
        or "主要投资于固定收益类资产" in full_text
        or "固定收益类资产为主要投资对象" in full_text
    )
    allows_equity_or_cbond = "股票" in full_text or bool(
        re.search(r"可转债|可转换(?:公司)?债券|可交换公司债券", full_text)
    )
    gross = _first_match(
        pages,
        [
            r"(?:本基金的?)?(?:基金总资产|基金资产总值|总资产)"
            r"(?:不得|不应|不)超过(?:基金净资产|基金资产净值)的"
            r"(?P<ratio>\d+(?:\.\d+)?)%"
        ],
    )
    gross_values = [float(match.group("ratio")) / 100 for match, _, _ in gross]
    gross_upper = gross_values[0] if gross_values and len(set(gross_values)) == 1 else None
    effective = re.search(
        r"自(20\d{2})年(\d{1,2})月(\d{1,2})日起(?:正式)?生效", full_text
    )
    effective_date = (
        str(
            pd.Timestamp(
                int(effective.group(1)), int(effective.group(2)), int(effective.group(3))
            ).date()
        )
        if effective
        else None
    )
    extraction_verified = bool(
        substantive
        and fixed_income_primary
        and (selected_bond is not None or selected_cbond is not None)
    )
    nav_constraint_usable = bool(
        selected_stock
        and selected_stock["denominator"] == "fund_nav"
        and effective_date
    )
    parsed_denominators = {
        value["denominator"]
        for value in (selected_stock, selected_bond, selected_cbond)
        if value is not None
    }
    requires_gross_asset_constraint = bool(
        parsed_denominators - {"fund_nav"}
    )
    point_in_time_hard_constraint_usable = bool(
        nav_constraint_usable and passive_exception_days is None
    )
    if nav_constraint_usable and passive_exception_days is not None:
        solver_constraint_status = "verified_document_requires_compliance_state"
    elif nav_constraint_usable:
        solver_constraint_status = "direct_nav_constraint_usable"
    elif (
        extraction_verified
        and requires_gross_asset_constraint
        and passive_exception_days is not None
    ):
        solver_constraint_status = (
            "verified_document_requires_denominator_and_compliance_state"
        )
    elif extraction_verified and requires_gross_asset_constraint:
        solver_constraint_status = "verified_document_requires_denominator_model"
    elif extraction_verified and passive_exception_days is not None:
        solver_constraint_status = (
            "verified_document_requires_effective_date_and_compliance_state"
        )
    elif extraction_verified:
        solver_constraint_status = "verified_document_missing_effective_date"
    else:
        solver_constraint_status = "unverified_extraction"
    return {
        "document_substantive": substantive,
        "text_length": len(full_text),
        "stock_lower": selected_stock["lower"] if selected_stock else None,
        "stock_upper": selected_stock["upper"] if selected_stock else None,
        "stock_denominator": selected_stock["denominator"] if selected_stock else None,
        "stock_scope": selected_stock.get("scope") if selected_stock else None,
        "stock_page": selected_stock["page"] if selected_stock else None,
        "original_text": selected_stock["original_text"] if selected_stock else None,
        "stock_candidate_count": stock_candidate_count,
        "stock_extraction_unambiguous": bool(selected_stock),
        "direct_stock_allowed": direct_stock_allowed,
        "converted_stock_disposal_days": disposal_days,
        "converted_stock_original_text": disposal_text,
        "portfolio_ratio_exception_days": passive_exception_days,
        "portfolio_ratio_exception_page": passive_exception_page,
        "portfolio_ratio_exception_original_text": passive_exception_text,
        "bond_lower": selected_bond["lower"] if selected_bond else None,
        "bond_denominator": selected_bond["denominator"] if selected_bond else None,
        "bond_page": selected_bond["page"] if selected_bond else None,
        "bond_original_text": selected_bond["original_text"] if selected_bond else None,
        "bond_candidate_count": bond_candidate_count,
        "cbond_lower": selected_cbond["lower"] if selected_cbond else None,
        "cbond_denominator": selected_cbond["denominator"] if selected_cbond else None,
        "cbond_page": selected_cbond["page"] if selected_cbond else None,
        "cbond_original_text": (
            selected_cbond["original_text"] if selected_cbond else None
        ),
        "cbond_candidate_count": cbond_candidate_count,
        "fixed_income_primary": fixed_income_primary,
        "allows_equity_or_cbond": allows_equity_or_cbond,
        "gross_assets_upper": gross_upper,
        "financing_upper": gross_upper - 1 if gross_upper is not None else None,
        "effective_date": effective_date,
        "extraction_verified": extraction_verified,
        "document_constraint_verified": extraction_verified,
        "requires_gross_asset_constraint": requires_gross_asset_constraint,
        "solver_constraint_status": solver_constraint_status,
        "nav_constraint_usable": nav_constraint_usable,
        "point_in_time_hard_constraint_usable": (
            point_in_time_hard_constraint_usable
        ),
    }


def extract_contract_constraints(path: Path) -> dict:
    return extract_contract_constraints_from_pages(_pdf_pages(path))


def collect_contract_evidence(pilot: pd.DataFrame, cutoff, output: Path, *, workers=6):
    cutoff = pd.Timestamp(cutoff)
    selected, errors = [], []

    def locate(row):
        codes = [row.fund_code, *[str(value) for value in row.share_codes]]
        seen = set()
        notices = []
        for code in codes:
            if code in seen:
                continue
            seen.add(code)
            try:
                notices.extend(
                    dict(value, source_fund_code=code)
                    for value in fetch_notice_index(code, notice_type=1)
                )
            except Exception:
                continue
        contracts = [
            value
            for value in notices
            if "基金合同" in str(value.get("TITLE") or "")
            and "摘要" not in str(value.get("TITLE") or "")
            and "公告" not in str(value.get("TITLE") or "")
            and "托管协议" not in str(value.get("TITLE") or "")
            and pd.notna(pd.to_datetime(value.get("PUBLISHDATE"), errors="coerce"))
            and pd.Timestamp(value["PUBLISHDATE"]) <= cutoff
        ]
        if not contracts:
            return None
        contracts = sorted(contracts, key=lambda value: value["PUBLISHDATE"], reverse=True)
        return {
            "fund_code": row.fund_code,
            "master_code": row.master_code,
            "fund_name": row.fund_name,
            "selection_group": row.selection_group,
            "contract_notices": contracts[:8],
            "latest_contract_notice_date": pd.Timestamp(
                contracts[0]["PUBLISHDATE"]
            ).normalize(),
        }

    records = list(pilot.drop_duplicates("master_code").itertuples(index=False))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(locate, row) for row in records]
        for future in as_completed(futures):
            value = future.result()
            if value:
                selected.append(value)
    document_root = output / "contract_documents"

    def process(row):
        attempts = []
        attempt_errors = []
        try:
            chosen = None
            for notice in row["contract_notices"]:
                ann_date = pd.Timestamp(notice["PUBLISHDATE"]).normalize()
                target = (
                    document_root
                    / row["fund_code"]
                    / f"{ann_date:%Y-%m-%d}_{notice['ID']}.pdf"
                )
                try:
                    detail = (
                        {
                            "document_sha256": file_hash(target),
                            "document_bytes": target.stat().st_size,
                            "document_path": str(target.resolve()),
                            "document_url": (
                                f"https://pdf.dfcfw.com/pdf/H2_{notice['ID']}_1.pdf"
                            ),
                        }
                        if target.exists()
                        and target.read_bytes()[:4] == b"%PDF"
                        else download_report_pdf(notice["ID"], target)
                    )
                    extracted = extract_contract_constraints(target)
                except Exception as exc:
                    attempt_errors.append(
                        {"notice_id": notice["ID"], "error": type(exc).__name__}
                    )
                    continue
                candidate = {
                    "ann_date": ann_date,
                    "notice_id": notice["ID"],
                    "title": notice["TITLE"],
                    "source_fund_code": notice["source_fund_code"],
                    "source_index_url": (
                        f"{API}?fundcode={notice['source_fund_code'].split('.')[0]}&type=1"
                    ),
                } | detail | extracted
                attempts.append(candidate)
                if extracted["document_constraint_verified"]:
                    chosen = candidate
                    break
            if not attempts:
                raise ValueError("NO_READABLE_CONTRACT_DOCUMENT")
            chosen = chosen or attempts[0]
            later = sum(value["ann_date"] > chosen["ann_date"] for value in attempts)
            chosen["later_contract_notice_count"] = later
            if later:
                chosen["nav_constraint_usable"] = False
            base = {key: value for key, value in row.items() if key != "contract_notices"}
            return base | chosen | {
                "document_verified": True,
                "error": None,
                "attempt_errors": json.dumps(attempt_errors, ensure_ascii=False),
            }
        except Exception as exc:
            base = {key: value for key, value in row.items() if key != "contract_notices"}
            return base | {
                "document_sha256": None,
                "document_path": None,
                "document_verified": False,
                "extraction_verified": False,
                "nav_constraint_usable": False,
                "error": type(exc).__name__,
                "attempt_errors": json.dumps(attempt_errors, ensure_ascii=False),
            }

    processed = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process, row) for row in selected]
        for future in as_completed(futures):
            processed.append(future.result())
    evidence = pd.DataFrame(processed)
    if not evidence.empty:
        evidence = evidence.sort_values(["selection_group", "master_code"])
    constraints = []
    for row in evidence.loc[evidence.document_verified].to_dict("records"):
        constraints.append(
            {
                "fund_code": row["fund_code"],
                "ann_date": row["ann_date"],
                "effective_date": pd.Timestamp(
                    row.get("effective_date") or row["ann_date"]
                ),
                "effective_date_source": (
                    "contract_text" if row.get("effective_date") else "announcement_date_placeholder"
                ),
                "document_hash": row["document_sha256"],
                "source_uri": row["document_url"],
                "source": "Eastmoney mirror of fund contract",
                "verified": bool(row.get("nav_constraint_usable", False)),
                "document_constraint_verified": bool(
                    row.get("document_constraint_verified", False)
                ),
                "solver_constraint_status": row.get("solver_constraint_status"),
                "fixed_income_primary": bool(row.get("fixed_income_primary", False)),
                "allows_equity_or_cbond": bool(row.get("allows_equity_or_cbond", False)),
                "stock_lower": row.get("stock_lower"),
                "stock_upper": row.get("stock_upper"),
                "stock_denominator": row.get("stock_denominator"),
                "stock_scope": row.get("stock_scope"),
                "direct_stock_allowed": row.get("direct_stock_allowed"),
                "converted_stock_disposal_days": row.get(
                    "converted_stock_disposal_days"
                ),
                "bond_lower": row.get("bond_lower"),
                "bond_denominator": row.get("bond_denominator"),
                "cbond_lower": row.get("cbond_lower"),
                "cbond_denominator": row.get("cbond_denominator"),
                "gross_assets_upper": row.get("gross_assets_upper"),
                "financing_upper": row.get("financing_upper"),
                "original_text": row.get("original_text"),
                "original_page": row.get("stock_page"),
            }
        )
    constraints = pd.DataFrame(constraints)
    summary = {
        "status": (
            "complete"
            if len(evidence) == len(records) and evidence.document_verified.all()
            else "partial"
        ),
        "funds_requested": len(records),
        "contracts_indexed": len(selected),
        "documents_verified": int(evidence.document_verified.sum()) if len(evidence) else 0,
        "extractions_verified": int(evidence.extraction_verified.sum()) if len(evidence) else 0,
        "document_constraints_verified": (
            int(evidence.document_constraint_verified.sum()) if len(evidence) else 0
        ),
        "nav_constraints_usable": int(evidence.nav_constraint_usable.sum()) if len(evidence) else 0,
        "cutoff": str(cutoff.date()),
        "source": "Eastmoney fund announcement index and PDF mirror",
        "source_role": "third_party_document_mirror",
        "errors": errors,
        "retrieved_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
    }
    atomic_parquet(output / "contract_evidence.parquet", evidence)
    atomic_parquet(output / "constraints.parquet", constraints)
    atomic_json(output / "contract_evidence_summary.json", summary)
    return evidence, constraints, summary


def register_contract_evidence(supplements: Path, summary: dict):
    manifest_path = supplements / "manifest.json"
    content = json.loads(manifest_path.read_text(encoding="utf8"))
    entries = [
        {
            "table": "contract_evidence",
            "file": "contract_evidence.parquet",
            "sha256": file_hash(supplements / "contract_evidence.parquet"),
            "keys": ["fund_code", "document_sha256"],
        },
        {
            "table": "constraints",
            "file": "constraints.parquet",
            "sha256": file_hash(supplements / "constraints.parquet"),
            "keys": ["fund_code", "effective_date", "document_hash"],
        },
    ]
    content["files"] = [
        row
        for row in content["files"]
        if row.get("table") not in {"contract_evidence", "constraints"}
    ]
    for entry in entries:
        entry.update(
            source=summary["source"],
            source_role=summary["source_role"],
            verified_at=summary["retrieved_at"],
            priority="fill_gaps",
            cutoff=summary["cutoff"],
        )
        content["files"].append(entry)
    atomic_json(manifest_path, content)
