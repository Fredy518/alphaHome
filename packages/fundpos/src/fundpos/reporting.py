from __future__ import annotations

import hashlib
import html
import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from .constants import ASSET_NAMES, ASSETS, FIXED_INCOME_OUTPUTS, SW_CODES
from .storage import atomic_json, code_fingerprint

LABELS = {
    "fund_code": "基金代码",
    "master_code": "产品代码",
    "fund_name": "基金名称",
    "category": "基金类型",
    "valuation_date": "估值日",
    "information_cutoff": "信息截止日",
    "status": "状态",
    "reason": "原因",
    "aum": "已知规模（元）",
    "aum_date": "规模日期",
    "stock_weight": "股票合计",
    "a_stock_weight": "A股合计",
    "non_equity": "非权益合计",
    "ordinary_bond": "普通债券",
    "convertible_bond": "转债",
    "financing": "融资及其他负债",
    "gross_assets": "资产总额",
    "stock_quality": "股票结果质量",
    "cbond_quality": "转债结果质量",
    "cbond_reliability": "转债诊断状态",
    "cbond_control_weight": "上期公开转债仓位",
    "cbond_mark_to_market_weight": "转债无交易市值滚动",
    "cbond_disclosure_gap": "估算与上期披露差异",
    "cbond_mark_to_market_gap": "估算与市值滚动差异",
    "cbond_sensitivity_lower": "转债融资情景下界（兼容字段）",
    "cbond_sensitivity_upper": "转债融资情景上界（兼容字段）",
    "cbond_financing_scenario_lower": "转债融资情景下界",
    "cbond_financing_scenario_upper": "转债融资情景上界",
    "cbond_method_envelope_lower": "转债三方法范围下界",
    "cbond_method_envelope_upper": "转债三方法范围上界",
    "cbond_priced_coverage": "转债逐券定价覆盖率",
    "cbond_proxy_ratio": "转债宽基代理比例",
    "cbond_state_total_variation": "转债状态路径总变差",
    "cbond_holdings_report_date": "转债持仓报告期",
    "ordinary_bond_quality": "普通债券结果质量",
    "proxy_ratio": "代理比例",
    "r2": "拟合R²",
    "return_mae": "收益MAE",
    "holdings_report_date": "持仓报告期",
    "holdings_ann_date": "持仓公告日",
    "holdings_age_days": "持仓年龄（天）",
    "weighting": "加权方法",
    "universe_count": "应覆盖产品数",
    "valid_count": "有效产品数",
    "count_coverage": "数量覆盖率",
    "aum_coverage": "规模覆盖率",
    "aum_known_count": "规模已知产品数",
    "known_aum": "已知规模合计（元）",
    **ASSET_NAMES,
}
STATUS_NAMES = {
    "ok": "有效估算",
    "degraded": "条件估算",
    "unavailable": "无法估算",
    "complete": "完整",
    "partial": "部分",
}

def report_payload(run: Path, history: pd.DataFrame | None = None) -> dict:
    estimates = pd.read_parquet(run / "estimates.parquet")
    aggregates = pd.read_parquet(run / "aggregates.parquet")
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    model_family = manifest.get("model_family", "equity")
    fixed_income = model_family in {"fixed_income_plus", "convertible_dominant"}
    convertible_dominant = model_family == "convertible_dominant"
    assets = FIXED_INCOME_OUTPUTS if fixed_income else ASSETS
    synthetic = manifest["provenance"].get("synthetic", False)
    model_name = manifest["configuration"]["model"]["name"]
    overview = [
        ["估值日", manifest["valuation_date"]],
        ["信息截止日", manifest["information_cutoff"]],
        ["生成时间", manifest["created_at"]],
        [
            "模型",
            (
                {
                    "cbond_state_space_v1": "转债主导状态路径模型",
                    "personalized": "转债主导专属静态基线",
                    "index": "转债主导公共因子基线",
                }
                if convertible_dominant
                else (
                    {
                        "personalized": "固收+专属资产收益基线",
                        "index": "固收+公共因子基线",
                    }
                    if fixed_income
                    else {"personalized": "专属行业基线", "index": "行业指数基线"}
                )
            ).get(model_name, model_name),
        ],
        [
            "模型族",
            (
                "转债主导专属仓位"
                if convertible_dominant
                else ("固收+分层仓位" if fixed_income else "权益行业仓位")
            ),
        ],
        ["范围版本", manifest.get("scope_version", "configured")],
        [
            "数据来源",
            "合成数据（非真实基金）" if synthetic else manifest["provenance"].get("provider"),
        ],
        ["输入范围", manifest["provenance"].get("universe_scope", "specified_dataset")],
        ["有效估算", manifest["status_counts"].get("ok", 0)],
        ["条件估算", manifest["status_counts"].get("degraded", 0)],
        ["无法估算", manifest["status_counts"].get("unavailable", 0)],
        [
            "汇总发布状态",
            "合成演示，未作真实验证"
            if synthetic
            else (
                "正式完整汇总"
                if manifest.get("formal_publication")
                else "仅指定样本或部分结果；详见覆盖率"
            ),
        ],
    ]
    preferred = [
        "fund_code",
        "fund_name",
        "category",
        "status",
        "reason",
        "valuation_date",
        "stock_weight",
        "hk",
        "convertible_bond",
        "ordinary_bond",
        "financing",
        "non_equity",
        "proxy_ratio",
        "aum",
        "r2",
        "holdings_report_date",
    ]
    if convertible_dominant:
        preferred = preferred[:9] + [
            "cbond_control_weight",
            "cbond_mark_to_market_weight",
            "cbond_reliability",
            "cbond_quality",
            "cbond_disclosure_gap",
            "cbond_mark_to_market_gap",
            "cbond_financing_scenario_lower",
            "cbond_financing_scenario_upper",
            "cbond_method_envelope_lower",
            "cbond_method_envelope_upper",
            "cbond_priced_coverage",
            "cbond_proxy_ratio",
            "cbond_state_total_variation",
            "cbond_holdings_report_date",
        ] + preferred[9:]
    columns = [c for c in preferred if c in estimates] + [c for c in assets if c not in preferred]
    selected = estimates.reindex(columns=columns)
    selected = selected.copy()
    selected["status"] = selected.status.map(STATUS_NAMES)
    display_aggregates = aggregates.copy()
    display_aggregates["status"] = display_aggregates.status.map(STATUS_NAMES)
    display_aggregates["weighting"] = display_aggregates.weighting.map(
        {"equal": "等权", "aum": "规模加权"}
    )
    sheets = [
        {
            "name": "运行概览",
            "columns": ["项目", "结果"],
            "keys": ["item", "value"],
            "rows": overview,
        },
        {
            "name": "基金明细",
            "keys": columns,
            "columns": [LABELS.get(c, c) for c in columns],
            "rows": json.loads(
                selected.to_json(orient="values", date_format="iso", force_ascii=False)
            ),
        },
        {
            "name": "群体汇总",
            "keys": list(aggregates.columns),
            "columns": [LABELS.get(c, c) for c in aggregates.columns],
            "rows": json.loads(
                display_aggregates.to_json(orient="values", date_format="iso", force_ascii=False)
            ),
        },
    ]
    if history is not None and not history.empty:
        sheets.append(
            {
                "name": "历史序列",
                "keys": list(history.columns),
                "columns": [LABELS.get(c, c) for c in history.columns],
                "rows": json.loads(
                    history.to_json(orient="values", date_format="iso", force_ascii=False)
                ),
            }
        )
    custom_file = run / "custom_portfolio.parquet"
    if custom_file.exists():
        custom = pd.read_parquet(custom_file)
        sheets.append(
            {
                "name": "组合穿透",
                "keys": list(custom.columns),
                "columns": [LABELS.get(c, c) for c in custom.columns],
                "rows": json.loads(
                    custom.to_json(orient="values", date_format="iso", force_ascii=False)
                ),
            }
        )
    return {
        "title": (
            "公募基金转债主导仓位测算"
            if convertible_dominant
            else (
                "公募基金股票、转债与债券仓位测算"
                if fixed_income
                else "公募基金行业仓位测算"
            )
        )
        + (" · 合成数据演示" if synthetic else ""),
        "valuation_date": manifest["valuation_date"],
        "sheets": sheets,
        "manifest": manifest,
        "assets": list(assets),
        "estimates": json.loads(estimates.to_json(orient="records", force_ascii=False)),
    }


def report_content_hash(payload: dict) -> str:
    """Fingerprint the values shared by JSON, HTML and the workbook."""
    content = {
        "source_run_id": payload["manifest"]["run_id"],
        "valuation_date": payload["valuation_date"],
        "assets": payload["assets"],
        "sheets": payload["sheets"],
    }
    encoded = json.dumps(
        content,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def export_html(payload: dict, path: Path, history: pd.DataFrame | None = None):
    estimates = pd.DataFrame(payload["estimates"])
    assets = tuple(payload.get("assets", ASSETS))
    eligible = estimates.loc[estimates.status.isin(["ok", "degraded"])]
    figure = go.Figure()
    chart_rows, options = [], []
    for i, (_, row) in enumerate(eligible.iterrows()):
        chart_assets = (
            ("hk", *SW_CODES)
            if payload["manifest"].get("model_family")
            in {"fixed_income_plus", "convertible_dominant"}
            else ASSETS[2:]
        )
        weights = pd.to_numeric(row.reindex(chart_assets), errors="coerce").sort_values(
            ascending=False
        )
        chart_rows.append(
            {
                "title": f"{row.fund_name} · 估算行业仓位",
                "x": [LABELS.get(a, ASSET_NAMES.get(a, a)) for a in weights.index],
                "y": weights.to_list(),
            }
        )
        options.append(
            f'<option value="{i}">{html.escape(row.fund_code + " " + row.fund_name)}</option>'
        )
    chart = "<p class='empty'>当前没有具备完整输入的估算，具体缺口见基金明细。</p>"
    if len(eligible):
        first = chart_rows[0]
        figure.add_trace(
            go.Bar(
                x=first["x"],
                y=first["y"],
                marker_color="#385a79",
                hovertemplate="%{x}: %{y:.2%}<extra></extra>",
            )
        )
        figure.update_layout(
            template="plotly_white",
            height=460,
            margin=dict(l=55, r=25, t=95, b=100),
            yaxis_tickformat=".0%",
            yaxis_title="占基金净值",
            showlegend=False,
            font=dict(family="Microsoft YaHei, Arial", size=12),
            title=first["title"],
        )
        # One chart and O(funds * industries) data, instead of an O(funds squared) visibility menu.
        encoded = json.dumps(chart_rows, ensure_ascii=False).replace("</", "<\\/")
        chart = '<label>选择基金 <select id="fund-picker">' + "".join(options) + "</select></label>"
        chart += figure.to_html(full_html=False, include_plotlyjs=True, div_id="industry-chart")
        chart += f"""<script id="industry-data" type="application/json">{encoded}</script><script>
        const fundChartRows = JSON.parse(document.getElementById('industry-data').textContent);
        document.getElementById('fund-picker').addEventListener('change', function () {{
          const row = fundChartRows[Number(this.value)];
          Plotly.restyle('industry-chart', {{x:[row.x],y:[row.y]}}, [0]);
          Plotly.relayout('industry-chart', {{'title.text':row.title}});
        }});</script>"""
    history_chart = ""
    if history is not None and not history.empty and "stock_weight" in history:
        fig = go.Figure()
        for name, frame in history.groupby("fund_code"):
            frame = frame.sort_values("valuation_date")
            fig.add_trace(
                go.Scatter(
                    x=frame.valuation_date,
                    y=frame.stock_weight,
                    name=name,
                    mode="lines",
                    connectgaps=False,
                )
            )
        fig.update_layout(
            template="plotly_white", title="历史股票仓位", yaxis_tickformat=".0%", height=360
        )
        history_chart = fig.to_html(full_html=False, include_plotlyjs=not bool(len(eligible)))
    overview = payload["sheets"][0]["rows"]
    context = "".join(
        f"<div><span>{html.escape(str(k))}</span><strong>{html.escape(pd.Timestamp(v).strftime('%Y-%m-%d %H:%M:%S') if k == '生成时间' else str(v))}</strong></div>"
        for k, v in overview
    )
    table_columns = [
        c
        for c in [
            "fund_code",
            "fund_name",
            "category",
            "status",
            "stock_weight",
            "hk",
            "convertible_bond",
            "ordinary_bond",
            "financing",
            "proxy_ratio",
            "reason",
        ]
        if c in estimates
    ]
    if payload["manifest"].get("model_family") == "convertible_dominant":
        table_columns = table_columns[:-1] + [
            c
            for c in [
                "cbond_control_weight",
                "cbond_mark_to_market_weight",
                "cbond_reliability",
                "cbond_priced_coverage",
                "cbond_financing_scenario_lower",
                "cbond_financing_scenario_upper",
                "cbond_method_envelope_lower",
                "cbond_method_envelope_upper",
            ]
            if c in estimates
        ] + table_columns[-1:]
    table = estimates[table_columns].copy()
    if "status" in table:
        table["status"] = table.status.map(STATUS_NAMES)
    for col in (
        "stock_weight",
        "hk",
        "convertible_bond",
        "ordinary_bond",
        "financing",
        "proxy_ratio",
        "cbond_control_weight",
        "cbond_mark_to_market_weight",
        "cbond_priced_coverage",
        "cbond_financing_scenario_lower",
        "cbond_financing_scenario_upper",
        "cbond_method_envelope_lower",
        "cbond_method_envelope_upper",
    ):
        if col in table:
            table[col] = table[col].map(lambda x: f"{x:.2%}" if pd.notna(x) else "—")
    table = (
        table.fillna("—")
        .rename(columns=LABELS)
        .to_html(index=False, escape=True, na_rep="—", border=0)
    )
    group_sheet = next(s for s in payload["sheets"] if s["name"] == "群体汇总")
    group_frame = pd.DataFrame(group_sheet["rows"], columns=group_sheet["keys"])
    group_columns = [
        "category",
        "weighting",
        "status",
        "valid_count",
        "universe_count",
        "count_coverage",
        "aum_coverage",
    ]
    group_frame = group_frame[group_columns].copy()
    for col in ("count_coverage", "aum_coverage"):
        group_frame[col] = group_frame[col].map(lambda x: f"{x:.2%}" if pd.notna(x) else "未知")
    group_table = group_frame.rename(columns=LABELS).to_html(
        index=False, escape=True, na_rep="—", border=0
    )
    custom_html = ""
    for sheet in payload["sheets"]:
        if sheet["name"] == "组合穿透":
            frame = pd.DataFrame(sheet["rows"], columns=sheet["keys"])
            for asset in assets:
                frame[asset] = frame[asset].map(
                    lambda value: f"{value:.2%}" if pd.notna(value) else "未知"
                )
            custom_html = (
                "<section><h2>自定义组合穿透</h2>"
                + frame.rename(columns=LABELS).to_html(index=False, escape=True, border=0)
                + "</section>"
            )
    title = html.escape(payload["title"])
    model_family = payload["manifest"].get("model_family")
    fixed_income = model_family in {"fixed_income_plus", "convertible_dominant"}
    subtitle = (
        "申万 2021 版一级行业 · 转债状态、公开仓位与市值滚动对照"
        if model_family == "convertible_dominant"
        else (
            "申万 2021 版一级行业 · 港股、转债与普通债券单列"
            if fixed_income
            else "申万 2021 版一级行业 · 行业比例以基金净值为分母"
        )
    )
    integrity = json.dumps(
        {
            "source_run_id": payload["manifest"]["run_id"],
            "estimate_rows": len(estimates),
            "aggregate_rows": len(group_frame),
            "report_content_sha256": report_content_hash(payload),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).replace("</", "<\\/")
    document = f"""<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <script id="fundpos-integrity" type="application/json">{integrity}</script>
    <title>{title}</title><style>
    body{{margin:0;background:#f2f4f6;color:#203144;font:14px/1.65 'Microsoft YaHei',Arial,sans-serif}}
    main{{max-width:1440px;margin:36px auto;padding:0 28px}}h1{{font-size:28px;font-weight:600;margin:0 0 8px}}h2{{font-size:20px;margin:0 0 18px}}
    .subtitle{{color:#607084;margin-bottom:26px}}.context{{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;border-top:2px solid #385a79;padding-top:20px}}
    .context span{{display:block;font-size:12px;color:#66788b}}.context strong{{font-size:15px;font-weight:500}}
    section{{background:white;margin-top:24px;padding:24px;border:1px solid #dce2e8;border-radius:6px;overflow:auto}}
    table{{border-collapse:collapse;width:100%;white-space:nowrap;font-size:12px}}th{{text-align:left;color:#fff;background:#385a79;padding:12px}}
    td{{border-bottom:1px solid #e5eaf0;padding:10px}}tr:nth-child(even){{background:#f8fafc}}.empty{{padding:40px;color:#80522d;background:#fff8ef}}
    footer{{font-size:12px;color:#66788b;margin:22px 0}}@media(max-width:900px){{.context{{grid-template-columns:repeat(2,1fr)}}}}
    </style><main><h1>{title}</h1><div class="subtitle">{subtitle} · 估值日 {payload["valuation_date"]} · 仓位以基金净值为分母</div>
    <div class="context">{context}</div><section><h2>基金行业分布</h2>{chart}</section>
    {f"<section>{history_chart}</section>" if history_chart else ""}<section><h2>基金明细与数据状态</h2>{table}</section>
    <section><h2>基金群体覆盖率</h2>{group_table}</section>{custom_html}
    <footer>条件估算保留具体数据限制，不计入正式群体覆盖率。历史结果按公告日期重建；上线后保留实际运行快照。</footer></main></html>"""
    path.write_text(document, encoding="utf-8")


def export_report(
    root: Path, run: Path, *, history: pd.DataFrame | None = None
) -> dict:
    """Create an explicit, disposable HTML view of a frozen run.

    AlphaDB is the maintained result store. This helper is only used when a
    person explicitly requests a local inspection view; it is never part of
    the production estimate/ingest/reconcile chain.
    """

    payload = report_payload(run, history)
    report_dir = run / "report"
    report_dir.mkdir(exist_ok=True)
    atomic_json(report_dir / "report_data.json", payload)
    export_html(payload, report_dir / "report.html", history)
    result = {"html": str(report_dir / "report.html"), "json": str(report_dir / "report_data.json")}
    result.update(
        report_code_hash=code_fingerprint(root),
        source_run_id=payload["manifest"]["run_id"],
    )
    atomic_json(report_dir / "export_status.json", result)
    return result
