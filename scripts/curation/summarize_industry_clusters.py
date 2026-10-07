#!/usr/bin/env python
"""Build a Chinese catalog and cross-month diagnostics from a saved cluster run."""
from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

import pandas as pd


def markdown_table(frame):
    def clean(value):
        return str(value).replace("|", " / ").replace("\n", " ")
    return "\n".join([
        "| " + " | ".join(map(clean, frame.columns)) + " |",
        "| " + " | ".join("---" for _ in frame.columns) + " |",
        *["| " + " | ".join(map(clean, row)) + " |" for row in frame.itertuples(index=False, name=None)],
    ])


def pairs(groups):
    return {tuple(sorted((a, b))) for group in groups for i, a in enumerate(sorted(group))
            for b in sorted(group)[:i]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.output_dir
    with gzip.open(root / "series.json.gz", "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    state = payload["snapshots"][-1]["state"]
    date = state["asof"]
    members = pd.read_csv(root / "latest_members.csv")
    groups = pd.read_csv(root / "latest_groups.csv")
    names = members.set_index("index_code").index_name.to_dict()
    controls = pd.read_csv(root / "control_memberships.csv.gz")
    old = controls.loc[controls.asof_date.eq(date) & controls.variant.eq("stock60")].copy()
    old_map = old.set_index("index_code").cluster_id
    old_groups = list(old.groupby("cluster_id").index_code.apply(list))
    new_groups = [s.split("|") for s in groups.indices]
    common = set(old.index_code) & set(c for group in new_groups for c in group)
    old_pairs = pairs([set(g) & common for g in old_groups])
    new_pairs = pairs([set(g) & common for g in new_groups])
    transition = members.loc[members.index_code.isin(common)].copy()
    transition["stock60_cluster_id"] = transition.index_code.map(old_map)
    transition.to_csv(root / "old_new_membership.csv", index=False, encoding="utf-8-sig")
    split_old = int(transition.groupby("stock60_cluster_id").cluster_id.nunique().gt(1).sum())
    merged_new = int(transition.groupby("cluster_id").stock60_cluster_id.nunique().gt(1).sum())
    cache = Path(payload.get("input_cache_dir", root / "inputs" / payload["input_receipt"]["request_hash"][:16]))
    classification = pd.read_csv(cache / "classification.csv.gz", dtype={"industry_code1": str, "industry_code2": str})
    classification.obs_date = pd.to_datetime(classification.obs_date)
    cutoff = pd.Timestamp(date)
    classification = classification.loc[
        classification.obs_date.le(cutoff) & classification.obs_date.ge(
            cutoff - pd.Timedelta(days=state["config"]["maximum_classification_age_days"]))
    ].sort_values(["ts_code", "obs_date"]).drop_duplicates("ts_code", keep="last").set_index("ts_code")

    def exposure(code, level):
        weights = pd.Series(state["weights"][code], name="weight").to_frame()
        weights["industry"] = weights.index.map(classification[f"industry_level{level}"])
        ranked = weights.groupby("industry").weight.sum().nlargest(3)
        return "；".join(f"{name} {weight:.1%}" for name, weight in ranked.items())

    catalog = []
    status_names = {"confirmed": "已确认", "price_pending": "走势待确认", "degraded_pending": "等待复核"}
    for row in groups.sort_values(["index_count", "cluster_id"], ascending=[False, True]).itertuples():
        codes = row.indices.split("|")
        valid = members.loc[members.index_code.isin(codes) & members.can_represent_cluster, "index_name"]
        source_ids = sorted(set(old_map.loc[codes]))
        catalog.append({
            "簇ID": row.cluster_id, "状态": status_names[row.status], "指数数": row.index_count,
            "中心代表": row.representative_name, "代表一级行业": exposure(row.representative_index_code, 1),
            "代表二级行业": exposure(row.representative_index_code, 2),
            "成员指数": " / ".join(names[c] for c in codes),
            "有代表资格的成员": " / ".join(valid) if len(valid) else "待确认",
            "原成分簇数": len(source_ids),
            "代表对成员最低252日相关": f"{row.worst_member_corr_252:.3f}" if pd.notna(row.worst_member_corr_252) else "—",
            "代表对成员最低252日残差相关": f"{row.worst_member_residual_corr_252:.3f}" if pd.notna(row.worst_member_residual_corr_252) else "—",
        })
    catalog = pd.DataFrame(catalog)
    catalog.to_csv(root / "representative_catalog.csv", index=False, encoding="utf-8-sig")
    monthly = pd.read_csv(root / "monthly_summary.csv")
    stability = []
    for label, prefix in [("旧成分重叠60%", "stock60_"), ("逐月重算多特征", "unbuffered_minimax_"), ("带维护的多特征簇", "")]:
        transitions = monthly.loc[monthly[prefix + "same_cluster_pair_jaccard"].notna()]
        stability.append({
            "方法": label, "可比月间转换": len(transitions),
            "共同成员分组完全不变次数": int(transitions[prefix + "changed_same_cluster_pairs"].eq(0).sum()),
            "同簇成员对Jaccard均值": float(transitions[prefix + "same_cluster_pair_jaccard"].mean()),
            "同簇成员对变化数合计": int(transitions[prefix + "changed_same_cluster_pairs"].sum()),
        })
    stability = pd.DataFrame(stability)
    stability.to_csv(root / "stability_comparison.csv", index=False, encoding="utf-8-sig")
    price = pd.read_csv(root / "price_coverage.csv.gz")
    price = price.loc[price.asof_date.eq(date) & price.window.eq(max(state["config"]["windows"]))]
    pending = members.loc[~members.status.eq("confirmed")].merge(price, on="index_code", how="left")
    pending.to_csv(root / "pending_indices.csv", index=False, encoding="utf-8-sig")
    readable_pending = pending[["index_code", "index_name", "status", "return_days", "market_common_days", "price_staleness_sessions"]].rename(columns={
        "index_code": "指数", "index_name": "名称", "status": "状态", "return_days": "252日收益观测",
        "market_common_days": "与基准共同观测", "price_staleness_sessions": "行情滞后交易日",
    }).fillna("—")
    diagnostics = {
        "asof_date": date, "old_stock60_clusters": len(old_groups), "maintained_clusters": len(groups),
        "old_clusters_split_across_new": split_old, "new_clusters_combining_old": merged_new,
        "new_same_cluster_pairs": len(new_pairs - old_pairs), "separated_old_same_cluster_pairs": len(old_pairs - new_pairs),
        "same_cluster_pairs_preserved": len(old_pairs & new_pairs),
        "monthly_membership_unique": not pd.read_csv(root / "membership_history.csv.gz").duplicated(["asof_date", "index_code"]).any(),
    }
    (root / "catalog_diagnostics.json").write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8")
    stable_display = stability.copy()
    stable_display["同簇成员对Jaccard均值"] = stable_display["同簇成员对Jaccard均值"].map(lambda x: f"{x:.3f}")
    latest = summary["latest"]
    count_table = pd.DataFrame([
        ["母库指数", latest["universe_indices"]], ["结构可用指数", latest["structure_ready_indices"]],
        ["走势可确认指数", latest["price_ready_indices"]], ["旧成分重叠簇", len(old_groups)],
        ["当前维护簇", len(groups)], ["已确认簇", latest["confirmed_clusters"]],
        ["走势待确认簇", latest["price_pending_clusters"]], ["等待复核簇", latest["degraded_pending_clusters"]],
    ], columns=["项目", "数量"])
    monthly_display = monthly[["asof_date", "structure_ready_indices", "price_ready_indices", "stock60_clusters",
                               "maintained_clusters", "confirmed_clusters", "same_cluster_pair_jaccard", "representative_changes"]].copy()
    monthly_display["same_cluster_pair_jaccard"] = monthly_display["same_cluster_pair_jaccard"].map(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
    monthly_display.columns = ["截止月", "结构指数", "走势指数", "旧成分簇", "维护簇", "已确认簇", "与上月同簇对Jaccard", "代表变更"]
    sections = [
        f"# AlphaHome行业代表簇：{date}", "",
        f"母库：{payload['source_description']}", "",
        "## 最新结果", "", markdown_table(count_table), "",
        f"新旧分组间保留{len(old_pairs & new_pairs)}对同簇关系，新增{len(new_pairs - old_pairs)}对，拆开{len(old_pairs - new_pairs)}对。"
        f"有{merged_new}个新簇组合了多个旧簇，{split_old}个旧簇被分到多个新簇。簇数净变化不能概括这些调整。", "",
        "所有已确认簇都存在满足约束的中心代表，成员也逐一标明代表资格。走势待确认簇仅保留身份，不能参加要求代表资格的策略选择。", "",
        "## 跨月维护", "", markdown_table(stable_display), "",
        "Jaccard只比较两个月共同且结构可用的成员，不计新增指数本身。值越高，分组越接近；单成员簇占比较高时，"
        "不能仅靠这个数字断言算法更好。季度集中合并可能造成一次较大的分组变化，须结合代表质量一起看。", "",
        markdown_table(monthly_display), "",
        "## 等待数据的指数", "", markdown_table(readable_pending) if len(pending) else "无。", "",
        "## 完整簇清单", "",
        "一级、二级行业按中心代表的实际成分权重列前三项；板块与主题以指数名称及行业暴露解释，不作为强制同簇规则。", "",
        markdown_table(catalog), "",
        "## 使用口径", "",
        "算法采用成分、申万一级/二级行业暴露、120/252日原始及市场残差走势的约束minimax聚类。"
        "季度合并、连续确认、加入与保留阈值不同；参数是预先给定的初始方案，未按收益调优。", "",
        "历史输出使用截止当时的结构和价格，但当前母库含按最新可用池选出的指数。它是历史重建与维护行为诊断，"
        "不代表当年已经发布的结果，也不能替代历史ETF可用池约束。", "",
        "[算法与运行说明](../../../docs/business/industry_representative_clusters.md) · "
        "[成员与旧簇对照](old_new_membership.csv) · [代表资格完整数据](latest_members.csv) · "
        "[簇质量](latest_groups.csv) · [月度变化](monthly_summary.csv) · [事件记录](events.json)", "",
    ]
    (root / "README.md").write_text("\n".join(sections), encoding="utf-8")
    print(json.dumps(diagnostics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
