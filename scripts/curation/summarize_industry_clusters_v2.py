#!/usr/bin/env python
"""Summarize V2 selection groups, including explicitly labelled stock fallbacks."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd
from summarize_industry_clusters import markdown_table


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.output_dir
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    m = pd.read_csv(root / "latest_members.csv")
    g = pd.read_csv(root / "latest_selection_groups.csv")
    core = pd.read_csv(root / "latest_groups.csv").set_index("cluster_id")
    monthly = pd.read_csv(root / "monthly_summary.csv")
    modes = {"multiview": "多特征确认", "stock_fallback": "成分回退", "unresolved_singleton": "资料待补独立项"}
    rows = []
    for item in g.sort_values(["index_count", "selection_group_id"], ascending=[False, True]).itertuples():
        members = m.loc[m.selection_group_id.eq(item.selection_group_id)]
        proven = item.selection_method == "multiview"
        quality = core.loc[members.iloc[0].cluster_id] if proven else None
        rows.append({
            "选择分组": item.selection_group_id, "计算方法": modes[item.selection_method],
            "指数数": item.index_count, "自身行情充分": item.price_ready_indices,
            "全部成员": " / ".join(members.index_name),
            "可供策略选择的成员": " / ".join(members.loc[members.can_represent_selection_group, "index_name"]),
            "中心代表": quality.representative_name if proven else "按投射评分选择",
            "最低252日相关性": f"{quality.worst_member_corr_252:.3f}" if proven else "未作多特征确认",
            "最低252日残差相关": f"{quality.worst_member_residual_corr_252:.3f}" if proven else "未作多特征确认",
        })
    catalog = pd.DataFrame(rows)
    catalog.to_csv(root / "selection_catalog.csv", index=False, encoding="utf-8-sig")
    display = monthly[["asof_date", "structure_ready_indices", "price_ready_indices", "confirmed_clusters",
                       "selection_groups", "multiview_selection_groups", "stock_fallback_selection_groups",
                       "unresolved_selection_groups", "partial_classification_indices", "selection_same_cluster_pair_jaccard"]].copy()
    display["selection_same_cluster_pair_jaccard"] = display["selection_same_cluster_pair_jaccard"].map(
        lambda v: f"{v:.3f}" if pd.notna(v) else "—")
    display.columns = ["截止日", "结构指数", "行情指数", "核心确认簇", "选择分组", "多特征分组", "成分回退分组", "待补独立项", "部分分类缺失指数", "选择分组月间Jaccard"]
    last = summary["latest"]
    parts = [
        f"# 行业代表簇V2：{summary['asof_date']}", "",
        f"母库标识：{summary['universe_id']}。母库标识中的v1表示固定成员版本；算法版本为industry_minimax_v2。", "",
        "## 当前结果", "",
        f"{last['universe_indices']}个指数中，{last['structure_ready_indices']}个具有可用结构，"
        f"{last['price_ready_indices']}个满足行情观测要求。当前有{last['confirmed_clusters']}个已确认的核心多特征簇。", "",
        f"供策略去重的分组共{last['selection_groups']}个：多特征确认{last['multiview_selection_groups']}个，"
        f"成分回退{last['stock_fallback_selection_groups']}个，资料待补独立项{last['unresolved_selection_groups']}个。"
        "部分核心簇与缺数据成员通过旧成分簇相连时，供选择的整个相关分组采用成分回退，并保留较低的确认等级。", "",
        "## 缺数据如何处理", "",
        "- 一级、二级行业分别保留已知权重；未知部分单列，不归一化，也不放入一个共同未知行业来增加重叠。", "",
        "- 自身数据充分的成员照常接受多特征检查；行情不足的成员不参与这部分的代表性认证。", "",
        "- 未确认成员按当时可用的60%成分重叠分组回退。与已确认簇相连时，合并相关选择分组，确保一个指数只占一个分组。", "",
        "- 无法形成成分向量的成员保留独立身份。其是否可投资仍须经过消费端原有ETF池和评分条件；尚未发布、非A股等明确冲突不会被放开。", "",
        "- 多特征分组要求合格代表；成分回退分组按原成分策略挑选，不宣称已满足多特征代表门槛。", "",
        "## 月度记录", "",
        "选择分组Jaccard按共同母库身份计算，包含资料待补独立项；不可与旧版仅对结构充分成员计算的Jaccard直接混比。", "",
        markdown_table(display), "",
        "## 完整选择清单", "", markdown_table(catalog), "",
        "[成员与缺失权重](latest_members.csv) · [核心簇质量](latest_groups.csv) · "
        "[策略分组数据](latest_selection_groups.csv) · [算法与使用说明](../../../docs/business/industry_representative_clusters.md)", "",
        "历史来源按截止日过滤，实际保存时间单独记录。这是历史重建；本文件不改变ETF可用池或策略预算。", "",
    ]
    (root / "README.md").write_text("\n".join(parts), encoding="utf-8")
    print(json.dumps({"universe_id": summary["universe_id"], "selection_groups": len(g)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
