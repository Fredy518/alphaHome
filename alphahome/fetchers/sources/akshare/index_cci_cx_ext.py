"""财新新版公开图表接口，保持 AkShare index_cci_cx 的列与数值口径。"""

from datetime import date
from math import isfinite

import pandas as pd
import requests


def index_cci_cx() -> pd.DataFrame:
    """获取 CCI 全历史；变化值为相邻观测的涨跌幅（%）。

    官方页面 https://yun.ccxe.com.cn/dataindices/indices 使用 POST，
    month 为空表示全部历史。旧 /api/index/pro/cxIndexTrendInfo 已返回 HTML 404。
    新接口只有日期和点位，先在全历史上计算涨跌幅，再交由任务筛选增量窗口。
    """
    response = requests.post(
        "https://yun.ccxe.com.cn/dataindices/cci",
        data={"month": ""},
        timeout=(10, 30),
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError("财新 CCI 接口未返回 JSON，请检查源站接口是否变更") from exc

    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise ValueError("财新 CCI 接口返回非成功响应")
    series = payload.get("data")
    if not isinstance(series, dict):
        raise ValueError("财新 CCI 响应缺少历史数据")
    dates, values = series.get("month"), series.get("data")
    if (
        not isinstance(dates, list)
        or not isinstance(values, list)
        or not dates
        or len(dates) != len(values)
    ):
        raise ValueError("财新 CCI 历史日期和点位必须是非空、等长列表")

    data = pd.DataFrame({"日期": dates, "大宗商品指数": values})
    data["日期"] = pd.to_datetime(data["日期"], errors="raise").dt.date
    data["大宗商品指数"] = pd.to_numeric(data["大宗商品指数"], errors="raise")
    if (
        data["日期"].isna().any()
        or not (data["大宗商品指数"].map(isfinite) & data["大宗商品指数"].gt(0)).all()
    ):
        raise ValueError("财新 CCI 历史数据包含无效日期或点位")
    data = (
        data.drop_duplicates(subset=["日期"], keep="last")
        .sort_values("日期")
        .reset_index(drop=True)
    )
    # 只有完整历史的基期才定义涨跌幅为 0，避免截断响应产生伪造的首日值。
    if data.loc[0, "日期"] != date(2009, 1, 9) or data.loc[0, "大宗商品指数"] != 100:
        raise ValueError("财新 CCI 全历史缺少基期 2009-01-09（100点）")
    data["变化值"] = data["大宗商品指数"].pct_change(fill_method=None) * 100
    data.loc[0, "变化值"] = 0.0
    return data[["日期", "大宗商品指数", "变化值"]]
