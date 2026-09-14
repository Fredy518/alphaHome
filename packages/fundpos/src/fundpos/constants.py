"""SW2021 L1 identifiers. Never map disclosure industries by renaming them."""

SW_INDUSTRIES = {
    "801010.SI": "农林牧渔",
    "801030.SI": "基础化工",
    "801040.SI": "钢铁",
    "801050.SI": "有色金属",
    "801080.SI": "电子",
    "801110.SI": "家用电器",
    "801120.SI": "食品饮料",
    "801130.SI": "纺织服饰",
    "801140.SI": "轻工制造",
    "801150.SI": "医药生物",
    "801160.SI": "公用事业",
    "801170.SI": "交通运输",
    "801180.SI": "房地产",
    "801200.SI": "商贸零售",
    "801210.SI": "社会服务",
    "801230.SI": "综合",
    "801710.SI": "建筑材料",
    "801720.SI": "建筑装饰",
    "801730.SI": "电力设备",
    "801740.SI": "国防军工",
    "801750.SI": "计算机",
    "801760.SI": "传媒",
    "801770.SI": "通信",
    "801780.SI": "银行",
    "801790.SI": "非银金融",
    "801880.SI": "汽车",
    "801890.SI": "机械设备",
    "801950.SI": "煤炭",
    "801960.SI": "石油石化",
    "801970.SI": "环保",
    "801980.SI": "美容护理",
}
SW_CODES = tuple(SW_INDUSTRIES)
ASSETS = ("cash", "bond", "hk", *SW_CODES)
ASSET_NAMES = {"cash": "现金", "bond": "债券", "hk": "港股", **SW_INDUSTRIES}
CATEGORIES = ("普通股票型", "偏股混合", "灵活配置")
CATEGORY_BOUNDS = {
    "普通股票型": (0.8, 0.95),
    "偏股混合": (0.6, 0.95),
    "灵活配置": (0, 0.95),
    # Classification approximation only. Historical fund-contract bounds remain
    # authoritative when their announcement/effective dates are verified.
    "增强指数型": (0.8, 0.95),
}

# v3 keeps the equity estimator's 34-column contract intact and defines the
# fixed-income-plus model separately.  Convertible bonds are never counted as
# stock exposure, even when their underlying shares have an SW classification.
CLASSIFICATION_CODES = {
    "TSJJ020106": "普通股票型",
    "TSJJ020306": "偏股混合",
    "TSJJ020303": "灵活配置",
    "TSJJ020301": "平衡混合",
    "TSJJ020302": "偏债混合",
    "TSJJ020206": "普通债基",
    "TSJJ020202": "可转债债基",
    "TSJJ020103": "增强指数型",
}
EXPANSION_CATEGORIES = tuple(CLASSIFICATION_CODES.values())
FIXED_INCOME_PLUS_CATEGORIES = ("平衡混合", "偏债混合", "普通债基", "可转债债基", "灵活配置")

FIXED_INCOME_ASSETS = (
    "cash",
    "rate_short",
    "rate_long",
    "credit_short",
    "credit_long",
    "convertible_bond",
    "hk",
    *SW_CODES,
)
FIXED_INCOME_FACTOR_COLUMNS = (*FIXED_INCOME_ASSETS, "financing_cost")
FIXED_INCOME_OUTPUTS = (
    "cash",
    "ordinary_bond",
    "convertible_bond",
    "financing",
    "hk",
    *SW_CODES,
)

# Used only to reconcile a reported CSRC L1 label, never to select a SW industry.
DISCLOSURE_GROUPS = {
    "农、林、牧、渔业": "A",
    "采矿业": "B",
    "制造业": "C",
    "电力、热力、燃气及水生产和供应业": "D",
    "建筑业": "E",
    "批发和零售业": "F",
    "交通运输、仓储和邮政业": "G",
    "住宿和餐饮业": "H",
    "信息传输、软件和信息技术服务业": "I",
    "金融业": "J",
    "房地产业": "K",
    "租赁和商务服务业": "L",
    "科学研究和技术服务业": "M",
    "水利、环境和公共设施管理业": "N",
    "居民服务、修理和其他服务业": "O",
    "教育": "P",
    "卫生和社会工作": "Q",
    "文化、体育和娱乐业": "R",
    "综合": "S",
}
