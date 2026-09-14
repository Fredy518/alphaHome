from fundpos.contract_evidence import extract_contract_constraints_from_pages


def contract_page(body: str) -> list[str]:
    return [f"测试基金基金合同投资范围投资组合比例{body}{'测试文本' * 3000}"]


def test_contract_parser_extracts_portfolio_constraints_and_denominators():
    result = extract_contract_constraints_from_pages(
        contract_page(
            "本基金投资于固定收益类资产的比例不低于基金资产的80%；"
            "其中，可转换债券投资比例不低于固定收益类资产的80%；"
            "权益类资产的比例不高于基金资产的20%。"
        )
    )
    assert result["document_constraint_verified"] is True
    assert result["bond_lower"] == 0.8
    assert result["bond_denominator"] == "fund_assets"
    assert result["cbond_lower"] == 0.8
    assert result["cbond_denominator"] == "fixed_income_assets"
    assert result["stock_upper"] == 0.2
    assert result["stock_denominator"] == "fund_assets"
    assert result["stock_scope"] == "equity_assets"
    assert result["nav_constraint_usable"] is False
    assert result["solver_constraint_status"] == (
        "verified_document_requires_denominator_model"
    )


def test_contract_parser_does_not_treat_single_issuer_cap_as_stock_allocation():
    result = extract_contract_constraints_from_pages(
        contract_page(
            "本基金不直接投资股票、权证等权益类资产。"
            "因可转换债券转股形成的股票，在其可上市交易后不超过10个交易日的时间内卖出。"
            "本基金投资于债券的比例不低于基金资产的80%；"
            "可转换债券的投资比例不低于非现金基金资产的80%；"
            "本基金持有一家公司发行的股票，其市值不超过基金资产净值的10%。"
        )
    )
    assert result["stock_upper"] is None
    assert result["direct_stock_allowed"] is False
    assert result["converted_stock_disposal_days"] == 10
    assert result["document_constraint_verified"] is True


def test_contract_parser_marks_only_effective_nav_constraint_as_directly_usable():
    result = extract_contract_constraints_from_pages(
        contract_page(
            "本基金投资于债券资产的比例不低于基金资产净值的60%；"
            "股票资产占基金资产净值的比例为0%-30%。"
            "本合同自2021年7月30日起正式生效。"
        )
    )
    assert result["stock_lower"] == 0
    assert result["stock_upper"] == 0.3
    assert result["stock_denominator"] == "fund_nav"
    assert result["effective_date"] == "2021-07-30"
    assert result["nav_constraint_usable"] is True
    assert result["solver_constraint_status"] == "direct_nav_constraint_usable"


def test_contract_parser_handles_cbond_parenthesis_and_exchangeable_bonds():
    result = extract_contract_constraints_from_pages(
        contract_page(
            "本基金投资于债券资产的比例不低于基金资产的80%；"
            "投资于可转换债券（含可分离交易可转债）及可交换债券的比例合计"
            "不低于非现金基金资产的80%。"
        )
    )
    assert result["cbond_lower"] == 0.8
    assert result["cbond_denominator"] == "non_cash_fund_assets"
    assert result["document_constraint_verified"] is True


def test_contract_parser_records_passive_breach_grace_period():
    result = extract_contract_constraints_from_pages(
        contract_page(
            "本基金投资于债券的比例不低于基金资产的80%。"
            "因证券市场波动、上市公司合并、基金规模变动等基金管理人之外的因素"
            "致使基金投资组合不符合上述投资比例的，基金管理人应当在十个交易日内"
            "进行调整。"
        )
    )
    assert result["portfolio_ratio_exception_days"] == 10
    assert result["portfolio_ratio_exception_page"] == 1
    assert result["point_in_time_hard_constraint_usable"] is False
    assert result["solver_constraint_status"] == (
        "verified_document_requires_denominator_and_compliance_state"
    )
