CREATE TABLE IF NOT EXISTS fundpos.fund_scenario_exposure (
    run_id text NOT NULL,
    valuation_date date NOT NULL,
    product_id text NOT NULL,
    scenario_code text NOT NULL,
    asset_code text NOT NULL,
    denominator text NOT NULL,
    exposure numeric NOT NULL,
    is_primary boolean NOT NULL DEFAULT false,
    diagnostic_only boolean NOT NULL DEFAULT true,
    PRIMARY KEY (
        run_id,
        valuation_date,
        product_id,
        scenario_code,
        asset_code,
        denominator
    ),
    FOREIGN KEY (run_id, valuation_date, product_id)
        REFERENCES fundpos.fund_estimate(run_id, valuation_date, product_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_fund_scenario_product_date
ON fundpos.fund_scenario_exposure(product_id, valuation_date DESC, scenario_code);
