# AkShare Fund Tasks - Learnings

## T3: AkShareFundPurchaseEmTask Scaffold

### Pattern (from akshare_fund_cf_em.py)

All AkShare fund tasks follow a standard scaffold pattern:

1. **Metadata Block** (domain, name, description, table_name, data_source)
2. **Key Definitions** (primary_keys, date_column)
3. **API Config** (api_name, api_params)
4. **Column Mapping** (Chinese to English field names)
5. **Type Conversions** (transformations dict)
6. **Schema Definition** (schema_def with SQL types)
7. **Indexes** (for performance)
8. **Validations** (data quality checks)
9. **Methods**:
   - `get_batch_list()` - returns single batch for snapshot tasks
   - `process_data()` - column filtering + date injection

### fund_purchase_em Specifics

- **API**: Single batch snapshot API (no params needed)
- **Date Column**: snapshot_date (current date at fetch time)
- **Primary Keys**: [fund_code, snapshot_date] - unique pair
- **Field Mapping**: 6 fields (fund_code, fund_name, purchase_status, daily_limit_amount, redemption_status, latest_nav)
- **Schema**: Minimal schema with DATE type for snapshot
- **Skip Logic**: Uses `_should_skip_by_recent_update_time(max_age_days=1)` for daily snapshot

### Column Fields Available

Per AkShare fund_purchase_em API:
- 基金代码 -> fund_code
- 基金简称 -> fund_name  
- 申购状态 -> purchase_status
- 日累计限定金额 -> daily_limit_amount
- 赎回状态 -> redemption_status
- 最新净值 -> latest_nav

All mapped in column_mapping dict.

### process_data Implementation

1. Call parent `super().process_data()` for base transforms
2. Filter columns to schema_def keys (remove unmapped fields)
3. Add snapshot_date as current date if missing

### Inheritance Chain

AkShareFundPurchaseEmTask <- AkShareTask <- FetcherTask

Parent handles:
- prepare_params() - merges api_params with batch params
- fetch_batch() - calls API via self.api
- Data transformer - applies column_mapping, transformations
