# Fund Purchase Limit Task - Learnings

## Date: 2026-03-23

## Successful Approaches

### 1. Pattern Following
- Following the existing `akshare_fund_cf_em.py` pattern worked well
- Inheriting from `AkShareTask` provided most of the infrastructure
- Using `@task_register()` decorator simplified registration

### 2. Schema Design
- Using composite primary key `["fund_code", "snapshot_date"]` correctly enforces one-row-per-fund-per-day
- Indexing both columns separately supports both fund-history and daily-snapshot query patterns
- Including `update_time` (auto-managed by base class) is essential for incremental sync

### 3. Normalization Strategy
- Handling unlimited/blank values at the `process_data()` level keeps the logic centralized
- Using `pd.to_numeric(errors='coerce')` followed by large-number check (≥1e9) covers most edge cases:
  - Empty strings → NaN
  - Text like "无限制" → NaN
  - Large numbers (unlimited indicator) → NaN
  - Normal values preserved

### 4. Testing Strategy
- Creating dedicated fixture module enabled reusable test data across multiple test files
- Parametrized pytest fixtures allowed testing multiple scenarios with same test logic
- Mocking database connection (`_MockDB`) avoided real database dependencies

## Conventions Established

### Column Naming
- Chinese source columns mapped to English snake_case
- Keep semantic meaning: `日累计限定金额` → `daily_limit_amount`
- Preserve status fields: `申购状态` → `purchase_status`

### Date Handling
- Snapshot date uses `datetime.now().strftime("%Y-%m-%d")` format
- All dates stored as DATE type (not DATETIME) for daily granularity

### Null Semantics
- `NULL` in `daily_limit_amount` means "unlimited/no limit"
- This is distinct from `purchase_status` which can be "开放申购" even with a limit

## Gotchas Encountered

1. **Source API Returns Wide Data**: `fund_purchase_em()` returns 12+ columns, need to filter to schema columns only
2. **Type Conversion Order**: Must call `super().process_data()` first (applies transformations), then do custom normalization
3. **Test Isolation**: Tests must not depend on real API calls - always use fixtures

## Reusable Components

### Fixture Pattern
```python
@pytest.fixture
def akshare_normal_limits():
    return pd.DataFrame({...})
```

### Mock DB Pattern
```python
class _MockDB:
    async def fetch(self, query, *args, **kwargs):
        return self._rows
```

## Performance Notes

- Single-batch task (all funds in one API call) is efficient for snapshot data
- No pagination needed for ~10,000 funds
- Daily update frequency is sufficient (limits don't change intraday typically)
