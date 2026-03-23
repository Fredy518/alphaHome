# Fund Purchase Limit Snapshot Task Plan

## TL;DR

> **Quick Summary**: Add a new AkShare-based fund purchase-limit ingestion task backed by `fund_purchase_em()`, storing daily historical snapshots so limit changes remain queryable over time.
>
> **Deliverables**:
> - New AkShare fund purchase-limit task in `alphahome/fetchers/tasks/fund/`
> - Snapshot table schema, field mapping, validation, and task registration
> - Automated tests for mapping, normalization, and snapshot/idempotent behavior
>
> **Estimated Effort**: Medium
> **Parallel Execution**: YES - 2 implementation waves + final verification
> **Critical Path**: T1 -> T2 -> T4 -> T8 -> F1-F4

---

## Context

### Original Request
Design and land a data task for fund purchase limit data using AkShare.

### Interview Summary
**Key Discussions**:
- Scope is full landing in repository, not design-only.
- Automated tests should be added after implementation.
- Storage strategy is **historical daily snapshots**, not current-state overwrite.

**Research Findings**:
- AkShare exposes the needed source via `fund_purchase_em()` with field `日累计限定金额`.
- Existing repository pattern for an AkShare fund task lives at `alphahome/fetchers/tasks/fund/akshare_fund_cf_em.py:31`.
- AkShare task base behavior is defined in `alphahome/fetchers/sources/akshare/akshare_task.py:28`.
- Fund task exports are centralized in `alphahome/fetchers/tasks/fund/__init__.py:1`.
- Test infrastructure exists via `pyproject.toml:58` and `pytest.ini:1`.

### Metis Review
**Identified Gaps** (addressed in this plan):
- Primary key and storage mode must be explicit because source API returns a current snapshot.
- Unlimited/empty limit normalization must be defined instead of assumed numeric.
- Scope creep must be blocked: no monitoring, derived views, or redundant metadata work.

---

## Work Objectives

### Core Objective
Introduce a production-ready AlphaHome fetcher task that ingests AkShare fund purchase-limit data into a historical snapshot table, preserving daily state changes with deterministic mapping, validation, and tests.

### Concrete Deliverables
- New task file for AkShare fund purchase-limit ingestion
- New snapshot table design under `akshare` schema with explicit primary keys and indexes
- Registration/export wiring so the task is discoverable by the task system
- Unit/integration-style tests covering mapping, normalization, and repeat-run semantics

### Definition of Done
- [ ] Task class is importable from fund task package
- [ ] Schema definition includes snapshot date in primary key design
- [ ] Source fields `基金代码` / `申购状态` / `日累计限定金额` are mapped and normalized
- [ ] Tests for mapping, unlimited/null handling, and same-day idempotence pass

### Must Have
- Historical daily snapshot storage
- Clear normalization rule for unlimited or unavailable limit values (`daily_limit_amount = NULL`, with status retained in `purchase_status`)
- Idempotent same-day reruns without duplicate snapshot rows
- No dependency on manual user verification

### Must NOT Have (Guardrails)
- No alerting, dashboard, or monitoring scope
- No derived view/materialized view creation
- No redundant expansion into unrelated fund metadata domains
- No modification of unrelated existing fund tasks beyond required registration/export hooks

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — all acceptance criteria are agent-executable.

### Test Decision
- **Infrastructure exists**: YES
- **Automated tests**: Tests-after
- **Framework**: `pytest`

### QA Policy
Every implementation task below includes agent-executed QA scenarios. Evidence is saved under `.sisyphus/evidence/`.

- **Library/Module**: Use Python/pytest via Bash to import task classes, inspect schema defs, and run focused tests
- **Data task behavior**: Use mocked AkShare responses in tests; use dry-run style task execution where feasible
- **Failure cases**: Explicitly test missing/empty/"unlimited" limit values and duplicate same-day ingestion inputs

---

## Execution Strategy

### Parallel Execution Waves

```text
Wave 1 (Start immediately - contract, schema, scaffold, tests prep):
├── T1: Verify source contract and normalization rules [quick]
├── T2: Design snapshot schema and key/index strategy [quick]
├── T3: Create task scaffold and base field mapping [quick]
├── T4: Add registration/export wiring [quick]
└── T5: Build reusable mocked test fixtures for AkShare response shapes [quick]

Wave 2 (After Wave 1 - behavior hardening and verification):
├── T6: Implement normalization and snapshot-date enrichment [unspecified-high]
├── T7: Implement validations and same-day idempotent behavior [unspecified-high]
├── T8: Add unit tests for mapping and edge cases [quick]
├── T9: Add task execution/integration-style test coverage [quick]
└── T10: Run focused verification suite and fix task-level issues [unspecified-high]

Wave FINAL (After all implementation tasks - independent review, 4 parallel):
├── F1: Plan compliance audit (oracle)
├── F2: Code quality review (unspecified-high)
├── F3: Real QA execution (unspecified-high)
└── F4: Scope fidelity check (deep)

Critical Path: T1 -> T2 -> T6 -> T9 -> T10 -> F1-F4
Parallel Speedup: ~55% faster than sequential
Max Concurrent: 5
```

### Dependency Matrix

- **T1**: blocked by none -> blocks T2, T3, T5, T6
- **T2**: blocked by T1 -> blocks T6, T7, T9
- **T3**: blocked by T1 -> blocks T4, T6, T8
- **T4**: blocked by T3 -> blocks T9
- **T5**: blocked by T1 -> blocks T8, T9
- **T6**: blocked by T1, T2, T3 -> blocks T7, T8, T9, T10
- **T7**: blocked by T2, T6 -> blocks T10
- **T8**: blocked by T3, T5, T6 -> blocks T10
- **T9**: blocked by T2, T4, T5, T6 -> blocks T10
- **T10**: blocked by T6, T7, T8, T9 -> blocks F1-F4

### Agent Dispatch Summary

- **Wave 1**: T1-T5 -> `quick`
- **Wave 2**: T6-T7 -> `unspecified-high`, T8-T9 -> `quick`, T10 -> `unspecified-high`
- **FINAL**: F1 -> `oracle`, F2 -> `unspecified-high`, F3 -> `unspecified-high`, F4 -> `deep`

---

## TODOs

- [ ] T1. Verify source contract and normalization rules

  **What to do**:
  - Inspect `ak.fund_purchase_em()` return columns and document the exact fields required for this task.
  - Confirm how `日累计限定金额` appears for normal limits, empty values, and unlimited/no-limit cases.
  - Lock the canonical normalization rule for `purchase_status`, `daily_limit_amount`, and any raw-text backup field if needed; default to `daily_limit_amount = NULL` for unlimited/blank cases.

  **Must NOT do**:
  - Do not add fallback data sources.
  - Do not start schema or task implementation before the source-field contract is written down.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: focused inspection/documentation of one API contract and one field family.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `playwright`: no browser workflow is needed.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T2, T3, T4, T5)
  - **Blocks**: T2, T3, T5, T6
  - **Blocked By**: None

  **References**:
  - `akshare_repo/akshare/fund/fund_em.py:32` - Source implementation of `fund_purchase_em`, including column names and field order.
  - `alphahome/fetchers/sources/akshare/akshare_task.py:138` - Standard AkShare fetch path the new task will rely on after the source contract is known.

  **Acceptance Criteria**:
  - [ ] Plan-level contract lists all required source columns for the task.
  - [ ] Normalization rule explicitly states how unlimited/blank limit values are stored.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - source columns are confirmed
    Tool: Bash (python)
    Preconditions: Local environment can import AkShare source code or inspect repository copy
    Steps:
      1. Read/inspect the `fund_purchase_em` implementation from `akshare_repo/akshare/fund/fund_em.py`.
      2. Extract the returned column list and record the presence of `基金代码`, `申购状态`, and `日累计限定金额`.
      3. Save the extracted contract summary to `.sisyphus/evidence/task-t1-source-contract.txt`.
    Expected Result: Evidence file lists the required fields and their exact source names.
    Failure Indicators: Any required source field is absent or ambiguous.
    Evidence: .sisyphus/evidence/task-t1-source-contract.txt

  Scenario: Failure path - unlimited/blank rule is unresolved
    Tool: Bash (python)
    Preconditions: Contract review completed
    Steps:
      1. Attempt to map a sample unlimited/blank value without a documented normalization rule.
      2. Confirm the task is blocked until the normalization rule is written into the implementation notes/tests.
    Expected Result: Missing normalization rule is flagged explicitly instead of silently guessed.
    Evidence: .sisyphus/evidence/task-t1-normalization-gap.txt
  ```

  **Commit**: NO

- [ ] T2. Design snapshot schema and key/index strategy

  **What to do**:
  - Define the target table name, snapshot date column, primary keys, and indexes for historical snapshots.
  - Choose the stored field set: essential purchase-limit fields only, plus minimal metadata required to interpret a snapshot.
  - Specify how same-day reruns behave (upsert on snapshot key rather than duplicate inserts).

  **Must NOT do**:
  - Do not design a current-state-only table.
  - Do not add foreign keys, derived columns, or analytics-oriented schema extensions.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: bounded schema design based on one source contract and one task pattern.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `git-master`: no git/history work is needed.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T1, T3, T4, T5)
  - **Blocks**: T6, T7, T9
  - **Blocked By**: T1

  **References**:
  - `alphahome/fetchers/tasks/fund/akshare_fund_cf_em.py:40` - Example of task-level schema, index, and validation declarations for an AkShare fund task.
  - `alphahome/fetchers/tasks/stock/akshare_stock_limitup_reason.py:56` - Example of explicit index design and task schema for snapshot-like source data.

  **Acceptance Criteria**:
  - [ ] Primary key design includes snapshot date and fund identifier.
  - [ ] Index set supports querying by fund and by snapshot date.
  - [ ] Same-day rerun behavior is explicitly documented as idempotent.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - schema matches snapshot design
    Tool: Bash (python)
    Preconditions: Proposed schema_def and primary_keys are implemented in the task class
    Steps:
      1. Import the new task class and inspect `schema_def`, `primary_keys`, and `indexes`.
      2. Assert `snapshot_date` is present in both the schema and the primary key list.
      3. Save the inspected values to `.sisyphus/evidence/task-t2-schema-check.txt`.
    Expected Result: Snapshot schema is historical, not current-state overwrite.
    Failure Indicators: Missing `snapshot_date`, missing fund identifier, or no date/fund index.
    Evidence: .sisyphus/evidence/task-t2-schema-check.txt

  Scenario: Failure path - same-day duplicate key risk
    Tool: Bash (python)
    Preconditions: Primary keys defined
    Steps:
      1. Simulate two rows with the same `fund_code` and `snapshot_date`.
      2. Verify the plan/test notes require upsert or replacement behavior instead of duplicate inserts.
    Expected Result: Duplicate same-day snapshots are treated as idempotent collisions, not distinct rows.
    Evidence: .sisyphus/evidence/task-t2-idempotence-note.txt
  ```

  **Commit**: NO

- [ ] T3. Create task scaffold and base field mapping

  **What to do**:
  - Create the new AkShare fund task file under `alphahome/fetchers/tasks/fund/`.
  - Define task metadata (`domain`, `name`, `description`, `table_name`, `api_name`) and the initial `column_mapping`.
  - Reuse the established single-batch AkShare task shape because the source returns a current all-fund snapshot.

  **Must NOT do**:
  - Do not copy unrelated transformation logic from other tasks.
  - Do not leave source-field names half-mapped; every stored column must have a deliberate origin.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: one-file scaffold following an existing repository pattern.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `frontend-ui-ux`: irrelevant to backend data task work.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T1, T2, T4, T5)
  - **Blocks**: T4, T6, T8
  - **Blocked By**: T1

  **References**:
  - `alphahome/fetchers/tasks/fund/akshare_fund_cf_em.py:31` - Closest AkShare fund task scaffold, including metadata, `api_name`, `column_mapping`, and `schema_def` placement.
  - `alphahome/fetchers/sources/akshare/akshare_task.py:61` - Constructor and base behavior the new task inherits.

  **Acceptance Criteria**:
  - [ ] New task file exists in the fund task package.
  - [ ] `api_name` targets `fund_purchase_em`.
  - [ ] `column_mapping` includes all persisted fields derived from the verified source contract.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - task module imports cleanly
    Tool: Bash (python)
    Preconditions: New task file created
    Steps:
      1. Run `python -c "from alphahome.fetchers.tasks.fund.akshare_fund_purchase_em import AkShareFundPurchaseEmTask; print('IMPORT_OK')"`.
      2. Inspect `api_name` and `column_mapping` on the imported class.
      3. Save output to `.sisyphus/evidence/task-t3-import.txt`.
    Expected Result: Import succeeds and `api_name` equals `fund_purchase_em`.
    Failure Indicators: ImportError, missing class attribute, or incomplete mapping.
    Evidence: .sisyphus/evidence/task-t3-import.txt

  Scenario: Failure path - missing required source mapping
    Tool: Bash (python)
    Preconditions: Task class importable
    Steps:
      1. Compare required source columns from T1 against the task's `column_mapping`.
      2. Fail if `基金代码`, `申购状态`, or `日累计限定金额` has no destination field.
    Expected Result: Missing mappings are caught automatically.
    Evidence: .sisyphus/evidence/task-t3-mapping-check.txt
  ```

  **Commit**: NO

- [ ] T4. Add registration and package export wiring

  **What to do**:
  - Ensure the new task uses the standard registration decorator and is exported through the fund task package.
  - Update the relevant `__init__` export surface so task discovery/import paths are stable.
  - Keep wiring minimal and limited to the new task.

  **Must NOT do**:
  - Do not reorganize the fund task package.
  - Do not rename unrelated task exports.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: small, localized package-wiring work touching 1-2 files.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `skill-creator`: unrelated to repository code wiring.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T1, T2, T3, T5)
  - **Blocks**: T9
  - **Blocked By**: T3

  **References**:
  - `alphahome/fetchers/tasks/fund/__init__.py:1` - Existing export list that must include the new task.
  - `alphahome/fetchers/tasks/fund/akshare_fund_cf_em.py:31` - Example of `@task_register()` usage on a fund task.

  **Acceptance Criteria**:
  - [ ] Task is importable from `alphahome.fetchers.tasks.fund`.
  - [ ] Registration decorator is present on the task class.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - package-level import works
    Tool: Bash (python)
    Preconditions: `__init__.py` updated
    Steps:
      1. Run `python -c "from alphahome.fetchers.tasks.fund import AkShareFundPurchaseEmTask; print('PACKAGE_IMPORT_OK')"`.
      2. Save output to `.sisyphus/evidence/task-t4-package-import.txt`.
    Expected Result: Package import succeeds without touching unrelated tasks.
    Failure Indicators: ImportError or missing symbol in `__all__`.
    Evidence: .sisyphus/evidence/task-t4-package-import.txt

  Scenario: Failure path - registration decorator omitted
    Tool: Bash (python)
    Preconditions: Task class exists
    Steps:
      1. Inspect the task class source or registration metadata.
      2. Fail if the standard `@task_register()` hook is absent.
    Expected Result: Discovery wiring is enforced before integration tests run.
    Evidence: .sisyphus/evidence/task-t4-registration-check.txt
  ```

  **Commit**: NO

- [ ] T5. Build reusable mocked test fixtures for AkShare response shapes

  **What to do**:
  - Create representative mocked DataFrame fixtures for normal, blank-limit, unlimited-limit, and duplicate-row cases.
  - Keep fixtures minimal but realistic enough to drive deterministic unit tests.
  - Centralize fixture shape so later tests do not handcraft inconsistent source payloads.

  **Must NOT do**:
  - Do not make real network calls inside unit tests.
  - Do not build oversized fixtures that obscure the normalization cases.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: focused test data setup with no broad architectural work.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `playwright`: not applicable.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T1, T2, T3, T4)
  - **Blocks**: T8, T9
  - **Blocked By**: T1

  **References**:
  - `tests/unit/test_tinysoft_fund_minute_task.py:7` - Example of using lightweight dummy objects and focused DataFrame payloads in task tests.
  - `pyproject.toml:58` - Confirms pytest-based test environment already exists.

  **Acceptance Criteria**:
  - [ ] Fixture set covers normal numeric limit, unlimited/blank limit, and duplicate same-day snapshot rows.
  - [ ] Unit tests can import fixtures without network/database requirements beyond local stubs.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - fixtures cover all critical source shapes
    Tool: Bash (python)
    Preconditions: Test fixture module created
    Steps:
      1. Import the fixture builders.
      2. Instantiate each fixture variant and print their columns/row counts.
      3. Save output to `.sisyphus/evidence/task-t5-fixtures.txt`.
    Expected Result: Fixtures exist for normal, unlimited/blank, and duplicate-row cases.
    Failure Indicators: Missing edge-case fixture or inconsistent columns across fixtures.
    Evidence: .sisyphus/evidence/task-t5-fixtures.txt

  Scenario: Failure path - unit tests accidentally depend on live API
    Tool: Bash (python)
    Preconditions: Fixtures and tests created
    Steps:
      1. Run the targeted unit tests with AkShare API fully mocked/stubbed.
      2. Confirm no outbound network call is attempted.
    Expected Result: Tests execute using only local fixtures and stubs.
    Evidence: .sisyphus/evidence/task-t5-no-network.txt
  ```

  **Commit**: NO

- [ ] T6. Implement normalization and snapshot-date enrichment

  **What to do**:
  - Add transformation logic for `daily_limit_amount`, including unlimited/blank normalization.
  - Enrich each fetched row with a deterministic snapshot date for historical storage.
  - Preserve enough source semantics so analysts can distinguish open purchase, restricted purchase, and suspended purchase cases.

  **Must NOT do**:
  - Do not silently coerce ambiguous source strings into incorrect numeric values.
  - Do not omit the snapshot date from persisted rows.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: this is the highest-risk logic area because source semantics and storage behavior meet here.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `ultrabrain`: not needed; logic is non-trivial but bounded.

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: T7, T8, T9, T10
  - **Blocked By**: T1, T2, T3

  **References**:
  - `alphahome/fetchers/tasks/fund/akshare_fund_cf_em.py:162` - Example of overriding `process_data` for task-specific post-fetch enrichment.
  - `alphahome/fetchers/tasks/stock/akshare_stock_limitup_reason.py:127` - Example of task-level data normalization after base processing.

  **Acceptance Criteria**:
  - [ ] Persisted rows include `snapshot_date`.
  - [ ] Unlimited/blank limit values are normalized per T1's contract decision.
  - [ ] Purchase-status semantics remain queryable after normalization.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - numeric limits and snapshot date are normalized
    Tool: Bash (pytest)
    Preconditions: Task process_data logic implemented, fixture module available
    Steps:
      1. Run the focused normalization test against a fixture with numeric `日累计限定金额` values.
      2. Assert `snapshot_date` is populated and `daily_limit_amount` is numeric.
      3. Save pytest output to `.sisyphus/evidence/task-t6-normalization.txt`.
    Expected Result: Numeric limit rows are preserved with the expected snapshot date.
    Failure Indicators: Missing snapshot date, failed conversion, or wrong destination column names.
    Evidence: .sisyphus/evidence/task-t6-normalization.txt

  Scenario: Failure path - unlimited/blank value handling
    Tool: Bash (pytest)
    Preconditions: Edge-case fixture available
    Steps:
      1. Run the focused test using blank/unlimited source values.
      2. Assert the normalized field becomes the agreed canonical representation without raising unexpected exceptions.
    Expected Result: Unlimited/blank cases are stored consistently.
    Evidence: .sisyphus/evidence/task-t6-unlimited.txt
  ```

  **Commit**: NO

- [ ] T7. Implement validations and same-day idempotent behavior

  **What to do**:
  - Add validations for required fields and sane row-level constraints.
  - Ensure same-day reruns do not create duplicate snapshot rows for the same fund/date key.
  - Align task behavior with repository insert/upsert expectations for snapshot tables.

  **Must NOT do**:
  - Do not treat same-day reruns as separate history points.
  - Do not overfit validations to speculative business rules beyond the verified source contract.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: combines storage integrity with task-runtime behavior.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `git-master`: unrelated.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with T8, T9)
  - **Blocks**: T10
  - **Blocked By**: T2, T6

  **References**:
  - `alphahome/fetchers/tasks/fund/akshare_fund_cf_em.py:98` - Example of task-level validations structure.
  - `alphahome/common/task_system/base_task.py:705` - Central task system context for how schema/data-source identity is interpreted during persistence.

  **Acceptance Criteria**:
  - [ ] Required-field validations exist for fund identifier and snapshot date.
  - [ ] Same-day duplicate rows are prevented by key strategy and verified by tests.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - same-day rerun is idempotent
    Tool: Bash (pytest)
    Preconditions: Storage behavior tests implemented
    Steps:
      1. Execute a focused test that ingests the same fixture twice for the same `snapshot_date`.
      2. Assert only one logical row per `fund_code + snapshot_date` remains after the rerun.
    Expected Result: Same-day re-execution updates/replaces instead of duplicating rows.
    Failure Indicators: Duplicate row count or unique-key conflict left unhandled.
    Evidence: .sisyphus/evidence/task-t7-idempotence.txt

  Scenario: Failure path - missing required key fields
    Tool: Bash (pytest)
    Preconditions: Validation rules implemented
    Steps:
      1. Run a fixture missing `fund_code` or `snapshot_date`.
      2. Assert validation failure is surfaced clearly.
    Expected Result: Invalid rows are rejected or reported deterministically.
    Evidence: .sisyphus/evidence/task-t7-validation.txt
  ```

  **Commit**: NO

- [ ] T8. Add unit tests for mapping and edge cases

  **What to do**:
  - Add unit tests for column mapping, normalized limit values, and process-data output shape.
  - Cover the most important semantics: restricted purchase with numeric limit, suspended purchase, unlimited/blank limit, and duplicate same-day source rows.
  - Keep tests narrow, fast, and fully mocked.

  **Must NOT do**:
  - Do not rely on the live AkShare API.
  - Do not bury multiple behaviors inside one oversized test.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: narrow test creation once fixture and transformation contracts are settled.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `writing`: not a docs task.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with T7, T9)
  - **Blocks**: T10
  - **Blocked By**: T3, T5, T6

  **References**:
  - `tests/unit/test_tinysoft_fund_minute_task.py:29` - Existing async/unit test style with local stubs.
  - `pyproject.toml:94` - Pytest discovery conventions to follow for new test files.

  **Acceptance Criteria**:
  - [ ] Focused unit test file exists for the new task.
  - [ ] Tests cover mapping, unlimited handling, and duplicate same-day semantics.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - focused unit tests pass
    Tool: Bash (pytest)
    Preconditions: Unit test file added
    Steps:
      1. Run `pytest` against the new test file only.
      2. Assert all targeted mapping/normalization tests pass.
      3. Save output to `.sisyphus/evidence/task-t8-pytest.txt`.
    Expected Result: New task unit tests pass in isolation.
    Failure Indicators: Any failing assertion around mapping, normalization, or snapshot date.
    Evidence: .sisyphus/evidence/task-t8-pytest.txt

  Scenario: Failure path - regression when source columns drift
    Tool: Bash (pytest)
    Preconditions: Mapping assertions implemented
    Steps:
      1. Run a test with one required source column removed from the fixture.
      2. Assert the code/test fails clearly instead of silently producing partial output.
    Expected Result: Source-contract drift is detected immediately.
    Evidence: .sisyphus/evidence/task-t8-contract-drift.txt
  ```

  **Commit**: NO

- [ ] T9. Add task execution and snapshot persistence verification

  **What to do**:
  - Add an integration-style test that exercises the task through its task interface using stubs/mocks where appropriate.
  - Verify the persisted/output shape matches the historical snapshot contract, including repeated same-day execution.
  - Ensure package-level import and task discovery paths remain stable after wiring changes.

  **Must NOT do**:
  - Do not expand into full end-to-end database environment provisioning if a local stub or existing test helper suffices.
  - Do not make this test depend on unrelated fund tables beyond the minimum required context.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: focused verification of one task's execution path using existing test patterns.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `dev-browser`: not relevant.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with T7, T8)
  - **Blocks**: T10
  - **Blocked By**: T2, T4, T5, T6

  **References**:
  - `alphahome/fetchers/tasks/fund/__init__.py:1` - Package export surface that the test should import through.
  - `tests/unit/test_tinysoft_fund_minute_task.py:55` - Example of verifying `process_data` output shape against a synthetic payload.

  **Acceptance Criteria**:
  - [ ] Integration-style verification covers task import, processing path, and snapshot semantics.
  - [ ] Same-day rerun behavior is asserted in a task-level test, not only in unit helpers.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - task-level execution test passes
    Tool: Bash (pytest)
    Preconditions: Integration-style test file added
    Steps:
      1. Run the targeted task execution test file/case.
      2. Assert the task processes a mocked AkShare payload into snapshot-shaped rows.
      3. Save output to `.sisyphus/evidence/task-t9-task-exec.txt`.
    Expected Result: Task-level execution succeeds through the expected repository interfaces.
    Failure Indicators: Import path failure, shape mismatch, or snapshot-date omission.
    Evidence: .sisyphus/evidence/task-t9-task-exec.txt

  Scenario: Failure path - duplicate same-day execution
    Tool: Bash (pytest)
    Preconditions: Duplicate execution test implemented
    Steps:
      1. Execute the same task payload twice for the same snapshot date.
      2. Assert duplicates are not persisted/represented as separate same-day history rows.
    Expected Result: Historical storage remains one-row-per-fund-per-day.
    Evidence: .sisyphus/evidence/task-t9-duplicate-exec.txt
  ```

  **Commit**: NO

- [ ] T10. Run focused verification suite and fix task-level issues

  **What to do**:
  - Run all new focused tests plus import checks for the new task.
  - Fix any issues found in mapping, normalization, export wiring, or idempotent snapshot behavior.
  - Capture the final focused evidence set needed by the final verification wave.

  **Must NOT do**:
  - Do not broaden the change set to unrelated tests unless failures prove the new task broke shared behavior.
  - Do not leave flaky or partially mocked tests unresolved.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: final tightening pass requires judgement across task code and tests.
  - **Skills**: `[]`
  - **Skills Evaluated but Omitted**:
    - `oracle`: reserved for the independent final review wave.

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: F1, F2, F3, F4
  - **Blocked By**: T6, T7, T8, T9

  **References**:
  - `pytest.ini:8` - Standard pytest CLI options already used by the repository.
  - `pyproject.toml:94` - Secondary pytest configuration reference and test discovery baseline.

  **Acceptance Criteria**:
  - [ ] Package import check passes for the new task.
  - [ ] Focused pytest suite for the new task passes cleanly.
  - [ ] Evidence files exist for the key scenarios from T6-T9.

  **QA Scenarios**:
  ```text
  Scenario: Happy path - focused verification suite passes cleanly
    Tool: Bash
    Preconditions: Task and tests implemented
    Steps:
      1. Run `python -c "from alphahome.fetchers.tasks.fund import AkShareFundPurchaseEmTask; print('IMPORT_OK')"`.
      2. Run `pytest` for the new task's focused test file(s).
      3. Save combined output to `.sisyphus/evidence/task-t10-focused-suite.txt`.
    Expected Result: Import succeeds and all focused tests pass.
    Failure Indicators: Any focused test failure, import error, or missing evidence artifact.
    Evidence: .sisyphus/evidence/task-t10-focused-suite.txt

  Scenario: Failure path - unresolved regression remains
    Tool: Bash
    Preconditions: Focused suite executed
    Steps:
      1. Search the focused suite output for `FAILED`, `ERROR`, or traceback markers.
      2. Fail the task if any remain unresolved.
    Expected Result: No unresolved focused regressions remain before final review.
    Evidence: .sisyphus/evidence/task-t10-regression-scan.txt
  ```

  **Commit**: YES
  - Message: `feat(akshare-fund): add purchase limit snapshot task`
  - Files: `alphahome/fetchers/tasks/fund/akshare_fund_purchase_em.py`, `alphahome/fetchers/tasks/fund/__init__.py`, `tests/unit/test_akshare_fund_purchase_em_task.py`
  - Pre-commit: `pytest tests/unit/ -k "fund_purchase or akshare_fund_purchase" -v`

---

## Final Verification Wave

- [ ] F1. **Plan Compliance Audit** — `oracle`
  Verify every deliverable and guardrail against the final diff and evidence files. Reject if historical snapshot behavior or source-field mapping is missing.

- [ ] F2. **Code Quality Review** — `unspecified-high`
  Run focused checks (`pytest`, import checks, any repo-standard lint/type checks relevant to touched files). Reject on dead code, sloppy normalization, or unexplained magic values.

- [ ] F3. **Real QA Execution** — `unspecified-high`
  Execute every QA scenario from T1-T10, capture outputs/screenshots/logs into `.sisyphus/evidence/final-qa/`, and verify repeat-run snapshot semantics.

- [ ] F4. **Scope Fidelity Check** — `deep`
  Confirm the work only adds the fund purchase-limit snapshot task and minimal required wiring/tests, with no monitoring, analytics, or unrelated schema work.

---

## Commit Strategy

- **1**: `feat(akshare-fund): add purchase limit snapshot task` — task file + package export + focused tests

---

## Success Criteria

### Verification Commands
```bash
python -c "from alphahome.fetchers.tasks.fund import AkShareFundPurchaseEmTask; print('IMPORT_OK')"
pytest tests/unit/ -k "fund_purchase or akshare_fund_purchase" -v
```

### Final Checklist
- [ ] Historical snapshot storage is implemented
- [ ] Same-day reruns do not create duplicate rows
- [ ] Unlimited/empty limit values are normalized consistently
- [ ] All focused tests pass
