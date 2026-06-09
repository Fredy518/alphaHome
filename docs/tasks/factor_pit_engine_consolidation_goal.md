# Goal 指令书：因子 / PIT 计算引擎归位重构

> 交付对象：codex（自主 goal 任务）
> 类型：大升级 / 重构 + 废弃清理
> 状态：待启动

---

## 1. 背景与问题（为什么做）

当前项目最大的技术债集中在「业务逻辑被困在脚本/研究目录、且大量重复 fork」：

### 1.1 P/G 因子逻辑——同源三份、已漂移
| 位置 | 规模 | 说明 |
| --- | --- | --- |
| `research/pgs_factor/processors/production_p_factor_calculator.py` | 949 行 | 研究侧主实现，依赖 `ResearchContext` |
| `research/pgs_factor/processors/production_g_factor_calculator.py` | 867 行 | 同上 |
| `scripts/production/factor_calculators/p_factor/production_p_factor_calculator.py` | 769 行 | 与 research 版**已漂移 ~875 行**，无 import 关联 |
| `alphahome/factors/`（`core/` `definitions/` `pipelines/` `utils/`） | **空骨架** | 4 个空 `__init__.py`，本该归位但从未填充 |

外加 `scripts/production/factor_calculators/` 下 **12+ 个近乎重复的并行 runner 变体**：
`calculate_*_for_specific_dates` / `*_parallel_by_quarter` / `*_parallel_by_year` /
`start_parallel_*` / `start_parallel_*_quarterly` / `production_*_runner` /
`batch_calculate_missing_factors` / `batch_calculate_recent_missing_factors`
（参数模式高度雷同：`--worker_id/--total_workers/--start_year/--end_year/--start_quarter/--end_quarter/--dates/--workers`）。

### 1.2 PIT 财务计算——同样三处重复
| 位置 | 规模 |
| --- | --- |
| `research/pit_data/`（`pit_income_quarterly_manager.py` 1732 行、`calculators/financial_indicators_calculator.py` 1049 行、`pit_balance_quarterly_manager.py` 910 行 等） | ~6000 行 |
| `scripts/production/data_updaters/pit/`（`pit_*_manager.py`、`calculators/financial_indicators_calculator.py`） | research 版的漂移副本 |

### 1.3 一次性脚本散落（96–341 天未动）
`scripts/analysis/*`、`scripts/maintenance/fix_*|backfill_*|recreate_*|migrate_db_name`、
`scripts/database/migrate_*|delete_g_factor_data`、根目录 `scripts/migrate_existing_tables_to_rawdata.py`、
`scripts/run_tinysoft_*`、`scripts/production/database/migrate_bse_code_mapping.py`。

---

## 2. 目标（做成什么样）

1. **逻辑归位**：把 P/G 因子与 PIT 财务指标的业务逻辑抽取、去重，沉淀到 `alphahome/` 包内（因子进 `alphahome/factors/`，PIT 进新建 `alphahome/factors/pit/` 或 `alphahome/pit/`，与现有 `common/task_system` 风格一致）。
2. **单一引擎**：用一个 **config 驱动的并行引擎**取代 12+ 个 runner 变体，统一时间维度（dates / quarter / year）、并行（worker_id/total_workers/workers）、增量检测与缺口回补语义。
3. **脚本瘦身**：`scripts/production/factor_calculators/` 与 `.../data_updaters/pit/` 退化为**薄入口**（仅解析参数 + 调用包内引擎），删除重复实现；保留对外命令行为兼容。
4. **废弃归档**：把第 1.3 节一次性脚本移入 `archive/`，并从 README / docs 摘除其入口说明。
5. **消除 fork 漂移**：research 与 scripts 两份重复实现合并为对包的单一引用，确保「只有一处真实实现」。

---

## 3. 范围边界

**纳入（In scope）**
- `alphahome/factors/`、（新）PIT 包、`scripts/production/factor_calculators/`、`scripts/production/data_updaters/pit/`
- `research/pgs_factor/`、`research/pit_data/` 中**可复用为引擎的核心逻辑**（抽取到包后，研究侧改为引用）
- README、`docs/architecture/system_overview.md`、`docs/tasks/` 相关说明同步更新

**不纳入（Out of scope / 非目标）**
- 不改因子/指标的**数值算法本身**（重构是搬运+去重，不是重算口径；任何口径变化必须显式标注并默认关闭）
- 不动 `alphahome/fetchers`、`alphahome/features`、`alphahome/gui` 的现有行为
- 不引入新的因子/指标、不做性能优化以外的「顺手改进」
- 不删除 `research/` 模板与 `ResearchContext`/`ResearchPipeline`（研究侧仍需可用）
- 不碰 `research/hikyuu_source/`（vendored 第三方）

---

## 4. 分阶段计划（每阶段独立可验证、可回退）

### 阶段 0：基线与契约（只读，产出报告）
- 跑 `pytest tests/unit/ -m "not requires_db and not requires_api"`，记录通过数（基线当前为 317）。
- 用 diff 量化 research 版 vs scripts 版 calculator 的差异，逐行确认**哪份是权威实现**、差异点是 bug 修复还是漂移。
- 产出 `docs/tasks/factor_pit_consolidation_inventory.md`：脚本清单 + 每个脚本判定（归档 / 合并 / 保留为薄入口）+ 公共接口契约（类名、方法签名、参数语义）。
- **门槛**：报告完成，不改任何运行代码。

### 阶段 1：废弃归档（低风险，先行）
- 将第 1.3 节一次性脚本移入 `archive/scripts/`（保留 git 历史，用 `git mv`）。
- 更新 README、`docs/architecture/system_overview.md` 移除其入口。
- **门槛**：`pytest`（无 DB/API marker）仍为基线通过数；`python -c "import alphahome"` 正常；无残留 import 指向已归档脚本（用 `rg` 校验）。

### 阶段 2：因子引擎归位
- 在 `alphahome/factors/core/` 落地 `PFactorCalculator` / `GFactorCalculator`（从权威实现抽取，去除对 `ResearchContext` 的硬依赖，改为接收注入的 db/config，与 `common` 风格一致）。
- 在 `alphahome/factors/pipelines/` 落地**单一并行引擎** `FactorEngine`，统一支持：按日期(`--dates`)、按季度(`--quarter/--start_quarter/--end_quarter`)、按年(`--start_year/--end_year`)、并行(`--workers` 或 `--worker_id/--total_workers`)、增量缺口检测(`batch_missing/recent`)。
- 为引擎写**单元测试**（mock db，覆盖日期生成、模式检测、缺口过滤、参数互斥校验等纯逻辑）。
- **门槛**：新单测通过；`production_p_factor_calculator` 等的纯逻辑函数（`generate_calculation_dates`/`detect_execution_mode`/`_filter_missing_dates`）有等价测试覆盖。

### 阶段 3：脚本瘦身 + 研究侧改引用
- `scripts/production/factor_calculators/` 全部改为薄入口调用 `alphahome.factors`，删除重复实现；**保留原命令行参数与行为**（README 列出的命令必须仍可用）。
- `research/pgs_factor/processors/*` 改为从 `alphahome.factors` 引用（消除 fork）。
- **门槛**：所有保留脚本 `--help` 正常；`python -m py_compile` 全绿；`pytest` 通过；`rg` 确认无重复 calculator 实体残留。

### 阶段 4：PIT 引擎归位（重复阶段 2–3 的模式）
- 对 `research/pit_data/` 与 `scripts/.../data_updaters/pit/` 执行同样的抽取去重，落到包内 PIT 模块，脚本/研究侧改引用。
- **门槛**：同阶段 3。

### 阶段 5：收尾
- 更新 `docs/architecture/system_overview.md` 的模块边界与数据流图、README 命令清单。
- 产出 `docs/tasks/factor_pit_consolidation_report.md`：迁移前后行数对比、删除/归档清单、保留命令清单、已知风险与待 DB 验证项。

---

## 5. 完成标准（Definition of Done）

- [ ] P/G 因子与 PIT 财务指标**只有一处真实实现**（在 `alphahome/` 包内），research 与 scripts 均为引用/薄入口。
- [ ] 12+ 并行 runner 变体收敛为 1 个 config 驱动引擎；README 列出的因子/PIT 命令行为保持兼容。
- [ ] 一次性脚本已归档，文档无悬挂入口（`rg` 校验通过）。
- [ ] `pytest tests/unit/ -m "not requires_db and not requires_api"` ≥ 基线通过数，且新增引擎单测通过。
- [ ] `python -c "import alphahome"`、`python -m py_compile`（全仓 .py）无错。
- [ ] 两份交付报告（inventory + report）齐备。

---

## 6. 验证手册（codex 每阶段必跑）

```bash
# 单元测试（不需 DB/API）
python -m pytest tests/unit/ -m "not requires_db and not requires_api" -q

# 导入与编译健康
python -c "import alphahome"
python -m py_compile $(rg --files -g '*.py' alphahome scripts)

# 保留脚本入口可用（举例）
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --help
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --help

# 残留重复/悬挂引用扫描
rg -n "ProductionPFactorCalculator|ProductionGFactorCalculator" alphahome scripts research
rg -n "migrate_existing_tables_to_rawdata|delete_g_factor_data" README.md docs
```

> 需要 DB/API 凭据的端到端校验**不作为完成门槛**；codex 应把这类校验列入报告的「待人工 DB 验证项」，并尽量提供 dry-run / `--limit` 路径。

---

## 7. 约束与注意事项

- **小步提交**：每阶段一组提交，commit message 说明「搬运/去重，无算法变更」。不要一次大爆炸式改动。
- **保持行为兼容**：README 与 docs 中列出的命令是对外契约，参数与默认值不得静默变更。
- **遇到 research vs scripts 差异**：以阶段 0 报告判定的权威实现为准；若两版都含独有修复，必须合并而非二选一，并在报告中记录。
- **不要为了过测试而 hardcode**：测试反映正确逻辑，而非相反。
- **不可逆操作**（删除文件、`git mv` 大批量）执行前在报告中列清单；优先用 `git mv` 保留历史。
- **不碰**他人正在改动的工作区文件无关部分；当前工作区已有未提交改动，聚焦本任务范围。
