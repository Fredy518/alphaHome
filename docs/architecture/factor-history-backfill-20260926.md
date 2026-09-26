# P/G 历史补齐与 NAS 对齐（2026-09-26）

## 范围和顺序

本次按用户指定顺序，先补齐本机 `alphadb`，完成只读验收后才处理 NAS `192.168.5.6:15432/alphadb`。计算截至 2026-09-25，重算区间为 2024-09-20 至 2026-09-25 的 106 个自然周五，按 13 个日期一批运行 `FactorCoordinator` 的 P→G 同日依赖链。G 的两年历史窗口之前，本机和 NAS 的 P 分数及股票集合在 1,275 个日期、G 在 1,133 个日期一致；G 所需的前置 P 历史在 104 个周五的行数及关键输入字段哈希一致。证据位于 Y 盘本次目录中的 `prewindow-factor-score-parity.json` 与 `prewindow-p-history-parity.json`。

所用代码固定为 `codex/factor-backfill-20260926` 的 `6d5b36f`：修正 NaN 资产的财务资格判断，逐个计划日期检查财务输入缺口，以真实 `in_date` 补充 CI 行业历史候选，并将 SW/CI 来源水位纳入因子计划。离线测试 1,226 项通过、4 项跳过。执行期间代码保持在隔离分支，数据库验收通过后再并入 `main`。

## 恢复点和来源对齐

所有本次归档都在 `Y:\AlphaHomeBackups\factor-backfill-20260926`。其中本机因子归档 `local-factors-before-repair.dump` 为 458,136,867 字节，SHA-256 `1eea00fc204e4ecd72ab4ca73b4f354d596e1eb03962c92dc873a017bc5d866f`。NAS 因子 schema 归档 `nas-factors-before-repair.dump` 为 548,325,005 字节，SHA-256 `ab79913b895aede7d29f6ad5e4fcad28b49b16d6d04d7be63bebb5aefb7c0865`。NAS 四张 PIT 来源表归档 `nas-factor-sources-before-sync.dump` 为 97,485,532 字节，SHA-256 `98c9591f6ba51e0bcf1006342316d4be3deaa3be4e349c10ade238732309abc3`。归档完成后检查了目录条目和所需表数据条目，并记录哈希；本次没有做独立实例的完整恢复演练。

NAS 来源同步仅覆盖审查确认的差异：`pit_income_quarterly` 更新 17 行、补入 2 行，`pit_balance_quarterly` 更新 908 行，`pit_financial_indicators` 更新 335 行，`pit_industry_classification` 更新 6 行、补入 24,336 行。同步前复核了差异数量；旧值存入 NAS 上带 `presync_archive_20260926` 后缀的四张归档表。同步后，四张来源表按年份计算的全部业务列指纹及行数与本机一致，分别为 486,030、380,386、342,518 和 1,504,898 行。证据为 `nas-factor-source-sync-evidence.json` 与 `nas-factor-source-parity-after-sync.json`。

NAS 原有 29 个非周五因子日期与正式计算日合同冲突。已先存入 `factors.p_factor_nonfriday_archive_20260926` 和 `factors.g_factor_nonfriday_archive_20260926`，再分别删除主表中的 149,685 和 149,681 行；归档和删除数逐项相等。周五约束已添加并验证。证据为 `nas-nonfriday-cleanup-plan.json` 与 `nas-nonfriday-cleanup-evidence.json`。

最初一次本机执行因脚本从 Y 盘加载了已安装的旧版本代码，在第一批局部写入后被停止；运行 `b777fe89-dc64-4c2a-ada2-d0bd8f1b18b7` 已标记 `cancelled`。随后脚本明确固定工作树和提交 `6d5b36f`，从第一批重新执行全部 106 个日期。旧计划与来源水位文件保留 `oldcode` 后缀供审计，不作为本次成功证明。

## 验收

本机验收文件 `local-factor-history-acceptance.json` 的状态为 `accepted`：P/G 均覆盖 106 个周五、各 575,433 行，最少每日期 5,339 行、2026-09-25 各 5,513 行；无缺失日期、无无效行、无 P/G 股票集合差异，且当前任务账本与实际行数一致。`920106.BJ` 在 2024-12-20 的 P/G 均有结果。

NAS 验收文件 `nas-factor-history-acceptance.json` 的状态同样为 `accepted`：P/G 各 106 个周五、各 575,433 行，最少每日期 5,339 行；2026-09-18 与 2026-09-25 的 P/G 每日期均为 5,513 行。两端均无无效行、重复业务键、缺失日期、非周五主表记录或 P/G 股票集合差异，周五约束有效，当前任务账本的成功状态和输出行数相符；本机与 NAS 的九批结果均为 `success`。`local-nas-factor-history-parity.json` 对 106 个日期的所有因子列（仅排除写入时间戳）逐日期比对，P/G 均零差异。`920106.BJ` 在两端 2024-12-20 的 P/G 均有结果。

## 后续边界

本次是以现存来源快照为基础的历史重建，不是当年的实时可用性证明。`PITBalanceQuarterlyManager._fetch_tushare_data` 对同一 `ts_code/end_date/ann_date` 的修订版未指定稳定顺序，`_preprocess_data` 又保留最后一行；两端原始记录可相同而物化 PIT 值不同。[Tushare 资产负债表文档](https://tushare.pro/document/2?doc_id=36)将 `f_ann_date` 定义为实际公告日期，故晚于 `ann_date` 的版本还可能涉及回看偏差。此次将 NAS 的物化快照对齐本机，但没有修复该上游版本选择规则；后续应单独制定确定性、实际可得时间优先的选择与重算方案，并以封存采集时间验证实时资格。另一个边界是 3900X 长期维护电脑仍关机，未在该机部署或验证首次自然运行。数据库结果和代码合并不能替代该机的接管验收。
