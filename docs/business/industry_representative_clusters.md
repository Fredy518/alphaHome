# 行业代表簇

行业投射等下游策略可复用的指数分组能力。代码位于 [industry_clusters](../../alphahome/curation/industry_clusters/)，执行入口为 [run_industry_clusters.py](../../scripts/curation/run_industry_clusters.py)。

模块维护ETF策略使用的指数母库、簇成员、代表指数、质量指标与变更记录。新增指数必须有达标月度可用ETF作为依据，继续遵守上市满3个完整自然月、规模至少5亿元、20日日均成交额至少3000万元及可用池的数据完整性要求。聚类的行业或行情数据不足使用V2回退；这一回退不豁免ETF可用资格。已有基线及合法准入成员保留跨期维护状态，下游仍按当月可用池检查实际ETF资格。

## AlphaHome日常维护入口

2026-09-30新增稳定母库及自动月度维护，代码为[automation.py](../../alphahome/curation/industry_clusters/automation.py)。GUI日常更新依次增加“行业指数母库维护”和“行业代表簇月度维护”；运行日常更新时自动判断本月是否需要计算。母库依赖Features、候选档案标签维护及月度可用池发布，聚类依赖母库及PIT。母库或上游失败时，聚类不发布新结果；FundPos仍按自己的依赖执行。

母库`a_share_industry_v1`以已有115指数及V2维护状态初始化。新增来源是已实际保存的月度可用池快照，只消费`PRIMARY/BACKUP/STANDALONE`达标成员；候选档案仅补充“中国A股/行业板块”标签与跟踪身份，候选档案中的非达标ETF不会触发扩容。核对产品与官方ETF跟踪指数身份，分类待复核保留标记；身份缺失或冲突列入待查。2026-09-30初次实现曾从候选档案直接扩到188个，已按用户指令撤回73个错误准入，当前恢复115个；旧修订和事件保留，后续只有ETF达标才可能重新准入。准入规则更新为`industry_library_monthly_v2`，聚类算法仍是`industry_minimax_v2`。原115和71指数两套固定研究母库及历史继续保留。

母库扩容生成不可覆盖的修订，同时保存可用池快照、事实月份、实际记录时间与规则版本；对应聚类计算使用一个不可变的指数集合版本，并通过`previous_batch_id`继承原有维护状态、簇ID和确认次数。ETF临时失格、候选标签改变或产品从最新档案消失，都不会自动删除已有合法准入指数。此次撤回纠正的是错误准入来源，修订记录保留原188成员。指数终止或明确身份变更的退出，通过带理由的`retire`命令记录；当前源表没有可靠的指数终止日期，不推断自动退出。

聚类按完整自然月逐月推进，复用月度可用池V5的决策日历：次月首个交易日09:00以后检查，目标09:30开盘生效。先发布当月可用池，再更新母库，再计算聚类；采用计算时已实际保存且可用池事实月份与目标月份一致的母库修订。母库可能在09:00披露窗口结束后生成，09:30前完成可服务当次开盘；晚完成仍顺延真实下一开盘，不能倒填为09:00前已知。缺少当月可用池依据时不发布聚类。基准指数月末行情必须齐备，个别成员的数据不足继续采用V2回退。重复月份直接跳过，不能重复累计确认次数。每月计算与发布前再次核对来源行；来源漂移则重新预览，不移动已发布指针。

每月的簇结果、母库版本和发布记录在同一事务保存。`available_from`使用真实完成后的可用开盘：09:30之后完成时顺延下一交易日开盘。历史初始化保留`historical_reconstruction`标记，正常新增月度发布记为`observed`；真实时点查询不返回初始化重建，也不把晚发布结果回填成开盘前已知。补跑已过有效期的月份可保留维护链，实际时点查询仍按有效区间过滤。

| 接口 | 用途 |
| --- | --- |
| `fund_pool_on.industry_index_library_current` | 当前母库成员、状态、首次可得时间及退出记录 |
| `fund_pool_on.industry_index_library_revision` | 母库修订、来源哈希、增量事件和父版本 |
| `fund_pool_on.industry_cluster_monthly_publication` | 月份、母库修订、簇批次、计划开盘及真实可用时间 |
| `fund_pool_on.industry_cluster_managed_current` | 自动维护链的最新结果，含初始化或观察标记 |
| `fund_pool_on.industry_cluster_managed_as_of(ts)` | 指定实际时点已发布且仍有效的观察结果 |

独立维护命令为[scripts/curation/run_industry_cluster_automation.py](../../scripts/curation/run_industry_cluster_automation.py)，不依赖ETF工作台或外部母库CSV：

```powershell
python -B scripts/curation/run_industry_cluster_automation.py schema --output-dir outputs/industry-cluster-schema
python -B scripts/curation/run_industry_cluster_automation.py schema --execute --expected-plan-hash <迁移哈希> --output-dir outputs/industry-cluster-schema
python -B scripts/curation/run_industry_cluster_automation.py library --output-dir outputs/industry-library-plan
python -B scripts/curation/run_industry_cluster_automation.py library --execute --expected-plan-hash <计划哈希> --output-dir outputs/industry-library-plan
python -B scripts/curation/run_industry_cluster_automation.py clusters --output-dir outputs/industry-cluster-plan
python -B scripts/curation/run_industry_cluster_automation.py clusters --execute --expected-plan-hash <计划哈希> --output-dir outputs/industry-cluster-plan
# 仅在有明确终止或身份变更事实时记录退出：
python -B scripts/curation/run_industry_cluster_automation.py retire --execute --index-code <指数代码> --reason <确认理由> --output-dir outputs/industry-library-exit
```

GUI直接调用相同服务，执行前重算计划。独立命令只执行已保存的同一计划；`no_op`和未到运行窗口无需执行。旧`run_industry_clusters.py build/update/publish`仍用于固定母库研究。初次迁移、真实数据回放、重复运行和事务回滚检查见[维护接入记录](../../outputs/industry_cluster_automation_20260930/README.md)。

已安装旧视图的数据库需重新执行上述 `schema` 预览及迁移。`industry_cluster_automation_v2` 会识别缺少 `selection_confidence` 的旧视图，并同步更新研究当前视图、自动维护当前视图和 `managed_as_of` 查询；迁移不重算或改写已有聚类结果。该字段取自原成员记录的 `detail`，补齐后数据库查询结果可直接传入默认选择适配器。

## 当前版本：V2的数据不足回退

`industry_minimax_v2`为当前默认配置，V1结果保留作严格数据约束对照。ETF可用池、评分和交易资格由消费端按原规则检查；聚类数据是否齐全不再直接决定ETF能否入选。

1. 一级、二级行业分别保留已知权重，未知部分不归一化，不放进同一个“未知行业”增加相似性。相似性只获得已知行业重叠的贡献。
2. 是否有足够指数行情按成员逐一检查。数据充分的成员参与多特征维护；不因其他成员缺行情而暂停整簇。
3. 数据不足或正在等待复核的成员使用当时的60%成分重叠分组。若该成分组与已确认多特征簇相连，合并相关的策略选择分组，直到每个指数只属于一个选择分组。回退组允许按原成分策略选取ETF，不宣称其代表通过了多特征门槛。
4. 尚无可计算成分的成员保留独立身份；不补造行业或走势。其入选仍须通过消费端原有评分与可用池条件，明确的未发布、非A股等冲突不放开。
5. 多特征簇的门槛及跨月维护参数维持原值。策略预算由调用者固定，分组变多不自动增加名额。

因此，`status`记录核心多特征的确认情况；`selection_group_id`、`selection_method`、`selection_eligible`及`can_represent_selection_group`记录下游应如何选择。消费V2时不再自行使用`status='confirmed'`作为ETF准入条件。

## 共同的相似度算法

参数集中在 `ClusterConfig`，每次运行保存完整 `config.json`。V2的增量配置见[配置文件](../../artifacts/industry_clusters/industry_minimax_v2.json)。这些参数按代表性和可解释性设定，未按策略年化收益挑选。

对两个指数，股票重叠为成分股权重逐只取较小值后求和；行业重叠对申万行业权重作同样计算。令一级、二级行业重叠分别为L1、L2，股票重叠为S：

- 行业距离：`0.25×(1-L1) + 0.75×(1-L2)`；
- 股票距离：`1-S`；
- 走势距离：120日和252日的 `1-原始相关性`、`1-市场残差相关性`，四项等权平均；
- 综合距离：`45%×行业距离 + 20%×股票距离 + 35%×走势距离`。

每次合并检查合并后的全部成员，从真实成员中寻找与最远成员距离最小的代表指数，即Minimax代表点。仅在存在满足以下全部条件的代表时合并。相同距离按指数代码稳定排序，计算不使用随机初始化。

| 代表与所有成员之间的要求 | 新合并/加入 | 已有簇保留 |
| --- | ---: | ---: |
| 综合距离最大值 | ≤0.25 | ≤0.30 |
| 一级行业重叠 | ≥75% | ≥70% |
| 二级行业重叠 | ≥65% | ≥55% |
| 原始收益相关性，两个窗口均满足 | ≥90% | ≥85% |
| 市场残差相关性，两个窗口均满足 | ≥80% | ≥70% |
| 波动率比例，两个窗口均满足 | ≤1.30 | ≤1.50 |
| 年化跟踪误差，两个窗口均满足 | ≤15个百分点 | ≤20个百分点 |

另外，同簇任意两成员的一级行业重叠至少60%、二级至少45%，防止仅通过中心指数串起经济方向不同的成员。股票重叠参与综合距离，不单设60%合并门槛。簇数由约束决定，可保留单指数簇。

Minimax来源：[Bien与Tibshirani，2011](https://pmc.ncbi.nlm.nih.gov/articles/PMC4527350/)。跨期维护及金融领域阈值是本模块的实现选择。

## 月度维护

- 首次建立母库时直接聚类；此后每月检查数据、经济边界、代表质量和合并候选。
- 合并候选连续两个月满足加入标准，并在3、6、9、12月末执行。
- 已有簇连续两个月不满足保留要求时重新分组；缺失价格是未知，不累计相似性失败次数。
- 行业边界被突破，或成员自身与上次已知股票组成重叠不足50%，立即重新评估受影响簇。
- 可用代表尽量沿用。季度重评时，新代表改善最差综合距离至少0.02才替换；原代表已不合格时及时选择合格替代。
- 簇ID按上一期成员交集匹配，合并/拆分保留父簇信息。相同月份不能重复推进确认次数；跨月断档会重置连续确认。
- 最新月度结构尚不可用时，使用当时最近一份已知完整结构，最大年龄183天。行业分类也只取截止当时最近记录，最大年龄183天。新近已知的非A股成分不会被更早纯A股快照覆盖。

`confirmed`表示当前代表满足相应加入或保留条件；`price_pending`表示价格覆盖不足；`classification_pending`表示已知行业权重不足以通过多特征门槛；`degraded_pending`表示已有簇正在等待第二次失败确认；`structure_pending`表示结构或上市日期等输入不足。V2将这些状态作为分组方法与确认程度信息；缺失收益不填成0。

## 输入与时点

数据查询默认只读，使用：

- `pit.pit_etf_index_members_monthly`：`official_then_disclosed_etf_holdings_v1`、`full_index`、非proxy，逐份完整快照检查；
- `pit.pit_industry_classification`：申万分类代码，按截止日选择；
- `rawdata.index_factor_pro`：指数收盘价、官方昨收和日涨跌；
- `rawdata.index_basic`、`rawdata.fund_etf_basic`：指数发布日期及缺失时的首只ETF上市日期；
- `rawdata.others_calendar`：上交所交易日历。

收益优先使用当日收盘/官方昨收−1。这样即使前日数据库行缺失，当日已知收益仍可使用。官方昨收缺失时，仅允许使用日历连续两日的收盘差。数据表涨跌幅与计算值明显冲突时排除该行；空缺交易日不前填。基准为中证全指`000985.CSI`，窗口内分别拟合含截距市场beta，再计算残差相关性。

120日窗口至少100个共同观测，252日至少200个；个别指数满足自身观测数但指数对的共同日期不足时，该对仍为未知。最新价格最多滞后2个交易日。当前使用指数价格收益，不混入ETF代理或总收益序列。

输入按请求和内容保存在输出目录的 `inputs/` 下。输入快照与配置哈希进入结果。`asof_date`表示业务截止日，`recorded_at`表示本次实际入库时间；历史数据重建统一标为 `historical_reconstruction`，不能据此认定结果在当年实际存在。

## 使用

首次建立最新池的研究母库并回放：

```powershell
python -B scripts/curation/run_industry_clusters.py build `
  --universe-id industry_usable_202608_v1 `
  --universe-file artifacts/industry_clusters/industry_usable_202608_v1.csv `
  --start 2025-08-31 --as-of 2026-08-31 `
  --output-dir outputs/industry_clusters/industry_usable_202608_v2
```

`build`只读源表并生成文件。将已生成的结果保存到本模块的研究结果表：

```powershell
python -B scripts/curation/run_industry_clusters.py publish `
  --output-dir outputs/industry_clusters/industry_usable_202608_v2
```

此后使用已保存的母库及上期状态推进。例如9月结束且数据齐备后：

```powershell
python -B scripts/curation/run_industry_clusters.py update `
  --universe-id industry_usable_202608_v1 --as-of 2026-09-30 `
  --output-dir outputs/industry_clusters/industry_usable_202609_update
```

`update`先计算文件，再用同一 `publish` 命令入库。默认使用V2配置，可通过 `--config` 指定完整或部分参数；用V1保存的config.json可复现严格版本。参数改变需新建计算序列，不与旧配置状态混用。更改母库成员时使用新的 `universe-id`；母库标识中的v1不代表算法版本。使用 `--input-cache-dir` 可复用另一计算的冻结源数据。

## 输出接口

| 对象 | 内容 |
| --- | --- |
| `industry_cluster_universe` | 固定指数母库及其来源 |
| `industry_cluster_batch` | 每月配置、输入、维护状态与真实记录时间 |
| `industry_cluster_member` | 每个指数的簇、代表、状态及代表资格 |
| `industry_cluster_group` | 簇级最差成员、最差成员对及风险差异 |
| `industry_cluster_event` | 合并、重评、待确认及代表更换 |
| `industry_cluster_current` | 按母库与算法版本分别展示最新研究结果 |

以上对象均在 `fund_pool_on`。旧 `cluster_result` 以及ETF可用池、候选池表保持各自用途。

文件接口包含 `latest_members.csv`、`latest_groups.csv`、`membership_history.csv.gz`、`group_history.csv.gz`、`monthly_summary.csv`、`structure_sources.csv.gz`、`price_coverage.csv.gz`及`events.json`。

策略适配函数为 `service.select_projection(candidates, membership, slots)`：外部先完成原可用池及评分筛选，再传入固定名额。V2按选择分组每组最多选一个，多特征分组选合格代表，成分回退组选该组投射评分最高者。每个已选名额权重为 `1/slots`，不足部分留现金。可用 `policy='strict'` 明确复现V1；`require_representative=False`用于全部成员对照。

V2补充输出 `latest_selection_groups.csv`、`selection_groups_history.csv.gz`。数据库 `industry_cluster_current` 中用 `algorithm_version='industry_minimax_v2'` 读取当前算法；按母库、版本和截止日筛选后再交给适配器，避免把不同版本混为一批。

V2的`industry_cluster_group`保存参与多特征维护的核心簇质量；数据不足的成员不一定有对应核心质量行。完整候选和策略分组应读取成员接口及selection字段，不能通过内连接核心质量表再次丢弃回退成员。

## 研究诊断

初始母库冻结为最新3个月可用池的115个行业/产业主题指数。历史回放使用当时结构和价格，但母库本身按最新截面选定，因此它用于分组质量与维护行为诊断，不能当作完整历史可投资母库。

另对投射V3原有71指数研究范围运行2019-07至2026-08的86个月分组，供ETF工作台固定名额对照。控制组依次保留原V3、匹配数据覆盖与母库的成分聚类、多特征簇所有成员、仅允许合格代表。这样可以分辨数据约束、分组、代表资格带来的变化。

第一轮仅取当月快照的诊断暴露出临时缺失引起的成员消失，已修正为最近已知有效结构。初次输出保存在研究输出目录的 `initial_exact_month_series.json.gz`，不作为当前接口。

本次完整结果及运行检查见[当前研究入口](../../outputs/industry_clusters/README.md)。V2选择分组清单可用 `scripts/curation/summarize_industry_clusters_v2.py --output-dir <结果目录>` 重新生成；旧版清单继续使用summarize_industry_clusters.py。
