# FTTM 一级、二级行业 PIT 数据任务 HANDOFF

> 文档状态：Implementation Ready（仅 HANDOFF，代码尚未实现）
>
> 研究状态：Validate（数据合同与实现验收），不代表因子有效性已验证或已获生产晋级
>
> 目标仓库：AlphaHome
>
> 目标域：pit
>
> 截止核查日：2026-08-25
>
> 默认历史起点：2014-01-31，可配置
>
> 默认截面频率：月末

## 1. 交付目标

在 AlphaHome 中增加两项可注册、可审计、可全量回放、可增量更新的正式 PIT 任务：

1. 个股机构 FTTM 月末快照：pit.pit_stock_fttm_monthly
2. 申万一级、二级行业 FTTM 月末快照：pit.pit_industry_fttm_monthly

任务必须恢复原始 FTTM 设计：stock_report_rc 中的 Q4 不是“第四季度单季预测”，而是某一完整财年的年度预测。Q4 过滤本身正确，错误在于把该年度预测直接赋给 fttm_np。真正的未来十二个月预测，应在同一份研报事件内，将报告当年 FY1 和下一年 FY2 的两个 Q4 年度预测按报告发布日所在季度线性合成。

本文是实现 HANDOFF，不承担以下工作：

- 不在 PIT 表中生成 z-score、rank、择时分数或交易信号。
- 不把本任务直接声明为投资有效、策略有效或生产因子已晋级。
- 不改变 stock_report_rc、stock_dailybasic 或 pit_industry_classification 的现有数据生产逻辑。
- 不把 pgs_factors 作为正式写入层；如旧消费者需要兼容，只能另建只读视图。
- 不承诺所有二级行业在所有历史月份都达到研究使用门槛；结构行保留，质量状态显式标记。

## 2. 关键结论

### 2.1 原始 FTTM 公式

设一份研报事件的 report_date 位于自然季度 q，q 取 1、2、3、4：

- FY1：quarter 年份等于 year(report_date) 的 Q4 全年净利润预测。
- FY2：quarter 年份等于 year(report_date) + 1 的 Q4 全年净利润预测。
- w_fy1 = (5 - q) / 4。
- w_fy2 = (q - 1) / 4。
- fttm_np = w_fy1 × fy1_np + w_fy2 × fy2_np。

季度权重如下：

| report_date 所在季度 | FY1 权重 | FY2 权重 |
|---|---:|---:|
| Q1 | 1.00 | 0.00 |
| Q2 | 0.75 | 0.25 |
| Q3 | 0.50 | 0.50 |
| Q4 | 0.25 | 0.75 |

因此，原代码中“筛选 Q4”是正确步骤；“fttm_np = np”才是设计退化点。月末 obs_date 只负责选择当时可见的最新研报事件，不重新锚定 FY1/FY2，也不重新计算季度权重。

### 2.2 二级行业数据量结论

当前数据量支持把原始设计扩展到二级行业，但必须保留覆盖率和质量分层，不能假设每个行业、每个月都同质可用。

2026-08-25 的本地只读核查结果：

- rawdata.stock_report_rc：2,878,101 行、5,795 只股票、190 家机构，日期范围 2010-01-01 至 2026-08-24。
- 可用于 Q4 净利润或 EPS 路径的记录约 2,843,481 行。
- pit.pit_industry_classification：746,256 行、6,138 只股票、199 个连续月末，范围 2010-01-31 至 2026-07-31。
- 当前表内有 31 个申万一级行业、131 个申万二级行业。
- 2026-07-31 截面按当时上市、退市状态过滤后有 5,534 只有效成分股，行业归属覆盖率为 100%。
- 同一截面可形成 FTTM 的股票约 2,882 只，股票数覆盖约 52.1%，对应总市值覆盖约 89.2%。
- 131 个二级行业中，114 个覆盖股票数不少于 5，86 个不少于 10。
- 同时满足覆盖股票数不少于 5、覆盖总市值不少于 60% 的二级行业有 106 个。

这意味着：

- 一级行业可以作为标准产出。
- 二级行业可以作为标准产出，但必须同时输出结构全集和 eligible/quality 状态。
- 不合格行业不能被静默删除，也不能把缺失扩散度填成 0.5。

### 2.3 pit_industry_classification 的能力

pit.pit_industry_classification 已经具备合格的动态二级行业成分基础：

- 主键为 ts_code、obs_date、data_source。
- obs_date 为月末 PIT 观察日。
- 同时保存申万一级和二级行业编码、名称。
- 成分资格来自 in_date、out_date 的历史有效区间，而不是使用当前静态分类回填历史。
- 当前申万月末序列无断月、无重复主键、无二级行业缺失。

新行业 FTTM 任务应直接在同一个 obs_date 上连接该表，不应另建静态股票行业映射。

### 2.4 迁移行为基线

迁移时只读对照以下 strat_research 实现，不从 AlphaHome 运行时跨仓库导入代码或 CSV：

- E:\CodePrograms\strat_research\01_data_acquisition\process_earnings_forecast.py
  - _calculate_fttm_np：同一 report_date、org_name、author_name 内配对 FY1/FY2，权重锚定 report_date。
  - _forecasts_available_as_of：六个月左开右闭可见窗口。
  - _select_latest_org_forecasts：月末对股票、机构取最新研报事件。
- E:\CodePrograms\strat_research\01_data_acquisition\synthesize_index_forecast.py
  - _calculate_weighted_forecasts：先机构内成分加权，再对机构算术平均。
  - _calculate_diffusion_index：只作为扩散度来源参考；本 HANDOFF 明确修正其“任意最近历史月”和缺失填 0.5 的问题。

AlphaHome 的正式实现以本文数据合同为准。源脚本用于确认原始 FTTM 合成语义，不是生产依赖。

## 3. 数据流与任务边界

~~~text
rawdata.stock_report_rc
          |
          | 同研报事件 FY1/FY2 合成，再按 6 个月窗口取机构最新
          v
pit.pit_stock_fttm_monthly
          |                 pit.pit_industry_classification
          |                 同月末动态 L1/L2 成分
          |                             |
          |                 rawdata.stock_dailybasic
          |                 月末前最近交易日总市值
          |                             |
          |                 rawdata.stock_basic
          |                 PIT-time 上市/退市资格
          |                             |
          +-----------------------------+
                         v
              pit.pit_industry_fttm_monthly
              L1/L2 × 月末 × 权重口径
~~~

严格遵守“一项 PITTaskContract 对应一张 output_table”：

- 个股 FTTM 不能作为行业任务的临时副产物。
- 行业任务必须依赖已落库并审计通过的个股 FTTM 月末快照。
- 一级和二级行业可由同一个行业任务写入同一张表，以 industry_level 区分；它们共享完全相同的 PIT 时钟和聚合合同。

## 4. 时间与 PIT 合同

### 4.1 三种日期不得混用

| 日期 | 含义 | 用途 |
|---|---|---|
| report_date | 机构预测记录的可见日期 | 判断该预测在当时是否可用 |
| obs_date | 月末观察日 | 两张目标表的 PIT 主时间键 |
| weight_trade_date | obs_date 当日或之前最近的有效交易日 | 获取行业聚合权重 |

行业归属表使用 obs_date；财务报表 PIT 通常使用 ann_date；本任务不能把 ann_date 或 calc_date 借用为行业观察日。

### 4.2 月末观察日

- obs_date 使用自然月最后一日，与 pit.pit_industry_classification 对齐。
- 即使月末不是交易日，obs_date 仍保留自然月末。
- 自动增量只处理已经完整结束的自然月；运行日所在月不产出月末行。
- 行业任务的最新目标月不得晚于 pit_industry_classification 已存在的最大 obs_date；上游月份未就绪时明确失败或跳过，不生成半成品。
- 市值权重通过 weight_trade_date 获取，要求 weight_trade_date 小于等于 obs_date。
- 报告可见窗口、行业成分和上市状态均以 obs_date 回看，不使用未来记录。

### 4.3 预测可见窗口

对每个 obs_date，只允许使用：

~~~text
report_date > obs_date - 6 calendar months
report_date <= obs_date
~~~

左开右闭必须固定，避免全量与增量路径边界不一致。

先按第 5 节在每个精确研报事件内配对 FY1/FY2 并计算事件级 FTTM；再在窗口内对 ts_code、org_name 选择最新研报事件。不得把不同 report_date 或不同 author_name 的 FY1、FY2 拼成一对。

月末选择的稳定排序至少包括：

1. report_date 降序。
2. report_date 相同时，规范化后的 author_name 升序，保持与原始实现的稳定选择一致。
3. author_name 仍相同时，使用数据源原始稳定标识；如果源表没有唯一行标识，应先用全部业务字段确定性去重。
4. 不允许依赖数据库未声明的自然行序。

### 4.4 可用时点约定

源表只有日级 report_date，行业权重又使用月末收盘后的 total_mv，因此本任务只声明“月末收盘后可生成”的 PIT 快照：

- 研究或交易消费者最早从 obs_date 之后的首个交易日使用。
- 不允许把该月末快照用于 obs_date 当日开盘或盘中决策。
- created_at、updated_at 是系统运行时间，不代表经济信息在盘中已可见。
- 如未来需要日内或开盘前严格 PIT，必须取得报告发布时间戳并另建可用时点合同；仅靠 report_date 无法证明。

## 5. 个股 FTTM 计算合同

### 5.1 源字段

主源：rawdata.stock_report_rc。

最少使用字段：

- ts_code
- org_name
- author_name
- report_date
- quarter
- np
- eps
- 数据源可用的唯一行标识或稳定去重字段

EPS 回退需要 rawdata.stock_dailybasic.total_share：

- np 单位：万元。
- eps 单位：元/股。
- total_share 单位：万股。
- eps × total_share 的结果仍为万元，与 np 同量纲。

### 5.2 Q4 与财年识别

只保留 quarter 可严格解析为 YYYYQ4 的记录：

- forecast_year = YYYY。
- 不把 Q4 当作单季值。
- 非 Q4 记录不参与 V1 FTTM 合成。
- 无法解析的 quarter 进入审计计数，不静默猜测。
- org_name 为空或清洗后为空的记录不参与主键构造，必须进入独立审计计数。

### 5.3 净利润优先级

每条年度预测的 annual_np 按以下顺序产生：

1. np 非空时直接使用 np。
2. np 为空、eps 非空，且 report_date 当日存在 total_share 时，使用 eps × total_share。
3. 其余情况记为不可计算。

V1 的 EPS 回退只允许 report_date 与 trade_date 精确同日匹配，不向未来找股本，也不在没有差异报告的情况下擅自改成前值填充。未来如改为 report_date 之前最近交易日股本，必须单独验证并报告影响。

完成事件级 FTTM 后，如 selected_total_share 非空且不为零，可另算 fttm_eps = fttm_np / selected_total_share。该字段只用于审计和兼容，不参与 fttm_np 插值。

负值和零值均是合法预测：

- 不得用大于零条件过滤亏损预测。
- 不得把零替换为空。
- 百分比变化遇到零分母时返回空并记录原因。

### 5.4 FY1、FY2 配对

先定义精确研报事件键：

~~~text
(ts_code, org_name, author_name, report_date)
~~~

对每个事件键：

- report_year = year(report_date)。
- FY1 取 quarter 年份等于 report_year 的 Q4 annual_np。
- FY2 取 quarter 年份等于 report_year + 1 的 Q4 annual_np。
- 其他年份的 Q4 不参与该报告事件的 V1 FTTM。
- FY1 与 FY2 必须来自同一个事件键，不能分别在六个月窗口内各取一条最新记录。
- 同一事件键、同一财年出现重复源行时，先按数据源稳定标识确定性去重；不得依赖 pivot 或 SQL 聚合的未定义 first。
- 机构名称不得先做模糊归并；V1 只做 Unicode 字符串首尾空白清理，随后精确匹配 org_name。author_name 为空时统一为空字符串，并做相同首尾清理。
- 清理后发生机构名碰撞时必须进入审计计数。更强的别名归一化若有需求，应独立维护版本化映射表，不能静默改写历史机构身份。

事件级 FTTM 计算完成后，才按第 4.3 节在六个月窗口内选取每个 ts_code、org_name 的最新事件，形成 obs_date 月末快照。

### 5.5 单边缺失回退

保留原始设计的单边回退：

- FY1 缺失且 FY2 可用：令 FY1 = FY2。
- FY2 缺失且 FY1 可用：令 FY2 = FY1。
- 两者均缺失：不产出该研报事件。

单边回退不是完整双年度插值，必须标记：

- estimate_pair_status = both、fy1_only 或 fy2_only。
- is_single_year_fallback = true/false。

### 5.6 公式与可复算字段

对 report_date 所在季度 q：

~~~text
w_fy1 = (5 - q) / 4
w_fy2 = (q - 1) / 4
fttm_np = w_fy1 * fy1_np_used + w_fy2 * fy2_np_used
~~~

同一事件的季度权重在后续月末快照中保持不变。例如 2024-12-20 的报告在 2025-01-31 仍可见且仍为该机构最新报告时，继续使用 Q4 权重及 2024Q4、2025Q4 配对；不能因 obs_date 已进入 2025Q1 而改配 2025Q4、2026Q4。

目标表必须保存被月末选中的 report_date、author_name、FY1/FY2 原值、使用值、权重和回退状态，不能只保存最后一个 fttm_np。

### 5.7 个股表建议 DDL

文件建议：alphahome/pit/database/create_pit_stock_fttm_monthly_table.sql

~~~sql
CREATE TABLE IF NOT EXISTS pit.pit_stock_fttm_monthly (
    ts_code                  varchar(16)  NOT NULL,
    org_name                 varchar(255) NOT NULL,
    obs_date                 date         NOT NULL,
    selected_report_date     date         NOT NULL,
    selected_author_name     varchar(255) NOT NULL DEFAULT '',
    report_quarter           smallint     NOT NULL,
    fy1_year                 integer      NOT NULL,
    fy2_year                 integer      NOT NULL,
    fy1_np_raw               numeric,
    fy2_np_raw               numeric,
    fy1_np_used              numeric      NOT NULL,
    fy2_np_used              numeric      NOT NULL,
    fy1_value_source         varchar(16),
    fy2_value_source         varchar(16),
    fy1_weight               numeric(8,6) NOT NULL,
    fy2_weight               numeric(8,6) NOT NULL,
    fttm_np                  numeric      NOT NULL,
    selected_total_share     numeric,
    fttm_eps                 numeric,
    estimate_pair_status     varchar(16)  NOT NULL,
    is_single_year_fallback  boolean      NOT NULL,
    source_window_start      date         NOT NULL,
    source_window_end        date         NOT NULL,
    formula_version          varchar(32)  NOT NULL,
    source_max_report_date   date         NOT NULL,
    created_at               timestamptz  NOT NULL DEFAULT now(),
    updated_at               timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, org_name, obs_date),
    CHECK (report_quarter BETWEEN 1 AND 4),
    CHECK (fy1_year = extract(year FROM selected_report_date)::integer),
    CHECK (fy2_year = fy1_year + 1),
    CHECK (selected_report_date <= obs_date),
    CHECK (source_max_report_date = selected_report_date),
    CHECK (fy1_weight = (5 - report_quarter)::numeric / 4),
    CHECK (fy2_weight = (report_quarter - 1)::numeric / 4),
    CHECK (estimate_pair_status IN ('both', 'fy1_only', 'fy2_only')),
    CHECK (fy1_weight + fy2_weight = 1)
);

CREATE INDEX IF NOT EXISTS idx_pit_stock_fttm_obs_date
    ON pit.pit_stock_fttm_monthly (obs_date);

CREATE INDEX IF NOT EXISTS idx_pit_stock_fttm_stock_date
    ON pit.pit_stock_fttm_monthly (ts_code, obs_date);
~~~

建议公式版本首版固定为 fttm_q4_report_event_linear_v1。

## 6. 行业成分、有效股票与权重合同

### 6.1 动态成分

对每个 obs_date，从 pit.pit_industry_classification 获取 data_source = sw 的同日记录；目标表的 classification_source 原样保存小写 sw：

- 一级行业使用 industry_code1、industry_level1。
- 二级行业使用 industry_code2、industry_level2。
- 两个层级独立展开，不能由当前二级行业反推历史一级行业。
- 缺少相应行业代码的股票不进入该层级的聚合，但计入结构审计。

### 6.2 当时有效股票

必须从 rawdata.stock_basic 读取 list_date、delist_date，并按 obs_date 应用股票上市和退市日期：

- list_date 小于等于 obs_date。
- delist_date 为空或大于 obs_date。
- 只保留本任务明确支持的 A 股币种和市场范围。

不能用当前上市股票清单作为历史月份分母。审计报告同时保留：

- structural_member_count：行业分类表中的结构成分数。
- active_member_count：按当时上市、退市状态过滤后的有效成分数。

覆盖字段定义固定如下：

- weight_available_count：active 成分中具有有效 total_mv 的股票数。
- covered_stock_count：active 成分中至少有一家机构 FTTM 的股票数。
- structural_mv：具有有效 total_mv 的 active 成分总市值。
- covered_mv：同时具有有效 total_mv 和至少一家机构 FTTM 的 active 成分总市值。
- covered_stock_rate = covered_stock_count / active_member_count。
- covered_mv_rate = covered_mv / structural_mv。
- weight_data_coverage_rate = weight_available_count / active_member_count。

分母为零时相应比例为空，不填零。covered_mv_rate 的解释必须与 weight_data_coverage_rate 联用，避免缺权重股票被排除分母后造成虚高。

### 6.3 市值权重

V1 权重口径固定为 total_mv：

1. 对每个 obs_date，取 rawdata.stock_dailybasic 中小于等于 obs_date 的最近 trade_date。
2. 对每只股票取该 weight_trade_date 的 total_mv。
3. weight_trade_date 与 obs_date 的自然日差不得超过 31 天。
4. total_mv 必须非空且大于零。
5. 每个机构、每个行业只在该机构实际覆盖的股票集合内重新归一化权重。

total_mv 是研究代理权重，不等于申万官方指数权重。表中必须保存：

- weight_basis = total_mv。
- weight_trade_date。
- weight_staleness_days。
- weight_data_coverage_rate。

若未来增加 equal 或 free_float_mv，应新增 weight_basis 版本，不得悄悄改写历史 total_mv 结果。

## 7. 行业 FTTM 聚合合同

### 7.1 第一级：机构内行业聚合

对 obs_date、industry_level、industry_code、org_name，设机构覆盖股票集合为 C：

~~~text
normalized_weight_i = total_mv_i / sum(total_mv_j for j in C)
org_industry_fttm = sum(normalized_weight_i * stock_fttm_np_i for i in C)
~~~

约束：

- stock_fttm_np 来自同一 obs_date 的 pit.pit_stock_fttm_monthly。
- 行业归属来自同一 obs_date 的 pit.pit_industry_classification。
- 权重来自不晚于 obs_date 的 weight_trade_date。
- 不用 0 填充机构未覆盖股票。
- 不把不同机构的股票预测先混在一起做一遍股票级共识。
- 同时统计每个机构的 covered_stock_count 和 covered_mv_rate，并在行业行保存机构覆盖分布；否则行业并集覆盖率较高仍可能掩盖单家机构覆盖过窄。

### 7.2 第二级：机构间共识

对同一 obs_date、industry_level、industry_code：

~~~text
industry_fttm_np = arithmetic_mean(org_industry_fttm)
industry_fttm_np_median = median(org_industry_fttm)
org_count = count(distinct org_name)
~~~

原始设计的主值是机构间算术平均；median 只作为稳健性审计字段，不能替换主值。

### 7.3 环比变化

行业主值的基础环比：

~~~text
fttm_np_mom_abs = current_industry_fttm_np - previous_industry_fttm_np
fttm_np_mom_rate =
    fttm_np_mom_abs / abs(previous_industry_fttm_np)
~~~

当上月值为空或为零时，mom_rate 为空；mom_abs 仍可保留。

### 7.4 机构扩散度

扩散度只能使用本月和上月均存在的同名机构：

~~~text
matched_org_count = count(org in current AND previous)
up_org_count = count(org_industry_fttm_current > org_industry_fttm_previous)
diffusion_up = up_org_count / matched_org_count
~~~

规则：

- previous 指立即前一个自然月末，不是数据库中该行业任意最近一行。
- 相等不计为上升。
- matched_org_count 为 0 时 diffusion_up 为空。
- 不得以 0.5 代替未知。
- 必须保留 up_org_count、down_or_flat_org_count、matched_org_count。

### 7.5 质量状态

首版质量门槛应配置化，建议默认值：

| 指标 | 一级行业 | 二级行业 |
|---|---:|---:|
| covered_stock_count | 不少于 10 | 不少于 5 |
| covered_mv_rate | 不少于 60% | 不少于 60% |
| weight_data_coverage_rate | 不少于 98% | 不少于 98% |
| org_count | 不少于 5 | 不少于 5 |
| matched_org_count（扩散度） | 不少于 5 | 不少于 5 |

状态建议：

- eligible：主值质量门槛通过。
- ineligible_low_stock_coverage。
- ineligible_low_mv_coverage。
- ineligible_low_weight_coverage。
- ineligible_low_org_count。
- diffusion_ineligible_low_match。
- no_fttm_coverage。

一行可同时有多个失败原因，建议另存 quality_reasons 为排序稳定的文本数组或 JSONB。所有结构行业行都应落库，主值不足时允许为空。

### 7.6 行业表建议 DDL

文件建议：alphahome/pit/database/create_pit_industry_fttm_monthly_table.sql

~~~sql
CREATE TABLE IF NOT EXISTS pit.pit_industry_fttm_monthly (
    obs_date                    date         NOT NULL,
    classification_source      varchar(16)  NOT NULL,
    industry_level             varchar(8)   NOT NULL,
    industry_code              varchar(32)  NOT NULL,
    industry_name              varchar(128) NOT NULL,
    weight_basis               varchar(32)  NOT NULL,
    weight_trade_date          date,
    weight_staleness_days      integer,
    structural_member_count    integer      NOT NULL,
    active_member_count        integer      NOT NULL,
    weight_available_count     integer      NOT NULL,
    covered_stock_count        integer      NOT NULL,
    org_count                  integer      NOT NULL,
    median_org_stock_count     numeric,
    median_org_mv_coverage     numeric(12,8),
    p25_org_mv_coverage        numeric(12,8),
    matched_org_count          integer      NOT NULL,
    up_org_count               integer      NOT NULL,
    down_or_flat_org_count     integer      NOT NULL,
    structural_mv              numeric,
    covered_mv                 numeric,
    covered_stock_rate         numeric(12,8),
    covered_mv_rate            numeric(12,8),
    weight_data_coverage_rate  numeric(12,8),
    industry_fttm_np           numeric,
    industry_fttm_np_median    numeric,
    previous_industry_fttm_np  numeric,
    fttm_np_mom_abs            numeric,
    fttm_np_mom_rate           numeric,
    diffusion_up               numeric(12,8),
    is_eligible                boolean      NOT NULL,
    is_diffusion_eligible      boolean      NOT NULL,
    quality_reasons            jsonb        NOT NULL DEFAULT '[]'::jsonb,
    stock_formula_version      varchar(32)  NOT NULL,
    aggregation_version        varchar(32)  NOT NULL,
    quality_rule_version       varchar(32)  NOT NULL,
    source_max_report_date     date,
    created_at                 timestamptz  NOT NULL DEFAULT now(),
    updated_at                 timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (
        obs_date,
        classification_source,
        industry_level,
        industry_code,
        weight_basis
    ),
    CHECK (industry_level IN ('L1', 'L2')),
    CHECK (covered_stock_rate BETWEEN 0 AND 1 OR covered_stock_rate IS NULL),
    CHECK (covered_mv_rate BETWEEN 0 AND 1 OR covered_mv_rate IS NULL),
    CHECK (weight_data_coverage_rate BETWEEN 0 AND 1 OR weight_data_coverage_rate IS NULL),
    CHECK (median_org_mv_coverage BETWEEN 0 AND 1 OR median_org_mv_coverage IS NULL),
    CHECK (p25_org_mv_coverage BETWEEN 0 AND 1 OR p25_org_mv_coverage IS NULL),
    CHECK (diffusion_up BETWEEN 0 AND 1 OR diffusion_up IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_pit_industry_fttm_level_date
    ON pit.pit_industry_fttm_monthly (industry_level, obs_date);
~~~

建议聚合版本首版固定为 org_mv_weighted_then_equal_mean_v1，质量规则版本固定为 coverage_gate_v1。

median_org_mv_coverage 和 p25_org_mv_coverage 在 V1 先作为监控字段，不在缺少全历史分布证据时拍脑袋设硬门槛；full 回放报告必须给出其 L1/L2 分布，再决定 quality_rule_v2 是否增加机构内覆盖门槛。

### 7.7 与旧产物的语义映射

| strat_research 旧字段 | AlphaHome PIT 字段 | 说明 |
|---|---|---|
| month_end | obs_date | 自然月末 PIT 时钟 |
| fttm_np_weighted | industry_fttm_np | 先机构内加权、再机构间平均 |
| diffusion_index | diffusion_up | 新表严格使用相邻月同机构交集；缺失为空 |
| constituent_count | org_count | 旧实现实际统计机构行数，不是股票成分数 |
| fttm_np_zscore | 不进入 PIT | 留给研究/因子层 |
| diffusion_zscore | 不进入 PIT | 留给研究/因子层 |
| fttm_np_DI_zscore | 不进入 PIT | 留给研究/因子层 |
| sentiment_score / display score | 不进入 PIT | 属于策略展示口径 |

兼容视图不得把 org_count 再命名为含义错误的股票成分数；如必须保留旧列名，应在视图注释中明确其历史语义。

## 8. PIT 任务注册合同

建议新增 alphahome/pit/tasks/fttm.py，并注册两个独立任务。

### 8.1 个股任务

~~~python
PITTaskContract(
    task_name="pit_stock_fttm_monthly",
    domain="stock_fttm",
    source_tables=(
        "rawdata.stock_report_rc",
        "rawdata.stock_dailybasic",
    ),
    output_table="pit.pit_stock_fttm_monthly",
    pit_time_key="obs_date",
    primary_keys=("ts_code", "org_name", "obs_date"),
    dependencies=(),
    supported_modes=(
        "incremental",
        "full_backfill",
        "manual_range",
        "audit_only",
    ),
    manager_class=PITStockFTTMManager,
    audit_entity_keys=("ts_code",),
    audit_denominator="pit_time_active_stocks",
)
~~~

### 8.2 行业任务

~~~python
PITTaskContract(
    task_name="pit_industry_fttm_monthly",
    domain="industry_fttm",
    source_tables=(
        "pit.pit_stock_fttm_monthly",
        "pit.pit_industry_classification",
        "rawdata.stock_dailybasic",
        "rawdata.stock_basic",
    ),
    output_table="pit.pit_industry_fttm_monthly",
    pit_time_key="obs_date",
    primary_keys=(
        "obs_date",
        "classification_source",
        "industry_level",
        "industry_code",
        "weight_basis",
    ),
    dependencies=(
        "pit_stock_fttm_monthly",
        "pit_industry_classification",
    ),
    supported_modes=(
        "incremental",
        "full_backfill",
        "manual_range",
        "audit_only",
    ),
    manager_class=PITIndustryFTTMManager,
    audit_entity_keys=(
        "classification_source",
        "industry_level",
        "industry_code",
        "weight_basis",
    ),
    audit_denominator="pit_time_structural_industries",
)
~~~

如果当前 PITTaskContract 的 dependencies 只接受任务注册名而不接受 output_table 名称，应统一使用注册名，不能在调度器内再硬编码一套别名。

## 9. Manager 与计算器职责

### 9.1 PITStockFTTMManager

负责：

- 解析运行模式和目标月份。
- 调用 StockFTTMCalculator 批量计算。
- 生成源数据审计计数。
- 使用月级 staging 和事务替换目标月份。
- 提交任务运行元数据。

必须与 PITTask 适配器现有调用名兼容：

~~~python
incremental_update(months: int = 8, batch_size: int | None = None)
full_backfill(
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    batch_size: int = 12,
)
~~~

StockFTTMCalculator 负责：

- Q4 解析。
- np/EPS 路径。
- 同一研报事件内 FY1/FY2 配对和回退。
- 以 report_date 季度计算事件级权重与 FTTM。
- 应用六个月可见窗口。
- 对股票、机构做月末最新事件稳定选择。

### 9.2 PITIndustryFTTMManager

负责：

- 确认依赖月份已存在并审计通过。
- 获取同日动态行业成分和 PIT-time 有效股票。
- 获取月末市值权重。
- 同时生成 L1、L2 结构全集。
- 计算机构内聚合、机构间共识、上月匹配和扩散度。
- 应用质量门槛并原子替换目标月份。

同样实现 incremental_update 和 full_backfill；参数语义与个股 manager 一致。manual_range 由 PITTask 适配器调用 full_backfill，audit_only 由 PITAuditService 处理，不在 manager 中另造同名分支。

计算器不得自行连接“当前行业表”或当前股票清单。

## 10. 全量、增量与修订语义

### 10.1 Full

- 默认从 2014-01-31 起计算到最新完整月末。
- 历史起点必须可配置，不能散落硬编码。
- 计算时额外读取起点前一个月末作为机构扩散度锚点，只持久化配置区间内月份。
- 建议每批 12 个月；单批失败时整个批次回滚。
- 首次回放先建 staging 表并完成行数、唯一性和日期范围检查，再写正式表。
- 如需扩展至 2010 年，先输出年度覆盖率剖面，不直接假设早期结果与 2014 年后同质量。

### 10.2 Incremental

每次增量至少重算最近 8 个完整月：

- 六个月预测可见窗口。
- 一个前月锚点用于扩散度。
- 一个安全缓冲月。

写入区间为最近 8 个完整月；计算器还需额外计算写入区间前一个月末的机构行业结果作为只读锚点，该锚点不重复写入。若现有锚点不可复算，最早写入月的 diffusion_up 必须为空并给出质量原因。

滚动 8 个月只是最低修订覆盖，不代表能自动捕获任意久远的源表补录。若源表提供经过验证的 updated_at、批次审计表或行级变更日志，增量计划器还应：

- 将变更 report_date 展开到其可能影响的六个月月末窗口。
- 将 stock_dailybasic 的历史变更映射到 EPS 回退事件或对应权重月份。
- 将 pit_industry_classification 的历史变更映射到相同 obs_date。
- 合并滚动窗口、变更驱动月份和第 10.3 节的相邻月传播闭包。

若没有可靠变更时间戳，必须在运行报告中声明该限制，并通过定期 full/audit 哈希或人工 manual_range 修复久远历史，不能声称增量已覆盖全部历史修订。

不能只从 max(obs_date) 向后追加，因为 stock_report_rc 会出现历史报告修订、补录或机构记录变化。

### 10.3 相邻月传播

如果月份 t 的个股或行业结果发生变化：

- 必须重算月份 t。
- 必须重算月份 t + 1 的行业环比和扩散度。
- 若 t + 1 超过最新完整月，则不生成未来行。

增量计划器应先展开受影响月份闭包，再按依赖顺序执行。

### 10.4 原子替换

每个重算月份采用：

1. 写入 staging。
2. 校验主键唯一、行数、空值和日期。
3. 在同一事务中删除正式表对应 obs_date 的旧行。
4. 从 staging 插入新行。
5. 提交事务。

不能只做 upsert。若某机构或行业行因源数据修订而消失，upsert 无法删除旧脏行。

## 11. 统一更新器改造

在 alphahome/pit/pit_data_update_production.py 增加目标：

~~~python
TARGET_TO_TASK = {
    # existing targets...
    "stock_fttm": "pit_stock_fttm_monthly",
    "industry_fttm": "pit_industry_fttm_monthly",
}
~~~

默认 all 顺序至少满足：

~~~text
industry_classification
        |
        +----+
             v
stock_fttm -> industry_fttm
~~~

stock_fttm 与 industry_classification 可以并行；industry_fttm 必须等待两者成功。

当前更新器只对已有财务任务做了局部依赖处理。实现本任务时应改为读取 PITTaskContract.dependencies 的通用 DAG：

- 对显式 target 展开传递依赖闭包；单独请求 industry_fttm 时，默认也调度 stock_fttm 和 industry_classification。
- 拓扑排序。
- 检测循环依赖。
- 上游失败时下游标记 skipped_dependency_failed。
- parallel 模式只能并行同一拓扑层。
- all 与显式 target 的依赖行为一致。
- 如未来增加 skip-dependencies 选项，必须先验证上游目标月份已存在且 freshness/audit 通过。

不要继续为 FTTM 增加新的硬编码 if 分支。

### 11.1 生产编排与 freshness guard

PIT 更新器不替代 raw fetcher 编排。进入 FTTM DAG 前，外层生产流程至少确认：

- tushare_stock_report_rc 成功完成。
- tushare_stock_dailybasic 成功完成。
- tushare_stock_basic 可查询。
- tushare_index_swmember 及 pit_industry_classification 成功完成目标月。

强制 guard：

- 目标 obs_date 是最新完整月末，不是运行日所在月的未来月末。
- pit_industry_classification 在目标月存在 sw L1/L2 行。
- weight_trade_date 可用且不晚于 obs_date。
- raw 源最大日期、目标月、运行时间和各依赖 execution status 写入运行报告。
- 任一上游失败或目标月缺失时，industry_fttm 不得沿用旧依赖结果伪装成功。

建议至少月度强制运行一次；如 raw 源存在补录，可每日运行幂等 incremental，但只有源指纹或目标月份发生变化时才执行月级替换。

## 12. 审计服务必须扩展

现有 PIT 审计默认目标表具有 ts_code，并以“当前上市股票数”为分母。行业聚合表不具备 ts_code，且历史审计不能使用当前上市股票分母。

实现前必须在 PITTaskContract 尾部增加具有向后兼容默认值的 audit_entity_keys、audit_denominator，并同步更新 to_dict/from_dict 对序列字段的转换。旧任务不填写时保持现有行为；两个 FTTM task 使用第 8 节给出的显式值。

行业任务的等价审计合同为：

~~~python
audit_entity_keys = (
    "classification_source",
    "industry_level",
    "industry_code",
    "weight_basis",
)

audit_denominator = "pit_time_structural_industries"
~~~

coverage SQL 必须从 audit_entity_keys 构造 COUNT DISTINCT 或等价子查询；不得无条件引用 t.ts_code。pit_time_active_stocks 按被审计 obs_date 的 list_date/delist_date 计算，pit_time_structural_industries 按同日分类表、来源和层级计算。

建议至少报告：

### 个股表

- obs_date 行数。
- 股票数、机构数。
- 双年度配对率、单边回退率。
- np 路径占比、EPS 回退占比。
- source_max_report_date 不得晚于 obs_date。
- 主键重复数。
- 负值、零值和空值计数。

### 行业表

- L1/L2 结构行业数、产值行业数、eligible 行业数。
- structural coverage 与 eligible coverage 分开。
- 各失败原因数量。
- 市值覆盖分位数、机构数分位数、匹配机构数分位数，以及机构内市值覆盖的中位数/P25。
- 行业名称、代码在同一来源下的一致性。
- source_max_report_date 不得晚于 obs_date。
- diffusion_up 越界数和分母不一致数。

审计不能为了复用 stock 逻辑而给行业表伪造 ts_code。

## 13. 预计文件变更

建议实现者按以下边界修改：

### 新增

- alphahome/pit/tasks/fttm.py
- alphahome/pit/calculators/stock_fttm_calculator.py
- alphahome/pit/pit_stock_fttm_manager.py
- alphahome/pit/pit_industry_fttm_manager.py
- alphahome/pit/database/create_pit_stock_fttm_monthly_table.sql
- alphahome/pit/database/create_pit_industry_fttm_monthly_table.sql
- tests/unit/test_stock_fttm_calculator.py
- tests/unit/test_pit_stock_fttm_manager.py
- tests/unit/test_pit_industry_fttm_manager.py
- tests/unit/test_pit_data_update_production.py
- tests/integration/test_pit_fttm_pipeline.py

### 修改

- alphahome/pit/base/pit_task.py：为审计实体和分母增加向后兼容的 contract 元数据。
- alphahome/pit/pit_data_update_production.py：增加 target，并改为通用依赖 DAG。
- alphahome/pit/audit_service.py：支持非股票实体和 PIT-time 分母。
- tests/unit/test_pit_task_framework.py：覆盖新增 contract 字段的兼容序列化。
- tests/unit/test_pit_audit_service.py：覆盖行业实体和 PIT-time 分母。
- 必要的 GUI 任务清单或服务层：只从注册表读取，避免再维护重复清单。
- docs/pit_incremental_update_guide.md：增加 FTTM 的回放窗口与修订传播说明。
- scripts/production/README.md：增加运行示例。

alphahome/pit/tasks/__init__.py 当前通过 pkgutil.walk_packages 自动发现新模块，正常情况下无需增加手工导入；只需用注册发现测试证明 fttm.py 可见。

建表脚本沿用现有 alphahome/pit/database 目录；若实施时仓库已迁移到统一 migration 机制，以届时现行规范为准，不另造平行目录。

## 14. 测试要求

### 14.1 公式单元测试

必须覆盖：

- Q1、Q2、Q3、Q4 四组权重。
- report_date 位于 Q2 时，FY1=100、FY2=200，结果应为 125。
- report_date 位于 Q4 时，FY1=100、FY2=200，结果应为 175。
- FY1-only 和 FY2-only 回退。
- 两者均缺失不产出。
- 负值、零值不被过滤。
- np 优先于 EPS。
- np 缺失时 EPS × total_share 单位正确。
- 无同日 total_share 时 EPS 路径不向未来取值。
- FY1、FY2 只能在同一个 ts_code、org_name、author_name、report_date 事件内配对。
- 2024-12 报告的年度配对是 2024Q4、2025Q4；该报告在 2025-01 月末仍被选中时，不改配为 2025Q4、2026Q4。

### 14.2 PIT 与稳定性测试

必须覆盖：

- obs_date 之后的报告不会进入结果。
- 六个月边界左开右闭。
- 同机构同财年多报告只选窗口内最新。
- 先计算事件级 FTTM、后选择月末最新事件；顺序不得颠倒。
- 不同 report_date 的 FY1、FY2 不会被混合配对。
- 完全相同输入重复运行字节级或行级一致。
- 同 report_date 多作者时按 author_name 升序稳定选择；精确重复行也必须确定性去重。
- 月末为非交易日时权重取此前最近交易日。
- 过期超过 31 天的权重被标记或拒绝。

### 14.3 行业聚合测试

用最小手算夹具覆盖：

- 两股票、两机构的市值加权结果。
- 机构内权重只在覆盖股票中归一化。
- 机构间使用算术平均。
- L1 与 L2 同时正确。
- 当月动态调入、调出只影响对应月。
- 已退市股票不进入退市后的 active 分母。
- 本月与上月机构交集、上升数、扩散度正确。
- 无匹配机构时 diffusion_up 为空。
- 低覆盖行业保留结构行并正确标记 ineligible。

### 14.4 增量回放测试

必须覆盖：

- 新增一条历史报告后，最近 8 个月被重算。
- t 月结果变化会触发 t + 1 扩散度重算。
- 源行被删除后，正式表旧行能被月级替换清除。
- 上游失败时行业任务不运行。
- parallel 与串行结果相同。
- full 后再 incremental 不产生重复主键。

### 14.5 注册与审计回归测试

必须覆盖：

- discover_tasks 自动发现 pit_stock_fttm_monthly 和 pit_industry_fttm_monthly，无需手工 import。
- PITTaskContract 新增审计字段的 to_dict/from_dict 往返一致。
- 旧任务省略新增字段时行为不变。
- stock FTTM 覆盖率按 obs_date 当时 active 股票计算。
- industry FTTM 审计 SQL 不引用 ts_code，并分别报告 L1/L2 structural、valued、eligible 覆盖。
- industry 表不存在、为空、只有结构空值行和正常产值行四种状态均能审计，不抛 SQL 列不存在异常。

## 15. 数据验收 SQL

以下 SQL 是实现后的最低验收集。

### 15.1 主键唯一性

~~~sql
SELECT ts_code, org_name, obs_date, COUNT(*)
FROM pit.pit_stock_fttm_monthly
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;

SELECT obs_date, classification_source, industry_level,
       industry_code, weight_basis, COUNT(*)
FROM pit.pit_industry_fttm_monthly
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) > 1;
~~~

预期均返回 0 行。

### 15.2 未来数据泄漏

~~~sql
SELECT COUNT(*) AS leaked_rows
FROM pit.pit_stock_fttm_monthly
WHERE source_max_report_date > obs_date
   OR selected_report_date > obs_date
   OR selected_report_date <= obs_date - interval '6 months';

SELECT COUNT(*) AS leaked_rows
FROM pit.pit_industry_fttm_monthly
WHERE source_max_report_date > obs_date;
~~~

预期均为 0。

### 15.3 公式复算

~~~sql
SELECT COUNT(*) AS formula_mismatch
FROM pit.pit_stock_fttm_monthly
WHERE abs(
        fy1_weight - (5 - report_quarter)::numeric / 4
      ) > 0.000001
   OR abs(
        fy2_weight - (report_quarter - 1)::numeric / 4
      ) > 0.000001
   OR abs(
        fttm_np - (
            fy1_weight * fy1_np_used
            + fy2_weight * fy2_np_used
        )
      ) > 0.000001;
~~~

预期为 0。

### 15.4 月份连续性

~~~sql
WITH bounds AS (
    SELECT
        date_trunc('month', min(obs_date))::date AS min_month,
        date_trunc('month', max(obs_date))::date AS max_month
    FROM pit.pit_industry_fttm_monthly
),
expected AS (
    SELECT (m.month_start + interval '1 month - 1 day')::date AS obs_date
    FROM bounds b
    CROSS JOIN LATERAL generate_series(
        b.min_month,
        b.max_month,
        interval '1 month'
    ) AS m(month_start)
)
SELECT e.obs_date
FROM expected e
LEFT JOIN (
    SELECT DISTINCT obs_date
    FROM pit.pit_industry_fttm_monthly
) a USING (obs_date)
WHERE a.obs_date IS NULL
ORDER BY 1;
~~~

预期返回 0 行。

### 15.5 L1/L2 结构覆盖

~~~sql
WITH expected AS (
    SELECT obs_date, 'L1'::text AS industry_level,
           industry_code1 AS industry_code
    FROM pit.pit_industry_classification
    WHERE data_source = 'sw' AND industry_code1 IS NOT NULL
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT obs_date, 'L2'::text AS industry_level,
           industry_code2 AS industry_code
    FROM pit.pit_industry_classification
    WHERE data_source = 'sw' AND industry_code2 IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT e.obs_date, e.industry_level, e.industry_code
FROM expected e
LEFT JOIN pit.pit_industry_fttm_monthly a
  ON a.obs_date = e.obs_date
 AND a.classification_source = 'sw'
 AND a.industry_level = e.industry_level
 AND a.industry_code = e.industry_code
 AND a.weight_basis = 'total_mv'
WHERE a.industry_code IS NULL;
~~~

预期为 0 行。没有 FTTM 覆盖的结构行业也应有 ineligible 行。

### 15.6 扩散度内部一致性

~~~sql
SELECT COUNT(*) AS invalid_diffusion_rows
FROM pit.pit_industry_fttm_monthly
WHERE
    matched_org_count <> up_org_count + down_or_flat_org_count
    OR diffusion_up < 0
    OR diffusion_up > 1
    OR (matched_org_count = 0 AND diffusion_up IS NOT NULL)
    OR (matched_org_count > 0 AND diffusion_up IS NULL)
    OR (
        matched_org_count > 0
        AND abs(
            diffusion_up
            - up_org_count::numeric / matched_org_count
        ) > 0.000001
    );
~~~

预期为 0。

### 15.7 截面质量分布

~~~sql
SELECT
    obs_date,
    industry_level,
    COUNT(*) AS structural_industries,
    COUNT(*) FILTER (WHERE industry_fttm_np IS NOT NULL) AS valued_industries,
    COUNT(*) FILTER (WHERE is_eligible) AS eligible_industries,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY covered_mv_rate)
        AS median_mv_coverage,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY covered_stock_rate)
        AS median_stock_coverage,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY median_org_mv_coverage)
        AS cross_industry_median_org_mv_coverage
FROM pit.pit_industry_fttm_monthly
GROUP BY 1, 2
ORDER BY 1, 2;
~~~

该结果用于判断数据可用性，不作为策略有效性结论。

## 16. 运行与验证命令

当前生产入口支持 target、mode、parallel、workers。实现注册后，最小烟雾测试应能执行：

~~~powershell
python -m alphahome.pit.pit_data_update_production --target stock_fttm --mode incremental --log-level INFO
python -m alphahome.pit.pit_data_update_production --target industry_fttm --mode incremental --log-level INFO
python -m alphahome.pit.pit_data_update_production --target stock_fttm industry_fttm --mode incremental --parallel --workers 2 --log-level INFO
~~~

全量历史区间建议通过 manager 配置或统一任务配置传入；当前 CLI 尚无 start-date、end-date 参数，不应在文档中假装已有。

建议测试命令：

~~~powershell
pytest -q tests/unit/test_stock_fttm_calculator.py
pytest -q tests/unit/test_pit_stock_fttm_manager.py tests/unit/test_pit_industry_fttm_manager.py
pytest -q tests/unit/test_pit_task_framework.py tests/unit/test_pit_audit_service.py tests/unit/test_pit_data_update_production.py
pytest -q tests/integration/test_pit_fttm_pipeline.py
python -m alphahome.pit.pit_data_update_production --target stock_fttm industry_fttm --mode incremental --log-level INFO
~~~

## 17. 性能与资源验收

数据规模足以支持 L2，但实现必须避免构造全历史、全机构、全股票的笛卡尔积。

要求：

- 按 obs_date 或不超过 12 个月的批次计算。
- stock_report_rc 至少利用 report_date、ts_code、quarter 的索引或等价查询剪枝。
- 两张目标表按 obs_date 建索引；数据继续增长时评估按年分区。
- 中间机构行业结果可使用临时表或分批 DataFrame，不默认长期落正式表。
- 单月失败不留下半月数据。
- 日志输出每月源行数、候选行数、个股结果数、L1/L2 结果数、eligible 数、耗时和峰值内存。

首轮 full 回放必须留存基准：

- 总耗时。
- 每月中位数和 P95 耗时。
- 峰值内存。
- 两张表总行数和磁盘占用。
- full 与第二次 full 的结果哈希或逐列差异。
- full 与 incremental 重叠月份差异应为 0。

## 18. 实施顺序

### Phase 1：冻结夹具与公式

- 从真实源表抽取少量匿名化夹具。
- 先实现 Q4、FY1/FY2、回退和季度公式单测。
- 以 strat_research 原始设计的手算样例作为行为基线。

### Phase 2：个股 PIT

- 建表。
- 实现 calculator、manager、task contract。
- 完成 full 与 incremental 的确定性验证。
- 完成个股审计。

### Phase 3：行业 PIT

- 先用一个月份同时生成 L1、L2。
- 对照 SQL 手算机构内、市值加权和机构间平均。
- 加入覆盖率、质量状态、环比和扩散度。
- 建行业域审计。

### Phase 4：调度与回放

- 将更新器改成通用依赖 DAG。
- 完成最近 8 个月增量和 t+1 传播测试。
- 执行 2014 年以来 full。
- 对最新完整月做人工抽样复核。

### Phase 5：消费者接入

- 只在 PIT 数据验收完成后接入研究消费者。
- z-score、行业横截面标准化和策略评分留在研究或因子层。
- 消费者必须默认读取 is_eligible，并显式决定是否保留不合格结构行。

## 19. 完成定义

只有同时满足以下条件，才能把“FTTM 一级、二级行业 PIT 数据任务完成”写入实施报告：

- 两项任务均在正式 PIT registry 中可发现。
- 一任务一表，依赖关系由 contract 声明。
- Q4 年度预测在同一研报事件内按 FY1/FY2 公式合成，不再直接令 fttm_np 等于 np。
- FY1/FY2 年份与季度权重均锚定 report_date，而不是月末 obs_date。
- 负值和零值被保留，单边回退有质量标记。
- report_date 不晚于 obs_date，且六个月窗口边界有测试。
- 行业归属使用同月末 pit_industry_classification。
- L1、L2 结构全集均落库，质量不合格行未被静默删除。
- 市值权重日期、覆盖率和代理口径可审计。
- 扩散度只使用相邻月份同机构交集，未知不填 0.5。
- full、incremental、manual、audit 模式均可运行。
- 修订能重算最近窗口并传播到 t+1。
- 月级替换能删除源数据修订后消失的旧行。
- 审计服务支持行业实体，不伪造 ts_code，不使用当前股票分母审计历史。
- 单元测试、集成测试和第 15 节 SQL 全部通过。
- 留存真实 full 回放证据、性能基准和最新完整月质量报告。

“数据任务完成”仍不等于“行业 FTTM 因子有效”。如需将其用于选行业、择时或实盘，必须另做时间切分、稳健性、交易可实现性和样本外验证。

## 20. 已知风险与实现时不得擅自改变的决定

| 风险或歧义 | 本 HANDOFF 的决定 |
|---|---|
| Q4 是否只是第四季度单季预测 | 否；按完整财年预测处理 |
| fttm_np 是否可直接等于 np | 否；必须 FY1/FY2 合成 |
| FY1/FY2 和季度权重锚定什么日期 | 锚定同一研报事件的 report_date，不锚定 obs_date |
| 是否可跨 report_date 拼接 FY1/FY2 | 否；必须来自同一 ts_code、org_name、author_name、report_date |
| 亏损预测是否删除 | 否；负值和零值均保留 |
| 只有一个财年预测怎么办 | 单边回退并标记，不冒充完整配对 |
| 行业映射是否可用当前静态分类 | 否；使用同日 PIT 动态分类 |
| L2 数据是否足够 | 总体足够，但按月、按行业输出质量状态 |
| 行业权重是否等同官方申万权重 | 否；V1 是 total_mv 代理 |
| 缺失扩散度是否填 0.5 | 否；保持为空 |
| 增量是否只追加最新月 | 否；最近 8 月回放并处理 t+1 |
| 是否只 upsert | 否；重算月原子替换 |
| PIT 表是否保存策略分数 | 否；只保存可复算基础指标和质量字段 |
| 是否可复用当前股票审计分母 | 否；行业域使用 PIT-time 结构行业分母 |

如实现者确需改变上表任一决定，应先提交一份差异说明，至少包含：

1. 改动理由。
2. 新旧公式或数据合同。
3. 受影响的历史月份和行数。
4. 对 L1、L2 覆盖率与结果的并行对比。
5. 是否引入未来信息或修订偏差。
6. 回滚方案。

## 21. 最终实施报告模板

实现完成后，提交者应在 PR 或任务记录中填写：

~~~text
实现版本：
运行日期：
源数据最大 report_date：
行业分类最大 obs_date：
全量区间：
增量回放月数：

pit_stock_fttm_monthly：
  总行数：
  股票数：
  机构数：
  双年度配对率：
  单边回退率：
  EPS 回退率：
  主键重复数：
  未来数据行数：

pit_industry_fttm_monthly：
  L1 月份数 / 行数：
  L2 月份数 / 行数：
  最新月 L1 eligible / structural：
  最新月 L2 eligible / structural：
  最新月股票覆盖率：
  最新月市值覆盖率：
  主键重复数：
  未来数据行数：

验证：
  单元测试命令与结果：
  集成测试命令与结果：
  SQL 验收结果：
  full/incremental 重叠差异：
  重跑确定性差异：
  性能与峰值内存：

未解决问题：
策略或生产晋级状态：未授权 / 待独立验证
~~~
