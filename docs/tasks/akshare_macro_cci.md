# 上下文
文件名：akshare_macro_cci.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_cci.py`，封装 akshare `index_cci_cx`，获取大宗商品指数 CCI。输入型通胀综合指标——单看铜/油等单一商品不够，CCI 综合反映原材料价格趋势，是国内 PPI 输入型通胀的前瞻信号。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，直接列映射（无 melt）。与 future_daily（单品种）互补。

# 分析 (由 RESEARCH 模式填充)
`index_cci_cx`（akshare 1.18.64 实测可用）返回 4234 行，列 `日期/大宗商品指数/变化值`，日频。直接映射即可。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroCciTask`：
- `domain="macro"`, `name="akshare_macro_cci"`, `table_name="macro_cci"`, `api_name="index_cci_cx"`, `primary_keys=["date"]`。
- `column_mapping`：`日期→date, 大宗商品指数→cci, 变化值→change`。
- `schema_def`：`date DATE`, `cci NUMERIC(12,4)`, `change NUMERIC(12,4)`。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_cci.py`, `test_akshare_macro_tasks.py`, `akshare_macro_cci.md`
    * 更改摘要：新增大宗商品指数 CCI 采集任务，单元测试全绿。
    * 原因：补齐输入型通胀综合指标。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
2026-09-06 提交前复核：接口迁移的 45 项相关测试通过，全项目 673 项单元测试通过。新版接口实际读取 4,289 行，覆盖 2009-01-09 至 2026-09-04，日期无重复、三列无空值；最新点位 373.456598，涨跌幅 0.6477306151%。本次复核只读接口，未再次执行入库。

## 2026-09-05：smart 更新接口迁移修复

- 复现：AkShare 1.18.64 的旧地址 `/api/index/pro/cxIndexTrendInfo` 返回 HTTP 200，但内容是财新 HTML 404 页面，导致 JSON 解析失败；与 smart 日期窗口无关。
- 新版公开页面 `https://yun.ccxe.com.cn/dataindices/indices` 使用 `POST /dataindices/cci`，表单 `month=""` 获取全历史。项目通过 `AkShareAPI.EXTRA_FUNCS` 接入新版，无须修改全局 AkShare 安装。
- 新响应 `data.month` 是日期列表，`data.data` 是指数点位列表。`change` 的历史口径是相邻观测日涨跌幅（%），不是点位差；在全历史上计算 `(cci / prev_cci - 1) * 100`，基期 2009-01-09 为 100 点、涨跌幅为 0，再按 smart/manual 窗口筛选。
- 源站全历史与现有库重叠 4,287 行，点位与涨跌幅的最大绝对误差均小于 0.0001（库字段为四位小数）。新版全历史共 4,289 行，覆盖 2009-01-09 至 2026-09-04。
- 对 HTTP/非 JSON/非成功响应、日期与点位错位、非法数值及缺少基期的截断历史报错，避免将接口异常当作成功空数据。
- 验证：`python -X utf8 -m pytest tests/unit/test_akshare_cci_source.py tests/unit/test_akshare_macro_tasks.py tests/unit/test_smart_refresh_interval_tasks.py -q` 退出码 0，45 passed。
- 正式复跑：2026-09-05 08:26（Asia/Shanghai），通过 GUI 使用的 `run_tasks` 服务以“智能增量”执行本任务，状态 `success`，回写 13 行（净新增 2 行）。库内共 4,289 行，最新日期 2026-09-04，CCI 373.4566、涨跌幅 0.6477%，日期/点位/涨跌幅无空值。执行摘要保存于 `tmp/cci_smart_verification_20260905.json`。
