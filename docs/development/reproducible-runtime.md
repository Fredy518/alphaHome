# 主项目运行环境与验证

主项目以根目录 `pyproject.toml` + `uv.lock` 为依赖权威，推荐 Python 3.12，最低 Python 3.10。`pytest` 配置只保留在 pyproject；警告正常显示。`packages/fundpos` 的版本、锁、wheel、运行目录和资源完全独立，不用根环境覆盖。

在新目录创建环境，先验证再切换生产入口解释器。PowerShell 示例：

```powershell
$env:UV_PROJECT_ENVIRONMENT = 'E:\AlphaHomeRuntimes\candidate-20260914'
uv sync --locked --python 3.12 --extra test --extra akshare
uv lock --check --offline
& "$env:UV_PROJECT_ENVIRONMENT\Scripts\python.exe" scripts/verify_runtime.py
& "$env:UV_PROJECT_ENVIRONMENT\Scripts\python.exe" -m pytest tests/unit
```

`UV_PROJECT_ENVIRONMENT` 应为本项目的新专用环境。`uv sync` 默认精确同步，会移除目标环境中未锁定的包，因此不要指向共享 Python 或 fundpos 环境。`--locked` 在元数据与锁不一致时失败，不能用 `--frozen` 掩盖过期锁。[uv 官方说明](https://docs.astral.sh/uv/concepts/projects/sync/)

可选组：`akshare` / `tinysoft` 是供应商适配，`research` 是 notebook/研究工具，`backtest` 保留 Zipline。正式 alphahome 与 tests 没有 Zipline 导入，因此不再将其作为数据维护环境的必装项。Pandas 保持 2.x 主版本契约；锁更新必须重跑测试，不自动跟随包索引更新。

NumPy 暂限定 `<2.4`，本次锁定 2.2.6：初次清洁解析得到 2.5.3，Pandas 2.x 日期运算出现大量 generic timedelta 弃用警告。这里冻结已验证的兼容组合，后续升级应单独验证日期语义，不在本轮改动 P/G 或 PIT 的数学公式。

数据库测试只有显式 `ALPHAHOME_TEST_DATABASE_URL` 才能连接，必须为 literal loopback、非 5432 端口、`alphahome_test_` 前缀数据库；普通本地配置不能作为测试目标。无测试 DSN 时 `requires_db` 跳过。API 测试另需 `ALPHAHOME_TEST_ALLOW_API=1`，日常验证保持未设置。隔离事务验收使用新建的专用 PG 实例，不把生产库或只读副本当作测试库。

最低版本验证使用单独 Python 3.10 环境执行相同锁安装和 `scripts/verify_runtime.py`。编译覆盖正式 alphahome/scripts，服务导入与 GUI 导入分别验证。测试完成后保留解释器、锁哈希、测试结果和漏洞查询时间；本次新环境并不自动替换 Windows 计划任务的解释器。第三方库警告与版本风险应据实记录，不能由 `--disable-warnings` 隐藏。
