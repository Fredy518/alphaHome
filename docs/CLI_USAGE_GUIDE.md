# CLI 入口与旧命令退役

旧 `ah/alphahome-cli/refresh-materialized-view` 已从安装入口下线。残留 `alphahome.cli.main` 的业务命令返回 unavailable=3，不再成功空跑；帮助参数仍可用。

```powershell
python run.py
python -m alphahome.factors --help
python -m alphahome.features --help
python -m alphahome.pit.pit_data_update_production --help
```

Features `list` 只发现注册配方，`schema` 只输出安装 SQL，`refresh/create --dry-run --task NAME` 只读预览。初始化脚本属于显式维护入口，不是普通刷新前置动作。各域 GUI、CLI 和兼容脚本提交相同领域协调器，计划漂移时必须重新预览。

见[生产入口矩阵](architecture/production-entrypoints.md)与[生产切换手册](setup/production-cutover-runbook.md)。
