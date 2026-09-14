# 隔离 PostgreSQL 集成测试

测试不读取 `~/.alphahome/config.json`，也不接受 `DATABASE_URL` 或旧的因子专用测试变量
作为默认目标。未设置 `ALPHAHOME_TEST_DATABASE_URL` 时，`requires_db` 测试跳过。
同步 psycopg2 和异步 asyncpg 的连接在调用驱动前核验目标；配置缺失或目标不匹配时拒绝。

准备独立临时 PostgreSQL 实例后设置：

```powershell
# 仅示意：账号和口令应来自该临时实例；必须使用显式非 5432 端口。
$env:ALPHAHOME_TEST_DATABASE_URL = 'postgresql://test_owner:TEST_ONLY@127.0.0.1:55439/alphahome_test_local'
python -m pytest tests/integration -q
```

允许的主机只有 `127.0.0.1` / `::1`，库名必须以 `alphahome_test_` 开头；禁止 libpq
service/hostaddr 覆盖和不匹配的数据库/端口。不得指向生产服务上的影子 schema，也不得
重用生产账户。当前测试会在临时库中创建和删除自己的领域 schema；已有因子 schema 时
测试拒绝覆盖。需要更换到空临时库后重试。

无隔离实例时保留跳过结果，不允许连接生产库代替。外部 API 测试需要额外显式设置
`ALPHAHOME_TEST_ALLOW_API=1`；数据库授权不等于 API、付费服务或生产任务授权。
