# AlphaDB NAS 可持续同步

脚本入口：

```bash
python scripts/database/alphadb_nas_logical_sync.py bootstrap
python scripts/database/alphadb_nas_logical_sync.py sync-now
python scripts/database/alphadb_nas_logical_sync.py status
```

## 设计

- 本机 `alphadb` 作为 publisher
- NAS `alphadb` 作为 subscriber
- 使用 PostgreSQL 原生 logical replication 精确同步表级 `INSERT / UPDATE / DELETE / TRUNCATE`
- 离线期间变更留在本机 WAL；恢复到能连 NAS 的网络后，用 `sync-now` 补齐

## bootstrap 做什么

- 检查并设置本机 PostgreSQL：
  - `wal_level = logical`
  - `max_replication_slots >= 16`
  - `max_wal_senders >= 16`
  - `listen_addresses = '*'`
- 创建或更新复制用户 `alphadb_sync`
- 在本机 `pg_hba.conf` 增加 NAS 到本机的复制访问规则
- 为无主键表设置 `REPLICA IDENTITY FULL`
- 创建本地 publication：`alphadb_nas_pub`
- 创建本地 logical replication slot：`alphadb_nas_slot`
- 在 NAS 上创建或更新 subscription：`alphadb_nas_sub`

注意：

- 如果脚本提示需要重启 PostgreSQL，这是正常的。`wal_level` 变更必须重启后才能生效。
- Windows 防火墙仍需允许 NAS `192.168.5.6` 访问本机 PostgreSQL `5432/tcp`。

## sync-now 做什么

- 发现当前本机对 NAS 可达的局域网 IP
- 更新 NAS subscription 的 publisher 连接串
- 启用 subscription 并等待 NAS 追平到本机当前 WAL LSN
- 补齐 NAS 上的 sequence 值
- 可选刷新 NAS 全部 materialized view

建议：

```bash
python scripts/database/alphadb_nas_logical_sync.py sync-now
```

若需要把 NAS 上的 `features` 物化视图也重新算到最新：

```bash
python scripts/database/alphadb_nas_logical_sync.py sync-now --refresh-materialized-views
```

如果自动探测到的本机地址不是 NAS 能回连的 LAN IP，可以显式指定：

```bash
python scripts/database/alphadb_nas_logical_sync.py sync-now --publisher-host 192.168.5.x
```

## 限制

- PostgreSQL 逻辑复制不自动同步 DDL。若本机新增列、改列类型、改主键等，需要先让 NAS schema 对齐。
- PostgreSQL 逻辑复制不复制 sequence 对象本身，因此脚本会在追平后额外执行 `setval(...)`。
- PostgreSQL 逻辑复制不复制 materialized view，因此脚本只提供“在 NAS 端刷新”。
- 若本机离线时间太长且 WAL 保留策略不足，逻辑复制槽可能失效；当前本机 `max_slot_wal_keep_size = -1`，能保证精确追平，但会占用本机磁盘。
