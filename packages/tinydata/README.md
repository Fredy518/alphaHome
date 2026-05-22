# tinydata

`tinydata` 是一个基于天软 TS-OPI 的轻量级直连数据接口包。它在 AlphaHome 仓库内孵化，但运行时不依赖 AlphaHome、AlphaDB、GUI 或任务系统，也不使用 pyTSL 本地登录会话。

## 安装

开发安装：

```bash
cd packages/tinydata
pip install -e ".[test]"
```

## 配置

显式配置：

```python
import tinydata as td

td.configure(
    user="your_user",
    password="your_password",
    opi_url="https://opi.tinysoft.com.cn",
    opi_auth_mode="basic",
    timeout_ms=60000,
)
```

也可以使用环境变量：

```text
TINYDATA_USER
TINYDATA_PASSWORD
TINYDATA_OPI_URL
TINYDATA_OPI_AUTH_MODE
TINYDATA_OPI_SESSION_KEY
TINYDATA_OPI_SESSION_PASSWORD
TINYDATA_OPI_RUN_FUNC_NAME
TINYDATA_OPI_QUERY_FUNC_NAME
TINYDATA_CACHE_DIR
TINYDATA_CODE_DIR
```

或使用 `~/.tinydata/config.toml`：

```toml
[tinydata]
user = "your_user"
password = "your_password"
opi_url = "https://opi.tinysoft.com.cn"
opi_auth_mode = "basic"
timeout_ms = 60000
request_interval = 0.2
```

如果使用 SESSION-KEY / API-KEY 模式：

```toml
[tinydata]
opi_url = "https://opi.tinysoft.com.cn"
opi_auth_mode = "session-key"
session_key = "your_session_key"
session_password = "optional_session_password"
run_func_name = "your_run_wrapper"
query_func_name = "your_query_wrapper"
```

## 代码池

对需要 `of code` 的 Tinysoft 表，`codes=None` 时会从本机代码池读取。默认目录：

```text
~/.tinydata/codes/
```

示例 `~/.tinydata/codes/fund.csv`：

```csv
ts_code
000001.OF
000002.OF
```

示例 `~/.tinydata/codes/stock.csv`：

```csv
ts_code
000001.SZ
600000.SH
```

## 使用示例

```python
import tinydata as td

df = td.fund_fof_holding_detail(
    codes=["012345.OF"],
    report_period="20231231",
)

df = td.fund_bond_holding(
    codes=["000001.OF"],
    start_date="20230101",
    end_date="20231231",
)

df = td.stock_basic_ext(codes=["000001.SZ", "600000.SH"])
```

## 缓存

业务接口默认使用本机 parquet 缓存：

```text
~/.tinydata/cache/
```

`refresh=False` 时优先读缓存；`refresh=True` 时重新请求天软并覆盖缓存。

## 测试

不需要天软账号的单元测试：

```bash
cd packages/tinydata
python -m pytest
```

真实天软连接测试后续使用 `requires_tinysoft` 标记，默认不运行。
