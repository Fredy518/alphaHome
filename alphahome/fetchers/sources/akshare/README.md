# Akshare 数据源模块

## 概述

本模块为 akshare 数据源提供统一的架构封装，借鉴了 tushare 和 pytdx 的设计模式。

akshare 是一个免费的开源金融数据接口库，提供多种数据源的访问。

## 架构组成

### 1. AkshareAPI 类 (`akshare_api.py`)

API 封装类，负责：
- 统一处理 akshare 库的调用
- 提供市场数据的获取接口
- 异常处理和日志记录

### 2. AkshareTask 类 (`akshare_task.py`)

任务基类，继承自 `FetcherTask`，提供：
- 统一的批次数据获取流程
- 数据转换和验证
- 错误处理和资源管理

### 3. 具体任务实现

具体的数据获取任务继承 `AkshareTask`，实现：
- `_fetch_raw_data()`: 获取原始数据
- `_transform_data()`: 数据格式转换
- `get_batch_list()`: 生成批次列表

## 使用示例

### 创建新的 akshare 任务

```python
from alphahome.fetchers.sources.akshare import AkshareTask
from alphahome.common.task_system.task_decorator import task_register

@task_register()
class MyAkshareTask(AkshareTask):
    """自定义 akshare 任务"""

    domain = "stock"
    name = "my_akshare_task"
    description = "我的 akshare 数据任务"
    table_name = "my_akshare_data"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        # 实现批次列表生成逻辑
        return [{"market": "SH"}, {"market": "SZ"}]

    async def _fetch_raw_data(self, params: Dict) -> List[Dict]:
        # 实现数据获取逻辑
        market = params.get("market")
        return await self.api.get_all_stocks_info(market)

    def _transform_data(self, raw_data: List[Dict], params: Dict) -> pd.DataFrame:
        # 实现数据转换逻辑
        df = pd.DataFrame(raw_data)
        # 数据处理...
        return df
```

## 优势

1. **架构一致性**: 与 tushare、pytdx 保持相同的架构模式
2. **代码复用**: 公共逻辑在基类中实现，避免重复
3. **维护性**: akshare API 变化时只需修改 API 封装类
4. **扩展性**: 容易添加新的 akshare 数据获取任务
5. **测试友好**: 模块化设计便于单元测试

## 现有任务

- `AkshareStockBasicTask`: 获取股票基本信息（代码、名称、市场等）
