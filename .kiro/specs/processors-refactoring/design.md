# Design Document: Processors Module Refactoring

## Overview

本设计文档描述 alphahome/processors 模块的重构方案。重构目标是建立一个清晰的三层架构，并从 data_infra 模块迁移成熟的数据处理逻辑。

### 设计目标

1. **清晰的分层架构**: Engine → Task → Operation
2. **可复用的操作组件**: 原子级数据变换操作
3. **领域化的任务组织**: 按业务领域（market/index/style）组织任务
4. **完善的错误处理**: 优雅处理边界情况和异常

### 设计原则

- **单一职责**: 每个 Operation 只做一件事
- **无状态设计**: Operation 是纯函数，不保存状态
- **组合优于继承**: 通过 Pipeline 组合 Operation
- **防御性编程**: 处理空数据、零方差等边界情况

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     ProcessorEngine                          │
│  - 任务调度与并发控制                                          │
│  - 执行监控与状态追踪                                          │
│  - 错误处理与恢复                                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     ProcessorTaskBase                        │
│  - fetch_data(): 数据获取                                     │
│  - process_data(): 数据处理（编排 Operations）                 │
│  - save_result(): 结果保存                                    │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
    ┌──────────┐        ┌──────────┐        ┌──────────┐
    │  market/ │        │  index/  │        │  style/  │
    │  Tasks   │        │  Tasks   │        │  Tasks   │
    └──────────┘        └──────────┘        └──────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Operations Layer                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │ transforms  │  │  technical  │  │   missing   │          │
│  │  zscore     │  │  indicators │  │    data     │          │
│  │  rolling_*  │  │             │  │             │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Operations Layer

#### 1.1 Transform Operations (transforms.py)

从 data_infra/cleaners/transforms.py 迁移的核心变换函数：

```python
# 标准化函数
def zscore(series: pd.Series, mean: float = None, std: float = None) -> pd.Series
def minmax_scale(series: pd.Series, min_val: float = None, max_val: float = None) -> pd.Series

# 滚动计算函数
def rolling_zscore(series: pd.Series, window: int, min_periods: int = None) -> pd.Series
def rolling_percentile(series: pd.Series, window: int, min_periods: int = None) -> pd.Series
def rolling_sum(series: pd.Series, window: int, min_periods: int = None) -> pd.Series
def rolling_rank(series: pd.Series, window: int, min_periods: int = None) -> pd.Series
def rolling_slope(series: pd.Series, window: int, min_periods: int = None, method: str = "ols") -> pd.Series

# 去极值函数
def winsorize(series: pd.Series, window: int, n_std: float = 3.0, min_periods: int = None) -> pd.Series

# 分箱函数
def quantile_bins(series: pd.Series, boundaries: List[float] = None, quantiles: List[float] = None) -> pd.Series

# 收益率计算
def diff_pct(series: pd.Series, periods: int = 1) -> pd.Series
def log_return(series: pd.Series, periods: int = 1) -> pd.Series
def ema(series: pd.Series, span: int) -> pd.Series

# 高级特征函数
def price_acceleration(price: pd.Series, long_window: int = 252, short_window: int = 63, ...) -> pd.DataFrame
def rolling_slope_volatility_adjusted(price: pd.Series, window: int = 60, ...) -> pd.Series
def trend_strength_index(price: pd.Series, windows: List[int] = None, ...) -> pd.DataFrame
```

#### 1.2 Operation Base Class

```python
class Operation(ABC):
    """原子级数据处理操作基类"""
    
    def __init__(self, name: str = None, config: Dict = None):
        self.name = name or self.__class__.__name__
        self.config = config or {}
    
    @abstractmethod
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用操作到数据"""
        pass
```

#### 1.3 Operation Pipeline

```python
class OperationPipeline:
    """操作流水线，组合多个 Operation"""
    
    def add_operation(self, operation: Operation, condition: Callable = None) -> "OperationPipeline"
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame
```

### 2. Task Layer

#### 2.1 ProcessorTaskBase

```python
class ProcessorTaskBase(BaseTask, ABC):
    """处理任务基类"""
    
    task_type: str = "processor"
    source_tables: List[str] = []
    table_name: str = ""
    
    @abstractmethod
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """获取源数据"""
        pass
    
    async def process_data(self, data: pd.DataFrame, **kwargs) -> Optional[pd.DataFrame]:
        """处理数据（编排 Operations）"""
        pass
    
    @abstractmethod
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存处理结果"""
        pass
    
    async def run(self, **kwargs) -> Dict[str, Any]:
        """执行任务"""
        pass
```

#### 2.2 Task Directory Structure

```
alphahome/processors/tasks/
├── __init__.py
├── base_task.py
├── market/                    # 市场级特征
│   ├── __init__.py
│   └── market_technical.py    # 横截面技术特征
├── index/                     # 指数级特征
│   ├── __init__.py
│   └── index_volatility.py    # 指数波动率
└── style/                     # 风格因子
    ├── __init__.py
    └── style_momentum.py      # 风格动量
```

### 3. Engine Layer

#### 3.1 ProcessorEngine

```python
class ProcessorEngine:
    """处理引擎"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.task_status: Dict[str, str] = {}
    
    async def execute_task(self, task_name: str, **kwargs) -> Dict[str, Any]:
        """执行单个任务"""
        pass
    
    async def execute_tasks(self, task_names: List[str], **kwargs) -> List[Dict[str, Any]]:
        """并发执行多个任务"""
        pass
```

## Data Models

### 1. Task Execution Result

```python
@dataclass
class TaskExecutionResult:
    task_name: str
    status: str  # "success", "failed", "skipped"
    rows_processed: int
    execution_time: float
    error_message: Optional[str] = None
```

### 2. Market Technical Features Schema

MarketTechnicalTask 输出的 DataFrame 结构：

| Column | Type | Description |
|--------|------|-------------|
| trade_date | datetime | 交易日期（索引） |
| Mom_5D_Median | float | 5日动量中位数 |
| Mom_10D_Median | float | 10日动量中位数 |
| Mom_20D_Median | float | 20日动量中位数 |
| Mom_60D_Median | float | 60日动量中位数 |
| Mom_20D_Pos_Ratio | float | 20日正动量比例 |
| Vol_20D_Median | float | 20日波动率中位数 |
| Vol_60D_Median | float | 60日波动率中位数 |
| High_Vol_Ratio | float | 高波动股票比例 |
| Vol_Ratio_5D_Median | float | 5日量比中位数 |
| Vol_Expand_Ratio | float | 放量股票比例 |
| Price_Up_Vol_Down_Ratio | float | 价涨量缩比例 |
| Vol_Price_Aligned_Ratio | float | 量价同向比例 |
| *_ZScore | float | 各指标的滚动Z-Score |
| *_Pctl | float | 各指标的滚动百分位 |

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Transform output shape preservation
*For any* non-empty input Series, transform functions (zscore, rolling_zscore, rolling_percentile, winsorize) SHALL return a Series with the same length as the input.
**Validates: Requirements 1.1, 4.1, 4.2, 4.3, 4.4**

### Property 2: Rolling percentile value range
*For any* input Series and any window size, rolling_percentile SHALL return values in the range [0, 1] for all non-NaN outputs.
**Validates: Requirements 4.3**

### Property 3: Zscore zero variance handling
*For any* constant Series (all values equal), zscore and rolling_zscore SHALL return all zeros instead of NaN or infinity.
**Validates: Requirements 1.6, 4.1**

### Property 4: Winsorize bounds enforcement
*For any* input Series, winsorize output values SHALL be within n_std standard deviations of the rolling mean.
**Validates: Requirements 4.4**

### Property 5: Price acceleration output structure
*For any* non-empty price Series, price_acceleration SHALL return a DataFrame containing columns: slope_long, slope_short, acceleration, acceleration_zscore, slope_ratio.
**Validates: Requirements 4.6**

### Property 6: Trend strength consistency range
*For any* price Series, trend_strength_index output column 'trend_consistency' SHALL have values in range [0, 1] for all non-NaN outputs.
**Validates: Requirements 4.7**

### Property 7: Market technical feature completeness
*For any* valid market data input, MarketTechnicalTask SHALL produce output containing all required momentum, volatility, and volume features.
**Validates: Requirements 3.1, 3.2, 3.3, 3.4**

### Property 8: DataFrame serialization round-trip
*For any* DataFrame with datetime index, serializing then deserializing SHALL produce a DataFrame with equivalent index and column types.
**Validates: Requirements 6.3**

## Error Handling

### 1. Empty Data Handling

所有 Operation 和 Task 必须优雅处理空数据：

```python
async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    if data is None or data.empty:
        self.logger.warning("Received empty data, returning empty DataFrame")
        return pd.DataFrame()
    # ... normal processing
```

### 2. Zero Variance Handling

zscore 相关函数必须处理零方差情况：

```python
def zscore(series: pd.Series, ...) -> pd.Series:
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    return (series - mean) / std
```

### 3. Task Execution Error Handling

ProcessorEngine 在任务失败时继续执行其他任务：

```python
async def execute_tasks(self, task_names: List[str], **kwargs):
    results = []
    for name in task_names:
        try:
            result = await self.execute_task(name, **kwargs)
            results.append(result)
        except Exception as e:
            self.logger.error(f"Task {name} failed: {e}")
            results.append(TaskExecutionResult(
                task_name=name,
                status="failed",
                error_message=str(e)
            ))
    return results
```

## Testing Strategy

### Unit Testing

使用 pytest 进行单元测试：

- 测试每个 transform 函数的基本功能
- 测试边界情况（空数据、零方差、NaN 值）
- 测试 Task 的执行流程

### Property-Based Testing

使用 **hypothesis** 库进行属性测试：

```python
from hypothesis import given, strategies as st
import hypothesis.extra.pandas as pdst

@given(pdst.series(dtype=float, min_size=1))
def test_zscore_shape_preservation(series):
    """Property 1: Transform output shape preservation"""
    result = zscore(series)
    assert len(result) == len(series)

@given(pdst.series(dtype=float, min_size=10))
def test_rolling_percentile_range(series):
    """Property 2: Rolling percentile value range"""
    result = rolling_percentile(series, window=5)
    valid_values = result.dropna()
    assert all(0 <= v <= 1 for v in valid_values)
```

### Test Organization

```
alphahome/processors/tests/
├── __init__.py
├── conftest.py                    # pytest fixtures
├── test_transforms.py             # transform 函数测试
├── test_operations.py             # Operation 类测试
├── test_tasks/
│   ├── test_base_task.py
│   └── test_market_technical.py
└── test_engine.py                 # ProcessorEngine 测试
```

### Property Test Annotations

每个属性测试必须标注对应的设计文档属性：

```python
def test_zscore_zero_variance():
    """
    **Feature: processors-refactoring, Property 3: Zscore zero variance handling**
    **Validates: Requirements 1.6, 4.1**
    """
    # test implementation
```
