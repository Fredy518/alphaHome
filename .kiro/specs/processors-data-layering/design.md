# Design Document: Processors Data Layering

## Overview

本设计文档描述 alphahome/processors 模块的数据分层架构。重构目标是建立清晰的"处理层（Clean）vs 特征层（Feature）"分层规范，使数据处理和特征计算职责分离。

### Scope Summary (与需求文档对齐)

| 范围 | 内容 |
|------|------|
| 范围内 | 处理层组件、特征层接口契约、任务层增强、现有任务分类 |
| 范围外 | 策略级 alpha 评估、具体重构代码实现、线上服务部署 |

### Success Criteria (验收点)

| 验收点 | 验证方式 |
|--------|----------|
| Clean Layer 组件实现 | DataValidator/Aligner/Standardizer/LineageTracker/Writer 单元测试通过 |
| 18 个正确性属性 | 属性测试全部通过（见 Property Test Coverage Matrix） |
| clean schema 表创建 | DDL 执行成功，表结构符合设计 |
| 任务分类表完成 | 覆盖所有现有任务，评审通过 |
| 性能 SLA | 高优先级任务 T+1 09:00 前完成；低优先级任务 T+1 收盘前完成 |

### 设计目标

1. **清晰的分层边界**: 处理层负责数据清洗和标准化，特征层负责衍生计算
2. **统一的数据契约**: clean schema 作为所有下游消费者的统一输入
3. **可追溯的数据血缘**: 每条记录都有来源和处理时间戳
4. **幂等的数据入库**: 支持安全重试和增量更新

### 设计原则

- **单一职责**: 处理层只做清洗，特征层只做计算
- **纯函数设计**: 特征函数无副作用，易于测试和缓存
- **防御性编程**: 处理空数据、零方差、除零等边界情况
- **显式优于隐式**: 血缘字段内联，不依赖外部元数据表
- **Best-effort 策略**: 对齐和标准化采用尽力而为策略，记录警告但不阻断流程

### 实现状态说明

**已完成**:
- ✅ Clean Layer 核心组件（Validator, Aligner, Standardizer, LineageTracker）
- ✅ Feature Layer 接口契约和纯函数实现
- ✅ Task Layer 增强（fetch → clean → feature → save 流程）
- ✅ 18 个正确性属性的属性测试
- ✅ Clean schema DDL 定义
- ✅ 任务分类表和特征入库白名单

**占位实现**（需生产环境覆盖）:
- ⚠️ `ProcessorTaskBase._save_to_clean()` - 当前仅计数+日志，未实际写入数据库
  - 生产环境需覆盖此方法或引入 CleanLayerWriter 适配 DBManager
  - 参考实现见方法 docstring

**待实现**（扩展点）:
- 🔄 `ProcessorEngine._check_dependencies()` - 依赖检查功能
  - 当前仅记录日志，不执行实际验证
  - 中长期可挂接到统一任务状态表
  - 参考实现见方法 docstring

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Source Data                                    │
│  tushare.*, akshare.*, etc.                                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Clean Layer (处理层)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Validator  │  │  Aligner    │  │ Standardizer│  │  Lineage    │    │
│  │  - 类型校验  │  │  - 日期对齐  │  │  - 单位转换  │  │  - 血缘记录  │    │
│  │  - 缺列检测  │  │  - 标的映射  │  │  - 复权处理  │  │  - 版本追踪  │    │
│  │  - 重复去重  │  │  - 主键构建  │  │  - 币种统一  │  │  - 任务ID   │    │
│  │  - 空值检测  │  │             │  │             │  │             │    │
│  │  - 范围校验  │  │             │  │             │  │             │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │
│                                    │                                     │
│                                    ▼                                     │
│                          clean schema (PostgreSQL)                       │
│                          - clean.index_valuation_base                    │
│                          - clean.index_volatility_base                   │
│                          - clean.industry_base                           │
│                          - clean.market_technical_base                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Feature Layer (特征层)                            │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    operations/transforms.py                      │    │
│  │  - rolling_percentile, rolling_zscore, rolling_slope            │    │
│  │  - winsorize, quantile_bins                                     │    │
│  │  - price_acceleration, trend_strength_index                     │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                    │                                     │
│                                    ▼                                     │
│                          Feature Tables (可选入库，示例)                 │
│                          - processor_index_valuation                     │
│                          - processor_index_volatility                    │
│                          - (完整列表见任务分类表)                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Task Layer (任务层)                             │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  ProcessorTaskBase                                               │    │
│  │  - fetch_data() → clean_data() → compute_features() → save()    │    │
│  │  - feature_dependencies: List[str]                              │    │
│  │  - skip_features: bool                                          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Clean Layer Components

#### 1.1 DataValidator

**注意：以下为接口规范，非实现代码。**

```python
@dataclass
class TableSchema:
    """表 schema 定义"""
    required_columns: List[str]  # 必需列
    column_types: Dict[str, type]  # 列类型映射
    nullable_columns: List[str]  # 可空列
    value_ranges: Dict[str, Tuple[float, float]]  # 值域范围

@dataclass
class ValidationResult:
    """校验结果"""
    is_valid: bool
    missing_columns: List[str]
    type_errors: Dict[str, str]  # {column: "expected X, got Y"}
    null_fields: List[str]
    out_of_range_rows: pd.Index
    dropped_columns: List[str]  # 检测到的被丢弃列（应为空）

class DataValidator:
    """
    数据校验器
    
    关键约束：
    - 不得 silent drop/rename 列
    - schema 配置来源于 TableSchema 定义
    - 校验失败时抛出 ValidationError
    """
    
    def __init__(self, schema: TableSchema):
        self.schema = schema
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        校验 DataFrame 是否符合 schema
        
        Raises:
            ValidationError: 当校验失败时
        """
        pass
    
    def validate_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """校验列类型，返回类型不匹配的列及期望类型"""
        pass
    
    def detect_missing_columns(self, df: pd.DataFrame) -> List[str]:
        """检测缺失的必需列"""
        pass
    
    def detect_duplicates(self, df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
        """检测重复记录，返回重复的行"""
        pass
    
    def detect_nulls(self, df: pd.DataFrame, required_cols: List[str]) -> List[str]:
        """检测必需列中的空值"""
        pass
    
    def detect_out_of_range(self, df: pd.DataFrame, ranges: Dict[str, Tuple]) -> pd.Index:
        """检测超出有效范围的记录"""
        pass
    
    def detect_dropped_columns(self, input_cols: List[str], output_cols: List[str]) -> List[str]:
        """检测被丢弃的列（应返回空列表）"""
        pass
```

#### 1.2 DataAligner

**注意：以下为接口规范，非实现代码。**

```python
class DataAligner:
    """
    数据对齐器
    
    依赖：
    - security_master 表用于标的映射
    
    Fallback 策略：
    - 映射失败时记录日志并保留原值，添加 _mapping_failed 标记
    """
    
    def __init__(self, security_master_loader: Callable):
        """
        Args:
            security_master_loader: 加载 security_master 表的函数
        """
        self.security_master = None  # 延迟加载
    
    def align_date(self, df: pd.DataFrame, source_col: str) -> pd.DataFrame:
        """
        将日期列对齐到 trade_date 标准格式
        
        支持的源格式：
        - YYYY-MM-DD, YYYYMMDD, datetime, timestamp
        
        输出格式：YYYYMMDD (int) 或 datetime
        
        约束：不改变行顺序
        """
        pass
    
    def align_identifier(self, df: pd.DataFrame, source_col: str) -> pd.DataFrame:
        """
        将标的标识符对齐到 ts_code 格式
        
        支持的源格式：
        - 000001 → 000001.SZ (根据代码规则推断交易所)
        - sh600000 → 600000.SH
        - symbol → 查询 security_master
        
        输出格式：000001.SZ
        
        Fallback：映射失败时保留原值，添加 _mapping_failed=True
        
        映射失败处理策略：
        - 默认（strict_mapping=False）：允许入库，但标记 _mapping_failed=True
        - 严格模式（strict_mapping=True）：阻断写入，抛出 ValidationError
        """
        pass
    
    def build_primary_key(self, df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
        """构建复合主键，确保唯一性"""
        pass
```

#### 1.3 DataStandardizer

**注意：以下为接口规范，非实现代码。**

```python
class DataStandardizer:
    """
    数据标准化器
    
    约束：
    - 转换后保留原单位列（添加 _原单位 后缀）
    - 记录转换日志
    """
    
    # 单位转换因子
    UNIT_CONVERSIONS = {
        '万元': 10000,
        '亿元': 100000000,
        '手': 100,
    }
    
    def convert_monetary(
        self, 
        df: pd.DataFrame, 
        col: str, 
        source_unit: str,
        preserve_original: bool = True,
    ) -> pd.DataFrame:
        """
        将货币列转换为元
        
        Args:
            preserve_original: 是否保留原单位列（默认 True）
            
        Side effects:
            - 记录转换日志（通过 logger.info）：{col}: {source_unit} → 元, factor={factor}
        """
        pass
    
    def convert_volume(
        self, 
        df: pd.DataFrame, 
        col: str, 
        source_unit: str,
        preserve_original: bool = True,
    ) -> pd.DataFrame:
        """
        将成交量列转换为股
        
        Args:
            preserve_original: 是否保留原单位列（默认 True）
        """
        pass
    
    def preserve_unadjusted(self, df: pd.DataFrame, price_cols: List[str]) -> pd.DataFrame:
        """保留未复权价格列（添加 _unadj 后缀）"""
        pass
```

#### 1.4 LineageTracker

```python
class LineageTracker:
    """血缘追踪器"""
    
    def add_lineage(
        self, 
        df: pd.DataFrame, 
        source_tables: List[str],
        job_id: str,
        data_version: str = None,
    ) -> pd.DataFrame:
        """
        添加血缘元数据列
        
        添加的列：
        - _source_table: 源表名（逗号分隔）
        - _processed_at: 处理时间戳（UTC）
        - _data_version: 数据版本
        - _ingest_job_id: 任务执行ID
        """
        pass
```

#### 1.5 CleanLayerWriter

**注意：以下为接口规范，非实现代码。**

```python
class CleanLayerWriter:
    """
    Clean 层数据写入器
    
    默认配置：
    - batch_size: 10000（可配置）
    - max_retries: 3（可配置）
    - conflict_strategy: 'replace'（默认全量覆盖）
    
    约束：
    - 使用数据库事务保证原子性
    - 失败时整批回滚
    - 支持指数退避重试
    """
    
    def __init__(
        self, 
        db_connection, 
        batch_size: int = 10000, 
        max_retries: int = 3,
        retry_delay_base: float = 2.0,
    ):
        self.db = db_connection
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
    
    async def upsert(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        primary_keys: List[str],
        conflict_strategy: str = 'replace',
    ) -> int:
        """
        幂等写入数据
        
        Args:
            df: 要写入的数据
            table_name: 目标表名（clean schema）
            primary_keys: 主键列
            conflict_strategy: 冲突策略
                - 'replace': 全量覆盖（默认，推荐）
                - 'merge': 仅更新非空列（默认禁用，需显式启用并提供列级策略）
                  注：merge 策略需明确定义哪些列允许合并，否则可能导致数据不一致
                  启用方式：conflict_strategy='merge', merge_columns=['col1', 'col2']
            
        Returns:
            写入的行数
            
        Raises:
            WriteError: 重试耗尽后仍失败
        """
        pass
```

### 2. Feature Layer Interface

特征层函数已在 `alphahome/processors/operations/transforms.py` 中实现，本设计文档定义其接口契约：

```python
# 所有特征函数必须遵循的接口契约

def feature_function(
    data: pd.Series | pd.DataFrame,
    window: int = ...,
    min_periods: int = None,  # 默认等于 window
    **kwargs,
) -> pd.Series | pd.DataFrame:
    """
    特征函数接口契约
    
    约束：
    1. 不修改输入数据（immutable）
    2. 不访问外部状态（pure function）
    3. 输出索引与输入对齐（不改变排序）
    4. NaN 输入产生 NaN 输出（NaN preservation）
    5. 除零返回 NaN（not inf, not 0）
    6. 不足窗口返回 NaN（not fill with 0）
    7. min_periods 默认等于 window
    8. inf 值统一转换为 NaN
    
    输出列命名规范：
    - 格式：{base_name}_{window}D_{transform}
    - 示例：RV_20D_Pctl, PE_10Y_ZScore
    - 单位/频度/滞后性在 docstring 中说明
    """
    pass
```

#### 特征入库与版本管理

| 配置项 | 说明 |
|--------|------|
| 版本字段 | `_feature_version` 列（v1, v2, ...） |
| 分区策略 | 按 trade_date 月分区 |
| 回填触发 | 数据修正、参数变更、算法升级 |
| 重算流程 | 1) 新建版本号 2) 全量计算 3) 验证 4) 切换 |
| SLA | 日常更新 T+1 09:00 前完成 |
| 默认查询版本 | 最新版本（可通过参数指定历史版本） |
| 旧版本清理 | 保留最近 2 个版本，超期数据按月归档到冷存储 |

#### 特征元数据落盘方式

| 元数据类型 | 落盘方式 |
|------------|----------|
| 单位 | 列注释（COMMENT ON COLUMN） |
| 频度 | 表名后缀（_daily, _weekly, _monthly）或 _freq 列 |
| 滞后性 | docstring 文档约定 |
| 窗口参数 | 列名包含（如 _20D, _252D） |

*注：非日频特征（周/月/分钟）需在表名或 _freq 列中明确标注。优先使用表名后缀，_freq 列作为补充。*

### 3. Task Layer Enhancement

扩展现有 `ProcessorTaskBase` 以支持分层架构：

```python
class ProcessorTaskBase(BaseTask, ABC):
    """处理任务基类（增强版）"""
    
    # 现有属性
    task_type: str = "processor"
    source_tables: List[str] = []
    table_name: str = ""
    primary_keys: List[str] = ["trade_date"]
    
    # 新增属性
    clean_table: str = ""  # clean schema 目标表
    feature_dependencies: List[str] = []  # 依赖的特征函数
    skip_features: bool = False  # 是否跳过特征计算
    
    async def run(self, **kwargs) -> Dict[str, Any]:
        """
        执行任务（增强版流程）
        
        流程：fetch → clean → feature (optional) → save
        
        保存目标：
        - skip_features=True: 保存到 clean_table（处理层）
        - skip_features=False: 保存到 table_name（特征层）
        """
        # 1. 获取数据
        raw_data = await self.fetch_data(**kwargs)
        
        # 2. 清洗数据（新增）
        clean_data = await self.clean_data(raw_data, **kwargs)
        
        # 3. 保存 clean 数据（如果配置了 clean_table）
        if self.clean_table:
            await self._save_to_clean(clean_data, **kwargs)
        
        # 4. 计算特征（可选）
        if not self.skip_features:
            result = await self.compute_features(clean_data, **kwargs)
            # 5. 保存特征结果
            await self.save_result(result, **kwargs)
        else:
            result = clean_data
        
        return {"status": "success", "rows": len(result)}
    
    async def clean_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        清洗数据（新增方法）
        
        默认实现组合以下组件：
        1. DataValidator.validate() - 校验
        2. DataAligner.align_date() + align_identifier() - 对齐（best-effort）
        3. DataStandardizer.convert_*() - 标准化（best-effort）
        4. LineageTracker.add_lineage() - 添加血缘
        
        **异常语义**：
        对齐和标准化采用 best-effort 策略：
        - 遇到未知格式/单位或部分列缺失时记录 warning
        - 尽量完成可处理的部分
        - 不抛出致命异常（除非显式配置 strict 模式）
        
        子类可覆盖以自定义清洗逻辑
        """
        pass
    
    async def _save_to_clean(self, data: pd.DataFrame, **kwargs) -> int:
        """
        保存数据到 clean schema 表
        
        **重要提示**：
        当前实现仅为占位符（计数+日志），不执行真正的数据库写入。
        
        **生产环境使用要求**：
        子类必须覆盖此方法以实现真正的数据库写入逻辑，推荐方案：
        1. 引入 CleanLayerWriter 适配 DBManager
        2. 从 clean_table 解析 schema/table 名称
        3. 调用 writer.upsert() 执行幂等写入
        
        **中长期改进方向**：
        提供基于 CleanLayerWriter + DBManager 的默认实现
        
        Returns:
            int: 保存的行数（当前仅返回计数，未实际写入）
        """
        pass
    
    async def compute_features(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        计算特征（新增方法）
        
        子类实现，调用 operations/transforms.py 中的函数
        
        约束：
        - 必须通过 feature_dependencies 声明依赖的特征函数
        - 不得内嵌特征计算逻辑
        """
        return data  # 默认不计算特征
    
    def _validate_feature_dependencies(self):
        """校验 feature_dependencies 中的函数是否存在于 operations 模块"""
        from ..operations import transforms
        for dep in self.feature_dependencies:
            if not hasattr(transforms, dep):
                raise ValueError(f"Unknown feature dependency: {dep}")
```

#### Task Layer 实现状态

| 组件 | 状态 | 说明 |
|------|------|------|
| `run()` 流程 | ✅ 已实现 | fetch → clean → feature → save |
| `clean_data()` | ✅ 已实现 | 组合 Clean Layer 组件，best-effort 策略 |
| `_save_to_clean()` | ⚠️ 占位实现 | 仅计数+日志，生产环境需覆盖 |
| `compute_features()` | ✅ 已实现 | 默认不计算，子类覆盖 |
| `_validate_feature_dependencies()` | ✅ 已实现 | 校验特征函数存在性 |

## Data Models

### 1. Clean Schema Table Structure

所有 clean 表共享的基础结构：

| Column | Type | Description | Required |
|--------|------|-------------|----------|
| trade_date | INTEGER/DATETIME | 交易日期（主键） | Yes |
| ts_code | VARCHAR(20) | 标的代码（主键，股票级表） | Conditional |
| _source_table | VARCHAR(255) | 源表名 | Yes |
| _processed_at | TIMESTAMP | 处理时间（UTC） | Yes |
| _data_version | VARCHAR(50) | 数据版本 | Yes |
| _ingest_job_id | VARCHAR(100) | 任务执行ID | Yes |
| _validation_flag | INTEGER | 校验标记（0=正常） | Optional |

### 2. Clean Table Definitions

*注：以下为核心表示例，完整表定义详见任务分类表。*

#### clean.index_valuation_base

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| trade_date | INTEGER | No | YYYYMMDD | 交易日期（PK） |
| ts_code | VARCHAR(20) | No | - | 指数代码（PK） |
| pe_ttm | FLOAT | Yes | 倍 | 市盈率TTM |
| pb | FLOAT | Yes | 倍 | 市净率 |
| _source_table | VARCHAR(255) | No | - | 源表名 |
| _processed_at | TIMESTAMP | No | UTC | 处理时间 |
| _data_version | VARCHAR(50) | No | - | 数据版本 |
| _ingest_job_id | VARCHAR(100) | No | - | 任务执行ID |

**主键**: (trade_date, ts_code)  
**分区**: 按 trade_date 月分区  
**索引**: ts_code

#### clean.index_volatility_base

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| trade_date | INTEGER | No | YYYYMMDD | 交易日期（PK） |
| ts_code | VARCHAR(20) | No | - | 指数代码（PK） |
| close | FLOAT | Yes | 元 | 收盘价（复权） |
| close_unadj | FLOAT | Yes | 元 | 未复权收盘价 |
| _adj_method | VARCHAR(20) | Yes | - | 复权方式 |

**主键**: (trade_date, ts_code)

#### clean.industry_base

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| trade_date | INTEGER | No | YYYYMMDD | 交易日期（PK） |
| ts_code | VARCHAR(20) | No | - | 行业代码（PK） |
| close | FLOAT | Yes | 点 | 行业指数收盘价 |

**主键**: (trade_date, ts_code)

#### 其他 Clean 表（简要定义）

*注：以下表需包含标准血缘列（_source_table, _processed_at, _data_version, _ingest_job_id）*

| 表名 | 主键 | 核心列 | 说明 |
|------|------|--------|------|
| clean.futures_base | trade_date, ts_code | close, settle, oi | 期货基础数据 |
| clean.option_iv_base | trade_date, ts_code | iv, delta, gamma | 期权隐含波动率 |
| clean.style_base | trade_date, ts_code | close | 风格指数基础数据 |

*扩展说明：其余 clean 表需遵循"基础结构（trade_date/主键）+ 业务列 + 血缘列"的模式。*

#### clean.money_flow_base

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| trade_date | INTEGER | No | YYYYMMDD | 交易日期（PK） |
| total_net_mf_amount | FLOAT | Yes | 元 | 主力净流入金额 |
| total_circ_mv | FLOAT | Yes | 元 | 流通市值 |

**主键**: (trade_date)

#### clean.market_technical_base

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| trade_date | INTEGER | No | YYYYMMDD | 交易日期（PK） |
| ts_code | VARCHAR(20) | No | - | 股票代码（PK） |
| close | FLOAT | Yes | 元 | 收盘价 |
| vol | FLOAT | Yes | 股 | 成交量 |
| turnover_rate | FLOAT | Yes | % | 换手率 |

**主键**: (trade_date, ts_code)

### 3. Task Classification Table

| 任务 | 输入表 | 输出表 | 主键 | 时间列 | 特征列 | 分类 | 目标 clean 表 | 待提取特征函数 |
|------|--------|--------|------|--------|--------|------|---------------|----------------|
| index_valuation | tushare.index_dailybasic, akshare.macro_bond_rate | processor_index_valuation | trade_date | trade_date | *_Pctl_10Y, *_Pctl_12M, *_ERP | 混合需拆分 | clean.index_valuation_base | rolling_percentile |
| index_volatility | tushare.index_factor_pro | processor_index_volatility | trade_date | trade_date | *_RV_*, *_Pctl, *_Ratio | 混合需拆分 | clean.index_volatility_base | rolling_percentile |
| industry_return | tushare.index_swdaily | processor_industry_return | trade_date | trade_date | SW_* (pct_change) | 处理层保留 | clean.industry_base | - |
| industry_breadth | tushare.index_swdaily | processor_industry_breadth | trade_date | trade_date | *_Ratio, *_Std, *_Skew, *_5D | 特征下沉 | clean.industry_base | rolling mean |
| market_money_flow | tushare.stock_moneyflow, stock_dailybasic | processor_market_money_flow | trade_date | trade_date | *_ZScore, *_Pctl | 混合需拆分 | clean.money_flow_base | rolling_zscore, rolling_percentile |
| market_technical | tushare.stock_factor_pro | processor_market_technical | trade_date | trade_date | *_ZScore, *_Pctl | 混合需拆分 | clean.market_technical_base | rolling_zscore, rolling_percentile |
| style_index | - | processor_style_index | trade_date | trade_date | 多周期收益 | 特征下沉 | - | diff_pct |
| futures | tushare.fut_* | processor_futures | trade_date | trade_date | 基差分位 | 混合需拆分 | clean.futures_base | rolling_percentile |
| option_iv | - | processor_option_iv | trade_date | trade_date | IV期限结构 | 混合需拆分 | clean.option_iv_base | - |



## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Column type validation
*For any* DataFrame with columns that don't match the expected schema types, the DataValidator SHALL identify all type mismatches and return them in the validation result.
**Validates: Requirements 1.1**

### Property 2: Missing column detection
*For any* DataFrame missing required columns, the DataValidator SHALL raise an exception containing the complete list of missing column names.
**Validates: Requirements 1.2**

### Property 3: Duplicate key deduplication
*For any* DataFrame with duplicate primary keys, the deduplication process SHALL keep exactly one record per key (the latest) and the output length SHALL equal the number of unique keys.
**Validates: Requirements 1.3**

### Property 4: Null value rejection
*For any* DataFrame with null values in required fields, the DataValidator SHALL reject the batch and report all field names containing nulls.
**Validates: Requirements 1.4**

### Property 5: Range validation flagging
*For any* DataFrame with values outside valid ranges, the DataValidator SHALL add a `_validation_flag` column marking the out-of-range records.
**Validates: Requirements 1.5**

### Property 6: Column preservation
*For any* DataFrame processed by the Clean Layer, the output columns SHALL be a superset of input columns (plus lineage columns), with no columns silently dropped or renamed.
**Validates: Requirements 1.6**

### Property 7: Date format standardization
*For any* date value in supported formats (YYYY-MM-DD, YYYYMMDD, datetime), the DataAligner SHALL convert it to the standard trade_date format.
**Validates: Requirements 2.1, 2.4**

### Property 8: Identifier mapping
*For any* security identifier in supported formats (000001, sh600000), the DataAligner SHALL map it to the correct ts_code format (e.g., 000001.SZ).
**Validates: Requirements 2.2, 2.5**

### Property 9: Primary key uniqueness enforcement
*For any* DataFrame written to clean schema, the CleanLayerWriter SHALL enforce primary key uniqueness via UPSERT, with no duplicate keys in the final table.
**Validates: Requirements 2.6, 5.1**

### Property 10: Unit conversion correctness
*For any* monetary value in 万元 or 亿元, the DataStandardizer SHALL convert to 元 using the correct conversion factor. *For any* volume in 手, the DataStandardizer SHALL convert to 股.
**Validates: Requirements 3.1, 3.2, 3.5**

### Property 11: Unadjusted price preservation
*For any* price column that undergoes adjustment, the DataStandardizer SHALL preserve the original value in a column with `_unadj` suffix.
**Validates: Requirements 3.4**

### Property 12: Lineage metadata completeness
*For any* DataFrame processed by the Clean Layer, the output SHALL contain all lineage columns (_source_table, _processed_at, _data_version, _ingest_job_id) with non-null values.
**Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**

### Property 13: Feature function immutability
*For any* feature function call, the input DataFrame SHALL remain unchanged after the function returns.
**Validates: Requirements 6.1, 6.3**

### Property 14: Index alignment preservation
*For any* feature function call, the output index SHALL exactly match the input index.
**Validates: Requirements 6.4**

### Property 15: NaN preservation
*For any* input Series with NaN values, feature functions SHALL preserve NaN at the same positions in the output (NaN in → NaN out).
**Validates: Requirements 6.5**

### Property 16: Division by zero handling
*For any* feature function that involves division, when the divisor is zero, the function SHALL return NaN (not infinity, not zero).
**Validates: Requirements 6.6**

### Property 17: min_periods default behavior
*For any* rolling feature function called without explicit min_periods, the function SHALL use window size as the default min_periods.
**Validates: Requirements 6.7**

### Property 18: Insufficient window NaN handling
*For any* rolling calculation with fewer than min_periods observations, the function SHALL return NaN (not fill with 0 or other values).
**Validates: Requirements 6.8**

## Error Handling

### 1. Validation Errors

```python
class ValidationError(Exception):
    """数据校验错误"""
    def __init__(self, message: str, details: ValidationResult):
        super().__init__(message)
        self.details = details

# 使用示例
try:
    result = validator.validate(df)
    if not result.is_valid:
        if result.missing_columns:
            raise ValidationError(
                f"Missing required columns: {result.missing_columns}",
                result
            )
        if result.null_fields:
            raise ValidationError(
                f"Null values in required fields: {result.null_fields}",
                result
            )
except ValidationError as e:
    logger.error(f"Validation failed: {e.details}")
    raise
```

### 2. Write Errors

```python
class WriteError(Exception):
    """数据写入错误"""
    pass

async def upsert_with_retry(self, df, table_name, primary_keys):
    for attempt in range(self.max_retries):
        try:
            async with self.db.transaction():
                return await self._do_upsert(df, table_name, primary_keys)
        except Exception as e:
            if attempt == self.max_retries - 1:
                raise WriteError(f"Failed after {self.max_retries} attempts: {e}")
            logger.warning(f"Write attempt {attempt + 1} failed, retrying...")
            await asyncio.sleep(2 ** attempt)  # 指数退避
```

### 3. Feature Calculation Errors

特征函数不应抛出异常，而是返回 NaN：

```python
def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """安全除法，除零返回 NaN"""
    with np.errstate(divide='ignore', invalid='ignore'):
        result = numerator / denominator
        result = result.replace([np.inf, -np.inf], np.nan)
    return result
```

## Testing Strategy

### Unit Testing

使用 pytest 进行单元测试：

- 测试 DataValidator 的各种校验场景
- 测试 DataAligner 的格式转换
- 测试 DataStandardizer 的单位转换
- 测试 LineageTracker 的元数据添加
- 测试 CleanLayerWriter 的 UPSERT 行为

### Property-Based Testing

使用 **hypothesis** 库进行属性测试，验证设计文档中定义的正确性属性：

```python
from hypothesis import given, strategies as st
import hypothesis.extra.pandas as pdst

@given(pdst.data_frames([
    pdst.column('trade_date', dtype=int),
    pdst.column('ts_code', dtype=str),
    pdst.column('value', dtype=float),
]))
def test_column_preservation(df):
    """
    **Feature: processors-data-layering, Property 6: Column preservation**
    **Validates: Requirements 1.6**
    """
    original_cols = set(df.columns)
    result = clean_layer.process(df)
    result_cols = set(result.columns)
    
    # 输出列应包含所有输入列
    assert original_cols.issubset(result_cols)
    # 新增的只能是血缘列
    new_cols = result_cols - original_cols
    assert new_cols.issubset({'_source_table', '_processed_at', '_data_version', '_ingest_job_id', '_validation_flag'})


@given(st.floats(allow_nan=False, allow_infinity=False))
def test_unit_conversion_correctness(value):
    """
    **Feature: processors-data-layering, Property 10: Unit conversion correctness**
    **Validates: Requirements 3.1, 3.2, 3.5**
    """
    # 万元 → 元
    result = standardizer.convert_monetary(pd.Series([value]), 'amount', '万元')
    assert result.iloc[0] == value * 10000
    
    # 亿元 → 元
    result = standardizer.convert_monetary(pd.Series([value]), 'amount', '亿元')
    assert result.iloc[0] == value * 100000000


@given(pdst.series(dtype=float, min_size=1))
def test_feature_immutability(series):
    """
    **Feature: processors-data-layering, Property 13: Feature function immutability**
    **Validates: Requirements 6.1, 6.3**
    """
    original = series.copy()
    _ = rolling_zscore(series, window=5)
    pd.testing.assert_series_equal(series, original)


@given(pdst.series(dtype=float, min_size=10))
def test_index_alignment(series):
    """
    **Feature: processors-data-layering, Property 14: Index alignment preservation**
    **Validates: Requirements 6.4**
    """
    result = rolling_percentile(series, window=5)
    pd.testing.assert_index_equal(result.index, series.index)
```

### Test Organization

```
alphahome/processors/tests/
├── __init__.py
├── conftest.py
├── test_clean_layer/
│   ├── test_validator.py          # Property 1-5
│   ├── test_aligner.py            # Property 7-9
│   ├── test_standardizer.py       # Property 10-11
│   ├── test_lineage.py            # Property 12
│   └── test_writer.py             # Property 6, 9
├── test_feature_layer/
│   ├── test_transforms_properties.py  # Property 13-18
│   └── test_transforms_unit.py        # 单元测试
└── test_task_layer/
    ├── test_task_base.py
    └── test_task_classification.py
```

### Property Test Coverage Matrix

| Property | 测试文件 | 测试函数 |
|----------|----------|----------|
| 1. Column type validation | test_validator.py | test_column_type_validation |
| 2. Missing column detection | test_validator.py | test_missing_column_detection |
| 3. Duplicate key deduplication | test_validator.py | test_duplicate_deduplication |
| 4. Null value rejection | test_validator.py | test_null_value_rejection |
| 5. Range validation flagging | test_validator.py | test_range_validation_flagging |
| 6. Column preservation | test_writer.py | test_column_preservation |
| 7. Date format standardization | test_aligner.py | test_date_format_standardization |
| 8. Identifier mapping | test_aligner.py | test_identifier_mapping |
| 9. Primary key uniqueness | test_aligner.py, test_writer.py | test_primary_key_uniqueness |
| 10. Unit conversion | test_standardizer.py | test_unit_conversion_correctness |
| 11. Unadjusted price preservation | test_standardizer.py | test_unadjusted_preservation |
| 12. Lineage metadata | test_lineage.py | test_lineage_metadata_completeness |
| 13. Feature immutability | test_transforms_properties.py | test_feature_immutability |
| 14. Index alignment | test_transforms_properties.py | test_index_alignment |
| 15. NaN preservation | test_transforms_properties.py | test_nan_preservation |
| 16. Division by zero | test_transforms_properties.py | test_division_by_zero |
| 17. min_periods default | test_transforms_properties.py | test_min_periods_default |
| 18. Insufficient window NaN | test_transforms_properties.py | test_insufficient_window_nan |

### Property Test Annotations

每个属性测试必须标注对应的设计文档属性：

```python
def test_duplicate_deduplication():
    """
    **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
    **Validates: Requirements 1.3**
    """
    # test implementation
```

## Migration Plan

### Phase 1: 基础设施建立

1. 创建 clean schema
2. 实现 DataValidator, DataAligner, DataStandardizer, LineageTracker
3. 实现 CleanLayerWriter
4. 编写属性测试

### Phase 2: 任务迁移

按优先级迁移现有任务：

| 优先级 | 任务 | 目标 clean 表 | 完成标准 |
|--------|------|---------------|----------|
| 高 | index_valuation | clean.index_valuation_base | clean 表创建 + 数据迁移 + 测试通过 |
| 高 | index_volatility | clean.index_volatility_base | clean 表创建 + 数据迁移 + 测试通过 |
| 中 | industry_return | clean.industry_base | clean 表创建 + 数据迁移 + 测试通过 |
| 中 | market_money_flow | clean.money_flow_base | clean 表创建 + 数据迁移 + 测试通过 |
| 低 | style_index | - | 特征下沉 + 测试通过 |
| 低 | futures | clean.futures_base | clean 表创建 + 特征下沉 + 测试通过 |
| 低 | option_iv | clean.option_iv_base | clean 表创建 + 特征下沉 + 测试通过 |

**"混合需拆分"任务的迁移流程：**
1. 创建 clean_base 表
2. 实现 clean_data() 方法，输出到 clean_base
3. 将特征计算逻辑提取到 operations/transforms.py
4. 更新 compute_features() 调用提取的函数
5. 验证输出与原任务一致

### Phase 3: 特征下沉

1. 将任务中的特征计算逻辑提取到 operations/transforms.py
2. 任务仅调用特征函数，不内嵌计算逻辑
3. 更新任务的 feature_dependencies 属性
4. 验证特征输出与原实现一致

### 增量更新边界处理约定

在任务层增量计算时，必须遵循以下约定：

```python
# 增量更新时的回溯窗口计算
lookback_days = max(
    feature_func.window for feature_func in self.feature_dependencies
)
actual_start_date = requested_start_date - timedelta(days=lookback_days)
```

这确保滚动窗口特征在边界处不会失真。

## Open Questions

| 问题 | 建议方向 | 待决策人 | 目标时间 |
|------|----------|----------|----------|
| 血缘元数据的查询接口 | 暂不提供 API，通过 SQL 直接查询 _source_table 等字段 | Tech Lead | Phase 1 |
| 特征版本管理 | 使用 _feature_version 列 + 分区策略，保留最近 2 个版本 | Tech Lead | Phase 2 |
| 增量更新的边界处理 | 回溯 max(window) 天数据重算，确保窗口完整（固化到任务层约定） | Data Engineer | Phase 2 |
| clean schema 的权限管理 | 生产限制直接写入；dev/staging 允许写入但需审计日志 | DevOps | Phase 3 |
