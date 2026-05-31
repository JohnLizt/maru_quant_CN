# Multi-Asset Refactor Plan

## Goal

将当前偏 A 股单资产的架构，重构为支持多资产、多地区、多数据源的统一底座。

这次方案采用更收敛的建模方式：

- 不单独增加 `market` 字段
- 直接把市场信息编码进 `asset_type`

也就是把原来“资产类别 + 市场”两层维度，合并成一个扩展型 `asset_type`：

- `stock_CN`
- `etf_CN`
- `stock_US`
- `etf_US`
- `stock_JP`
- `etf_JP`

同时保留第三层独立维度：

- `data_source`：Tushare、Yahoo Finance、Polygon、AKShare 等数据来源

第一阶段仍然只要求落地最小可用范围，但底层模型要一次设计到位，避免后续继续做破坏性迁移。

建议第一阶段明确支持：

- `stock_CN`
- `etf_CN`

并为后续预留：

- `stock_US`
- `etf_US`
- `stock_JP`
- `etf_JP`

---

## Current State

### Existing strengths

当前项目已经具备这些可复用能力：

- `market.daily` 已经是通用 OHLCV 结构
- `factors.daily_factors` 是长表结构，天然适合多资产
- 技术因子目前主要依赖 OHLCV
- 因子流水线已经按 symbol 批量计算
- 信号打分层已经有横截面打分雏形：
  - `app/services/signal_score.py`
  - `app/signals/profiles.py`

### Current limitations

当前项目仍然有较强的“A 股 + 股票 + Tushare”假设：

1. `config/stock_pool.csv` 只表达股票池，没有资产域信息
2. `scripts/etl_daily.py` 写死使用 Tushare `daily`
3. 旧数据接入逻辑曾以 pipeline 命名承载 provider 适配职责，语义不清晰
4. `app/services/factor_backfill.py` 写死股票抓数逻辑
5. 核心表主键仍以 `symbol` 为主，默认 symbol 全局唯一
6. `signals` / `strategy` 没有资产域边界
7. 当前“数据获取层”没有抽象成可插拔 provider 接口

---

## Design Principles

### 1. Asset type is the core domain dimension

本方案里，`asset_type` 不再只是“股票 / ETF”这种窄分类，而是统一表达“资产类别 + 交易地区”的复合域。

例如：

- `stock_CN`
- `etf_CN`
- `stock_US`
- `etf_US`

这样做的好处：

- schema 更简单
- 查询条件更直接
- 迁移成本更低
- 调用方少一个必传字段

代价也很明确：

- `asset_type` 语义比传统命名更宽
- 如果将来要做更细的“国家 / 市场 / 交易所”分析，可能需要再拆辅助字段

对于当前项目阶段，这个取舍是合理的。

### 2. Shared storage, split behavior

存储层尽量共享，行为层按 `asset_type` 和 `data_source` 分发。

- 行情表共享
- 因子表共享
- 信号表共享
- universe / profile / strategy / loader provider 分域管理

### 3. Keep the canonical identity stable

建议把资产的数据库业务主身份固定为：

- `(asset_type, symbol)`

如未来存在跨源映射问题，可继续引入：

- `source_symbol`
- `source_asset_id`

但内部统一 identity 不应依赖第三方源的主键体系。

### 4. Data acquisition must be provider-based

历史上 `data_pipeline` 这个命名容易把“数据处理流水线”和“数据接入层”混在一起。

建议直接切换为：

- `app/data_loader/`

旧 `app/data_pipeline/` 不保留兼容层，调用方统一迁移到新路径。

并将其职责定义为：

- 对外部数据源的统一适配层
- 负责抓取、标准化、补充基础元数据
- 不承担因子、信号、策略逻辑

### 5. Reuse common factors, isolate domain-specific logic

通用技术因子可以跨多个 `asset_type` 复用：

- MA
- RSI
- MACD
- breakout
- volatility

特有逻辑则按域独立演进：

- `stock_CN`：涨停、ST、停牌、财务衍生
- `etf_CN`：折溢价、份额变化、跟踪误差
- `stock_US`：拆股、分红、盘前盘后规则
- `stock_JP`：本地交易日历、价格单位等

---

## Target Architecture

## 1. Asset Identity Model

### Core dimensions

建议统一使用以下概念：

- `asset_type`
  - `stock_CN`
  - `etf_CN`
  - `index_CN`
  - `stock_US`
  - `etf_US`
  - `stock_JP`
- `exchange`
  - `SSE`
  - `SZSE`
  - `NASDAQ`
  - `NYSE`
  - `TSE`
- `currency`
  - `CNY`
  - `USD`
  - `JPY`

其中：

- `asset_type` 表示“资产类别 + 地区域”
- `exchange` 表示具体交易所
- `currency` 表示计价货币

### Canonical key

建议内部统一用下面的业务主键表达资产身份：

- `(asset_type, symbol)`

例如：

- `stock_CN, 600519.SH`
- `etf_CN, 510300.SH`
- `stock_US, AAPL`
- `etf_US, QQQ`
- `stock_JP, 7203.T`

### Naming convention recommendation

建议统一采用：

- `{asset_class}_{region}`

例如：

- `stock_CN`
- `etf_CN`
- `stock_US`

这样比 `CN_stock` 更接近“先按资产归类，再按地区切分”的使用习惯，也更利于后面做因子支持范围配置。

---

## 2. Metadata Layer

### New table: `meta.assets`

建议新增统一资产主表，逐步替代当前 `meta.stocks` 的股票专用语义。

建议字段：

- `asset_type`
- `symbol`
- `name`
- `exchange`
- `currency`
- `lot_size`
- `timezone`
- `list_date`
- `delist_date`
- `is_active`
- `status`
- `extra_metadata JSONB`
- `updated_at`

建议主键：

- `(asset_type, symbol)`

说明：

- `status` 可表达 `active` / `delisted` / `suspended_listing` 等
- `extra_metadata` 用于存放域特有信息
- 例如 `etf_CN` 可扩展：
  - `tracking_index_code`
  - `tracking_index_name`
  - `manager_name`
  - `fund_type`
- `stock_US` 可扩展：
  - `gics_sector`
  - `primary_share_class`

### Optional table: `meta.asset_aliases`

如果后续要支持多数据源映射，建议预留别名表：

- `asset_type`
- `symbol`
- `data_source`
- `source_symbol`
- `source_exchange`
- `source_asset_id`
- `updated_at`

建议唯一键：

- `(data_source, asset_type, source_symbol)`

用途：

- 同一资产在 Tushare、Yahoo、Polygon 的 symbol 规则可能不同
- ETL / backfill 时先通过 alias 做 source mapping

### Calendar handling

既然不单独引入 `market` 字段，交易日历建议暂时按 `asset_type` 归属：

- `stock_CN` / `etf_CN` 共用中国市场日历
- `stock_US` / `etf_US` 共用美国市场日历
- `stock_JP` / `etf_JP` 共用日本市场日历

第一阶段可以不落表，只在 loader/provider 配置中约定。

---

## 3. Universe Layer

### Replace stock pool with asset-type registry + per-domain universes

建议将 `config/stock_pool.csv` 升级为两层配置：

```text
config/
  asset_types.csv
  universes/
    stock_CN.csv
    etf_CN.csv
    stock_US.csv
```

### `config/asset_types.csv`

作为配置层单一事实源，字段固定为：

- `asset_type`
- `display_name`
- `data_source`
- `calendar_key`
- `loader_key`
- `pipeline_universe`
- `enabled`

示例：

```csv
asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled
stock_CN,A股股票,tushare,CN,tushare,stock_CN,true
etf_CN,A股ETF,tushare,CN,tushare,etf_CN,true
stock_US,美股,yahoo,US,yahoo,stock_US,false
```

### `config/universes/{asset_type}.csv`

每个 `asset_type` 一份主 pipeline universe 文件，字段固定为：

- `symbol`
- `name`
- `is_active`

示例：

```csv
symbol,name,is_active
603019.SH,中科曙光,true
300059.SZ,东方财富,true
```

### Config rules

- `asset_type` 是否合法，以 `asset_types.csv` 为准
- `pipeline_universe` 由注册表指定，不允许调用方自行猜测文件名
- ETL / factor 默认只读取 `enabled=true` 的 asset_type
- 每个 asset_type 只读取自身注册表指定的 `pipeline_universe`
- universe 文件内不再携带 `asset_type` 列

---

## 4. Market Data Layer

### Shared table with asset type

保留共享表 `market.daily`，但新增以下核心字段：

- `asset_type`
- `data_source`

建议主键改为：

- `(time, asset_type, symbol)`

建议索引：

- `(asset_type, symbol, time DESC)`
- `(asset_type, time DESC)`
- `(data_source, asset_type, symbol, time DESC)`

建议保留现有通用 OHLCV 字段，并逐步补充：

- `adj_factor`
- `is_suspended`
- `is_limit_up`
- `is_limit_down`
- `session`（如未来分钟线或盘前盘后需要）

### Why keep one shared table

不建议按资产域物理拆表，例如：

- `market.stock_cn_daily`
- `market.etf_cn_daily`
- `market.stock_us_daily`

原因：

- SQL 与写入逻辑会快速复制
- 因子层会被迫跟着拆分
- 通用 loader / factor / query 层无法共享
- 当前项目规模没到必须按物理表拆分的程度

### Behavior differences still matter

共享表不代表逻辑完全相同：

- `stock_CN`
  - 可有停牌补齐
  - 有涨跌停概念
- `etf_CN`
  - 可能无股票式停牌补齐逻辑
- `stock_US`
  - 需考虑拆股、分红、复权来源
- `stock_JP`
  - 需使用对应本地市场日历

这些差异应放在 `data_loader provider` 和按 `asset_type` 分发的规则层，而不是靠拆表实现。

---

## 5. Factor Layer

### Shared factor table

保留 `factors.daily_factors`，新增：

- `asset_type`

可选新增：

- `universe`（一般不建议入主表，除非确有持久化需求）

建议主键改为：

- `(time, asset_type, symbol, factor_name)`

建议索引：

- `(asset_type, factor_name, time DESC)`
- `(asset_type, symbol, time DESC)`

### Factor support model

建议给 `FactorSpec` 增加：

- `supported_asset_types: tuple[str, ...]`

例如：

- `price_to_ma20`
  - `supported_asset_types=("stock_CN", "etf_CN", "stock_US", "etf_US", "stock_JP")`
- `limit_up`
  - `supported_asset_types=("stock_CN",)`

### Factor execution behavior

因子流水线按 `asset_type` 分组后执行：

- 同一 runner
- 同一长表
- 同一因子注册表
- 单个因子自行声明支持范围

这样可以做到：

- 共享计算框架
- 保持因子命名一致
- 限制域特有因子不要误跑到别的域

---

## 6. Signal Layer

### Shared signal table

保留 `signals.trading_signals`，新增：

- `asset_type`

建议主键改为：

- `(time, asset_type, symbol, strategy)`

### Signal profiles must be scoped

现有 `trend_v1` 不应继续作为“全资产共用 profile”。

建议 profile 明确命名为：

- `stock_cn_trend_v1`
- `etf_cn_rotation_v1`
- `stock_us_trend_v1`

即使因子名相同，也要允许：

- 不同归一化区间
- 不同权重
- 不同标签逻辑
- 不同调仓频率

### Universe-aware scoring

`query_signal_scores()` 后续建议支持：

- `asset_type`
- `universe`

避免出现“股票和 ETF 混合评分后再过滤”或“A 股和美股一起打分”的隐性耦合。

---

## 7. Strategy Layer

### Strategy categories

建议明确抽象出两类策略：

#### A. Single-name signal strategy

适用于：

- 股票趋势信号
- 个股择时
- 单 ETF 趋势信号

输出：

- 单标的买卖信号

#### B. Cross-sectional rotation strategy

适用于：

- ETF 轮动
- 行业轮动
- 风格轮动
- 跨 universe 排名策略

输入：

- 某个 `(asset_type, universe)` 的横截面因子或 composite score

输出：

- 调仓日目标持仓
- 或标准化后的买卖信号

### First target

第一版建议仍聚焦：

- `asset_type=etf_CN`
- `universe=cn_etf_rotation`
- `rebalance=weekly`
- `rank metric=composite_score`
- `hold=top 3`

---

## 8. Data Loader Layer

### Rename `data_pipeline` to `data_loader`

建议目录从：

- `app/data_pipeline/`

改为：

- `app/data_loader/`

原因：

- 当前模块承担的是“外部行情抓取与标准化”，不是完整 pipeline
- `pipeline` 一词在本项目里已经更适合指 ETL / factor / signal 的内部流水线
- `loader` 更符合 provider adapter 的职责边界

### Responsibilities of `data_loader`

`data_loader` 只负责：

- 连接第三方数据源
- 拉取原始数据
- 标准化为内部统一 schema
- 补充最基础的主数据字段

不负责：

- 因子计算
- 信号计算
- 策略逻辑
- 组合决策

### Recommended module layout

建议重构为：

```text
app/data_loader/
  __init__.py
  base.py
  registry.py
  types.py
  providers/
    __init__.py
    tushare.py
    yahoo.py
    akshare.py
  market_data.py
  asset_metadata.py
```

### Provider interface to define now

即使第一阶段只实现 Tushare，也建议先把接口定义出来。

建议抽象：

```python
class MarketDataLoader(Protocol):
    source_name: str

    def supports(self, asset_type: str) -> bool: ...

    def get_trading_dates(
        self,
        asset_type: str,
        start: str,
        end: str,
    ) -> list[str]: ...

    def fetch_daily_by_date(
        self,
        asset_type: str,
        trade_date: str,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame: ...

    def fetch_daily_by_symbol(
        self,
        asset_type: str,
        symbol: str,
        start: str,
        end: str,
    ) -> pl.DataFrame: ...

    def fetch_asset_metadata(
        self,
        asset_type: str,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame: ...
```

### Standardized output schema

所有 provider 返回的日线数据都应标准化为统一列：

- `time`
- `asset_type`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `amount`
- `pct_change`
- `is_suspended`
- `data_source`

### Initial provider mapping

建议先按能力映射：

- `TushareLoader`
  - `stock_CN`
  - `etf_CN`
- `YahooLoader`
  - `stock_US`
  - `etf_US`
  - `stock_JP`
  - `etf_JP`

注意：

- 第一阶段不需要把 Yahoo/AKShare 真正接进来
- 但接口与 registry 要先有，避免以后继续改调用方签名

### Loader registry

建议增加简单注册中心：

- 根据 `(asset_type, data_source)` 解析 loader
- universe 配置若指定 `data_source`，则优先走该 provider
- 若未指定，则走默认映射

---

## Refactor Phases

## Phase 1: Introduce asset model and loader interface

### Objective

先完成 schema、配置层、数据接入接口的模型升级，不急着一次性改完全部业务逻辑。

### Tasks

1. 新增 `meta.assets`
2. 给以下表新增 `asset_type`
   - `market.daily`
   - `factors.daily_factors`
   - `signals.trading_signals`
   - `meta.sync_status`
3. `market.daily` 额外新增 `data_source`
4. 升级 pool 配置为统一 universe 配置
5. 用 `app/data_loader/` 替换旧接入层路径，不保留 `app/data_pipeline/` 兼容包装
6. 先定义 provider protocol / registry / types
7. 新增基础资产工具函数：
   - normalize asset type
   - validate asset type
   - load asset universe
   - resolve universe symbols

### Deliverables

- schema 具备多资产域表达能力
- 配置层可描述 `stock_CN` / `etf_CN` 并预留 `US/JP`
- `data_loader` 抽象已存在
- 现有股票流程在兼容模式下仍能运行

### Risks

- schema 迁移涉及主键变更
- 需要统一历史 symbol-only 查询逻辑
- 当前 `init.sql` 首次启动才执行，需单独设计 migration 方式

---

## Phase 2: Make ETL loader-based

### Objective

让行情采集与回填不再直接依赖 Tushare，而是通过 `data_loader` provider 分发。

### Tasks

1. 改造 `scripts/etl_daily.py`
   - 按 `(asset_type, data_source)` 分组
   - 分组选择 loader
   - 写入共享 `market.daily`

2. 改造回填逻辑
   - `factor_backfill` 不再直接调用 `fetch_stock_daily`
   - 统一走 `fetch_daily_by_symbol()`

3. 实现 `TushareLoader`
   - `stock_CN -> daily`
   - `etf_CN -> fund_daily`

4. 将停牌补齐逻辑下沉为 asset-type-aware policy
   - `stock_CN` 开启
   - `etf_CN` 默认关闭

5. 调整 `meta.sync_status`
   - 用 `(data_type, asset_type, symbol, data_source)` 标识任务

### Deliverables

- 股票与 ETF 日线都可通过统一 loader 入库
- ETL 不再写死某个 provider API
- 自动补算可复用同一接入层

### Risks

- 各 provider 返回字段不一致，需要强制标准化
- 不同 `asset_type` 背后的交易日历来源不同
- ETF / 海外市场数据权限与频率限制需要文档说明

---

## Phase 3: Make factor pipeline asset-aware

### Objective

让通用因子可以对不同 `asset_type` 同时产出，并为特有因子预留边界。

### Tasks

1. `load_ohlcv()` 增加：
   - `asset_type`

2. `get_all_symbols()` 演进为可按：
   - `asset_type`
   - `universe`

3. `upsert_factors()` 写入：
   - `asset_type`

4. 因子查询 SQL 增加：
   - `asset_type`

5. `FactorSpec` 增加：
   - `supported_asset_types`

### Deliverables

- 通用技术因子可对 `stock_CN` / `etf_CN` 产出
- 后续新增 `stock_US` 时调用面不需要再改签名
- 不同域不会混写到同一逻辑空间

---

## Phase 4: Split signal scoring by asset domain

### Objective

让评分、信号解释和 universe 选择全部具备明确边界。

### Tasks

1. `query_signal_scores()` 增加：
   - `asset_type`
   - `universe`

2. `_query_universe_factors()` 只查询指定域
3. `_attach_symbol_names()` 改为从 `meta.assets` 或统一 universe 配置读取
4. 建立独立评分 profile：
   - `stock_cn_trend_v1`
   - `etf_cn_rotation_v1`
   - 后续 `stock_us_trend_v1`

### Deliverables

- ETF universe 可独立打分
- A 股与美股不会混合评分
- 后续策略可直接消费某一域的 composite score

---

## Phase 5: Add CN ETF rotation prototype

### Objective

在新的统一模型上落地第一版 ETF 横截面轮动策略。

### Tasks

1. 新增 `CN ETF rotation strategy`
2. 输入：
   - `asset_type=etf_CN`
   - `universe=cn_etf_rotation`
3. 逻辑：
   - 周频调仓
   - 排名取 Top N
   - 可选最低分阈值
4. 输出：
   - 目标持仓
   - 或标准买卖信号

### Deliverables

- 可运行的 ETF 轮动原型
- 不依赖完整回测引擎，也能先验证策略输出

---

## Database Migration Notes

## Recommended schema changes

### `meta.assets`

新增，主键：

- `(asset_type, symbol)`

### `market.daily`

新增：

- `asset_type VARCHAR(32) NOT NULL`
- `data_source VARCHAR(32) NOT NULL`

主键改为：

- `(time, asset_type, symbol)`

### `factors.daily_factors`

新增：

- `asset_type VARCHAR(32) NOT NULL`

主键改为：

- `(time, asset_type, symbol, factor_name)`

### `signals.trading_signals`

新增：

- `asset_type VARCHAR(32) NOT NULL`

主键改为：

- `(time, asset_type, symbol, strategy)`

### `meta.sync_status`

新增：

- `asset_type VARCHAR(32)`
- `data_source VARCHAR(32)`

唯一键改为：

- `(data_type, asset_type, symbol, data_source)`

### Backward compatibility defaults

短期兼容建议：

- 历史股票数据默认补为：
  - `asset_type='stock_CN'`
  - `data_source='tushare'`

---

## Code Refactor Map

## Config / metadata

- 新增统一 `asset universe` 服务模块

## Data loader

- `app/data_loader/` 作为唯一数据接入层
- 新增：
  - `app/data_loader/base.py`
  - `app/data_loader/registry.py`
  - `app/data_loader/providers/tushare.py`

## Market ETL

- `scripts/etl_daily.py`

## Factor pipeline

- `app/factors/pipeline/loader.py`
- `app/factors/pipeline/writer.py`
- `app/factors/registry.py`
- `app/services/factor_backfill.py`
- `app/factors/specs.py`

## Queries / services

- `app/services/factor_query.py`
- `app/services/signal_score.py`

## Signals / strategy

- `app/signals/profiles.py`
- `app/signals/composite.py`
- `app/strategy/base.py`
- `app/strategy/momentum.py`
- 新增 ETF rotation strategy module

## Schema

- `docker/timescaledb/init.sql`
- 新增增量 migration SQL

---

## Recommended MVP Scope

如果目标是尽快落地一个可演示、但底层不返工的 MVP，建议范围为：

1. schema 增加：
   - `asset_type`
   - `data_source`
2. universe 配置支持：
   - `stock_CN`
   - `etf_CN`
   - 并预留 `US/JP`
3. 落地 `data_loader` 接口与 `TushareLoader`
4. ETL 支持：
   - `stock_CN -> daily`
   - `etf_CN -> fund_daily`
5. 因子层支持：
   - `asset_type`
6. 新增 `etf_cn_rotation_v1` profile
7. 新增 `CN ETF` 横截面 Top N 策略

MVP 暂不包含：

- 完整回测引擎接入
- 多 provider 真正联调
- ETF 高级专属因子
- 跨市场组合优化

---

## Open Questions

以下问题建议在实现前明确：

1. `asset_type` 命名是否统一采用 `{asset_class}_{region}`，例如 `stock_CN`？
2. universe 配置是继续 CSV，还是升级到 YAML / DB 表？
3. `data_source` 是否由 universe 配置指定，还是由系统默认映射推断？
4. `meta.assets` 是否第一阶段就落地，还是先只给现有表补字段？
5. `US/JP` 的默认 loader 是否先占位为 `YahooLoader`，还是先只保留 registry 接口？
6. A 股 ETF 是否需要单独维护扩展元数据：
   - 跟踪指数
   - 管理人
   - 基金类型
