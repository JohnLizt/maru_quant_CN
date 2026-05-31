# ETF Rotation 参数地图

本文整理当前 `etf_CN` 轮动策略中，会影响结果的主要参数与默认值。目标是回答三个问题：

1. 现在策略到底在用什么参数
2. 这些参数分布在哪一层
3. 后续如果要调策略，应该先改哪里

## 1. 总览

当前 ETF 轮动链路分成四层：

1. `universe` 层：决定候选 ETF 池，以及每只 ETF 的 `tag`
2. `signal` 层：决定使用哪些因子、如何归一化、如何加权组合成综合分
3. `strategy` 层：决定如何从全池排名中选出实际持仓
4. `backtest` 层：决定调仓频率、执行延迟、手续费、滑点，以及净值计算方式

当前 ETF 应用层日报与回测默认使用：

- `asset_type = etf_CN`
- `profile = trend_etf_v1`
- `strategy = etf_rotation_v1`

## 2. Universe 层

### 2.1 资产域注册

来源：

- [config/asset_types.csv](/Users/eason/dev/code/maru_quant_CN/config/asset_types.csv:1)

当前配置：

- `asset_type = etf_CN`
- `data_source = tushare`
- `calendar_key = CN`
- `loader_key = tushare`
- `pipeline_universe = etf_CN`
- `enabled = true`

### 2.2 当前 ETF universe 文件

来源：

- [config/universes/etf_CN.csv](/Users/eason/dev/code/maru_quant_CN/config/universes/etf_CN.csv:1)

字段：

- `symbol`
- `name`
- `is_active`
- `tag`

当前有效标的数：

- `207`

说明：

- `signal` 查询会过滤到当前 active universe，所以数据库里历史残留 ETF 不会再进入当前排名
- `tag` 会直接影响 strategy 层的 `max_per_tag` 限仓逻辑

### 2.3 Universe 自动生成默认参数

来源：

- [scripts/generate_etf_cn_universe.py](/Users/eason/dev/code/maru_quant_CN/scripts/generate_etf_cn_universe.py:1)

当前默认值：

- `lookback_days = 60`
- `min_avg_amount = 300_000`
- `min_valid_days = 40`

候选纳入规则：

- 名称包含 `ETF`
- 排除 `LOF`
- 排除 `REIT`
- 数据来自 `Tushare fund_basic(market='E', status='L')`

流动性过滤规则：

- 近 `60` 个交易日平均成交额 `>= 300,000`
- 近 `60` 个交易日至少 `40` 天有有效成交

### 2.4 Tag 规则

来源：

- 主规则：[scripts/generate_etf_cn_universe.py](/Users/eason/dev/code/maru_quant_CN/scripts/generate_etf_cn_universe.py:1)
- 人工覆盖：[config/universe_rules/etf_cn_tag_overrides.csv](/Users/eason/dev/code/maru_quant_CN/config/universe_rules/etf_cn_tag_overrides.csv:1)

说明：

- `tag` 不是 Tushare 原生字段
- 先走 override，再走名称关键词规则
- 当前策略里，`tag` 的核心用途不是展示，而是“同 tag 限仓”

当前 tag 分布中数量较多的类别：

- `bond = 50`
- `broad_market = 25`
- `cross_border_hk = 21`
- `growth_index = 12`
- `innovative_drug = 10`
- `cross_border_us = 9`
- `gold = 9`
- `chip = 8`

结论：

- 当前 universe 已经不是单纯行业 ETF 池，而是“中资上市 ETF 多资产池”
- 债券、港股、跨境、美股映射 ETF 都已经在池子里

## 3. Signal 层

### 3.1 当前 profile

来源：

- [app/signals/profiles.py](/Users/eason/dev/code/maru_quant_CN/app/signals/profiles.py:1)

当前 ETF profile：

- `name = trend_etf_v1`
- `signal_mode = cross_sectional`
- `normalization_scope = full_universe`
- `supported_asset_types = ("etf_CN",)`

解释：

- 这是横截面打分，不是单标的时序买卖信号
- 每个交易日会对整个 `etf_CN` universe 打分和排序

### 3.2 因子集合与权重

`trend_etf_v1` 当前使用 4 个因子：

1. `rsi14`
2. `price_to_ma20`
3. `macd_norm`
4. `ma_cross`

当前权重：

- `rsi14 = 0.4633`
- `price_to_ma20 = 0.3314`
- `macd_norm = 0.1606`
- `ma_cross = 0.0447`

说明：

- 权重来自 `etf_CN` 最近一版 `factor_ic` 的 `10d IC` 结果
- 目前 `rsi14` 是最重要因子
- `ma_cross` 仍保留，但权重已经很低

### 3.3 因子归一化规则

来源：

- [app/signals/profiles.py](/Users/eason/dev/code/maru_quant_CN/app/signals/profiles.py:1)

当前规则：

- `rsi14`
  - 方法：`piecewise`
  - `left_score = -0.8`
  - `right_score = 0.2`
  - 分段：
    - `40 -> 52` 映射 `-0.2 -> 0.4`
    - `52 -> 68` 映射 `0.4 -> 1.0`
    - `68 -> 82` 映射 `1.0 -> 0.2`
- `price_to_ma20`
  - 方法：`linear_clip`
  - `clip_lower = -0.08`
  - `clip_upper = 0.10`
- `macd_norm`
  - 方法：`linear_clip`
  - `clip_lower = -0.03`
  - `clip_upper = 0.03`
- `ma_cross`
  - 方法：`linear_clip`
  - `clip_lower = -0.10`
  - `clip_upper = 0.10`

影响：

- `rsi14` 并不是越高越好，过热区会回落
- `price_to_ma20`、`macd_norm`、`ma_cross` 都是裁剪后线性映射

### 3.4 综合分标签阈值

来源：

- [app/signals/profiles.py](/Users/eason/dev/code/maru_quant_CN/app/signals/profiles.py:1)
- [app/signals/composite.py](/Users/eason/dev/code/maru_quant_CN/app/signals/composite.py:1)

当前阈值：

- `strong_threshold = 0.5`
- `positive_threshold = 0.15`
- `neutral_lower_threshold = -0.15`
- `weak_threshold = -0.5`

标签映射：

- `>= 0.5` -> `strong`
- `>= 0.15` -> `positive`
- `> -0.15` -> `neutral`
- `> -0.5` -> `weak`
- 其他 -> `very_weak`

### 3.5 contributor 解释逻辑

来源：

- [app/signals/composite.py](/Users/eason/dev/code/maru_quant_CN/app/signals/composite.py:1)

当前会根据 profile 中包含的因子动态生成解释标签，例如：

- `trend_structure_strong / weak`
- `price_above_ma20 / below_ma20`
- `rsi_in_healthy_trend_zone / rsi_overheated / rsi_weak`
- `macd_momentum_strong / weak`

## 4. Strategy 层

### 4.1 当前策略类

来源：

- [app/strategy/etf_rotation.py](/Users/eason/dev/code/maru_quant_CN/app/strategy/etf_rotation.py:1)

当前策略：

- `strategy_name = etf_rotation_v1`
- `strategy_mode = cross_sectional`
- `supported_signal_modes = ("cross_sectional",)`
- `supported_asset_types = ("etf_CN",)`

### 4.2 当前选仓参数

构造参数默认值：

- `top_n = 5`
- `profile_name = trend_etf_v1`
- `max_per_tag = 1`

当前选仓逻辑：

1. 读取某天全池 `SignalSnapshot`
2. 先按 `composite_score desc, symbol asc` 排序
3. 同一 `tag` 最多保留 `1` 只
4. 从高到低选出最终前 `5` 只
5. 单日等权

实际持仓权重：

- 每日入选组合中，`target_weight = 1 / 当日入选数`

说明：

- 即使应用层日报展示前 `10` 名，策略实际入选仍是前 `5`
- 展示排名和最终持仓不是一回事

### 4.3 应用层展示参数

来源：

- [app/cli/query_etf_rotation.py](/Users/eason/dev/code/maru_quant_CN/app/cli/query_etf_rotation.py:1)

当前默认行为：

- 构造策略时仍使用 `top_n=5, max_per_tag=1`
- CLI 参数 `--top` 默认 `10`

因此当前输出分成两份：

- `results`
  - 原始全池排名前 `10`
- `selected_results`
  - 策略最终选中的前 `5`

## 5. Backtest 层

### 5.1 当前回测入口默认值

来源：

- [app/cli/backtest_etf_rotation.py](/Users/eason/dev/code/maru_quant_CN/app/cli/backtest_etf_rotation.py:1)
- [app/backtest/runner.py](/Users/eason/dev/code/maru_quant_CN/app/backtest/runner.py:370)

CLI 默认值：

- `start_date = 2025-06-03`
- `end_date = 当前日期`
- `top_n = 5`
- `max_per_tag = 1`
- `rebalance_weekday = 2`
- `execution_lag = 1`
- `commission_bps = 5.0`
- `slippage_bps = 5.0`
- `profile = trend_etf_v1`
- `asset_type = etf_CN`

解释：

- `rebalance_weekday = 2` 采用 Python weekday 语义，即周三
- `execution_lag = 1` 表示信号后顺延 `1` 个交易日执行
- 成本按 `5 + 5 = 10 bps` 单边总成本近似处理

### 5.2 当前回测实现方式

来源：

- [app/backtest/runner.py](/Users/eason/dev/code/maru_quant_CN/app/backtest/runner.py:1)

当前实现逻辑：

1. 从 strategy 层拿 `StrategyDecisionTable`
2. 过滤成指定调仓频率
3. 识别调仓日
4. 应用 `execution_lag`
5. 在两次有效调仓日之间持有原组合
6. 用 `market.daily.pct_change` 计算日收益
7. 按换手扣成本
8. 累乘得到净值曲线

### 5.3 当前成本计算

当前总成本率：

- `cost_rate = (commission_bps + slippage_bps) / 10000`

也就是：

- 当前默认 `commission_bps = 5`
- 当前默认 `slippage_bps = 5`
- 总成本率 = `0.001 = 0.1%`

成本按换手扣减：

- `cost = turnover * cost_rate`

说明：

- 这是简化成本模型
- 当前没有区分宽基 ETF、主题 ETF、债券 ETF 的不同滑点

## 6. 当前最关键的可调参数

如果后续要调策略，优先级建议如下：

### 第一优先级：直接影响组合风格

- `config/universes/etf_CN.csv`
  - 决定候选池组成
- `tag`
  - 决定 `max_per_tag`
- `top_n`
  - 决定持仓集中度
- `max_per_tag`
  - 决定主题分散程度

### 第二优先级：直接影响打分结果

- `trend_etf_v1` 的因子集合
- `trend_etf_v1` 的因子权重
- `rsi14` / `price_to_ma20` / `macd_norm` / `ma_cross` 的归一化区间
- `strong/positive/...` 标签阈值

### 第三优先级：直接影响回测表现

- `rebalance_weekday`
- `execution_lag`
- `commission_bps`
- `slippage_bps`

## 7. 当前策略的一句话配置摘要

截至当前代码版本，ETF rotation 的默认实盘/回测口径可以概括为：

- 在 `etf_CN` 的 `207` 只 active ETF 中
- 使用 `trend_etf_v1`
- 以 `rsi14 + price_to_ma20 + macd_norm + ma_cross` 横截面打分
- 其中 `rsi14` 权重最高
- 每日得到全池排名
- 策略实际取 `top 5`
- 同一 `tag` 最多保留 `1` 只
- 回测按周三调仓
- 信号后 `1` 个交易日执行
- 单边成本按 `5 bps 手续费 + 5 bps 滑点` 处理

## 8. 相关源码定位

- Universe 自动生成：
  - [scripts/generate_etf_cn_universe.py](/Users/eason/dev/code/maru_quant_CN/scripts/generate_etf_cn_universe.py:1)
- 当前 ETF universe：
  - [config/universes/etf_CN.csv](/Users/eason/dev/code/maru_quant_CN/config/universes/etf_CN.csv:1)
- Signal profile：
  - [app/signals/profiles.py](/Users/eason/dev/code/maru_quant_CN/app/signals/profiles.py:1)
- Composite score：
  - [app/signals/composite.py](/Users/eason/dev/code/maru_quant_CN/app/signals/composite.py:1)
- Strategy：
  - [app/strategy/etf_rotation.py](/Users/eason/dev/code/maru_quant_CN/app/strategy/etf_rotation.py:1)
- Strategy service：
  - [app/services/strategy_service.py](/Users/eason/dev/code/maru_quant_CN/app/services/strategy_service.py:1)
- Backtest runner：
  - [app/backtest/runner.py](/Users/eason/dev/code/maru_quant_CN/app/backtest/runner.py:1)
- Backtest CLI：
  - [app/cli/backtest_etf_rotation.py](/Users/eason/dev/code/maru_quant_CN/app/cli/backtest_etf_rotation.py:1)
