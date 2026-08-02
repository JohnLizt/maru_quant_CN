# Dual Momentum（GEM）研究方案

## 1. 研究目标

研究 Gary Antonacci 的 Global Equities Momentum（GEM），验证“全球权益相对动量 + 美国权益绝对动量 + 债券避险”能否在本仓库的统一总回报口径下，相对全仓 VTI 提高长期收益质量。

本阶段首先忠实复现公开规则，不优化 ETF 池，不扫描大量动量窗口。只有原始框架通过长期门槛后，才研究代理资产和参数敏感性。

## 2. 资料与规则口径

主要依据：

- Gary Antonacci，[Risk Premia Harvesting Through Dual Momentum](https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=2042750)
- 作者官网，[Global Equities Momentum](https://www.optimalmomentum.com/global-equities-momentum/)
- 作者说明，[Extended Backtest of Global Equities Momentum](https://www.optimalmomentum.com/extended-backtest-of-global-equities-momentum/)
- 作者 FAQ，[Optimal Momentum FAQ](https://www.optimalmomentum.com/faq/)

作者公开说明使用 S&P 500、MSCI ACWI ex-US 和美国综合债券指数，按月复核，以过去 12 个月总回报同时表达相对动量和绝对动量。作者 FAQ 明确指出，书中主流程先用 S&P 500 判断绝对动量；美国股票趋势为负时直接进入综合债券，即使海外股票相对更强也可能不持有海外股票。

### 2.1 忠实复现规则

每月最后一个交易日收盘后：

1. 计算美国权益、全球除美国权益和 3 个月美国国库券的过去 12 个月总回报。
2. 若美国权益 12 个月总回报不高于国库券，下一期持有美国综合债券。
3. 若美国权益通过绝对动量门槛，在美国权益和全球除美国权益中选择 12 个月总回报较高者。
4. 组合始终只持有一个资产，目标权重为 100%。
5. 信号使用当月最后一个有效收盘价，下一交易日执行，持有至下次月度调仓。

形式化表示：

```text
if TR12(US) <= TR12(TBill):
    hold Aggregate Bond
else:
    hold argmax(TR12(US), TR12(ex-US))
```

不把“先比较两类权益，再对赢家做绝对动量”的流程混入主结果；该流程只能作为明确标注的敏感性版本。

## 3. 资产映射

### 3.1 原版 ETF 代理

| 经济暴露 | 首选 ETF | 当前仓库 | 处理 |
| --- | --- | --- | --- |
| S&P 500 | SPY | 缺失 | 增加 |
| MSCI ACWI ex-US | ACWX；VEU 为流动性代理 | 缺失 | 原版研究优先增加 ACWX |
| 美国综合债券 | AGG | 缺失，已有 BND | 原版用 AGG，BND 做代理敏感性 |
| 3 月国库券 | BIL 或权威 T-Bill 序列 | 缺失 | 增加 BIL；长期回测需无风险利率序列 |

### 3.2 仓库已有近似映射

`VTI / VEA / EEM / BND / CASH` 可以构造近似版本，但不能命名为原版 GEM：

- VTI 比 S&P 500 更宽；
- VEA + EEM 不是一只可直接交易的 ACWI ex-US，总回报合成还需要固定权重规则；
- BND 与 AGG 接近但并非完全相同；
- 固定 `cash_interest_rate` 不能替代随时间变化的 T-Bill 门槛。

## 4. 与当前仓库的差距

1. Yahoo loader 下载 `Adj Close`，但当前只用它计算 `pct_change`，回测估值仍使用未复权 `close`。
2. `market.daily` 有 `adj_factor` 字段，但 Yahoo 数据没有填充该字段。
3. 回测器仅支持 `daily / weekly / biweekly` 决策过滤，不支持月末调仓。
4. 当前 ETF 轮动策略依赖横截面 `composite_score`，GEM 需要独立的时序绝对动量和两资产相对比较。
5. 当前现金收益率是常数，无法准确表达动态 T-Bill 总回报。
6. 当前报告缺少相对 VTI 的滚动超额收益、上/下行捕获率和最长落后期。

## 5. 实现计划

### 阶段 A：公共基础设施

- 正确保存和使用美国 ETF 总回报价格或复权因子；
- 给回测器增加 `monthly`，定义为每月最后一个有效交易日产生信号；
- 保留 `execution_lag=1`，避免用月末收盘信号在同一收盘成交；
- 支持时间变化的现金/T-Bill 日收益；
- 增加纯 VTI、纯 SPY 和静态全球资产配置基准。

实施状态（2026-08-02）：已完成。

- Yahoo loader 保存 `Adj Close / Close` 到 `market.daily.adj_factor`，回测 OHLC 统一按该因子调整；
- 回测器支持 `monthly` 和可选 `cash_return_series`，未覆盖日期回退到原固定现金利率；
- `app/backtest/baselines.py` 提供 VTI、SPY、静态 GAA decision table 及 BIL 日收益构造器；
- 对应单元测试位于 `tests/test_gem_stage_a.py`；
- 存量美国 ETF 历史数据仍需重新回填，旧记录不会仅因代码升级自动获得正确复权因子。

### 阶段 B：GEM 决策模块

建议新增：

```text
app/strategy/dual_momentum_gem.py
scripts/backtest_dual_momentum_gem.py
tests/test_dual_momentum_gem.py
```

实施状态（2026-08-02）：已完成。

- `build_gem_signal_snapshot` 从月末总回报价格计算美国、海外和 T-Bill 的 12 个月收益；
- `DualMomentumGEMStrategy` 严格执行“美国绝对动量优先，再比较美股与海外股”的主流程；
- 相等边界按绝对动量未通过处理，直接进入综合债券；
- `scripts/backtest_dual_momentum_gem.py` 固定使用 `SPY / ACWX / AGG / BIL`、月末信号和默认次日执行；
- 决策 metadata 已包含下列全部诊断字段，规则测试位于 `tests/test_dual_momentum_gem.py`。

决策元数据至少记录：

- `us_return_12m`
- `ex_us_return_12m`
- `t_bill_return_12m`
- `absolute_momentum_pass`
- `selected_symbol`
- `selection_reason`

### 阶段 C：忠实复现

只运行一组主配置：

```text
lookback = 12 months
rebalance = month-end
US = SPY
ex-US = ACWX
defensive = AGG
absolute benchmark = BIL/T-Bill
```

先用 ETF 共同历史期验证，再在获得可靠指数或基金代理数据后扩展到更长周期。

### 阶段 D：有限敏感性

主配置通过后才测试：

- VTI 替代 SPY；
- VEU 替代 ACWX；
- BND 替代 AGG；
- `12m` 与 `12m skip 1m`；
- 绝对优先与相对优先流程；
- 调仓日平移前后 3 个交易日；
- 单边成本 5、10、20 bps。

## 6. 基准与判定

主基准：

1. VTI buy-and-hold；
2. SPY buy-and-hold；
3. GEM 三类资产按长期平均暴露构造的静态组合；
4. 60/40 股票债券组合。

必须报告：

- CAGR、Sharpe、Calmar、最大回撤和恢复时间；
- 相对 VTI 的滚动 1/3/5 年超额收益；
- 权益上涨和下跌捕获率；
- 进入债券的月份比例及债券贡献；
- 信号切换次数、换手和成本；
- 最长连续落后 VTI 时间。

若 GEM 长期收益明显低于 VTI，且回撤改善不足 25%，则不进入参数优化。若收益略低但回撤、Sharpe 和最差年度显著改善，只能归类为防御配置，不能称为 VTI 增强。

## 7. 关键风险

- 100% 单资产持仓会产生集中暴露和切换时点风险；
- 12 个月信号对快速 V 型反弹反应较慢；
- 长期历史若使用指数拼接，必须记录每段数据来源，防止代理选择偏差；
- T-Bill 门槛和债券总回报不能用固定现金利率替代；
- 所有结果必须使用分红再投资后的总回报数据。

## 8. 输出

```text
logs/experiments/dual_momentum_gem/<experiment_id>/
```

至少输出 `experiment_config.json`、`monthly_signals.csv`、`returns.csv`、`benchmark_comparison.csv`、`rolling_relative_metrics.csv`、`summary.json` 和 `experiment_report.md`。
