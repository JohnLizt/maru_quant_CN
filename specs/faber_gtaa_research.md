# Faber GTAA（10-Month MA）研究方案

## 1. 研究目标

研究 Meb Faber 在《A Quantitative Approach to Tactical Asset Allocation》中提出的五资产战术配置框架，验证“跨资产静态分散 + 每个资产独立的 10 月趋势过滤”能否相对 VTI 改善长期收益质量和尾部风险。

GTAA-5 不做横截面 TopK，也不预测哪个资产最好。每个 20% 资产袖套独立判断趋势，未通过趋势的袖套进入现金。

## 2. 资料与原始规则

主要依据：

- Meb Faber，[A Quantitative Approach to Tactical Asset Allocation](https://mebfaber.com/wp-content/uploads/2016/05/SSRN-id962461.pdf)
- 论文索引页，[SSRN 962461](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=962461)

论文使用五类月度总回报序列：S&P 500、MSCI EAFE、美国 10 年国债、GSCI 商品和 NAREIT。静态组合五类资产各占 20%；择时组合对每类资产单独应用同一条 10 月简单移动平均规则。

### 2.1 忠实复现规则

每月最后一个交易日收盘后，对每个资产分别计算：

```text
SMA10_i(t) = mean(P_i(t), P_i(t-1), ..., P_i(t-9))
```

其中 `P` 为月末总回报价格。

- 若 `P_i(t) > SMA10_i(t)`，下一月该袖套持有资产 `i`，权重 20%；
- 否则该袖套持有现金或短期国库券，权重 20%；
- 五个袖套独立决策，现金比例可以是 0%、20%、40%、60%、80% 或 100%；
- 月末产生信号，下一交易日执行；
- 不额外叠加 TopK、止损、波动阈值或当前 `risk_overlay`。

当价格恰好等于均线时，研究实现默认视为未通过趋势门槛，并在配置和测试中锁定该规则。

## 3. 资产映射

### 3.1 原始经济暴露与 ETF

| 原始资产类 | 忠实 ETF 代理 | 当前仓库 | 仓库近似代理 |
| --- | --- | --- | --- |
| S&P 500 | SPY | 缺失 | VTI |
| MSCI EAFE | EFA | 缺失 | VEA |
| 美国债券 | IEF 或 AGG，需区分版本 | IEF/BND 已有 | IEF |
| 商品 | DBC | 已有 | DBC |
| 美国 REIT | VNQ | 已有 | VNQ |
| 现金/T-Bill | BIL/SHY | 缺失 | CASH |

论文原始序列是美国 10 年国债，不是综合债券。现代常见 GTAA ETF 实现经常使用 AGG；本研究主版本使用 `IEF` 对应原始经济暴露，`BND/AGG` 只能作为明确标注的代理版本。

### 3.2 两套结果必须分开

```text
GTAA5-faithful: SPY / EFA / IEF / DBC / VNQ / BIL
GTAA5-repo-proxy: VTI / VEA / IEF / DBC / VNQ / CASH
```

代理版只用于判断仓库现有数据下的可行性，不能用于声称复现论文收益。

## 4. 与当前仓库的差距

1. 尚未用复权总回报价格驱动信号和回测净值；
2. 回测器不支持月末调仓；
3. 当前策略输出更适合“选中若干资产后权重归一”，而 GTAA 要保留五个固定 20% 袖套，未投资部分不能被其他资产重新归一；
4. 现金收益率目前是常数，长期研究需要真实 T-Bill 回报；
5. 报告需要增加月度现金比例、各袖套在线率和趋势切换贡献；
6. 数据库缺少 SPY、EFA、BIL/SHY。

## 5. 实现计划

### 阶段 A：公共基础设施

- 完成总回报价格与复权因子支持；
- 增加月末调仓和下一交易日执行；
- 支持显式现金目标权重，不把剩余权重重新分配；
- 支持动态 T-Bill 收益；
- 增加静态五资产等权回测基准。

### 阶段 B：GTAA 策略

建议新增：

```text
app/strategy/faber_gtaa.py
scripts/backtest_faber_gtaa.py
tests/test_faber_gtaa.py
```

每月决策记录：

- 月末总回报价格；
- 10 月 SMA；
- 每个袖套的 risk-on/risk-off 状态；
- 风险资产目标权重；
- 现金目标权重；
- 下一次有效执行日期。

单元测试必须覆盖 10 个月预热不足、价格等于均线、缺失月末、连续现金和重新进入风险资产。

### 阶段 C：实验顺序

1. 五资产静态等权；
2. GTAA5-faithful；
3. GTAA5-repo-proxy；
4. VTI buy-and-hold；
5. 五资产全部使用当前 `risk_overlay` 的反事实对照。

### 阶段 D：有限敏感性

主版本通过后才测试：

- SMA 8、10、12 月的宽邻域；
- `IEF` 与 `AGG/BND`；
- 现金为 BIL、SHY 或动态 T-Bill；
- 调仓日平移；
- 加倍交易成本。

不同资产不得分别优化均线长度，否则会显著扩大自由度并偏离论文“统一参数”的核心原则。

## 6. 评价与归因

主基准为 VTI buy-and-hold，结构基准为五资产静态等权。报告至少包括：

- CAGR、Sharpe、Calmar、最大回撤、恢复时间；
- 相对 VTI 的滚动 1/3/5 年超额收益；
- 五类资产分别因趋势过滤增加或损失的收益；
- 平均现金比例、满仓和全现金月份比例；
- 每类资产趋势切换次数及 whipsaw 损失；
- 2008、2020、2022 和快速反弹阶段表现；
- 成本和现金收益对结果的贡献。

GTAA 的核心假设是用一部分牛市收益换取更浅回撤。若长期收益显著低于 VTI且最大回撤改善不足 25%，则没有替代 VTI 的价值；若只改善风险调整表现，则归类为防御型配置。

## 7. 关键风险

- 月度均线在震荡市场会反复进出；
- 10 月信号可能错过快速反弹的前几个月；
- 商品 ETF 的期货展期收益与 GSCI 指数并不完全一致；
- REIT、债券和权益分红若未复权会严重扭曲结果；
- 用 AGG 替换 10 年国债会改变久期和信用结构；
- 论文长历史依赖指数，ETF 共同历史明显更短，二者不得混报。

## 8. 输出

```text
logs/experiments/faber_gtaa/<experiment_id>/
```

至少输出 `experiment_config.json`、`monthly_sleeve_signals.csv`、`cash_allocation.csv`、`benchmark_comparison.csv`、`rolling_relative_metrics.csv`、`summary.json` 和 `experiment_report.md`。

