# Adaptive Asset Allocation（AAA）研究方案

## 1. 研究目标

研究 Butler、Philbrick、Gordillo 与 Varadi 的 Adaptive Asset Allocation（AAA）框架，验证“中期动量选择 + 短期波动/相关性估计 + 最小方差配置”是否比当前等权 TopK 更稳定，并能否相对 VTI 提供可重复的风险调整优势。

AAA 不是一条完全公开、唯一确定的交易规则。原论文明确说明其目标是建立概念框架，而不是提供可逐步复制的完整算法。因此本研究分为：

1. 论文 Exhibit 5 可复现基线；
2. 参数和约束完全公开的工程版本；
3. 两者不得与 ReSolve 后续商业化 AAA 结果等同。

## 2. 资料与规则边界

主要依据：

- Butler, Philbrick, Gordillo & Varadi，[Adaptive Asset Allocation: A Primer](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2328254)
- 论文正文镜像，[Adaptive Asset Allocation: A Primer](https://studylib.net/doc/8171894/adaptive-asset-allocation--a-primer)
- ReSolve 对方法演进的说明，[2017 Year-End Report](https://investresolve.com/2017-year-end-report/)

论文逐步比较：静态等权、逆波动、Top Half 动量等权、Top Half 动量逆波动、Top Half 动量最小方差，最后讨论更频繁调仓和组合波动率目标。

### 2.1 论文 Exhibit 5 基线

每月最后一个交易日：

1. 对 10 类资产计算过去 6 个月总回报；
2. 选择排名前 5 的资产；
3. 使用近期波动率和相关性估计协方差矩阵；
4. 在 Top5 内求解 long-only、权重和为 1 的最小方差组合；
5. 下一交易日执行并持有至下月。

优化目标：

```text
minimize    w' * Sigma * w
subject to  sum(w) = 1
            w_i >= 0
```

原论文没有完整规定 Exhibit 5 的协方差窗口、收缩方法、权重上下限和数值求解细节，不能把任意实现称为唯一原版。

### 2.2 工程化透明版本 AAA-T

为了得到可审计结果，第一版锁定：

```text
momentum = 126 个交易日总回报
selection = Top5 / 10
correlation_window = 126 日
volatility_window = 20 日
Sigma_ij = Corr126_ij * Vol20_i * Vol20_j
constraints = long-only, sum(weights)=1
rebalance = month-end, execution_lag=1
```

这是本项目为消除论文歧义而预先锁定的透明工程口径。文档和报告中统一命名为 `AAA-T`，不声称它是唯一原版，也不声称等于 ReSolve 的完整 AAA。

数值稳定措施必须预先固定：

- 协方差矩阵对称化；
- 对角线加入固定 ridge；
- 求解失败时回退到 Top5 逆波动，而不是事后人工修权重；
- 极小权重是否截断必须作为配置记录，主版本默认不截断。

## 3. 原始资产类与仓库映射

| 资产类 | 常用 ETF 代理 | 当前仓库 | 处理 |
| --- | --- | --- | --- |
| 美国股票 | SPY | 缺失 | 增加 |
| 欧洲股票 | EZU | 缺失 | 增加 |
| 日本股票 | EWJ | 缺失 | 增加 |
| 新兴市场股票 | EEM | 已有 | 直接使用 |
| 美国 REIT | VNQ | 已有 | 直接使用 |
| 国际 REIT | RWX | 缺失 | 增加 |
| 美国中期国债 | IEF | 已有 | 直接使用 |
| 美国长期国债 | TLT | 已有 | 直接使用 |
| 商品 | DBC | 已有 | 直接使用 |
| 黄金 | GLD | 已有 | 直接使用 |

AAA 对相关性结构敏感，不应随意用 VEA 同时替代欧洲、日本，也不应删除 RWX 后仍按 Top5 计算。代理变化会改变选择比例和协方差矩阵，必须作为不同 Universe 单独报告。

## 4. 与当前仓库的差距

1. 缺少总回报价格和稳定的日收益面板；
2. 回测器没有月末调仓；
3. 缺少 126 日总回报、20/126 日波动率与滚动相关矩阵的通用研究层；
4. 当前策略只支持等权选中资产，没有任意连续目标权重生成器；
5. 项目没有受约束二次优化依赖，需要选择并锁定求解器；
6. 缺少协方差正则化、不可逆矩阵和求解失败处理；
7. 缺少 SPY、EZU、EWJ、RWX；
8. 当前报告没有预测波动、实际波动、权重集中度和优化器状态。

## 5. 实现计划

### 阶段 A：数据和月频

- 完成总回报价格和复权测试；
- 增加月末信号及下一交易日执行；
- 构建无缺口的 Top10 日收益面板；
- 明确 ETF 上市前不做回填的共同历史基线；
- 如使用指数/基金代理扩展历史，单独输出数据血缘。

### 阶段 B：组合研究模块

建议新增：

```text
app/portfolio/covariance.py
app/portfolio/optimization.py
app/strategy/adaptive_asset_allocation.py
scripts/backtest_adaptive_asset_allocation.py
tests/test_portfolio_optimization.py
tests/test_adaptive_asset_allocation.py
```

优化测试必须覆盖：

- 权重非负且和为 1；
- 对角协方差下偏向低波动资产；
- 高相关资产不会同时获得不合理大权重；
- 奇异协方差可稳定求解或确定性回退；
- 输入资产顺序变化不改变结果；
- 不使用调仓日后的收益。

### 阶段 C：机制拆解

| 编号 | 组合 | 目的 |
| --- | --- | --- |
| A0 | 10 资产静态等权 | 分散基准 |
| A1 | 10 资产逆波动 | 波动估计贡献 |
| A2 | Top5 6 月动量等权 | 动量选择贡献 |
| A3 | Top5 6 月动量逆波动 | 动量 + 波动 |
| A4 | Top5 6 月动量最小方差（AAA-T） | 相关性增量贡献 |
| A5 | A4 + 8% 波动率目标 | 仅作后续扩展 |

先比较 A0-A4。A5 涉及现金、潜在杠杆和更频繁风险控制，不能与 A4 混成一个实验。

### 阶段 D：有限敏感性

A4 通过后才测试：

- 动量 3/6/9/12 月的宽邻域；
- Top4/Top5/Top6；
- 波动 20/60 日、相关 60/126 日；
- sample covariance 与固定 shrinkage；
- 单资产权重上限 35%/50%；
- 调仓日平移和成本翻倍。

禁止对每个资产使用不同回看期，也不以历史最高 Sharpe 选择 ridge 或权重上限。

## 6. 基准和评价

主基准为 VTI buy-and-hold；结构基准为 10 资产静态等权和 A2 动量等权。必须输出：

- CAGR、Sharpe、Calmar、最大回撤和恢复时间；
- 相对 VTI 滚动 1/3/5 年超额收益；
- 预测波动与下月实际波动误差；
- 有效持仓数 `1 / sum(w_i^2)`；
- Top1/Top3 权重集中度和资产贡献集中度；
- 优化器成功率、回退次数和最小特征值；
- 月度换手、成本和小权重交易占比；
- A2、A3、A4 之间的增量收益和增量回撤。

若 A4 不稳定地优于 A3，说明相关性优化没有可靠增量，应采用更简单的逆波动方案。若 AAA 只在某个协方差窗口表现好，则停止优化，不进入生产候选。

## 7. 样本外验证

AAA 自由度高于 GEM、GTAA 和 HAA，必须优先做 Walk-Forward：

```text
训练/估计只使用调仓日前数据
主评估：固定 AAA-T 参数的滚动历史回测
参数研究：4 年训练 -> 1 年样本外 -> 每年滚动
最终：锁定参数后的未来模拟盘
```

现有 10 年历史已经被多次查看，只能视为研究样本，不能再声称是真正未见样本。所有求解器约束和回退规则必须在查看策略结果前锁定。

## 8. 关键风险

- 最小方差对协方差估计误差非常敏感；
- 短期相关性在危机时会突然变化；
- 优化器容易产生集中或极小权重，增加无效换手；
- ETF 共同历史偏短，容易高估参数稳定性；
- 原论文展示的完整周频、8% 波动目标 AAA 没有公开全部细节，不能作为可复现基准；
- 不复权的债券、REIT 和股票收益会同时扭曲动量、波动和相关性，影响比普通等权策略更严重。

## 9. 输出

```text
logs/experiments/adaptive_asset_allocation/<experiment_id>/
```

至少输出 `experiment_config.json`、`monthly_rankings.csv`、`covariance_diagnostics.csv`、`target_weights.csv`、`optimizer_events.csv`、`benchmark_comparison.csv`、`rolling_relative_metrics.csv`、`summary.json` 和 `experiment_report.md`。
