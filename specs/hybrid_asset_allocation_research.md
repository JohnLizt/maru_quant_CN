# Hybrid Asset Allocation（HAA）研究方案

## 1. 研究目标

研究 Wouter Keller 与 Jan Willem Keuning 的 Hybrid Asset Allocation（HAA-Balanced），验证 TIP canary、跨周期动量和防御资产替换能否解决当前 ETF 轮动在股债双杀、通胀和快速风险切换中的缺陷。

HAA 与当前 `8 进 4` 外观相似，但机制不同：它使用月频 13612U 总回报动量、TIP 二元风险状态和绝对动量替换，不使用 20 日回归动量、tag 限制或当前风险半仓逻辑。

## 2. 资料与原始规则

主要依据：

- Keller & Keuning，[Dual and Canary Momentum with Rising Yields/Inflation: Hybrid Asset Allocation](https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=4346906)
- 作者 Jan Willem Keuning 的规则说明，[Introducing Hybrid Asset Allocation](https://indexswingtrader.blogspot.com/2023/02/introducing-hybrid-asset-allocation-haa.html)

HAA-Balanced 使用：

- canary：`TIP`；
- offensive：`SPY, IWM, VEA, VWO, VNQ, DBC, IEF, TLT`；
- defensive：`BIL, IEF`；
- offensive Top4，等权；
- 月末调仓；
- 动量为过去 1、3、6、12 个月总回报的非加权平均。

### 2.1 13612U

```text
M13612U(asset) = [TR1 + TR3 + TR6 + TR12] / 4
```

这里是非加权平均，不是 BAA/VAA 常见的 `13612W = 12*TR1 + 4*TR3 + 2*TR6 + TR12`。主研究不得混用。

### 2.2 忠实复现规则

每月最后一个交易日收盘后：

1. 为 canary、offensive 和 defensive 全部资产计算 13612U。
2. 在 `BIL/IEF` 中选出分数更高的单一防御资产 `D*`。
3. 若 `TIP <= 0`，组合 100% 持有 `D*`。
4. 若 `TIP > 0`，按 13612U 选 offensive Top4，每个槽位 25%。
5. offensive Top4 中分数 `<= 0` 的槽位由 `D*` 替代；多个替换槽位合并到同一防御资产。
6. 信号在月末产生，下一交易日执行并持有一个月。

例如 TIP 为正、Top4 中两只资产动量非正时，组合为两只有效 offensive 各 25%，防御资产 50%。

## 3. 资产与仓库映射

| 角色 | 原版 | 当前仓库 | 处理 |
| --- | --- | --- | --- |
| Canary | TIP | 缺失 | 必须增加 |
| 美国大盘 | SPY | 缺失，已有 VTI | 原版增加 SPY |
| 美国小盘 | IWM | 已有 | 直接使用 |
| 发达市场 | VEA | 已有 | 直接使用 |
| 新兴市场 | VWO | 缺失，已有 EEM | 原版增加 VWO；EEM 做代理 |
| 美国 REIT | VNQ | 已有 | 直接使用 |
| 商品 | DBC | 已有 | 直接使用 |
| 中期国债 | IEF | 已有 | 直接使用 |
| 长期国债 | TLT | 已有 | 直接使用 |
| 短期国库券 | BIL | 缺失 | 必须增加 |

10 年 ETF 实盘历史足以覆盖这些标的；如扩展到 2003 年以前，需要使用可审计的指数或基金代理，不能把回填序列与 ETF 实盘区间混为一体。

## 4. 与当前仓库的差距

1. 缺少总回报价格，13612U 不能用未复权 close 计算；
2. 回测器不支持月末调仓；
3. 缺少 1/3/6/12 月总回报因子和绝对动量判断；
4. 当前 ETF 轮动策略会把入选资产权重重新等分，不能直接表达“无效槽位替换为同一防御资产”；
5. 当前 `max_per_tag=1` 不属于 HAA，必须关闭；
6. 当前 risk overlay 不属于 HAA，忠实复现中必须关闭；
7. 缺少 TIP、BIL、SPY、VWO 数据；
8. 报告缺少 canary 状态、替换槽位数和 defensive contribution。

## 5. 实现计划

### 阶段 A：公共能力

- 完成总回报价格和动态现金收益；
- 增加月末调仓、下一交易日执行；
- 增加通用 `total_return_1m/3m/6m/12m`；
- 增加 13612U 计算器，并用确定性月末样本测试；
- 支持相同 symbol 接收多个目标槽位后聚合权重。

### 阶段 B：HAA 策略

建议新增：

```text
app/strategy/hybrid_asset_allocation.py
scripts/backtest_hybrid_asset_allocation.py
tests/test_hybrid_asset_allocation.py
config/strategy_universes/haa_balanced_US.csv
```

每月元数据必须记录：

- TIP 13612U 和 risk mode；
- offensive 完整排名；
- Top4 原始选择；
- 每个槽位是否通过绝对动量；
- 最优 defensive 及其分数；
- 替换后的最终目标权重。

### 阶段 C：实验矩阵

第一轮仅包含：

| 编号 | 配置 | 用途 |
| --- | --- | --- |
| H0 | VTI buy-and-hold | 主基准 |
| H1 | Offensive 8 资产静态等权 | 分散基准 |
| H2 | 13612U Top4，无 canary/绝对门槛 | 相对动量贡献 |
| H3 | H2 + 负动量槽位替换 | 绝对动量贡献 |
| H4 | 完整 HAA-Balanced | canary 增量价值 |
| H5 | VTI + TIP canary + 同一 defensive | 判断 canary 本身是否足够 |

这一拆解用于区分收益来自 Top4、绝对动量、TIP canary，还是单纯持有债券。

### 阶段 D：有限敏感性

完整 HAA 通过后才测试：

- VWO 与 EEM；
- SPY 与 VTI；
- Top3/Top4/Top5；
- TIP 阈值附近的简单缓冲带；
- 13612U 与单一 12 月动量；
- 单边成本 5、10、20 bps；
- 月末执行日平移。

不测试细粒度权重组合，也不把 13612W 混入第一轮。

## 6. 评价与基准

必须与 VTI、60/40、offensive 静态等权、当前生产 8/4 在同一总回报区间比较，并输出：

- CAGR、Sharpe、Calmar、最大回撤和恢复时间；
- 相对 VTI 滚动 1/3/5 年表现；
- TIP risk-off 月份比例和误报/漏报阶段；
- offensive、IEF、TLT、BIL 分别贡献；
- 平均 defensive 比例和 100% defensive 持续时间；
- 2020、2022、2023 反弹阶段；
- 换手、成本和 whipsaw；
- Top1/Top3 资产贡献集中度。

HAA 若无法在 10 年总回报口径下至少接近 VTI 收益，并显著降低回撤，则不进入 Universe 优化。若完整 HAA 不如 H3，说明 TIP canary 没有增量价值，应保留更简单的双动量框架。

## 7. 关键风险

- 单一 TIP canary 是二元开关，存在模型集中风险；
- TIP 同时受实际利率、通胀预期和久期影响，不是纯经济状态指标；
- 2022 年启发了该策略设计，历史评估必须区分论文发表前后；
- 月末信号会滞后于月内急跌，也可能错过快速反弹；
- IEF 同时出现在 offensive 和 defensive，贡献归因必须避免重复；
- BIL/TIP 分红和债息不复权会直接破坏 canary 与防御排序。

## 8. 输出

```text
logs/experiments/hybrid_asset_allocation/<experiment_id>/
```

至少输出 `experiment_config.json`、`monthly_momentum.csv`、`canary_states.csv`、`slot_replacements.csv`、`benchmark_comparison.csv`、`regime_metrics.csv`、`summary.json` 和 `experiment_report.md`。

