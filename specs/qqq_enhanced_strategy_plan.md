# QQQ Enhanced Strategy 实验计划

## 1. 背景与目标

当前 `etf_rotation_US` 属于多资产 ETF 轮动策略。它的核心结构是：

```text
候选池 8 只
TopK = 4
max_per_tag = 1
入选标的等权
```

这个结构适合做“分散化轮动”，但不适合做“QQQ 增强”。原因很直接：

- `TopK=4` 且等权，单只 ETF 默认只有约 `25%` 权重；
- `max_per_tag=1` 会限制科技/成长类资产同时入选；
- 即使科技主线非常强，组合也会被黄金、债券、美元、金融、地产、医疗等资产长期稀释；
- 2023 年 QQQ 强反弹阶段，策略没有完成从防守资产切回成长主线的进攻切换。

因此，新策略不应继续围绕 `8进4` 小修小补，而应重新定义为：

> 以 QQQ/科技成长 beta 为主收益来源，在风险升高时通过防守 overlay 降低回撤。

第一阶段目标不是立刻跑赢所有年份的 QQQ，而是形成一个清晰、可解释、可迭代的 QQQ 增强框架：

1. 牛市或成长主线行情中，尽量保持较高 QQQ/科技暴露。
2. 熊市或高波动阶段，能主动降低权益仓位或切向防守资产。
3. 长期收益接近或超过 QQQ，同时最大回撤显著低于 QQQ。
4. 净值曲线比纯 QQQ 更平滑，尤其降低 2022 类似阶段的深回撤。

## 2. 基线观察

### 2.1 10 年对照

当前 10 年回测显示：

| 组合 | 总收益 | 年化 | Sharpe | 最大回撤 |
| --- | ---: | ---: | ---: | ---: |
| QQQ buy-and-hold | 552.32% | 待统一报告 | 待统一报告 | 待统一报告 |
| VTI buy-and-hold | 238.38% | 待统一报告 | 待统一报告 | 待统一报告 |
| Best 当前 8/4 | 82.05% | 6.25% | 0.5721 | -27.51% |
| n8 phase1_n08_002 | 90.64% | 6.75% | 0.4901 | -30.96% |
| n12 phase1_n12_041 | 57.16% | 4.68% | 0.3901 | -27.25% |

当前轮动策略没有跑赢 QQQ，也没有跑赢 VTI。它更像是低 beta、多资产分散策略，而不是成长增强策略。

### 2.2 2022-2023 拆解

| 区间 | Best | n8 | n12 | QQQ | VTI |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2022 | -22.25% | -5.15% | -15.07% | -33.71% | -21.31% |
| 2023 | 10.83% | -10.10% | 1.89% | 54.84% | 24.58% |
| 2022-2023 | -13.33% | -15.31% | -14.19% | 1.95% | -2.37% |
| 2023 Q1-Q3 | 4.58% | -4.01% | -2.85% | 35.46% | 11.55% |

关键结论：

- 2022 年策略相对 QQQ 有一定防守价值；
- 真正的问题是 2023 年没有充分参与 QQQ 修复；
- 当前结构在成长牛市里暴露不足，在防守后重新进攻的速度不够。

## 3. 新策略定位

新策略命名建议：

```text
qqq_enhanced
```

策略定位：

- 不是多资产均衡轮动；
- 不是简单 QQQ 择时；
- 是“QQQ 核心仓 + 成长增强仓 + 防守 overlay”的组合。

核心原则：

1. **QQQ 是默认主仓，不是候选池里的一票。**
2. **成长资产可以集中，不再受 `max_per_tag=1` 限制。**
3. **防守资产用于风险状态切换，而不是长期等权稀释主线。**
4. **风控目标是减少大回撤，不是频繁逃顶抄底。**

## 4. 策略结构设计

### 4.1 资产层

第一版建议分三类资产。

#### 核心成长资产

```text
QQQ  纳指 100 核心 beta
XLK  科技板块
SMH  半导体增强
VGT  科技增强备选
IGV  软件增强备选
```

说明：

- 第一版可以先只用 `QQQ / XLK / SMH`，降低自由度；
- `VGT / IGV` 需要确认历史数据覆盖和流动性后再加入；
- 若数据暂时不完整，可放到第二阶段。

#### 防守资产

```text
GLD  黄金
IEF  中久期美债
TLT  长久期美债
UUP  美元
CASH  现金
```

说明：

- `GLD / IEF / UUP` 来自现有 Best 经验；
- `TLT` 波动更高，可能在降息预期阶段有用，但 2022 类阶段会有利率风险；
- 现金应作为显式资产或回测中的仓位状态处理。

#### 参考宽基

```text
VTI
SPY
```

说明：

- 主要用于 benchmark；
- 是否进入策略候选池需要谨慎，避免把 QQQ 增强重新稀释成宽基轮动。

### 4.2 权重层

不再使用当前的简单等权 TopK，而采用 regime-based allocation。

建议第一版权重：

| 风险状态 | QQQ/成长资产 | 防守资产 | 现金 |
| --- | ---: | ---: | ---: |
| Risk-on | 80%-100% | 0%-20% | 0% |
| Neutral | 50%-70% | 20%-40% | 0%-20% |
| Risk-off | 0%-30% | 40%-80% | 0%-40% |

第一版可以从更简单的三档开始：

```text
Risk-on:  70% QQQ + 30% growth leader
Neutral:  50% QQQ + 25% GLD + 25% IEF
Risk-off: 25% QQQ + 35% GLD + 25% IEF + 15% UUP/cash
```

其中 `growth leader` 可在 `XLK / SMH` 中按动量选择。

## 5. 风险状态识别

### 5.1 趋势条件

以 QQQ 自身为主信号，避免被防守资产排名牵着走。

候选条件：

- QQQ 收盘价高于 200 日均线；
- QQQ 收盘价高于 100 日均线；
- QQQ 20 日动量为正；
- QQQ 60 日动量为正；
- QQQ 相对 VTI 强度为正。

第一版建议：

```text
Risk-on:
  QQQ > MA200
  且 QQQ 60 日收益 > 0

Neutral:
  QQQ > MA200 但 60 日收益 <= 0
  或 QQQ < MA200 但 20 日收益 > 0

Risk-off:
  QQQ < MA200
  且 QQQ 60 日收益 < 0
```

### 5.2 波动与回撤条件

趋势信号可能滞后，因此需要叠加风险条件：

- QQQ 20 日波动率；
- QQQ 当前回撤；
- QQQ 是否触发 10%-15% trailing stop；
- QQQ 是否出现短期急跌。

第一版建议：

```text
若 QQQ 从近 60 日高点回撤超过 12%，至少降为 Neutral。
若 QQQ 从近 60 日高点回撤超过 18%，降为 Risk-off。
若 QQQ 20 日波动率显著高于过去 252 日中位数，成长仓降权。
```

### 5.3 状态切换防抖

为了避免频繁切换，需要加入 hysteresis：

- Risk-off 回到 Neutral 需要连续 2 个调仓周期满足修复条件；
- Neutral 回到 Risk-on 需要 QQQ 重新站上 MA100 或 MA200；
- 每次状态变化后的最短持有周期为 2 周；
- 状态变化优先于单周排名变化。

## 6. 增强仓选择逻辑

成长增强仓不使用全市场 TopK，而只在成长子池里做选择。

候选：

```text
QQQ / XLK / SMH / VGT / IGV
```

第一版排序因子：

- `momentum_reg_20`
- 60 日收益；
- 120 日收益；
- 波动惩罚后的动量；
- 相对 QQQ 强度。

建议先做两种简单版本：

### 6.1 QQQ 固定主仓版

```text
Risk-on:
  70% QQQ
  30% 成长子池第一名

Neutral:
  50% QQQ
  25% GLD
  25% IEF

Risk-off:
  25% QQQ
  35% GLD
  25% IEF
  15% UUP/cash
```

优点：

- 解释性强；
- 不容易错过 QQQ 主升浪；
- 适合作为第一条基线。

缺点：

- 如果 QQQ 本身进入长期弱势，仍保留一定权益暴露。

### 6.2 成长篮子动态版

```text
Risk-on:
  50% QQQ
  25% 成长子池第一名
  25% 成长子池第二名

Neutral:
  40% QQQ
  20% 成长子池第一名
  20% GLD
  20% IEF

Risk-off:
  20% QQQ
  30% GLD
  30% IEF
  20% UUP/cash
```

优点：

- 更像 QQQ 增强；
- 能在 SMH/XLK 明显强于 QQQ 时提高进攻性。

缺点：

- 对择时和相对强弱排序更敏感；
- 可能出现 n8 在 2023 的 SMH 踩节奏问题。

## 7. 实验阶段

### 7.1 阶段 0：数据准备

确认以下 ETF 的 10 年数据和核心因子覆盖：

```text
QQQ, XLK, SMH, GLD, IEF, TLT, UUP, VTI
可选：VGT, IGV, SPY
```

检查：

- `market.daily` 覆盖 2016-07-13 至最新交易日；
- `momentum_reg_20` 覆盖完整；
- 均线、回撤、波动率可从日线直接计算；
- 若 `SPY / VGT / IGV` 缺数据，先不纳入第一版。

### 7.2 阶段 1：基线对照

先建立统一 benchmark：

```text
QQQ buy-and-hold
VTI buy-and-hold
当前 Best 8/4
当前 n8 phase1_n08_002
当前 n12 phase1_n12_041
```

统一输出：

- 年化收益；
- Sharpe；
- 最大回撤；
- Calmar；
- 2022 收益；
- 2023 收益；
- 2022-2023 合计收益；
- 周期平滑度；
- 最差 6 周、12 周收益；
- 回撤恢复时间。

### 7.3 阶段 2：状态识别实验

只测试 QQQ 风险状态，不做复杂资产选择。

候选状态规则：

```text
A: MA200 + 60d momentum
B: MA100 + 60d momentum
C: MA200 + drawdown trigger
D: MA100/MA200 双均线 + hysteresis
```

每个状态规则先输出：

- 每年 Risk-on / Neutral / Risk-off 天数；
- 2022 是否及时降风险；
- 2023 是否及时回 Risk-on；
- 状态切换次数；
- 最长 Risk-off 持续时间；
- 错过 QQQ 强反弹的窗口。

这一阶段先不追求收益，只验证状态机是否符合直觉。

### 7.4 阶段 3：固定 QQQ 主仓实验

测试 `6.1 QQQ 固定主仓版`。

参数网格：

```text
Risk-on QQQ weight: 60%, 70%, 80%
Neutral QQQ weight: 40%, 50%, 60%
Risk-off QQQ weight: 0%, 15%, 25%
防守资产组合: GLD/IEF, GLD/IEF/UUP, GLD/TLT/UUP
```

目标：

- 年化收益尽量接近 QQQ；
- 最大回撤明显小于 QQQ；
- 2023 不能严重错过反弹；
- 2022 应明显优于 QQQ。

### 7.5 阶段 4：成长增强仓实验

测试 `6.2 成长篮子动态版`。

候选成长子池：

```text
Set A: QQQ, XLK, SMH
Set B: QQQ, XLK, SMH, VGT
Set C: QQQ, XLK, SMH, VGT, IGV
```

排序口径：

```text
momentum_reg_20
60d return
120d return
vol-adjusted momentum
relative strength vs QQQ
```

重点检查：

- 是否真的提高 Risk-on 阶段收益；
- 是否增加 2022/2025 类回撤；
- 是否再次出现 SMH 有大涨但策略实际亏损的节奏问题；
- Top1/Top2 成长资产贡献是否过度集中。

### 7.6 阶段 5：风控与防抖

在阶段 3/4 中选出 3-5 个候选方案后，再做风控细化：

- 状态切换最短持有周期：1/2/4 周；
- Risk-off 恢复确认周期：1/2/3 周；
- trailing stop：10%/12%/15%/18%；
- 波动降权阈值；
- 是否允许现金。

重点不只是降低最大回撤，而是避免过度交易和反复打脸。

### 7.7 阶段 6：压力测试

至少拆分以下阶段：

```text
2018 Q4 加息杀估值
2020 疫情急跌与反弹
2021 成长牛市尾部
2022 通胀加息熊市
2023 AI/纳指强反弹
2025-2026 最新样本
```

每个阶段输出：

- 策略收益；
- QQQ 收益；
- 超额收益；
- 最大回撤；
- 状态分布；
- 主要资产贡献。

## 8. 评价指标

### 8.1 主指标

新策略不能只看 Sharpe，应使用以下指标共同筛选：

- 相对 QQQ 的年化超额收益；
- 相对 QQQ 的最大回撤改善；
- 相对 QQQ 的 Calmar 改善；
- 2022 防守收益；
- 2023 修复参与度；
- 10 年总收益；
- 周期收益标准差；
- 最差 6 周、12 周收益；
- 最长连续亏损周期；
- top1/top3 资产贡献集中度。

### 8.2 建议准入线

第一版候选策略至少需要满足：

```text
10 年总收益 >= VTI
10 年最大回撤 <= QQQ 最大回撤的 70%
2022 收益显著好于 QQQ
2023 收益至少达到 QQQ 的 50%
年化收益不低于 QQQ 的 70%
Sharpe 高于 QQQ
```

如果无法达到这些条件，说明该方向还不是合格的 QQQ 增强，只能算防守型权益策略。

### 8.3 2023 修复参与度

建议新增专门指标：

```text
rebound_capture_2023 = strategy_return_2023 / QQQ_return_2023
rebound_capture_2023_q1q3 = strategy_return_2023_q1q3 / QQQ_return_2023_q1q3
```

当前 Best：

```text
2023 capture = 10.83% / 54.84% = 19.75%
2023 Q1-Q3 capture = 4.58% / 35.46% = 12.92%
```

这个捕获率太低，是新策略必须重点修复的问题。

## 9. 实现建议

### 9.1 不直接复用 ETF Rotation Strategy

当前 `BaseETFUniverseRotationStrategy` 的核心假设是：

```text
截面排序
TopK 入选
tag 限制
等权
```

这与 QQQ Enhanced 的核心假设冲突。建议新增独立策略类，而不是在当前类里继续加参数。

建议文件：

```text
app/strategy/qqq_enhanced.py
app/cli/backtest_qqq_enhanced.py
scripts/qqq_enhanced_experiment.py
```

### 9.2 数据与信号

第一版可以不依赖已有 signal profile，直接从日线计算：

- MA100；
- MA200；
- 20/60/120 日收益；
- 20 日波动率；
- 60 日回撤；
- 相对 QQQ 强度。

好处：

- 策略逻辑更透明；
- 不受现有 `momentum_reg_20` 排名框架约束；
- 更容易解释状态切换。

后续再考虑与 `signal_profiles` 合并。

### 9.3 回测输出

必须复用当前通用回测报告中的结构指标：

- `summary.json`
- 周期平滑度与集中度；
- 资产贡献集中度；
- top/worst periods；
- top/worst symbols。

额外新增：

- regime 日历；
- regime 切换记录；
- 每个 regime 的收益贡献；
- QQQ rebound capture；
- QQQ downside protection ratio。

## 10. 第一版实验矩阵

建议第一轮只跑 12-24 组，不要一开始网格爆炸。

### 10.1 状态规则

```text
R1 = MA200 + 60d momentum
R2 = MA100 + 60d momentum
R3 = MA200 + 60d momentum + 12%/18% drawdown trigger
```

### 10.2 权重模板

```text
W1:
  Risk-on  = 70% QQQ + 30% best_growth
  Neutral  = 50% QQQ + 25% GLD + 25% IEF
  Risk-off = 25% QQQ + 35% GLD + 25% IEF + 15% UUP

W2:
  Risk-on  = 80% QQQ + 20% best_growth
  Neutral  = 60% QQQ + 20% GLD + 20% IEF
  Risk-off = 15% QQQ + 40% GLD + 30% IEF + 15% UUP

W3:
  Risk-on  = 50% QQQ + 25% best_growth_1 + 25% best_growth_2
  Neutral  = 40% QQQ + 20% best_growth_1 + 20% GLD + 20% IEF
  Risk-off = 20% QQQ + 30% GLD + 30% IEF + 20% UUP
```

### 10.3 成长子池

```text
G1 = QQQ, XLK, SMH
G2 = QQQ, XLK, SMH, VGT, IGV
```

如果 `VGT / IGV` 数据不足，第一轮只使用 `G1`。

## 11. 预期结果与判断

理想策略应该呈现：

- 2022 显著少亏；
- 2023 能捕获至少一半以上 QQQ 反弹；
- 10 年收益明显高于 VTI；
- 最大回撤显著低于 QQQ；
- 曲线不应像当前轮动策略一样在成长牛市里过度钝化。

如果第一轮结果显示：

- 2022 防守很好，但 2023 仍低捕获；
- 或 2023 捕获很好，但 2022 回撤接近 QQQ；

则需要继续调整状态恢复规则和 Risk-off 权重，而不是简单换更多 ETF。

## 12. 当前最高优先级

下一步建议按以下顺序执行：

1. 建立 QQQ/VTI buy-and-hold 的标准 backtest 报告，补齐 Sharpe、最大回撤、周期结构。
2. 新增 `qqq_enhanced` 第一版实验脚本，不接入生产自动化。
3. 先跑 `R1/R2/R3 x W1/W2/W3 x G1` 的 9 组实验。
4. 对比 QQQ、VTI、Best 8/4、n8、n12。
5. 重点检查 2022 防守和 2023 rebound capture。

第一轮完成后，再决定是否把最优方案正式实现为 CLI 和策略类。
