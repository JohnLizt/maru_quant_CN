你这个版本我不建议马上去“堆更多因子”。现在最有价值的是先搞清楚：**这 8 个 ETF + 21 日回归动量 + Top4 等权，到底靠什么赚钱，在哪些环境失效。**

你的当前策略可以抽象成：

\[
\text{Fixed Universe}
\rightarrow
\text{21d Trend Strength}
\rightarrow
\text{Top 4}
\rightarrow
25\%\text{ Equal Weight}
\]

其中你的指标

\[
Momentum = (e^{250\beta}-1)\times R^2
\]

本质上同时奖励：

- 趋势斜率高；
- 趋势平滑。

这个思路本身没问题，但 **21 天其实非常短**。对于 ETF/TAA 来说，它更接近“短期 relative strength rotation”，而经典美股 Tactical Asset Allocation 往往使用几个月到 12 个月的趋势/动量，并强调参数在一段范围内都应该有效，而不是某个精确窗口特别好。Faber 的经典框架就强调 6–14 个月移动平均都有相当的稳定性，而且趋势过滤的主要作用往往是降低波动和回撤，不一定是单纯提高 CAGR。

我建议下面按优先级研究。

## 第一优先：先做参数稳定性，不要调最优参数

你现在第一个应该跑的是：

\[
Lookback=
21,\ 42,\ 63,\ 126,\ 189,\ 252
\]

或者：

- 1个月
- 2个月
- 3个月
- 6个月
- 9个月
- 12个月

但研究目标**不是找 Sharpe 最高的窗口**。

而是画：

\[
Lookback \rightarrow CAGR
\]

\[
Lookback \rightarrow Sharpe
\]

\[
Lookback \rightarrow MaxDD
\]

如果结果是：

| Lookback | Sharpe |
|---:|---:|
| 21 | 1.60 |
| 42 | 1.51 |
| 63 | 1.45 |
| 126 | 1.38 |
| 252 | 1.31 |

这是非常健康的。

但如果：

| Lookback | Sharpe |
|---:|---:|
| 15 | 0.7 |
| 18 | 0.9 |
| **21** | **1.6** |
| 24 | 0.8 |
| 30 | 0.6 |

那我反而会非常担心：

> **21 日可能只是历史拟合出来的 lucky parameter。**

经典 TAA 文献非常重视这种 parameter stability。

---

# 第二优先：把你的 `R²` 拆开研究

你现在：

\[
Score=AnnualizedSlope\times R^2
\]

这里其实隐含了一个很强的假设：

> 平滑上涨 > 剧烈上涨。

这个假设非常值得验证。

至少做三个 benchmark：

### A. 普通收益动量

\[
M_1=\frac{P_t}{P_{t-21}}-1
\]

### B. Regression slope

\[
M_2=e^{250\beta}-1
\] 

### C. 你的 slope × R²

\[
M_3=(e^{250\beta}-1)R^2
\]

然后不要只比较最终 Sharpe。

还要比较：

- Top ETF 后续 1M return；
- 排名稳定性；
- turnover；
- crash period；
- 每个 ETF 的选中频率。

有可能你最后发现：

\[
R^2
\]

真正贡献的不是 alpha，而是：

> **降低换手 + 排除短期 spike。**

这也是有价值的。

---

# 第三优先：21 日和更长期动量结合

这是我觉得最值得实验的一项。

你现在只有：

\[
1M Momentum
\]

它很容易受到：

- 单月反弹；
- FOMC；
- CPI；
- earnings season；
- oversold rebound

影响。

可以构造：

\[
M=
w_1M_{21}
+
w_2M_{63}
+
w_3M_{126}
\]

第一版甚至不用优化权重：

\[
M=\frac{M_{21}+M_{63}+M_{126}}3
\]

或者经典一点：

\[
M=
\frac{R_{3M}+R_{6M}+R_{12M}}3
\]

Faber 的 relative-strength 框架本身就测试过 3、6、12 月混合排名，而不是只依赖单一短期窗口。

我会特别测试：

**短期动量有没有增量信息。**

例如：

\[
LongMomentum=M_{126}
\]

\[
ShortMomentum=M_{21}
\]

然后看：

> 长期趋势为正的资产里，短期 momentum 是否还具有预测力？

这比直接混在一起更有研究价值。

---

# 第四优先：给 Relative Momentum 加 Absolute Momentum Filter

我认为这是你当前策略**最自然的升级**。

你的策略目前有一个潜在问题：

假设八只 ETF 全都跌：

| ETF | momentum |
|---|---:|
| GLD | -2 |
| IEF | -3 |
| XLV | -5 |
| UUP | -6 |
| XLF | -10 |
| ... | ... |

你依然会：

> Top 4 满仓。

这就是只有 **relative momentum** 的问题。

可以加：

\[
AbsoluteMomentum_i > 0
\]

或者：

\[
P_i>MA_{200}
\]

才有资格持有。

不足四只的时候：

\[
Remaining\ Weight\rightarrow Cash/SGOV
\]

例如：

- 4 个合格 → 各 25%
- 3 个合格 → 75% risk assets + 25% cash
- 1 个合格 → 25% + 75% cash
- 0 个 → 100% cash

这和 Dual Momentum / Faber trend filter 的核心思想非常接近。Faber 的经典方法就是对各资产独立做长期趋势过滤，低于趋势时转到现金/T-bills。

我很看好这个实验。

---

# 第五优先：Top 4 到底是不是合理？

你现在：

\[
8\rightarrow4
\]

等于保留前 50%。

需要分别测试：

\[
Top1,\ Top2,\ Top3,\ Top4,\ Top5
\]

但同样，不要为了找最高 Sharpe。

你真正想观察的是：

### Concentration / diversification curve

通常：

Top1：

\[
Return\uparrow,\ Volatility\uparrow
\]

Top8：

\[
Return\rightarrow Universe\ Average
\]

中间应该存在一个比较稳定的区域。

如果 Top3、Top4、Top5 都差不多，我会选择：

> **Top4**

因为更加稳健。

如果只有 Top4 特别好，也是红旗。

---

# 第六优先：我觉得你当前最大的结构问题其实是 ETF Pool

你的 8 只：

- EEM
- VNQ
- GLD
- IEF
- UUP
- XLF
- XLK
- XLV

乍看很分散。

但实际上有一个问题：

### Equity bucket

有：

- EEM
- VNQ
- XLF
- XLK
- XLV

5/8 都有比较强的权益属性。

真正明显异质：

- GLD
- IEF
- UUP

所以 Top4 很容易出现：

\[
XLK+XLF+XLV+VNQ
\]

你看起来持有四个 ETF，

实际上可能：

\[
80\%+
\]

风险都来自 equity beta。

因此我强烈建议下一阶段计算：

\[
Rolling\ Correlation
\]

和：

\[
Portfolio\ Effective\ Number\ of\ Bets
\]

至少先看：

持仓之间 63D / 126D correlation。

例如 Top4：

\[
XLK,\ XLF,\ XLV,\ VNQ
\]

如果平均 correlation：

\[
0.7
\]

那么：

> “25% 等权”并不等于风险等权。

---

# 第七优先：Equal Weight → Inverse Volatility

这是我认为成本非常低、但价值很高的一项。

当前：

\[
w_i=25\%
\]

可以改成：

\[
w_i
=
\frac{1/\sigma_i}
{\sum_j1/\sigma_j}
\]

例如：

| ETF | Vol | Equal weight | Inv-vol |
|---|---:|---:|---:|
| XLK | 25% | 25% | 15% |
| EEM | 20% | 25% | 18% |
| GLD | 14% | 25% | 26% |
| IEF | 9% | 25% | 41% |

实际当然可以设：

\[
w_i\le35\%
\]

防止 IEF 权重过大。

目的不是提高收益，而是看看：

> **Sharpe / DD 能不能变得更稳定。**

---

# 第八优先：Volatility Targeting

再进一步：

不是让 Portfolio 永远 100% gross exposure。

而是：

\[
Exposure=
\frac{\sigma_{target}}
{\sigma_{portfolio}}
\]

例如目标：

\[
10\%\ annualized\ vol
\]

当市场剧烈波动：

\[
PortfolioVol=20\%
\]

那么：

\[
Exposure=50\%
\]

剩余放现金。

这实际上比复杂的 stop-loss 更容易研究。

---

# 第九优先：研究调仓频率

你的信号是 21 日，却没有说具体调仓频率。

这会极其重要。

建议测试：

- Daily
- Weekly
- Biweekly
- Monthly

我预计：

**daily 很可能过于敏感。**

因为一个月 momentum 每日更新会导致大量 rank flipping。

ETF/TAA 的经典框架非常偏向**月度调仓**，主要目的就是降低噪声和换手；Faber 原始 TAA 模型就只在月末更新。

但你的 21D signal 较短，因此 weekly 很可能也是合理 candidate。

---

# 第十优先：Rank Buffer / Hysteresis

这是非常实用的工程改进。

现在：

昨天：

\[
ETF_A Rank=4
\]

今天：

\[
ETF_A Rank=5
\]

你可能：

卖 A

买 B。

第二天：

A又Rank4。

就来回交易。

可以改成：

> 新资产必须进入 Top3 才买；已有资产跌出 Top5 才卖。

叫：

**buffer / hysteresis**。

例如：

\[
EntryRank\le4
\]

\[
ExitRank>5
\]

可以显著降低：

\[
Turnover
\]

而不一定明显降低 alpha。

---

# 然后才到 Market Regime

我反而把它排到后面。

因为目前如果 baseline 没研究透，加入 Regime 很容易过拟合。

等上面完成以后，我建议从最简单的 regime 开始：

\[
SPY>MA200
\]

vs

\[
SPY<MA200
\]

然后分别统计你的策略：

- CAGR
- Sharpe
- Win Rate
- DD
- turnover

看看：

> 你的策略究竟是不是只在 risk-on 时有效。

第二版再加入：

\[
VIX
\]

或：

\[
HYG/IEF
\]

信用状态。

不要上来就 HMM。

---

# 一个我非常建议你做的研究：ETF contribution decomposition

因为你只有 8 只，完全可以把策略拆开。

统计每个 ETF：

| ETF | 被选次数 | 平均持有期 | Contribution | Sharpe贡献 | Worst DD contribution |
|---|---:|---:|---:|---:|---:|
| XLK | | | | | |
| XLF | | | | | |
| XLV | | | | | |
| EEM | | | | | |
| VNQ | | | | | |
| GLD | | | | | |
| IEF | | | | | |
| UUP | | | | | |

我特别想看两个东西：

### ① UUP 是否真的创造 alpha

还是：

只在某一两年救过组合。

### ② XLK 是否贡献了绝大部分收益

如果：

\[
Strategy\ Alpha
\approx
XLK\ Timing
\]

那你的策略表面是 diversified ETF rotation，

实际上是：

> **tech timing strategy。**

这个结论非常重要。

---

# 另外，一定做 Leave-One-Out Test

非常适合你这个 8 ETF pool。

每次删除一个：

\[
Pool-\{XLK\}
\]

\[
Pool-\{GLD\}
\]

……

然后重新跑。

如果删除某一个 ETF 后：

\[
Sharpe:1.6\rightarrow0.8
\]

说明策略高度依赖该标的。

这属于：

**Universe fragility。**

如果：

删掉任意一个：

\[
Sharpe仍在1.3\sim1.6
\]

那我会明显更有信心。

---

# 你的研究顺序，我会这样排

不要同时改很多东西，否则你不知道 performance improvement 来源。

### Phase 1：验证现有 signal

1. 21 / 42 / 63 / 126 / 252D parameter stability
2. Raw return vs slope vs slope×R²
3. Top1~Top5
4. Weekly vs Monthly rebalance
5. ETF contribution
6. Leave-one-out

做到这里，先回答：

> **你的 alpha 是否真实、稳定、可解释？**

### Phase 2：改善风险

7. Absolute momentum / MA200 filter
8. Cash asset
9. Inverse vol weighting
10. Volatility targeting
11. Rank buffer

回答：

> **能否减少 DD，而不是继续挖收益？**

### Phase 3：改善 signal

12. 1M + 3M + 6M momentum
13. Short/Long momentum interaction
14. Momentum acceleration
15. Trend + momentum combination

### Phase 4：Market Regime

16. SPY MA200
17. VIX state
18. HYG/IEF credit state
19. USD/rate regime

最后才考虑：

- HMM
- GMM
- ML regime classifier

---

## 如果只能让我从这里挑三个马上做

我会选：

**① Lookback stability**

因为先确认你的 21D 是否偶然。

**② Absolute Momentum Filter**

因为这是你从 A 股式 Top-K rotation 转向美股 TAA 最核心的一步。

**③ Portfolio exposure / correlation analysis**

因为你现在最大的隐性风险可能不是 signal，而是：

\[
Top4\neq4\ independent\ bets
\]

---

我现在对你的初版最感兴趣的其实不是 Sharpe，而是一个结果：

> **如果把 21 日改成 63、126、252 日，你的 Sharpe 曲线是什么样？**

这个结果基本就能告诉我们，下一步应该继续研究 **短周期轮动**，还是转向 **中长期 TAA / trend-following**。