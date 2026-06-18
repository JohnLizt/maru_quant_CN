# Momentum Factor Shortlist

本文整理一版适合当前 `maru_quant` 架构的动量因子清单，目标是为后续 ETF / 多资产轮动研究提供候选列表。重点放在：

- 容易落地
- 适合 `cross_sectional` 排名
- 与当前 `signal -> strategy -> backtest` 链路兼容

## 1. 研究目标

当前最值得优先研究的是：

- `cross-sectional momentum rank`
- 中期价格动量
- 趋势状态型动量
- 风险调整后的动量

如果目标是做 ETF rotation，而不是个股择时，那么动量因子更适合被设计成：

- 每日先对全池计算原始值
- 再做横截面归一化
- 最后进入 `SignalSnapshot` 排名体系

## 2. 最值得优先做的一组

建议第一批优先研究以下 6 个：

1. `ret_20_rank`
2. `ret_60_rank`
3. `ret_120_rank`
4. `ret_240_ex_20_rank`
5. `dist_to_52w_high_rank`
6. `vol_adjusted_ret_60_rank`

这 6 个已经足够搭一版比较完整的 ETF 动量框架。

## 3. 因子清单

### 3.1 Cross-Sectional Momentum Rank

这是当前最值得优先做的方向。

核心思想：

- 先计算每只资产过去 `N` 日收益
- 再在同一个交易日、同一个 universe 内做横截面排序
- 将排序结果映射成 `0~1` 或 `-1~1`

候选定义：

- `ret_20_rank`
  - 过去 20 个交易日收益的横截面排名
- `ret_60_rank`
  - 过去 60 个交易日收益的横截面排名
- `ret_120_rank`
  - 过去 120 个交易日收益的横截面排名
- `ret_240_rank`
  - 过去 240 个交易日收益的横截面排名

优点：

- 直接适配当前 `cross_sectional` signal 模式
- 和 ETF rotation 的“谁更强就排前面”逻辑天然一致
- 对不同价格尺度、不同绝对波动水平更鲁棒

风险：

- 容易把短期过热资产排得过高
- 在强反转阶段会失效

实现建议：

- 第一优先做 `ret_60_rank`
- 第二优先做 `ret_120_rank`
- `ret_20_rank` 可作为短周期补充

### 3.2 Skip-Period Momentum

这是经典动量里非常重要的一类。

核心思想：

- 看中长期收益
- 剔除最近一段短期反转噪音

候选定义：

- `ret_60_ex_5`
  - 过去 60 日收益，剔除最近 5 日
- `ret_120_ex_20`
  - 过去 120 日收益，剔除最近 20 日
- `ret_240_ex_20`
  - 过去 240 日收益，剔除最近 20 日
- `ret_240_ex_20_rank`
  - 上述值的横截面排名

说明：

- 经典股票动量常见的是 `12-1 momentum`
- 在日频 ETF 场景里，可以等价写成 `ret_240_ex_20`

优点：

- 比直接 `ret_240` 更能抑制短期反转
- 很适合中期轮动

建议：

- 这是 `cross-sectional momentum rank` 之后最值得做的一类

### 3.3 Absolute Price Momentum

这类先不做横截面排序，只看单资产绝对涨幅。

候选定义：

- `ret_20`
- `ret_60`
- `ret_120`
- `ret_240`

优点：

- 最简单
- 易解释
- 适合作为 baseline

缺点：

- 不同资产类别之间直接比较时，尺度不够统一

建议：

- 作为原始因子保留
- 在 signal 层更推荐用其横截面 rank 版本

### 3.4 Moving-Average Trend Momentum

这类是当前系统已经部分具备的方向。

候选定义：

- `price_to_ma20`
- `price_to_ma60`
- `price_to_ma120`
- `ma20_to_ma60`
- `ma20_to_ma120`
- `ma60_to_ma120`

说明：

- 当前系统已有：
  - `price_to_ma20`
  - `ma_cross`
- 后续可以扩成中周期版本

优点：

- 比简单累计收益更稳定
- 更容易刻画“趋势结构是否完整”

建议：

- 对 ETF rotation，`price_to_ma60` 值得优先补

### 3.5 52-Week High Proximity

候选定义：

- `dist_to_52w_high`
  - 当前价格距离过去 252 交易日高点的比例
- `dist_to_52w_high_rank`
  - 横截面排名版本

直觉：

- 越接近 52 周新高，通常说明趋势越强

优点：

- 很适合趋势轮动
- 可解释性强

风险：

- 容易在极端过热时追高

建议：

- 适合作为 ETF 专用 profile 的重要候选因子

### 3.6 Breakout / Channel Momentum

候选定义：

- `breakout_20`
  - 是否突破过去 20 日高点
- `breakout_60`
  - 是否突破过去 60 日高点
- `channel_pos_60`
  - 当前价格在过去 60 日价格区间中的位置

优点：

- 对趋势延续型行情敏感

缺点：

- 容易受短期噪音影响

建议：

- 可以第二阶段再做

### 3.7 RSI / Oscillator Momentum

候选定义：

- `rsi14`
- `rsi21`
- `rsi_ratio`
  - 不同 RSI 周期的组合

说明：

- 当前系统已有 `rsi14`
- 它本质上也是动量因子，只是更偏振荡器

建议：

- 继续保留
- 但不要把它当唯一动量来源

### 3.8 MACD-Type Momentum

候选定义：

- `macd_norm`
- `macd_hist_norm`
- `macd_signal_gap`

说明：

- 当前系统已有 `macd_norm`

建议：

- 作为辅助因子，而不是主因子

### 3.9 Risk-Adjusted Momentum

候选定义：

- `ret_60 / vol_60`
- `ret_120 / vol_120`
- `downside_adjusted_ret_60`
- `sharpe_like_60`

优点：

- 能抑制“涨得快但噪音大”的资产
- 对跨资产轮动尤其有价值

建议：

- 这是未来做“全球、全资产类别 ETF 轮动”时非常值得加的一类

### 3.10 Momentum Consistency

候选定义：

- `up_day_ratio_20`
  - 过去 20 日上涨天数占比
- `up_week_ratio_12`
  - 过去 12 周上涨周占比
- `positive_return_streak`
  - 连续正收益长度

优点：

- 能补充“趋势是否稳定”

建议：

- 第二阶段再做

## 4. 推荐优先级

### 第一批：最值得先做

1. `ret_60_rank`
2. `ret_120_rank`
3. `ret_240_ex_20_rank`
4. `price_to_ma60`
5. `dist_to_52w_high_rank`
6. `ret_60 / vol_60`

### 第二批：适合增强

1. `ret_20_rank`
2. `ma20_to_ma60`
3. `breakout_60`
4. `channel_pos_60`
5. `up_day_ratio_20`

### 第三批：后续再看

1. `ret_240_rank`
2. `downside_adjusted_ret_120`
3. `positive_return_streak`
4. 更细粒度的 MACD 衍生项

## 5. 推荐的研究顺序

建议按下面顺序推进：

1. 先实现原始收益型动量
   - `ret_20`
   - `ret_60`
   - `ret_120`
   - `ret_240_ex_20`

2. 再实现它们的横截面 rank 版本
   - `ret_20_rank`
   - `ret_60_rank`
   - `ret_120_rank`
   - `ret_240_ex_20_rank`

3. 再补趋势状态型
   - `price_to_ma60`
   - `dist_to_52w_high`

4. 最后补风险调整版
   - `ret_60 / vol_60`

## 6. 与当前系统的对接方式

### 6.1 Factor 层

新增因子可以先放在时间序列因子框架里，和现有：

- `price_to_ma20`
- `ma_cross`
- `rsi14`
- `macd_norm`

同一层生成。

### 6.2 Signal 层

优先把这些因子接进 `cross_sectional` profile，而不是直接单独做策略。

建议后续可以尝试：

- `trend_etf_v2`
  - 保留 `rsi14`
  - 弱化 `ma_cross`
  - 引入 `ret_60_rank`
  - 引入 `ret_240_ex_20_rank`
  - 引入 `dist_to_52w_high_rank`

### 6.3 Strategy 层

策略层不需要理解动量细节，只需要继续消费 `SignalSnapshot`。

也就是说：

- 因子研究发生在 factor + signal 层
- strategy 仍然只负责 `top_n`、`max_per_tag`、调仓频率

## 7. 当前最推荐的一条主线

如果只选一条最值得先做的主线：

1. 实现 `ret_60_rank`
2. 实现 `ret_120_rank`
3. 实现 `ret_240_ex_20_rank`
4. 跑 `factor_ic`
5. 再决定是否做 `trend_etf_v2`

原因：

- 这条线和 ETF 横截面轮动最匹配
- 研究价值最高
- 对现有系统改动最小

## 8. 一句话结论

当前最值得优先研究的动量因子，不是更复杂的振荡器，而是：

- **中期收益的横截面 rank**
- 尤其是 **`ret_60_rank`、`ret_120_rank`、`ret_240_ex_20_rank`**

这类因子最贴近 ETF rotation 的实际决策逻辑，也最适合接入现有的 `cross_sectional signal -> strategy -> backtest` 框架。
