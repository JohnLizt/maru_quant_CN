# Momentum Factor Research

## ETF RSI14 Experiment Notes

### Context

- Universe: `etf_CN`
- Strategy shell: `etf_rotation_v1`
- Rebalance: `weekly`, `rebalance_weekday=2` (Wednesday)
- Execution lag: `1`
- Portfolio construction: top `5`, `max_per_tag=1`
- Fees: `commission_bps=5`, `slippage_bps=5`

### Why RSI14 looked promising in research

`rsi14` has strong recent cross-sectional `RankIC`, especially on medium holding horizons:

- `lag=1`: `rank_ic_ir=0.1748`
- `lag=2`: `rank_ic_ir=0.2338`
- `lag=5`: `rank_ic_ir=0.3386`
- `lag=10`: `rank_ic_ir=0.4542`
- `lag=20`: `rank_ic_ir=0.6201`

Interpretation:

- `rsi14` is more like a short-to-medium trend persistence factor than a next-day timing factor
- The signal gets stronger as the forward window extends to `10-20` trading days

### Why strategy performance lagged factor effectiveness

There are several implementation-layer gaps between factor IC and live strategy behavior:

1. `RankIC` is computed on the full daily cross section, but the strategy only buys the top `5` names.
2. The strategy applies `max_per_tag=1`, so even a strong factor rank can be displaced by tag diversification.
3. Weekly Wednesday rebalance with `execution_lag=1` only samples one cross section per week, while the factor strength is measured on all daily cross sections.
4. For the original `trend_etf_rsi14` profile, `linear_clip(30, 80)` caused head saturation:
   many ETFs with `RSI >= 80` were collapsed to the same max score.

### Experiment Results

#### Baseline

- `trend_etf_v1` (`ret_30_rank`)
  - annualized return: `0.3346`
  - sharpe: `1.1278`
  - max drawdown: `-0.1973`
  - log: `logs/backtest/etf_rotation_20260618_220731/backtest.log`

#### RSI14 with 30/80 threshold clip

- `trend_etf_rsi14`
  - annualized return: `0.1784`
  - sharpe: `0.8707`
  - max drawdown: `-0.1703`
  - log: `logs/backtest/etf_rotation_20260618_220655/backtest.log`

Conclusion:

- Better than random-looking noise
- Clearly weaker than `ret_30_rank`
- Threshold clipping did not unlock enough edge

#### RSI14 raw cross-sectional rank

- `trend_etf_rsi14_raw`
  - annualized return: `0.1583`
  - sharpe: `0.7705`
  - max drawdown: `-0.1825`
  - log: `logs/backtest/etf_rotation_20260618_222430/backtest.log`

Conclusion:

- Removing the `30/80` threshold did not improve the ETF rotation outcome
- This suggests the main issue is not only score clipping
- The mapping from daily RSI cross-sectional strength to weekly top-5 rotation remains weak

### Current Hypothesis

The main remaining issue is likely signal noise at rebalance time:

- `rsi14` has useful information in the daily cross section
- But the strategy only observes one rebalance snapshot each week
- A single-day RSI ranking can be noisy for top-of-book portfolio selection

### New Experiment: smoothed RSI14 score

Profile:

- `trend_etf_rsi14_raw_smooth`
- factor: `rsi14`
- normalization: `rank_to_unit`
- smoothing: `5`-day rolling mean on `rsi14_score`

Design rationale:

- keep the factor raw
- keep cross-sectional ranking logic aligned with `RankIC`
- smooth the per-symbol score over recent days before weekly ranking
- reduce one-day noise without changing the weekly rebalance rule

### Smoothed RSI14 Result

- `trend_etf_rsi14_raw_smooth`
  - annualized return: `0.2277`
  - sharpe: `1.1139`
  - max drawdown: `-0.1203`
  - calmar: `1.8930`
  - log: `logs/backtest/etf_rotation_20260618_222954/backtest.log`

Comparison:

- vs `trend_etf_rsi14`
  - annualized return: `0.1784 -> 0.2277`
  - sharpe: `0.8707 -> 1.1139`
  - max drawdown: `-0.1703 -> -0.1203`

- vs `trend_etf_rsi14_raw`
  - annualized return: `0.1583 -> 0.2277`
  - sharpe: `0.7705 -> 1.1139`
  - max drawdown: `-0.1825 -> -0.1203`

Interpretation:

- Smoothing helps materially
- The earlier weak strategy result was not only a factor-quality issue
- A meaningful part of the problem was rebalance-date noise from using a single-day cross section

### Current Conclusion

For ETF rotation, `rsi14` is not strong enough as a direct unsmoothed weekly top-5 selector.
But after smoothing the cross-sectional score over `5` days, the strategy behavior improves a lot:

- still below `trend_etf_v1` on annualized return
- roughly catches up on Sharpe
- significantly better on max drawdown

Working conclusion:

- yes, for this setup, smoothing is worth doing
- the better place to smooth is the signal score layer, not the rebalance schedule
- next iteration should test `ret_30_rank + smoothed_rsi14` rather than replacing `ret_30_rank` outright
