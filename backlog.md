# Backlog

Ideas parked deliberately. Nothing here starts before Milestone 1 finishes its
7-day zero-unexplained-gaps soak (see CLAUDE.md).

## Weather markets as a third research track (Kalshi + Polymarket)

Origin: Instagram-promoted repo `suislanchez/polymarket-kalshi-weather-bot`
(reviewed 2026-08-09). The repo itself is a paper-trading simulator — read-only
Kalshi client, virtual bankroll, the advertised "$1.8k profit" is simulated —
and its BTC 5-min RSI/momentum strategy is exactly the chart-pattern category
this project excludes. Not adopted.

The salvageable hypothesis is real, though:

> Fraction of GFS ensemble members above a temperature threshold (Open-Meteo,
> 31 members) is a better probability estimate than the Kalshi/Polymarket
> market price, net of fees, **after per-station bias correction** against the
> official settlement source (NWS Daily Climate Report).

Why it's plausible: weather is one of the few prediction-market niches with a
documented systematic edge (NWP ensembles vs. casual traders), markets settle
on an objective source, and the data is free.

What doing it properly requires (in our pattern, not theirs):
1. Collector: archive ensemble forecasts (per city, per run) + market prices
   (Kalshi KXHIGH series, Polymarket temp markets) + NWS settlement values.
   Months of history before any conclusion.
2. Per-station bias correction of raw GFS 2m temp vs. the official station
   reading — this is where the actual edge work lives; raw member counts are
   miscalibrated.
3. Backtest net of fees (Kalshi ~1.2%, Polymarket ~0.1%) and realistic fill
   assumptions before any live consideration.

Useful references from the reviewed repo: Kalshi API auth (RSA request
signing) in `backend/data/kalshi_client.py`; its `RESEARCH.md` platform/fee
comparison; Open-Meteo ensemble endpoint usage in `backend/data/weather.py`.

## Benchmark discipline for the Milestone 2 backtest library (committed requirement)

Requested 2026-08-10: every strategy result must answer "why not just buy the
benchmark?" before it can be considered for capital. Not a nice-to-have — a
hard gate in the backtest report format.

Every backtest reports, net of realistic fees + slippage:

1. **Benchmarks**: S&P 500 total return (SPY) for the opportunity-cost
   question, AND buy-and-hold BTC (and ETH) for crypto strategies — for a
   crypto strat the damning comparison is usually holding BTC, not SPY.
2. **Risk-adjusted, not raw**: Sharpe, Sortino, max drawdown, and a
   vol-matched comparison (scale strategy exposure to benchmark volatility
   before comparing returns — raw return comparisons flatter leveraged
   strategies).
3. **Multiple-testing honesty**: we will test many hypotheses; the best
   backtest's Sharpe is inflated by selection. Report deflated Sharpe
   (Bailey & López de Prado) or an equivalent reality check. This is the
   discipline that separates us from prompt-evolution hype (see atlas-gic
   review, 2026-08-10: 54 mutations, keep-the-lucky-ones, −5.9% result).
4. **The verdict line**: each report ends with an explicit call — "this
   beats/loses to its benchmarks risk-adjusted." If nothing beats SPY/BTC
   buy-and-hold, the correct trade IS the benchmark, and the research
   still succeeded by preventing losses.

Benchmark data: daily SPY/BTC closes suffice (free sources fine at daily
granularity); no new collectors needed — pull at research time in notebooks.

Later, if/when live: daily equity snapshots vs benchmarks on the dashboard
(the M1 "no frontend" rule still applies until then).
