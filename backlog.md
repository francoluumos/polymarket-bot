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
