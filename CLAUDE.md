# Trading Research Platform

Dual-track market research system. **Research-first: no trading execution, no capital at risk until hypotheses are validated in backtests.**

## Tracks

1. **Crypto perps (Binance/Bybit):** collect trades, L2 orderbook snapshots, liquidations, funding rates, open interest. Research focus: liquidation cascades, funding-rate extremes, OI dynamics — NOT chart patterns.
2. **Polymarket wallet analysis:** poll Gamma/Data APIs (public, no auth) for markets, fills, wallet positions. Goal: skill-vs-luck classification of wallets (trade count, category specialization, timing vs. news, consistency). Copy trading itself is NOT the goal — understanding *why* wallets win is.

## Hard rules

- **No frontend until Milestone 1 is complete.** Dashboard-before-data is the failure mode we're avoiding.
- Data integrity beats features: every collector gap is a permanent hole in every future backtest.
- All strategy claims must be phrased as testable hypotheses and backtested before any live consideration.
- Never commit API keys. `.env` + docker secrets.

## Architecture

```
repo/
├── collectors/        # independent async services, one per source
│   ├── binance_ws.py  # trades, liquidations (forceOrder), funding, OI
│   ├── bybit_ws.py    # same, second venue for cross-validation
│   ├── orderbook.py   # L2 snapshots + deltas (BTC, ETH perps first)
│   └── polymarket.py  # REST poller: markets, fills, positions
├── storage/           # TimescaleDB schema, migrations, Parquet export
├── research/          # Jupyter notebooks + backtest lib (the real product)
├── api/               # FastAPI — build only when there's something to serve
├── docker-compose.yml # timescaledb + collectors
└── CLAUDE.md
```

## Stack decisions (settled — don't relitigate)

- Python 3.12, asyncio, `websockets`/`aiohttp`
- TimescaleDB (hypertables per data type; continuous aggregates for 1m/5m bars; retention/compression policies from day one)
- Parquet export job for research so notebooks never hammer the live DB
- Docker Compose; target deployment: Hostinger VPS (runs alongside existing Docker workloads)
- Frontend later: single React app, two views (Perps / Polymarket), same API. Streamlit acceptable as interim throwaway.

## Milestone 1 — acceptance criteria

- [ ] Binance + Bybit collectors: BTC & ETH perps — trades, liquidations, funding, OI, L2 snapshots (1s) flowing into TimescaleDB
- [ ] Polymarket poller: top-N active markets + fills, wallet position tracking
- [ ] Reconnect logic with gap detection + gap logging (know what you missed)
- [ ] Health monitoring: dead-collector alert within 5 min
- [ ] 7 consecutive days, zero unexplained gaps, on the VPS
- [ ] Parquet export job producing daily files

## Milestone 2 (after M1 only)

- Backtest lib + first hypotheses: (a) post-liquidation-cascade mean reversion, (b) funding-rate extreme as positioning signal, (c) Polymarket wallet skill score v1
- Evaluate everything net of realistic fees + slippage

## Context

- Owner: Franco — experienced with Odoo/Python integrations, APIs, Docker, Make/n8n. Explain trading/microstructure concepts when relevant; don't over-explain engineering.
- Origin: started from a scam-ad discussion; the standing agreement is honest skepticism — flag weak hypotheses and overfitting, no hype.
