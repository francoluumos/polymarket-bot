# Trading Research Platform

Market-data collection + research infrastructure. See [CLAUDE.md](CLAUDE.md) for the full project charter, hard rules, and milestones.

**Current state:** TimescaleDB schema + Binance perps collector (trades, liquidations, funding/mark price, open interest). Bybit, orderbook, and Polymarket collectors are next.

## Quickstart

```bash
cp .env.example .env      # then set a real POSTGRES_PASSWORD
docker compose up -d --build
```

That starts TimescaleDB, applies migrations (`migrate` runs once and exits), and launches the Binance collector.

## Verify data is flowing

```bash
docker compose logs -f binance-collector

docker compose exec timescaledb psql -U market -d marketdata -c \
  "SELECT venue, symbol, count(*), max(ts) FROM trades GROUP BY 1, 2"
```

Collector health and known data holes:

```sql
SELECT * FROM collector_heartbeats;   -- last_seen should be < 30s old
SELECT * FROM dead_collectors;        -- anything silent > 5 min
SELECT * FROM data_gaps ORDER BY gap_start DESC LIMIT 20;
```

Every disconnect, dropped-trade sequence jump, and collector restart is written to `data_gaps`. The milestone rule is *no unexplained gaps* — a gap that isn't in that table is a bug.

## Schema

Migrations live in `storage/migrations/` and are applied in filename order by `storage/migrate.sh` (tracked in `schema_migrations`). Add a new numbered `.sql` file and re-run the `migrate` service to change the schema — never edit applied migrations.

Hypertables: `trades`, `liquidations`, `funding`, `open_interest`, `orderbook_snapshots`, `pm_fills`, `pm_positions`. Continuous aggregates `trades_1m` / `trades_5m` (OHLCV + buy/sell taker volume split) are what research code should read — never the raw tick table.
