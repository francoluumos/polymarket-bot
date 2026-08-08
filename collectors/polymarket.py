"""Polymarket REST poller: markets, fills, wallet position snapshots.

Public APIs, no auth:
  - Gamma  /markets  -> pm_markets upserts (top-N active by 24h volume)
  - Data   /trades   -> pm_fills. The global taker feed covers ALL markets in
                        one call, which is what wallet skill scoring needs —
                        wallets trade across markets, and a top-N-only view
                        would bias the sample toward whales in big markets.
  - Data   /positions -> pm_positions snapshots for an explicit watchlist
                        (WATCH_WALLETS env; grows as we identify candidates).

Gap semantics for a poller: a failed cycle isn't automatically a data hole
(the next /trades page overlaps), so gaps are recorded only when consecutive
failures exceed the point where the fills feed could have rolled past us.
"""

import asyncio
import json
from datetime import datetime, timezone

import aiohttp

from collectors.common import config
from collectors.common.db import BatchWriter, connect_pool, heartbeat_loop, record_gap

log = config.setup_logging("polymarket")

VENUE = "polymarket"
GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
FILLS_PAGE = 500
# After this many consecutive failed fills polls, assume the feed rolled past
# what we'd seen and record a gap.
FILLS_FAIL_GAP_THRESHOLD = 6

SQL_MARKET = """
    INSERT INTO pm_markets (market_id, question, category, end_date, active, liquidity, volume, raw, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
    ON CONFLICT (market_id) DO UPDATE SET
        question = EXCLUDED.question, category = EXCLUDED.category,
        end_date = EXCLUDED.end_date, active = EXCLUDED.active,
        liquidity = EXCLUDED.liquidity, volume = EXCLUDED.volume,
        raw = EXCLUDED.raw, updated_at = now()
"""
SQL_FILL = """
    INSERT INTO pm_fills (ts, market_id, asset_id, maker, taker, side, price, size, tx_hash)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT DO NOTHING
"""
SQL_POSITION = """
    INSERT INTO pm_positions (ts, wallet, market_id, outcome, size, avg_price)
    VALUES ($1, $2, $3, $4, $5, $6)
"""


def _f(v) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _iso(v) -> datetime | None:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None


class PolymarketPoller:
    def __init__(self, pool, writer: BatchWriter, session: aiohttp.ClientSession):
        self.pool = pool
        self.writer = writer
        self.session = session
        self.top_n = int(config.env("PM_TOP_MARKETS", "100"))
        self.fills_interval = float(config.env("PM_FILLS_INTERVAL", "10"))
        self.markets_interval = float(config.env("PM_MARKETS_INTERVAL", "300"))
        self.positions_interval = float(config.env("PM_POSITIONS_INTERVAL", "300"))
        self.watch_wallets = [
            w.strip().lower() for w in config.env("WATCH_WALLETS", "").split(",") if w.strip()
        ]
        self.poll_count = 0
        self.fill_fail_streak = 0
        self.fills_ok_at: datetime | None = None

    async def _get(self, url: str, params: dict) -> list | dict:
        async with self.session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def poll_markets_loop(self) -> None:
        while True:
            try:
                markets = await self._get(f"{GAMMA}/markets", {
                    "active": "true", "closed": "false",
                    "order": "volume24hr", "ascending": "false", "limit": self.top_n,
                })
                async with self.pool.acquire() as conn:
                    for m in markets:
                        market_id = m.get("conditionId") or f"gamma:{m['id']}"
                        await conn.execute(
                            SQL_MARKET, market_id, m.get("question"), m.get("category"),
                            _iso(m.get("endDate")), m.get("active"),
                            _f(m.get("liquidity")), _f(m.get("volume")), json.dumps(m),
                        )
                self.poll_count += 1
                log.info("markets upserted: %d", len(markets))
            except Exception as e:
                log.warning("markets poll failed: %s", e)
            await asyncio.sleep(self.markets_interval)

    async def poll_fills_loop(self) -> None:
        while True:
            try:
                trades = await self._get(f"{DATA}/trades", {
                    "limit": FILLS_PAGE, "takerOnly": "true",
                })
                for t in trades:
                    self.writer.add(SQL_FILL, (
                        datetime.fromtimestamp(t["timestamp"], tz=timezone.utc),
                        t.get("conditionId"), t.get("asset"), None,
                        (t.get("proxyWallet") or "").lower(), t.get("side"),
                        float(t["price"]), float(t["size"]), t.get("transactionHash"),
                    ))
                self.poll_count += 1
                if self.fill_fail_streak >= FILLS_FAIL_GAP_THRESHOLD and self.fills_ok_at:
                    await record_gap(
                        self.pool, VENUE, "fills", None,
                        self.fills_ok_at, datetime.now(timezone.utc),
                        f"fills feed unreachable for {self.fill_fail_streak} polls; "
                        "page may have rolled past",
                    )
                self.fill_fail_streak = 0
                self.fills_ok_at = datetime.now(timezone.utc)
            except Exception as e:
                self.fill_fail_streak += 1
                log.warning("fills poll failed (streak %d): %s", self.fill_fail_streak, e)
            await asyncio.sleep(self.fills_interval)

    async def poll_positions_loop(self) -> None:
        if not self.watch_wallets:
            log.info("no WATCH_WALLETS configured; positions polling idle")
            return
        while True:
            now = datetime.now(timezone.utc)
            for wallet in self.watch_wallets:
                try:
                    positions = await self._get(f"{DATA}/positions", {
                        "user": wallet, "limit": 500,
                    })
                    for p in positions:
                        self.writer.add(SQL_POSITION, (
                            now, wallet, p.get("conditionId"), p.get("outcome"),
                            float(p.get("size") or 0), _f(p.get("avgPrice")),
                        ))
                    self.poll_count += 1
                except Exception as e:
                    log.warning("positions poll failed for %s: %s", wallet, e)
            await asyncio.sleep(self.positions_interval)

    def stats(self) -> tuple[int, dict]:
        return self.poll_count, {
            "rows_written": self.writer.rows_written,
            "fill_fail_streak": self.fill_fail_streak,
            "watch_wallets": len(self.watch_wallets),
            "top_n": self.top_n,
        }


async def main() -> None:
    log.info("starting polymarket poller")
    pool = await connect_pool(config.db_dsn())
    writer = BatchWriter(pool)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        poller = PolymarketPoller(pool, writer, session)
        await record_gap(
            pool, VENUE, "fills", None,
            datetime.now(timezone.utc), datetime.now(timezone.utc),
            "collector start",
        )
        async with asyncio.TaskGroup() as tg:
            tg.create_task(writer.run())
            tg.create_task(poller.poll_markets_loop())
            tg.create_task(poller.poll_fills_loop())
            tg.create_task(poller.poll_positions_loop())
            tg.create_task(heartbeat_loop(pool, "polymarket", poller.stats))


if __name__ == "__main__":
    asyncio.run(main())
