"""Bybit linear perps L2 orderbook snapshot collector.

Bybit has no ready-made partial-book snapshot stream, so unlike the Binance
collector this one maintains a local book: orderbook.50.<sym> sends one
snapshot then deltas (size "0" removes a level). We emit the top
ORDERBOOK_DEPTH levels once per second into orderbook_snapshots.

Integrity: delta update IDs (u) are sequential per symbol. On a mismatch the
local book can no longer be trusted — log the gap and force a reconnect,
which resubscribes and gets a fresh snapshot. Bybit also sends u=1 as an
in-band snapshot after their service restarts; that's a reset, not a gap.
"""

import asyncio
import json
from datetime import datetime, timezone

from collectors.common import config
from collectors.common.db import BatchWriter, connect_pool, heartbeat_loop, record_gap, utc_ms
from collectors.common.ws import ReconnectingWS

log = config.setup_logging("bybit_orderbook")

VENUE = "bybit"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

SQL_SNAPSHOT = """
    INSERT INTO orderbook_snapshots (ts, venue, symbol, bids, asks)
    VALUES ($1, $2, $3, $4, $5)
"""


class BookDesyncError(Exception):
    """Raised to force a reconnect + fresh snapshot."""


class BybitOrderbookCollector:
    def __init__(self, pool, writer: BatchWriter, syms: list[str],
                 snapshot_interval: float, depth: int):
        self.pool = pool
        self.writer = writer
        self.symbols = syms
        self.snapshot_interval = snapshot_interval
        self.depth = depth
        # symbol -> {"b": {price: size}, "a": {price: size}}
        self.books: dict[str, dict[str, dict[float, float]]] = {}
        self.last_u: dict[str, int] = {}
        self.last_written_bucket: dict[str, int] = {}
        self.topics = [f"orderbook.50.{s}" for s in syms]
        self.ws = ReconnectingWS(
            name="bybit_orderbook",
            url=WS_URL,
            handler=self.handle,
            on_gap=self.log_disconnect_gap,
            on_connect=self.subscribe,
            keepalive_payload=json.dumps({"op": "ping"}),
        )

    async def subscribe(self, ws) -> None:
        await ws.send(json.dumps({"op": "subscribe", "args": self.topics}))
        self.books.clear()
        self.last_u.clear()

    async def log_disconnect_gap(self, start: datetime, end: datetime, reason: str) -> None:
        await record_gap(
            self.pool, VENUE, "depth", None, start, end, reason,
            details={"symbols": self.symbols},
        )

    async def handle(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        topic: str = msg.get("topic", "")
        if not topic.startswith("orderbook."):
            if msg.get("success") is False:
                log.error("bybit op failed: %s", msg)
            return

        d = msg["data"]
        symbol = d["s"]
        u = d["u"]
        ts_ms = msg.get("cts") or msg["ts"]

        if msg["type"] == "snapshot" or u == 1:
            self.books[symbol] = {
                "b": {float(p): float(q) for p, q in d["b"]},
                "a": {float(p): float(q) for p, q in d["a"]},
            }
        else:
            prev_u = self.last_u.get(symbol)
            if prev_u is None or symbol not in self.books:
                return  # delta before snapshot; wait for resubscribe
            if u != prev_u + 1:
                await record_gap(
                    self.pool, VENUE, "depth", symbol,
                    utc_ms(ts_ms), utc_ms(ts_ms),
                    f"orderbook update ID discontinuity: expected u={prev_u + 1}, got u={u}",
                    details={"expected_u": prev_u + 1, "got_u": u},
                )
                raise BookDesyncError(f"{symbol}: u jumped {prev_u} -> {u}")
            book = self.books[symbol]
            for side in ("b", "a"):
                for p, q in d.get(side, []):
                    price, size = float(p), float(q)
                    if size == 0:
                        book[side].pop(price, None)
                    else:
                        book[side][price] = size
        self.last_u[symbol] = u

        bucket = int(ts_ms / 1000 / self.snapshot_interval)
        if self.last_written_bucket.get(symbol) == bucket:
            return
        self.last_written_bucket[symbol] = bucket

        book = self.books[symbol]
        bids = sorted(book["b"].items(), key=lambda x: -x[0])[: self.depth]
        asks = sorted(book["a"].items())[: self.depth]
        self.writer.add(SQL_SNAPSHOT, (
            utc_ms(ts_ms), VENUE, symbol,
            json.dumps([[p, q] for p, q in bids]),
            json.dumps([[p, q] for p, q in asks]),
        ))

    def stats(self) -> tuple[int, dict]:
        return self.ws.msg_count, {
            "connected": self.ws.connected,
            "last_msg_at": self.ws.last_msg_at.isoformat() if self.ws.last_msg_at else None,
            "rows_written": self.writer.rows_written,
            "symbols": self.symbols,
        }


async def main() -> None:
    syms = config.symbols()
    snapshot_interval = float(config.env("ORDERBOOK_SNAPSHOT_INTERVAL", "1"))
    depth = int(config.env("ORDERBOOK_DEPTH", "20"))
    log.info("starting bybit orderbook collector: symbols=%s interval=%ss depth=%d",
             syms, snapshot_interval, depth)

    pool = await connect_pool(config.db_dsn())
    writer = BatchWriter(pool)
    collector = BybitOrderbookCollector(pool, writer, syms, snapshot_interval, depth)

    await record_gap(
        pool, VENUE, "depth", None,
        datetime.now(timezone.utc), datetime.now(timezone.utc),
        "collector start", details={"symbols": syms},
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(writer.run())
        tg.create_task(collector.ws.run())
        tg.create_task(heartbeat_loop(pool, "bybit_orderbook", collector.stats))


if __name__ == "__main__":
    asyncio.run(main())
