"""Binance USDⓈ-M perps L2 orderbook snapshot collector.

Subscribes to <sym>@depth20@500ms partial-book streams — Binance pushes the
top 20 levels ready-made, so there's no local book to maintain and nothing to
drift out of sync. We keep the first message of each wall-clock second per
symbol, giving 1s snapshots into orderbook_snapshots.

Integrity: each depth event carries pu (final update ID of the previous
event). If pu doesn't match the last seen u, Binance dropped or we missed an
event in between — logged to data_gaps.

Separate service from binance_ws on purpose: depth streams are ~10x the
message volume of trades, and a stall here shouldn't take trades down.
"""

import asyncio
import json
from datetime import datetime, timezone

from collectors.common import config
from collectors.common.db import BatchWriter, connect_pool, heartbeat_loop, record_gap, utc_ms
from collectors.common.ws import ReconnectingWS

log = config.setup_logging("binance_orderbook")

VENUE = "binance"
WS_BASE = "wss://fstream.binance.com/stream?streams="

SQL_SNAPSHOT = """
    INSERT INTO orderbook_snapshots (ts, venue, symbol, bids, asks)
    VALUES ($1, $2, $3, $4, $5)
"""


class OrderbookCollector:
    def __init__(self, pool, writer: BatchWriter, syms: list[str], snapshot_interval: float):
        self.pool = pool
        self.writer = writer
        self.symbols = syms
        self.snapshot_interval = snapshot_interval
        self.last_update_id: dict[str, int] = {}
        self.last_written_bucket: dict[str, int] = {}
        streams = [f"{s.lower()}@depth20@500ms" for s in syms]
        self.ws = ReconnectingWS(
            name="binance_orderbook",
            url=WS_BASE + "/".join(streams),
            handler=self.handle,
            on_gap=self.log_disconnect_gap,
        )

    async def log_disconnect_gap(self, start: datetime, end: datetime, reason: str) -> None:
        await record_gap(
            self.pool, VENUE, "depth", None, start, end, reason,
            details={"symbols": self.symbols},
        )
        # Continuity IDs don't survive a reconnect; don't flag the first
        # event on the new connection as a drop.
        self.last_update_id.clear()

    async def handle(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        d: dict = msg.get("data", {})
        if d.get("e") != "depthUpdate":
            return
        symbol = d["s"]

        prev_u = self.last_update_id.get(symbol)
        if prev_u is not None and d["pu"] != prev_u:
            await record_gap(
                self.pool, VENUE, "depth", symbol,
                utc_ms(d["T"]), utc_ms(d["T"]),
                f"depth update ID discontinuity: expected pu={prev_u}, got pu={d['pu']}",
                details={"expected_pu": prev_u, "got_pu": d["pu"], "u": d["u"]},
            )
        self.last_update_id[symbol] = d["u"]

        # Downsample: keep the first event per snapshot bucket per symbol.
        bucket = int(d["T"] / 1000 / self.snapshot_interval)
        if self.last_written_bucket.get(symbol) == bucket:
            return
        self.last_written_bucket[symbol] = bucket

        self.writer.add(SQL_SNAPSHOT, (
            utc_ms(d["T"]), VENUE, symbol,
            json.dumps([[float(p), float(q)] for p, q in d["b"]]),
            json.dumps([[float(p), float(q)] for p, q in d["a"]]),
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
    log.info("starting orderbook collector: symbols=%s interval=%ss", syms, snapshot_interval)

    pool = await connect_pool(config.db_dsn())
    writer = BatchWriter(pool)
    collector = OrderbookCollector(pool, writer, syms, snapshot_interval)

    await record_gap(
        pool, VENUE, "depth", None,
        datetime.now(timezone.utc), datetime.now(timezone.utc),
        "collector start", details={"symbols": syms},
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(writer.run())
        tg.create_task(collector.ws.run())
        tg.create_task(heartbeat_loop(pool, "binance_orderbook", collector.stats))


if __name__ == "__main__":
    asyncio.run(main())
