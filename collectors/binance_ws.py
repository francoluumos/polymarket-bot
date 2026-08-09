"""Binance USDⓈ-M perps collector: trades, liquidations, funding/mark price, OI.

Websocket (combined stream on fstream.binance.com):
  - <sym>@aggTrade      -> trades          (gap-checked via sequential agg trade IDs)
  - <sym>@forceOrder    -> liquidations
  - <sym>@markPrice@1s  -> funding         (funding rate + mark/index at 1s)

REST (no auth, weight-cheap):
  - /fapi/v1/openInterest polled per symbol -> open_interest

L2 orderbook snapshots live in a separate collector (orderbook.py, next up) —
depth streams have very different volume/failure characteristics and shouldn't
share a connection with trades.
"""

import asyncio
import json
from datetime import datetime, timezone

import aiohttp

from collectors.common import config
from collectors.common.db import BatchWriter, connect_pool, heartbeat_loop, record_gap, utc_ms
from collectors.common.ws import ReconnectingWS

log = config.setup_logging("binance")

VENUE = "binance"
# Binance routed endpoints (2026-04-23 migration): aggTrade/forceOrder/markPrice
# live under /market. Legacy unrouted /stream still accepts connections but
# silently pushes no market-class data.
WS_BASE = "wss://fstream.binance.com/market/stream?streams="
REST_BASE = "https://fapi.binance.com"

SQL_TRADE = """
    INSERT INTO trades (ts, venue, symbol, trade_id, price, qty, is_buyer_maker)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT DO NOTHING
"""
SQL_LIQUIDATION = """
    INSERT INTO liquidations (ts, venue, symbol, side, price, qty)
    VALUES ($1, $2, $3, $4, $5, $6)
"""
SQL_FUNDING = """
    INSERT INTO funding (ts, venue, symbol, funding_rate, mark_price, index_price, next_funding_time)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT DO NOTHING
"""
SQL_OPEN_INTEREST = """
    INSERT INTO open_interest (ts, venue, symbol, open_interest)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT DO NOTHING
"""


class BinanceCollector:
    def __init__(self, pool, writer: BatchWriter, syms: list[str]):
        self.pool = pool
        self.writer = writer
        self.symbols = syms
        # Binance agg trade IDs are sequential per symbol — a jump means
        # dropped trades even without a visible disconnect.
        self.last_agg_id: dict[str, int] = {}
        streams = []
        for s in syms:
            ls = s.lower()
            streams += [f"{ls}@aggTrade", f"{ls}@forceOrder", f"{ls}@markPrice@1s"]
        self.ws = ReconnectingWS(
            name="binance",
            url=WS_BASE + "/".join(streams),
            handler=self.handle,
            on_gap=self.log_disconnect_gap,
        )

    async def log_disconnect_gap(self, start: datetime, end: datetime, reason: str) -> None:
        await record_gap(
            self.pool, VENUE, "websocket", None, start, end, reason,
            details={"symbols": self.symbols},
        )

    async def handle(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        stream: str = msg.get("stream", "")
        data: dict = msg.get("data", {})

        if stream.endswith("@aggTrade"):
            await self.on_agg_trade(data)
        elif stream.endswith("@forceOrder"):
            self.on_force_order(data)
        elif "@markPrice" in stream:
            self.on_mark_price(data)

    async def on_agg_trade(self, d: dict) -> None:
        symbol = d["s"]
        agg_id = int(d["a"])
        last = self.last_agg_id.get(symbol)
        if last is not None and agg_id > last + 1:
            await record_gap(
                self.pool, VENUE, "aggTrade", symbol,
                utc_ms(d["T"]), utc_ms(d["T"]),
                f"agg trade ID jump: {last} -> {agg_id} ({agg_id - last - 1} trades missed)",
                details={"from_id": last, "to_id": agg_id},
            )
        if last is None or agg_id > last:
            self.last_agg_id[symbol] = agg_id

        self.writer.add(SQL_TRADE, (
            utc_ms(d["T"]), VENUE, symbol, str(agg_id),
            float(d["p"]), float(d["q"]), bool(d["m"]),
        ))

    def on_force_order(self, d: dict) -> None:
        o = d["o"]
        # ap = average fill price; side SELL means a long position was liquidated
        self.writer.add(SQL_LIQUIDATION, (
            utc_ms(o["T"]), VENUE, o["s"], o["S"],
            float(o["ap"]), float(o["q"]),
        ))

    def on_mark_price(self, d: dict) -> None:
        rate = float(d["r"]) if d.get("r") not in (None, "") else None
        next_funding = utc_ms(d["T"]) if d.get("T") else None
        self.writer.add(SQL_FUNDING, (
            utc_ms(d["E"]), VENUE, d["s"], rate,
            float(d["p"]), float(d["i"]), next_funding,
        ))

    async def poll_open_interest(self, interval: float) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                for symbol in self.symbols:
                    try:
                        async with session.get(
                            f"{REST_BASE}/fapi/v1/openInterest", params={"symbol": symbol}
                        ) as resp:
                            resp.raise_for_status()
                            d = await resp.json()
                        self.writer.add(SQL_OPEN_INTEREST, (
                            utc_ms(d["time"]), VENUE, symbol, float(d["openInterest"]),
                        ))
                    except Exception as e:
                        # Missed polls are visible as holes in the series; log
                        # loudly but keep the loop alive.
                        log.warning("OI poll failed for %s: %s", symbol, e)
                await asyncio.sleep(interval)

    def stats(self) -> tuple[int, dict]:
        return self.ws.msg_count, {
            "connected": self.ws.connected,
            "last_msg_at": self.ws.last_msg_at.isoformat() if self.ws.last_msg_at else None,
            "rows_written": self.writer.rows_written,
            "symbols": self.symbols,
        }


async def main() -> None:
    syms = config.symbols()
    oi_interval = float(config.env("OI_POLL_INTERVAL", "15"))
    log.info("starting binance collector: symbols=%s oi_interval=%ss", syms, oi_interval)

    pool = await connect_pool(config.db_dsn())
    writer = BatchWriter(pool)
    collector = BinanceCollector(pool, writer, syms)

    # Startup is a known gap boundary: everything before this moment is missing.
    await record_gap(
        pool, VENUE, "websocket", None,
        datetime.now(timezone.utc), datetime.now(timezone.utc),
        "collector start", details={"symbols": syms},
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(writer.run())
        tg.create_task(collector.ws.run())
        tg.create_task(collector.poll_open_interest(oi_interval))
        tg.create_task(heartbeat_loop(pool, "binance_ws", collector.stats))


if __name__ == "__main__":
    asyncio.run(main())
