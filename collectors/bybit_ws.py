"""Bybit linear perps collector: trades, liquidations, funding/mark price, OI.

One v5 public websocket (stream.bybit.com/v5/public/linear):
  - publicTrade.<sym>    -> trades
  - allLiquidation.<sym> -> liquidations  (the older liquidation.* topic is
                            throttled to 1 msg/s and drops events)
  - tickers.<sym>        -> funding + open_interest

tickers is a snapshot-then-delta stream where deltas carry only changed
fields, so we keep a merged per-symbol state. Funding/mark rows are written
at most once per second (matching Binance's markPrice@1s cadence); OI rows
whenever openInterest actually changes.

Bybit requires an app-level {"op":"ping"} every ~20s and a subscribe frame
after connect — both handled via ReconnectingWS hooks, so reconnects
automatically resubscribe.
"""

import asyncio
import json
from datetime import datetime, timezone

from collectors.common import config
from collectors.common.db import BatchWriter, connect_pool, heartbeat_loop, record_gap, utc_ms
from collectors.common.ws import ReconnectingWS

log = config.setup_logging("bybit")

VENUE = "bybit"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

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
    INSERT INTO open_interest (ts, venue, symbol, open_interest, open_interest_value)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT DO NOTHING
"""


def _f(v) -> float | None:
    return float(v) if v not in (None, "") else None


class BybitCollector:
    def __init__(self, pool, writer: BatchWriter, syms: list[str]):
        self.pool = pool
        self.writer = writer
        self.symbols = syms
        self.ticker_state: dict[str, dict] = {}
        self.last_funding_second: dict[str, int] = {}
        self.last_oi: dict[str, float] = {}
        topics = []
        for s in syms:
            topics += [f"publicTrade.{s}", f"allLiquidation.{s}", f"tickers.{s}"]
        self.topics = topics
        self.ws = ReconnectingWS(
            name="bybit",
            url=WS_URL,
            handler=self.handle,
            on_gap=self.log_disconnect_gap,
            on_connect=self.subscribe,
            keepalive_payload=json.dumps({"op": "ping"}),
        )

    async def subscribe(self, ws) -> None:
        await ws.send(json.dumps({"op": "subscribe", "args": self.topics}))
        # tickers deltas are relative to the pre-disconnect snapshot; drop it
        self.ticker_state.clear()

    async def log_disconnect_gap(self, start: datetime, end: datetime, reason: str) -> None:
        await record_gap(
            self.pool, VENUE, "websocket", None, start, end, reason,
            details={"symbols": self.symbols},
        )

    async def handle(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        topic: str = msg.get("topic", "")
        if not topic:  # subscribe acks, pong frames
            if msg.get("success") is False:
                log.error("bybit op failed: %s", msg)
            return

        if topic.startswith("publicTrade."):
            self.on_trades(msg["data"])
        elif topic.startswith("allLiquidation."):
            self.on_liquidations(msg["data"])
        elif topic.startswith("tickers."):
            self.on_ticker(msg)

    def on_trades(self, items: list[dict]) -> None:
        for t in items:
            # S = taker side; taker Sell means the buyer was the maker
            self.writer.add(SQL_TRADE, (
                utc_ms(t["T"]), VENUE, t["s"], t["i"],
                float(t["p"]), float(t["v"]), t["S"] == "Sell",
            ))

    def on_liquidations(self, items: list[dict]) -> None:
        for l in items:
            # S = side of the liquidation order: Sell = long liquidated
            self.writer.add(SQL_LIQUIDATION, (
                utc_ms(l["T"]), VENUE, l["s"], l["S"].upper(),
                float(l["p"]), float(l["v"]),
            ))

    def on_ticker(self, msg: dict) -> None:
        d = msg["data"]
        symbol = d["symbol"]
        ts_ms = msg["ts"]
        state = self.ticker_state.setdefault(symbol, {})
        state.update({k: v for k, v in d.items() if v not in (None, "")})

        second = ts_ms // 1000
        if self.last_funding_second.get(symbol) != second and "markPrice" in state:
            self.last_funding_second[symbol] = second
            next_ft = state.get("nextFundingTime")
            self.writer.add(SQL_FUNDING, (
                utc_ms(ts_ms), VENUE, symbol,
                _f(state.get("fundingRate")),
                _f(state.get("markPrice")),
                _f(state.get("indexPrice")),
                utc_ms(int(next_ft)) if next_ft else None,
            ))

        oi = _f(state.get("openInterest"))
        if oi is not None and self.last_oi.get(symbol) != oi:
            self.last_oi[symbol] = oi
            self.writer.add(SQL_OPEN_INTEREST, (
                utc_ms(ts_ms), VENUE, symbol, oi, _f(state.get("openInterestValue")),
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
    log.info("starting bybit collector: symbols=%s", syms)

    pool = await connect_pool(config.db_dsn())
    writer = BatchWriter(pool)
    collector = BybitCollector(pool, writer, syms)

    await record_gap(
        pool, VENUE, "websocket", None,
        datetime.now(timezone.utc), datetime.now(timezone.utc),
        "collector start", details={"symbols": syms},
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(writer.run())
        tg.create_task(collector.ws.run())
        tg.create_task(heartbeat_loop(pool, "bybit_ws", collector.stats))


if __name__ == "__main__":
    asyncio.run(main())
