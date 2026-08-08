"""Reconnecting websocket client with gap logging.

Every disconnect window is written to data_gaps — the milestone rule is not
"no gaps" (impossible over websockets) but "no unexplained gaps".
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import websockets

log = logging.getLogger(__name__)

MAX_BACKOFF = 60.0


class ReconnectingWS:
    def __init__(
        self,
        name: str,
        url: str,
        handler,
        on_gap=None,
        max_conn_age: float = 23 * 3600,  # Binance force-closes at 24h; rotate early
    ):
        self.name = name
        self.url = url
        self.handler = handler
        self.on_gap = on_gap
        self.max_conn_age = max_conn_age
        self.msg_count = 0
        self.last_msg_at: datetime | None = None
        self.connected = False

    async def run(self) -> None:
        backoff = 1.0
        disconnected_at: datetime | None = None
        disconnect_reason = "initial connect"

        while True:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20, max_queue=None
                ) as ws:
                    self.connected = True
                    log.info("[%s] connected", self.name)
                    if disconnected_at is not None and self.on_gap is not None:
                        await self.on_gap(
                            disconnected_at, datetime.now(timezone.utc), disconnect_reason
                        )
                    disconnected_at = None
                    backoff = 1.0
                    connected_mono = time.monotonic()

                    async for raw in ws:
                        self.msg_count += 1
                        self.last_msg_at = datetime.now(timezone.utc)
                        await self.handler(raw)
                        if time.monotonic() - connected_mono > self.max_conn_age:
                            disconnect_reason = "planned rotation (max connection age)"
                            log.info("[%s] rotating connection", self.name)
                            break
                    else:
                        disconnect_reason = "server closed connection"
            except asyncio.CancelledError:
                raise
            except Exception as e:
                disconnect_reason = f"{type(e).__name__}: {e}"
                log.warning("[%s] connection error: %s", self.name, disconnect_reason)

            self.connected = False
            if disconnected_at is None:
                disconnected_at = datetime.now(timezone.utc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
