"""Database access: connection pool, batched inserts, gap log, heartbeat."""

import asyncio
import json
import logging
from datetime import datetime, timezone

import asyncpg

log = logging.getLogger(__name__)


async def connect_pool(dsn: str, retries: int = 30, delay: float = 2.0) -> asyncpg.Pool:
    for attempt in range(1, retries + 1):
        try:
            return await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        except (OSError, asyncpg.PostgresError) as e:
            if attempt == retries:
                raise
            log.warning("DB not ready (attempt %d/%d): %s", attempt, retries, e)
            await asyncio.sleep(delay)
    raise RuntimeError("unreachable")


class BatchWriter:
    """Buffers rows per insert statement and flushes them in batches.

    Collectors call add() from the websocket hot path; flushing happens in a
    background task so a slow DB never blocks message consumption. Insert
    statements use ON CONFLICT DO NOTHING where a dedupe index exists, so
    reconnect replays are harmless.
    """

    def __init__(self, pool: asyncpg.Pool, flush_interval: float = 2.0, max_buffer: int = 500):
        self.pool = pool
        self.flush_interval = flush_interval
        self.max_buffer = max_buffer
        self._buffers: dict[str, list[tuple]] = {}
        self._kick = asyncio.Event()
        self.rows_written = 0

    def add(self, sql: str, row: tuple) -> None:
        buf = self._buffers.setdefault(sql, [])
        buf.append(row)
        if len(buf) >= self.max_buffer:
            self._kick.set()

    async def run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._kick.wait(), timeout=self.flush_interval)
            except TimeoutError:
                pass
            self._kick.clear()
            await self.flush()

    async def flush(self) -> None:
        for sql in list(self._buffers):
            rows, self._buffers[sql] = self._buffers[sql], []
            if not rows:
                continue
            try:
                async with self.pool.acquire() as conn:
                    await conn.executemany(sql, rows)
                self.rows_written += len(rows)
            except Exception:
                # Put rows back so a transient DB error doesn't lose data.
                self._buffers[sql] = rows + self._buffers[sql]
                log.exception("flush failed for %d rows, will retry", len(rows))
                return


async def record_gap(
    pool: asyncpg.Pool,
    venue: str,
    channel: str,
    symbol: str | None,
    gap_start: datetime,
    gap_end: datetime | None,
    reason: str,
    details: dict | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO data_gaps (venue, channel, symbol, gap_start, gap_end, reason, details)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        venue, channel, symbol, gap_start, gap_end, reason,
        json.dumps(details) if details else None,
    )
    log.warning("GAP %s/%s %s: %s -> %s (%s)", venue, channel, symbol, gap_start, gap_end, reason)


async def heartbeat_loop(pool: asyncpg.Pool, collector: str, get_stats, interval: float = 10.0) -> None:
    """Upserts a heartbeat row so the dead_collectors view can flag silence."""
    while True:
        try:
            msg_count, details = get_stats()
            await pool.execute(
                """
                INSERT INTO collector_heartbeats (collector, last_seen, msg_count, details)
                VALUES ($1, now(), $2, $3)
                ON CONFLICT (collector) DO UPDATE
                SET last_seen = now(), msg_count = EXCLUDED.msg_count, details = EXCLUDED.details
                """,
                collector, msg_count, json.dumps(details),
            )
        except Exception:
            log.exception("heartbeat write failed")
        await asyncio.sleep(interval)


def utc_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
