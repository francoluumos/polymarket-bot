"""Daily Parquet export: research notebooks read these files, never the live DB.

Every EXPORT_CHECK_INTERVAL the job looks for complete UTC days (yesterday
back to EXPORT_BACKFILL_DAYS) missing from export_log and exports them per
table to EXPORT_DIR/<table>/<table>_<YYYY-MM-DD>.parquet. Rows are fetched
hour-by-hour and appended as row groups, so a full day of ticks never sits
in memory. Files are written to a temp path and renamed, so a crash never
leaves a half-written file that looks valid.

Explicit Arrow schemas per table: inference would flip nullable columns
(e.g. open_interest_value) to null-type on chunks where every value is NULL.
"""

import asyncio
import logging
import os
from datetime import date, datetime, time, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collectors.common import config  # noqa: E402
from collectors.common.db import connect_pool  # noqa: E402

log = config.setup_logging("parquet_export")

TS = pa.timestamp("us", tz="UTC")
SCHEMAS: dict[str, pa.Schema] = {
    "trades": pa.schema([
        ("ts", TS), ("venue", pa.string()), ("symbol", pa.string()),
        ("trade_id", pa.string()), ("price", pa.float64()), ("qty", pa.float64()),
        ("is_buyer_maker", pa.bool_()),
    ]),
    "liquidations": pa.schema([
        ("ts", TS), ("venue", pa.string()), ("symbol", pa.string()),
        ("side", pa.string()), ("price", pa.float64()), ("qty", pa.float64()),
    ]),
    "funding": pa.schema([
        ("ts", TS), ("venue", pa.string()), ("symbol", pa.string()),
        ("funding_rate", pa.float64()), ("mark_price", pa.float64()),
        ("index_price", pa.float64()), ("next_funding_time", TS),
    ]),
    "open_interest": pa.schema([
        ("ts", TS), ("venue", pa.string()), ("symbol", pa.string()),
        ("open_interest", pa.float64()), ("open_interest_value", pa.float64()),
    ]),
    "orderbook_snapshots": pa.schema([
        ("ts", TS), ("venue", pa.string()), ("symbol", pa.string()),
        ("bids", pa.string()), ("asks", pa.string()),
    ]),
    "pm_fills": pa.schema([
        ("ts", TS), ("market_id", pa.string()), ("asset_id", pa.string()),
        ("maker", pa.string()), ("taker", pa.string()), ("side", pa.string()),
        ("price", pa.float64()), ("size", pa.float64()), ("tx_hash", pa.string()),
    ]),
    "pm_positions": pa.schema([
        ("ts", TS), ("wallet", pa.string()), ("market_id", pa.string()),
        ("outcome", pa.string()), ("size", pa.float64()), ("avg_price", pa.float64()),
    ]),
}


async def export_day(pool, table: str, day: date, export_dir: str) -> None:
    schema = SCHEMAS[table]
    cols = schema.names
    day_start = datetime.combine(day, time.min, tzinfo=timezone.utc)

    out_dir = os.path.join(export_dir, table)
    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"{table}_{day.isoformat()}.parquet")
    tmp_path = final_path + ".tmp"

    row_count = 0
    writer: pq.ParquetWriter | None = None
    try:
        for hour in range(24):
            start = day_start + timedelta(hours=hour)
            end = start + timedelta(hours=1)
            rows = await pool.fetch(
                f"SELECT {', '.join(cols)} FROM {table}"
                " WHERE ts >= $1 AND ts < $2 ORDER BY ts",
                start, end,
            )
            if not rows:
                continue
            batch = pa.record_batch(
                [pa.array([r[c] for r in rows], type=schema.field(c).type) for c in cols],
                schema=schema,
            )
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, schema, compression="zstd")
            writer.write_batch(batch)
            row_count += len(rows)
    finally:
        if writer is not None:
            writer.close()

    file_rel = None
    if row_count > 0:
        os.replace(tmp_path, final_path)
        file_rel = os.path.join(table, os.path.basename(final_path))
    elif os.path.exists(tmp_path):
        os.remove(tmp_path)

    await pool.execute(
        """
        INSERT INTO export_log (table_name, day, row_count, file)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (table_name, day) DO UPDATE
        SET row_count = EXCLUDED.row_count, file = EXCLUDED.file, exported_at = now()
        """,
        table, day, row_count, file_rel,
    )
    log.info("exported %s %s: %d rows%s", table, day, row_count,
             f" -> {file_rel}" if file_rel else " (empty, no file)")


async def run_pending(pool, export_dir: str, backfill_days: int) -> None:
    today = datetime.now(timezone.utc).date()
    days = [today - timedelta(days=n) for n in range(1, backfill_days + 1)]
    done = {
        (r["table_name"], r["day"])
        for r in await pool.fetch(
            "SELECT table_name, day FROM export_log WHERE day = ANY($1)", days
        )
    }
    for day in sorted(days):
        for table in SCHEMAS:
            if (table, day) in done:
                continue
            try:
                await export_day(pool, table, day, export_dir)
            except Exception:
                log.exception("export failed for %s %s (will retry next cycle)", table, day)


async def main() -> None:
    export_dir = config.env("EXPORT_DIR", "/data/parquet")
    backfill_days = int(config.env("EXPORT_BACKFILL_DAYS", "3"))
    check_interval = float(config.env("EXPORT_CHECK_INTERVAL", "3600"))
    log.info("parquet export: dir=%s backfill=%dd check=%ss",
             export_dir, backfill_days, check_interval)

    pool = await connect_pool(config.db_dsn())
    while True:
        await run_pending(pool, export_dir, backfill_days)
        await asyncio.sleep(check_interval)


if __name__ == "__main__":
    asyncio.run(main())
