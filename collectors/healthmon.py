"""Health monitor: polls dead_collectors and alerts via webhook.

Alerts on state transitions (collector died / recovered), re-alerts every
REALERT_MINUTES while dead so a night-long outage can't be a single missed
message. ALERT_WEBHOOK_URL takes any JSON-accepting webhook (n8n, Slack,
Discord, ...); payload is {"text": ..., "dead": [...], "recovered": [...]}.
Without a webhook configured it logs CRITICAL only — fine for local dev,
not for the VPS.

The monitor also heartbeats itself, so a dead monitor is at least visible
in collector_heartbeats even though it can't self-report.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp

from collectors.common import config
from collectors.common.db import connect_pool

log = config.setup_logging("healthmon")


class HealthMonitor:
    def __init__(self, pool, session: aiohttp.ClientSession):
        self.pool = pool
        self.session = session
        self.webhook_url = config.env("ALERT_WEBHOOK_URL", "")
        self.interval = float(config.env("HEALTH_CHECK_INTERVAL", "60"))
        self.realert = timedelta(minutes=float(config.env("REALERT_MINUTES", "30")))
        self.known_dead: dict[str, datetime] = {}  # collector -> last alerted at

    async def send_alert(self, text: str, dead: list[dict], recovered: list[str]) -> None:
        log.critical(text)
        if not self.webhook_url:
            return
        try:
            async with self.session.post(self.webhook_url, json={
                "text": text, "dead": dead, "recovered": recovered,
                "source": "trading-research-healthmon",
            }) as resp:
                if resp.status >= 400:
                    log.error("alert webhook returned %d", resp.status)
        except Exception as e:
            log.error("alert webhook failed: %s", e)

    async def check(self) -> None:
        rows = await self.pool.fetch(
            "SELECT collector, last_seen, silent_for::text FROM dead_collectors"
        )
        now = datetime.now(timezone.utc)
        dead_now = {r["collector"]: r for r in rows}

        recovered = [c for c in self.known_dead if c not in dead_now]
        newly_dead = [c for c in dead_now if c not in self.known_dead]
        stale = [
            c for c, alerted_at in self.known_dead.items()
            if c in dead_now and now - alerted_at >= self.realert
        ]

        for c in recovered:
            del self.known_dead[c]
        for c in newly_dead + stale:
            self.known_dead[c] = now

        if newly_dead or recovered or stale:
            parts = []
            if newly_dead or stale:
                parts.append("DEAD: " + ", ".join(
                    f"{c} (silent {dead_now[c]['silent_for']})" for c in sorted(dead_now)
                ))
            if recovered:
                parts.append("RECOVERED: " + ", ".join(sorted(recovered)))
            await self.send_alert(
                "[trading-research] " + " | ".join(parts),
                [dict(r) | {"last_seen": r["last_seen"].isoformat()} for r in rows],
                recovered,
            )

    async def run(self) -> None:
        while True:
            try:
                await self.check()
                await self.pool.execute(
                    """
                    INSERT INTO collector_heartbeats (collector, last_seen, msg_count)
                    VALUES ('healthmon', now(), 0)
                    ON CONFLICT (collector) DO UPDATE SET last_seen = now()
                    """
                )
            except Exception:
                log.exception("health check failed")
            await asyncio.sleep(self.interval)


async def main() -> None:
    pool = await connect_pool(config.db_dsn())
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        monitor = HealthMonitor(pool, session)
        log.info(
            "health monitor started (webhook %s)",
            "configured" if monitor.webhook_url else "NOT configured — log-only",
        )
        await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
