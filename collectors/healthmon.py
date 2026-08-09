"""Health monitor: polls dead_collectors and alerts via webhook.

Alerts on state transitions (collector died / recovered), re-alerts every
REALERT_MINUTES while dead so a night-long outage can't be a single missed
message. ALERT_WEBHOOK_URL takes any JSON-accepting webhook (n8n, Slack,
Discord, ...); payload is {"text": ..., "dead": [...], "recovered": [...]}.
Optionally also sends the alert text as an iMessage via Sendblue
(SENDBLUE_API_KEY/SENDBLUE_API_SECRET/SENDBLUE_FROM_NUMBER/ALERT_IMESSAGE_TO).
Without any channel configured it logs CRITICAL only — fine for local dev,
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
        self.sendblue_key = config.env("SENDBLUE_API_KEY", "")
        self.sendblue_secret = config.env("SENDBLUE_API_SECRET", "")
        self.sendblue_from = config.env("SENDBLUE_FROM_NUMBER", "")
        self.imessage_to = config.env("ALERT_IMESSAGE_TO", "")
        self.interval = float(config.env("HEALTH_CHECK_INTERVAL", "60"))
        self.realert = timedelta(minutes=float(config.env("REALERT_MINUTES", "30")))
        self.known_dead: dict[str, datetime] = {}  # collector -> last alerted at

    @property
    def imessage_configured(self) -> bool:
        return bool(self.sendblue_key and self.sendblue_secret and self.imessage_to)

    async def send_alert(self, text: str, dead: list[dict], recovered: list[str]) -> None:
        log.critical(text)
        if self.webhook_url:
            try:
                async with self.session.post(self.webhook_url, json={
                    "text": text, "dead": dead, "recovered": recovered,
                    "source": "trading-research-healthmon",
                }) as resp:
                    if resp.status >= 400:
                        log.error("alert webhook returned %d", resp.status)
            except Exception as e:
                log.error("alert webhook failed: %s", e)
        if self.imessage_configured:
            # Sendblue delivers iMessages via REST; from_number must be the
            # Sendblue-provisioned line, not a personal number.
            payload = {"number": self.imessage_to, "content": text}
            if self.sendblue_from:
                payload["from_number"] = self.sendblue_from
            try:
                async with self.session.post(
                    "https://api.sendblue.com/api/send-message",
                    json=payload,
                    headers={
                        "sb-api-key-id": self.sendblue_key,
                        "sb-api-secret-key": self.sendblue_secret,
                    },
                ) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        log.error("sendblue alert returned %d: %s", resp.status, body[:300])
            except Exception as e:
                log.error("sendblue alert failed: %s", e)

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
        channels = [
            name for name, on in [
                ("webhook", bool(monitor.webhook_url)),
                ("imessage", monitor.imessage_configured),
            ] if on
        ]
        log.info(
            "health monitor started (alert channels: %s)",
            ", ".join(channels) if channels else "NONE — log-only",
        )
        await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
