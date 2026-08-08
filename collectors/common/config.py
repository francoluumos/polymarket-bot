import logging
import os
import sys


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def db_dsn() -> str:
    if url := os.environ.get("DATABASE_URL"):
        return url
    return (
        f"postgresql://{env('POSTGRES_USER')}:{env('POSTGRES_PASSWORD')}"
        f"@{env('DB_HOST', 'timescaledb')}:{env('DB_PORT', '5432')}"
        f"/{env('POSTGRES_DB')}"
    )


def symbols() -> list[str]:
    return [s.strip().upper() for s in env("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        stream=sys.stdout,
        level=env("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger(name)
