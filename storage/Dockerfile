# Export job image — separate from collectors so pyarrow doesn't bloat them.
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "asyncpg>=0.29,<0.31" "pyarrow>=16,<19"

COPY collectors/common /app/collectors/common
COPY collectors/__init__.py /app/collectors/__init__.py
COPY storage/parquet_export.py /app/storage/parquet_export.py

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app

CMD ["python", "/app/storage/parquet_export.py"]
