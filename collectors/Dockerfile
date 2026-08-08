FROM python:3.12-slim

WORKDIR /app

COPY collectors/requirements.txt /app/collectors/requirements.txt
RUN pip install --no-cache-dir -r collectors/requirements.txt

COPY collectors /app/collectors

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app

CMD ["python", "-m", "collectors.binance_ws"]
