FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STATE_DB=/data/tianqi.sqlite3

# tzdata: the market counts "this day" in the station's local zone (Asia/Shanghai); zoneinfo needs the system table.
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY main.py ./
COPY tests ./tests

# Offline suites (no network, no token): a failing test fails the build instead of shipping.
RUN python -m py_compile main.py && python tests/run_all.py && rm -rf tests

CMD ["python", "-u", "main.py"]
