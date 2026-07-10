#!/usr/bin/env python3
"""往 TimescaleDB binance 库写入 TESTUSDT 测试 K线数据。

价格波形（200 根 1m K线）：
  Bars   0-39:  100 → 200  (线性上涨)
  Bars  40-79:  200 →  50  (线性下跌)
  Bars  80-119:  50 → 200  (线性上涨)
  Bars 120-159: 200 →  80  (线性下跌)
  Bars 160-199:  80 → 150  (线性上涨)

配合 tests/strategies/test_full_flow.py（price<120 买，price>180 卖）可触发多次交易。

用法：
    python scripts/seed_test_data.py

环境变量（均有默认值）：
    TIMESCALE_HOST      默认 192.168.88.10
    TIMESCALE_PORT      默认 5432
    TIMESCALE_DB        默认 binance
    TIMESCALE_USER      默认 postgres
    TIMESCALE_PASSWORD  默认 postgres
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta

import psycopg2


# ── 配置 ──────────────────────────────────────────────────────────────────────

HOST = os.environ.get("TIMESCALE_HOST", "192.168.88.10")
PORT = int(os.environ.get("TIMESCALE_PORT", "5432"))
DB = os.environ.get("TIMESCALE_DB", "binance_2025")
USER = os.environ.get("TIMESCALE_USER", "postgres")
PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

FUTURES_TABLE = "futures_klines_testusdt_1m"
FUTURES_5M_TABLE = "futures_klines_testusdt_5m"
ALT_FUTURES_TABLE = "futures_klines_altusdt_1m"
SPOT_TABLE = "spot_klines_testusdt_1m"
SYMBOL = "TESTUSDT"
ALT_SYMBOL = "ALTUSDT"
NUM_BARS = 200
BASE_TIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


# ── 价格波形 ──────────────────────────────────────────────────────────────────

def _lerp(start: float, end: float, t: float) -> float:
    return start + (end - start) * t


def generate_prices() -> list[float]:
    """生成 200 个价格点。"""
    segments = [
        (0, 40, 100, 200),
        (40, 80, 200, 50),
        (80, 120, 50, 200),
        (120, 160, 200, 80),
        (160, 200, 80, 150),
    ]
    prices: list[float] = []
    for seg_start, seg_end, p_start, p_end in segments:
        n = seg_end - seg_start
        for i in range(n):
            prices.append(round(_lerp(p_start, p_end, i / (n - 1)), 2))
    return prices


# ── 建表 + 写入 ──────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {TABLE} (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          TEXT            NOT NULL,
    market          TEXT            NOT NULL DEFAULT 'futures',
    exchange        TEXT            NOT NULL DEFAULT 'binance',
    open_time       TIMESTAMPTZ     NOT NULL,
    close_time      TIMESTAMPTZ     NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume    DOUBLE PRECISION NOT NULL DEFAULT 0,
    num_trades      BIGINT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time, symbol)
);
"""

HYPERTABLE_SQL = """
SELECT create_hypertable('{TABLE}', 'time', if_not_exists => TRUE);
"""

INSERT_SQL = """
INSERT INTO {TABLE}
    (time, symbol, market, exchange, open_time, close_time,
     open, high, low, close, volume, quote_volume, num_trades)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _seed_table(
    cur,
    table_name: str,
    market: str,
    *,
    symbol: str = SYMBOL,
    interval_minutes: int = 1,
) -> None:
    """为指定表写入测试数据。"""
    create_sql = CREATE_TABLE_SQL.replace("{TABLE}", table_name)
    hyper_sql = HYPERTABLE_SQL.replace("{TABLE}", table_name)
    insert_sql = INSERT_SQL.replace("{TABLE}", table_name)

    cur.execute(create_sql)
    try:
        cur.execute(hyper_sql)
    except Exception:
        pass

    cur.execute(f"DELETE FROM {table_name} WHERE symbol = %s", (symbol,))

    prices = generate_prices()
    if interval_minutes > 1:
        prices = prices[::interval_minutes]
    rows = []
    for i, price in enumerate(prices):
        open_time = BASE_TIME + timedelta(minutes=i * interval_minutes)
        close_time = open_time + timedelta(minutes=interval_minutes) - timedelta(milliseconds=1)
        rows.append((
            open_time, symbol, market, "binance",
            open_time, close_time,
            price, price, price, price,
            1000.0, price * 1000.0, 100,
        ))

    cur.executemany(insert_sql, rows)
    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE symbol = %s", (symbol,))
    count = cur.fetchone()[0]
    print(f"  {table_name}: {count} rows ({market})")


def main() -> None:
    dsn = f"host={HOST} port={PORT} dbname={DB} user={USER} password={PASSWORD} sslmode=disable"
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    prices = generate_prices()
    print(f"Seeding TESTUSDT: {len(prices)} bars, price {min(prices)}→{max(prices)}")
    print(f"  Time range: {BASE_TIME.isoformat()} → {(BASE_TIME + timedelta(minutes=NUM_BARS-1)).isoformat()}")

    _seed_table(cur, FUTURES_TABLE, "futures")
    _seed_table(cur, FUTURES_5M_TABLE, "futures", interval_minutes=5)
    _seed_table(cur, ALT_FUTURES_TABLE, "futures", symbol=ALT_SYMBOL)
    _seed_table(cur, SPOT_TABLE, "spot")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
