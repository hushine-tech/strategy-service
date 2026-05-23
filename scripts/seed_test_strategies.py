#!/usr/bin/env python3
"""Seed 3 test strategies into the ``account`` database for TESTUSDT testing.

覆盖:
  - ``testusdt-spot-roundtrip`` v1.0.0     —— spot BUY/SELL 环路
  - ``testusdt-futures-long-roundtrip`` v1.0.0  —— futures LONG 开 + 平(SHORT 方向)
  - ``testusdt-futures-short-roundtrip`` v1.0.0 —— futures SHORT 开 + 平(LONG 方向)

每个策略都是 "阈值判断 + 简单状态机",配合 ``scripts/seed_test_data.py`` 生成的
200 bar 价格波(100→200→50→200→80→150) 在一次 backtest 里触发多轮买卖:

  bar 0   价格=100       → spot BUY / futures LONG 触发
  bar ~32 价格突破 180   → spot SELL / futures close-long / futures SHORT 开仓
  bar ~61 价格跌破 120   → 再次 BUY / LONG / close-short
  bar ~114 价格再破 180  → SELL / close-long / SHORT
  bar ~147 价格再跌 120  → BUY / LONG / close-short
  bar 199 价格=150       (持仓 / 无持仓取决于策略)

用法:
    python scripts/seed_test_strategies.py

环境变量(均有默认值):
    PGHOST        默认 192.168.88.10
    PGPORT        默认 5432
    PGDATABASE    默认 account
    PGUSER        默认 postgres
    PGPASSWORD    默认 postgres
    SEED_USERNAME 默认 test-user           (仅在该 user 不存在时创建)
    SEED_PASSWORD 默认 test-pass           (首次创建用户时的登录密码)

幂等性:
    - 用户按 username 去重;已存在则复用(不改密码)
    - 策略按 UNIQUE(name, version) 去重;已存在则打印"skip"
    - 可反复执行不产生副作用

本脚本**只创建策略模板**,不创建账号、不挂载/激活策略。挂载请走
``POST /api/accounts/{id}/strategies`` 或 ``MountStrategy``/``ActivateStrategy``
gRPC。
"""

from __future__ import annotations

import os
import sys
import textwrap

import bcrypt
import psycopg2


HOST = os.environ.get("PGHOST", "192.168.88.10")
PORT = int(os.environ.get("PGPORT", "5432"))
DB = os.environ.get("PGDATABASE", "account")
USER = os.environ.get("PGUSER", "postgres")
PASSWORD = os.environ.get("PGPASSWORD", "postgres")

SEED_USERNAME = os.environ.get("SEED_USERNAME", "test-user")
SEED_PASSWORD = os.environ.get("SEED_PASSWORD", "test-pass")


# ── Strategy code (stored verbatim in `strategies.code`) ─────────────────────

SPOT_ROUNDTRIP_CODE = textwrap.dedent('''\
    """Spot roundtrip on TESTUSDT.

    - 无持仓 + close < 120 → BUY  5 TESTUSDT
    - 持仓   + close > 180 → SELL 5 TESTUSDT

    The strategy declares ``(spot, TESTUSDT, 1m)`` in ``INPUTS`` so the runtime
    will only route spot-TESTUSDT 1m ticks here — no runtime ``data.market``
    guard is needed, and a futures tick simply won't reach ``on_market_data``.
    """
    from strategy_service.types import OrderDecision


    class MyStrategy:
        INPUTS = [{"market": "spot", "symbol": "TESTUSDT", "interval": "1m"}]

        def __init__(self):
            self._has_position = False

        def on_market_data(self, data, wallet):
            tick = data.market["spot"].symbol["TESTUSDT"].interval["1m"]
            if tick is None:
                return None
            price = float(tick.price)
            if not self._has_position and price < 120.0:
                self._has_position = True
                return OrderDecision(
                    symbol="TESTUSDT", side="BUY", qty=5.0, market="spot",
                )
            if self._has_position and price > 180.0:
                self._has_position = False
                return OrderDecision(
                    symbol="TESTUSDT", side="SELL", qty=5.0, market="spot",
                )
            return None
''')


FUTURES_LONG_ROUNDTRIP_CODE = textwrap.dedent('''\
    """Futures LONG open + close on TESTUSDT.

    - 无持仓 + close < 120 → LONG  0.1 (开多)
    - 持仓   + close > 180 → SHORT 0.1 (平多;wallet 识别为反向平仓,释放保证金)

    The runtime only routes declared ``(futures, TESTUSDT, 1m)`` ticks here,
    so ``on_market_data`` never sees other markets or intervals.
    """
    from strategy_service.types import OrderDecision


    class MyStrategy:
        INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

        def __init__(self):
            self._has_position = False

        def on_market_data(self, data, wallet):
            tick = data.market["futures"].symbol["TESTUSDT"].interval["1m"]
            if tick is None:
                return None
            price = float(tick.price)
            if not self._has_position and price < 120.0:
                self._has_position = True
                return OrderDecision(
                    symbol="TESTUSDT", side="LONG", qty=0.1, market="futures",
                )
            if self._has_position and price > 180.0:
                self._has_position = False
                return OrderDecision(
                    symbol="TESTUSDT", side="SHORT", qty=0.1, market="futures",
                )
            return None
''')


FUTURES_SHORT_ROUNDTRIP_CODE = textwrap.dedent('''\
    """Futures SHORT open + close on TESTUSDT.

    - 无持仓 + close > 180 → SHORT 0.1 (开空)
    - 持仓   + close < 120 → LONG  0.1 (平空;wallet 识别为反向平仓)
    """
    from strategy_service.types import OrderDecision


    class MyStrategy:
        INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

        def __init__(self):
            self._has_position = False

        def on_market_data(self, data, wallet):
            tick = data.market["futures"].symbol["TESTUSDT"].interval["1m"]
            if tick is None:
                return None
            price = float(tick.price)
            if not self._has_position and price > 180.0:
                self._has_position = True
                return OrderDecision(
                    symbol="TESTUSDT", side="SHORT", qty=0.1, market="futures",
                )
            if self._has_position and price < 120.0:
                self._has_position = False
                return OrderDecision(
                    symbol="TESTUSDT", side="LONG", qty=0.1, market="futures",
                )
            return None
''')


STRATEGIES: list[dict] = [
    {
        "name": "testusdt-spot-roundtrip",
        "version": "1.0.0",
        "description": "Spot BUY/SELL 环路; 配合 TESTUSDT 200-bar 波形触发 5 次订单",
        "code": SPOT_ROUNDTRIP_CODE,
    },
    {
        "name": "testusdt-futures-long-roundtrip",
        "version": "1.0.0",
        "description": "Futures 做多开仓+平仓环路; 同一波形触发 5 次订单",
        "code": FUTURES_LONG_ROUNDTRIP_CODE,
    },
    {
        "name": "testusdt-futures-short-roundtrip",
        "version": "1.0.0",
        "description": "Futures 做空开仓+平仓环路; 同一波形触发 4 次订单",
        "code": FUTURES_SHORT_ROUNDTRIP_CODE,
    },
]


def ensure_user(cur, username: str, password: str) -> int:
    """Return user id; create the user with bcrypt-hashed password when absent."""
    cur.execute("SELECT id FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    # users.password_hash is a bcrypt hash of the plaintext password.
    # Go's bcrypt.DefaultCost is 10 — the Python bcrypt default matches.
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    cur.execute(
        "INSERT INTO users (username, password_hash, created_at) "
        "VALUES (%s, %s, NOW()) RETURNING id",
        (username, hashed),
    )
    return int(cur.fetchone()[0])


def upsert_strategy(cur, user_id: int, spec: dict) -> tuple[int, bool]:
    """Insert a strategy row if it doesn't exist. Returns (strategy_id, created)."""
    cur.execute(
        "SELECT strategy_id FROM strategies WHERE name = %s AND version = %s",
        (spec["name"], spec["version"]),
    )
    row = cur.fetchone()
    if row:
        return int(row[0]), False
    cur.execute(
        "INSERT INTO strategies (name, version, description, code, user_id, archived, created_at) "
        "VALUES (%s, %s, %s, %s, %s, false, NOW()) RETURNING strategy_id",
        (spec["name"], spec["version"], spec["description"], spec["code"], user_id),
    )
    return int(cur.fetchone()[0]), True


def main() -> None:
    dsn = (
        f"host={HOST} port={PORT} dbname={DB} user={USER} "
        f"password={PASSWORD} sslmode=disable"
    )
    print(f"→ connecting to {HOST}:{PORT}/{DB}")
    try:
        conn = psycopg2.connect(dsn)
    except Exception as e:  # noqa: BLE001
        print(f"✗ failed to connect: {e}", file=sys.stderr)
        sys.exit(1)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        user_id = ensure_user(cur, SEED_USERNAME, SEED_PASSWORD)
        print(f"→ user id={user_id} (username={SEED_USERNAME!r})")

        print("→ seeding strategies:")
        for spec in STRATEGIES:
            sid, created = upsert_strategy(cur, user_id, spec)
            tag = "created" if created else "exists "
            print(f"   {tag}  strategy_id={sid:<4} {spec['name']} v{spec['version']}")
    finally:
        cur.close()
        conn.close()

    print()
    print("Done. 挂载 + 激活示例(替换 ACCOUNT_ID / STRATEGY_ID):")
    print("  POST /api/accounts/ACCOUNT_ID/strategies         {strategy_id: STRATEGY_ID}")
    print("  POST /api/accounts/ACCOUNT_ID/strategies/active  {strategy_id: STRATEGY_ID}")
    print()
    print("记住:")
    print("  - spot 策略需要账号有 spot.assets['TESTUSDT'] 槽位 + 足够 spot.free USDT")
    print("  - futures 策略需要账号有 futures.positions[('TESTUSDT', 0)] 槽位(one-way)")
    print("  - 两种价格波触发前先跑 `python scripts/seed_test_data.py` 生成 K 线")


if __name__ == "__main__":
    main()
