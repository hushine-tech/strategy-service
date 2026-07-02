#!/usr/bin/env python3
"""Seed a reconciliation-test strategy into the ``account`` database.

ETHUSDT futures 1m 触发式对账压测策略:

  - 声明输入: ``(binance, perpetual_futures, ETHUSDT, 1m)``
  - 每 tick 计算和参考价的涨跌幅 Δ:
      * Δ >= +0.1%  → BUY   (追涨) 1% 钱包余额的 ETH, 重置参考价
      * Δ <= -0.1%  → SELL  (杀跌) 1% 钱包余额的 ETH, 重置参考价
      * 其它         → 不动作
  - 订单量 = wallet_balance × 1% / price, 向下取整到 0.001 ETH (Binance ETHUSDT 步长)
  - 若 1% 钱包余额低于 20 USDT 最小成交名义金额, 跳过本次信号

设计目的: 为 ``mode=2`` (testnet) session 持续产生订单流, 让对账模块
(reconciliation runs + snapshot diff) 每 ~20 bar 都有新成交可以比对 —
local wallet ↔ 交易所 wallet 的漂移会立刻出现在 reconciliation_runs 里.

用法:

    python scripts/seed_reconciliation_test_strategy.py

环境变量 (均有默认值, 与 ``seed_test_strategies.py`` 保持一致):

    PGHOST        默认 192.168.88.10
    PGPORT        默认 5432
    PGDATABASE    默认 account
    PGUSER        默认 postgres
    PGPASSWORD    默认 postgres
    SEED_USERNAME 默认 test-user         (仅在该 user 不存在时创建)
    SEED_PASSWORD 默认 test-pass

幂等: 按 ``(name, version)`` 去重, 已存在时直接返回现有 strategy_id.

挂载 + 激活示例:

    POST /api/accounts/ACCOUNT_ID/strategies         {strategy_id: N}
    POST /api/accounts/ACCOUNT_ID/strategies/active  {strategy_id: N}

账号要求:

  - ``mode = 2`` (Binance testnet) 才会触发对账
  - futures 钱包里要有 ``ETHUSDT`` 的 position 槽位 (one-way 即可) +
    足够的 USDT 余额 (≥ 2000 USDT 建议, 让 1% ≥ 20 USDT 通过 minNotional)
  - 要启动一条 live K-line 流 (``POST /api/market-data/requests``
    symbol=ETHUSDT market=perpetual_futures kind=kline interval=1m)
"""

from __future__ import annotations

import os
import sys
import textwrap

import psycopg2


HOST = os.environ.get("PGHOST", "192.168.88.10")
PORT = int(os.environ.get("PGPORT", "5432"))
DB = os.environ.get("PGDATABASE", "account")
USER = os.environ.get("PGUSER", "postgres")
PASSWORD = os.environ.get("PGPASSWORD", "postgres")

SEED_USERNAME = os.environ.get("SEED_USERNAME", "test-user")
SEED_PASSWORD = os.environ.get("SEED_PASSWORD", "test-pass")


# ── Strategy code (stored verbatim in ``strategies.code``) ───────────────────
#
# Keep the body single-file + import-light: strategy-service ``exec``s this
# in a fresh namespace (see ``strategy/base._load_strategy_instance``), so the
# only import we need is ``OrderDecision`` from ``strategy_service.types``.

RECONCILIATION_TEST_CODE = textwrap.dedent('''\
    """ETHUSDT futures 对账压测策略.

    - 无持仓 + 第一 tick 只记录参考价
    - 每 +0.1% → BUY 1% 钱包; 每 -0.1% → SELL 1% 钱包
    - 每次触发后重置参考价, 需要再一次 ±0.1% 才会再下单

    INPUTS 声明决定运行时 router 只路由 (binance, perpetual_futures, ETHUSDT, 1m); 其它 ticks
    根本不会进入 on_market_data.
    """
    from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType, PositionSide


    class MyStrategy:
        INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]
        ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT"}]

        # 触发阈值: ±0.1% (千分之一).
        TRIGGER_PCT = 0.001
        # 每笔订单名义值 = 钱包余额 × 1%.
        SIZE_PCT = 0.01
        # Binance futures 最小成交名义金额. 低于该值交易所会拒单, 策略侧直接跳过.
        MIN_NOTIONAL_USDT = 20.0
        # Binance ETHUSDT USDT-M 期货的 step_size = 0.001 ETH.
        QTY_PRECISION = 3

        def __init__(self):
            self._ref_price = None

        def on_market_data(self, data, wallet):
            tick = data.exchange[Exchange.BINANCE].market[Market.PERPETUAL_FUTURES].symbol["ETHUSDT"].interval["1m"]
            if tick is None:
                return None
            price = float(tick.price)
            if price <= 0:
                return None

            # 首 tick: 只初始化参考价, 不下单 (避免 session 一启动就发订单).
            if self._ref_price is None:
                self._ref_price = price
                return None

            change = (price - self._ref_price) / self._ref_price
            if abs(change) < self.TRIGGER_PCT:
                return None

            # 从组合钱包读取目标 venue 的余额.
            try:
                futures_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
                wallet_balance = float(futures_wallet.get_wallet_balance())
            except Exception:
                return None
            if wallet_balance <= 0:
                return None

            # 1% 钱包名义值 → ETH 数量, 向下取整到 step_size 防止 Binance minQty 拒单.
            notional_usdt = wallet_balance * self.SIZE_PCT
            if notional_usdt < self.MIN_NOTIONAL_USDT:
                self._ref_price = price
                return None
            qty = notional_usdt / price
            step = 10 ** (-self.QTY_PRECISION)
            qty = int(qty / step) * step
            qty = round(qty, self.QTY_PRECISION)
            if qty <= 0:
                self._ref_price = price
                return None

            # 触发 → 先重置参考价 (否则一个大 tick 会让连续几条 tick 反复下单).
            self._ref_price = price

            if change > 0:
                # 涨了 → 追涨做多 1%
                return OrderDecision(
                    exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES,
                    symbol="ETHUSDT", side=OrderSide.BUY, qty=str(qty),
                    order_type=OrderType.MARKET, position_side=PositionSide.BOTH,
                )
            else:
                # 跌了 → 杀跌做空 1%
                return OrderDecision(
                    exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES,
                    symbol="ETHUSDT", side=OrderSide.SELL, qty=str(qty),
                    order_type=OrderType.MARKET, position_side=PositionSide.BOTH,
                )
''')


STRATEGIES: list[dict] = [
    {
        "name": "ethusdt-reconciliation-momentum",
        "version": "1.0.0",
        "description": (
            "ETHUSDT 1m futures; 每 +0.1% 追涨做多 1%, 每 -0.1% 杀跌做空 1%; "
            "用于 mode=2 对账压测 (持续产生订单流, 触发 order_fill + periodic_sample 对账)"
        ),
        "code": RECONCILIATION_TEST_CODE,
    },
]


def ensure_user(cur, username: str, password: str) -> int:
    """Return user id; create the user with bcrypt-hashed password when absent."""
    cur.execute("SELECT id FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    try:
        import bcrypt
    except ModuleNotFoundError as exc:
        raise RuntimeError("bcrypt is required when creating a seed user") from exc
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
    print("Done. 挂载 + 激活示例 (替换 ACCOUNT_ID / STRATEGY_ID):")
    print("  POST /api/accounts/ACCOUNT_ID/strategies         {strategy_id: STRATEGY_ID}")
    print("  POST /api/accounts/ACCOUNT_ID/strategies/active  {strategy_id: STRATEGY_ID}")
    print()
    print("对账测试前置条件:")
    print("  - 账号 mode = 2 (Binance testnet), 才会走 reconciliation compare 路径")
    print("  - 账号 futures 余额 ≥ 2000 USDT (1% ≥ 20 USDT, so orders pass Binance minNotional)")
    print("  - 账号 futures.positions 里要有 ETHUSDT one-way 槽位")
    print("  - 已声明 ETHUSDT 1m futures K 线流 (market-data request + stream running)")
    print()
    print("对账触发频次:")
    print("  - 每次成交 → snapshot_reason=1 (order_fill), 立刻对账")
    print("  - 每 ~20 bar → snapshot_reason=6 (periodic_sample), 按 PeriodicSample hybrid trigger 对账")
    print("  - 长时间无 tick 时空闲 5 分钟也会触发 periodic_sample")


if __name__ == "__main__":
    main()
