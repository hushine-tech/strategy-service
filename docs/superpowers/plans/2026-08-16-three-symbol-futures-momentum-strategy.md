# BTC/ETH/ZEC Multi-Symbol Futures Momentum Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one directly usable strategy template that trades BTCUSDT, ETHUSDT, and ZECUSDT independently after each ±0.1% move and emits stream-scoped Indicator V2 output.

**Architecture:** A single `MyStrategy` declares three stable market-data streams and three order targets, then dispatches each callback from `data.trigger.symbol` into a per-symbol reference-price map. Order sizing reads the routed Futures wallet and risk metadata so 1% of `wallet_balance` is used as margin at the required 10x leverage; the existing Runtime attaches each indicator frame to the triggering stream key.

**Tech Stack:** Python 3.13, `strategy_service.types`, `strategy_service.inputs.InputView`, `Decimal`, pytest, existing strategy validator.

## Global Constraints

- Futures initial balance is 1000 USDT.
- Margin mode is `cross`.
- Position mode is `one_way`; decisions use `PositionSide.BOTH`.
- BTCUSDT, ETHUSDT, and ZECUSDT must each be configured at exactly 10x leverage.
- Each successful trigger budgets `wallet_balance * 0.01` as margin and creates `margin_budget * 10` of order notional.
- Each symbol has an independent reference price; the first valid tick only initializes it.
- A move of at least +0.1% emits one BUY; a move of at most -0.1% emits one SELL; one callback emits at most one order.
- Successful order construction resets only the triggering symbol's reference price.
- Indicator keys are `reference_price`, `change_bps`, and `trade_signal`.
- No Runtime, wallet, order protocol, or Indicator V2 protocol changes are in scope.

---

### Task 1: Add the three-symbol strategy template and focused behavior tests

**Files:**
- Create: `strategy_templates/btc_eth_zec_cross_momentum.py`
- Create: `tests/test_btc_eth_zec_cross_momentum.py`

**Interfaces:**
- Consumes: `InputView.trigger`, `wallet.get(exchange, market)`, routed wallet `get_wallet_balance()`, Futures `margin_mode`, `position_mode`, and `risk_metadata[symbol]` with `configured_leverage` and `step_size`.
- Produces: `MyStrategy.INPUTS`, `MyStrategy.ORDER_TARGETS`, `MyStrategy.INDICATORS`, and `MyStrategy.on_market_data(data, wallet) -> OrderDecision | None`.

- [ ] **Step 1: Write the failing declaration, routing, sizing, and indicator tests**

Create `tests/test_btc_eth_zec_cross_momentum.py` with deterministic stubs. The core fixture and assertions must be:

```python
from pathlib import Path
from types import SimpleNamespace

import pytest

from strategy_service.inputs import InputView, parse_declared_inputs
from strategy_service.strategy_validator import validate_strategy_code
from strategy_service.types import Exchange, Market, MarketData, OrderDecision, OrderSide, PositionSide
from strategy_templates.btc_eth_zec_cross_momentum import MyStrategy


STEPS = {"BTCUSDT": 0.0001, "ETHUSDT": 0.001, "ZECUSDT": 0.001}


class IndicatorRecorder:
    def __init__(self):
        self.values = {}
        self.markers = []

    def set(self, key, value):
        self.values[key] = value

    def mark(self, key, text="", price=None, color="", *, position="", shape=""):
        self.markers.append((key, text, price, color, position, shape))


class NotifyRecorder:
    def __init__(self):
        self.calls = []

    def warn(self, message, *, title=""):
        self.calls.append((title, message))
        return True


class RouteWallet:
    def __init__(self, balance=1000.0, margin_mode="cross", position_mode="one_way", leverage=10.0):
        self.balance = balance
        self.futures = SimpleNamespace(
            margin_mode=margin_mode,
            position_mode=position_mode,
            risk_metadata={
                symbol: SimpleNamespace(configured_leverage=leverage, step_size=step)
                for symbol, step in STEPS.items()
            },
        )

    def get_wallet_balance(self):
        return self.balance


class PortfolioWallet:
    def __init__(self, route_wallet):
        self.route_wallet = route_wallet

    def get(self, exchange, market):
        assert exchange == Exchange.BINANCE
        assert market == Market.PERPETUAL_FUTURES
        return self.route_wallet


def tick(symbol, price):
    return MarketData(
        stream_id=f"futures-{symbol.lower()}-1m",
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        kind="kline",
        symbol=symbol,
        interval="1m",
        price=price,
        timestamp=0,
    )


def feed(strategy, view, wallet, symbol, price):
    assert view.update(tick(symbol, price)) is True
    return strategy.on_market_data(view, wallet)


def configured_strategy():
    strategy = MyStrategy()
    strategy.indicators = IndicatorRecorder()
    strategy.notify = NotifyRecorder()
    return strategy


def test_template_is_validator_accepted_and_declares_three_exact_streams_and_targets():
    path = Path("strategy_templates/btc_eth_zec_cross_momentum.py")
    result = validate_strategy_code(path.read_text(encoding="utf-8"))
    assert result.ok is True, result.issues
    assert [item["symbol"] for item in MyStrategy.INPUTS] == ["ZECUSDT", "ETHUSDT", "BTCUSDT"]
    assert [item["stream_id"] for item in MyStrategy.INPUTS] == [
        "futures-zecusdt-1m",
        "futures-ethusdt-1m",
        "futures-btcusdt-1m",
    ]
    assert [item["symbol"] for item in MyStrategy.ORDER_TARGETS] == ["ZECUSDT", "ETHUSDT", "BTCUSDT"]
    assert set(MyStrategy.INDICATORS) == {"reference_price", "change_bps", "trade_signal"}


def test_interleaved_symbols_keep_independent_references_and_emit_correct_orders():
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(MyStrategy.INPUTS))
    wallet = PortfolioWallet(RouteWallet())

    assert feed(strategy, view, wallet, "BTCUSDT", 100000.0) is None
    assert feed(strategy, view, wallet, "ETHUSDT", 3000.0) is None
    assert feed(strategy, view, wallet, "ZECUSDT", 50.0) is None

    btc = feed(strategy, view, wallet, "BTCUSDT", 100100.0)
    eth = feed(strategy, view, wallet, "ETHUSDT", 2997.0)
    assert feed(strategy, view, wallet, "ZECUSDT", 50.04) is None
    zec = feed(strategy, view, wallet, "ZECUSDT", 50.05)

    assert isinstance(btc, OrderDecision)
    assert (btc.symbol, btc.side, btc.qty, btc.position_side) == (
        "BTCUSDT", OrderSide.BUY, "0.0009", PositionSide.BOTH,
    )
    assert (eth.symbol, eth.side, eth.qty) == ("ETHUSDT", OrderSide.SELL, "0.033")
    assert (zec.symbol, zec.side, zec.qty) == ("ZECUSDT", OrderSide.BUY, "1.998")
    assert strategy._reference_prices == {
        "BTCUSDT": 100100.0,
        "ETHUSDT": 2997.0,
        "ZECUSDT": 50.05,
    }
    assert [marker[1] for marker in strategy.indicators.markers] == ["BUY", "SELL", "BUY"]


def test_wallet_balance_one_percent_is_margin_budget_at_ten_x():
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(MyStrategy.INPUTS))
    route = RouteWallet(balance=2000.0)
    wallet = PortfolioWallet(route)

    assert feed(strategy, view, wallet, "ETHUSDT", 3000.0) is None
    order = feed(strategy, view, wallet, "ETHUSDT", 3003.0)
    assert order is not None
    assert order.qty == "0.066"
    assert float(order.qty) * 3003.0 == pytest.approx(198.198)


@pytest.mark.parametrize(
    ("margin_mode", "position_mode", "leverage", "warning"),
    [
        ("isolated", "one_way", 10.0, "cross"),
        ("cross", "hedge", 10.0, "one_way"),
        ("cross", "one_way", 20.0, "10x"),
    ],
)
def test_invalid_account_contract_warns_skips_and_does_not_advance_reference(
    margin_mode, position_mode, leverage, warning,
):
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(MyStrategy.INPUTS))
    route = RouteWallet(
        margin_mode=margin_mode,
        position_mode=position_mode,
        leverage=leverage,
    )
    wallet = PortfolioWallet(route)

    assert feed(strategy, view, wallet, "ZECUSDT", 50.0) is None
    assert feed(strategy, view, wallet, "ZECUSDT", 50.05) is None
    assert strategy._reference_prices["ZECUSDT"] == 50.0
    assert warning in strategy.notify.calls[-1][1]


def test_indicator_values_and_marker_belong_to_each_triggered_callback():
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(MyStrategy.INPUTS))
    wallet = PortfolioWallet(RouteWallet())

    assert feed(strategy, view, wallet, "BTCUSDT", 100000.0) is None
    assert strategy.indicators.values == {"reference_price": 100000.0, "change_bps": 0.0}
    order = feed(strategy, view, wallet, "BTCUSDT", 100100.0)
    assert order is not None
    assert strategy.indicators.values["reference_price"] == 100100.0
    assert strategy.indicators.values["change_bps"] == pytest.approx(10.0)
    assert strategy.indicators.markers[-1][0:3] == ("trade_signal", "BUY", 100100.0)


def test_missing_symbol_metadata_warns_and_preserves_reference():
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(MyStrategy.INPUTS))
    route = RouteWallet()
    del route.futures.risk_metadata["ETHUSDT"]
    wallet = PortfolioWallet(route)

    assert feed(strategy, view, wallet, "ETHUSDT", 3000.0) is None
    assert feed(strategy, view, wallet, "ETHUSDT", 3003.0) is None
    assert strategy._reference_prices["ETHUSDT"] == 3000.0
    assert "risk metadata missing for ETHUSDT" in strategy.notify.calls[-1][1]
```

- [ ] **Step 2: Run the focused tests and verify the missing template fails**

Run:

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest tests/test_btc_eth_zec_cross_momentum.py -q
```

Expected: collection fails with `ModuleNotFoundError: No module named 'strategy_templates.btc_eth_zec_cross_momentum'`.

- [ ] **Step 3: Implement the minimal complete strategy template**

Create `strategy_templates/btc_eth_zec_cross_momentum.py` with these concrete units:

```python
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN

from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType, PositionSide


class MyStrategy:
    SYMBOLS = ("ZECUSDT", "ETHUSDT", "BTCUSDT")
    INPUTS = [
        {
            "stream_id": "futures-zecusdt-1m",
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "kind": "kline",
            "symbol": "ZECUSDT",
            "interval": "1m",
        },
        {
            "stream_id": "futures-ethusdt-1m",
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "kind": "kline",
            "symbol": "ETHUSDT",
            "interval": "1m",
        },
        {
            "stream_id": "futures-btcusdt-1m",
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "kind": "kline",
            "symbol": "BTCUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = [
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ZECUSDT"},
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT"},
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT"},
    ]
    INDICATORS = {
        "reference_price": {"name": "Reference Price", "type": "line", "pane": "price", "color": "#2563eb", "unit": "USDT"},
        "change_bps": {"name": "Change From Reference", "type": "line", "pane": "strategy", "color": "#7c3aed", "unit": "bps"},
        "trade_signal": {"name": "Trade Signal", "type": "marker", "pane": "price", "color": "#0f766e"},
    }
    TRIGGER_PCT = 0.001
    MARGIN_FRACTION = Decimal("0.01")
    REQUIRED_LEVERAGE = Decimal("10")
    EPSILON = 1e-12

    def __init__(self):
        self._reference_prices = {}

    def _warn(self, message):
        notify = getattr(self, "notify", None)
        callback = getattr(notify, "warn", None)
        if callable(callback):
            callback(message, title="Multi-Symbol Futures Test")

    def _set_indicators(self, reference_price, change_bps):
        indicators = getattr(self, "indicators", None)
        if indicators is not None:
            indicators.set("reference_price", reference_price)
            indicators.set("change_bps", change_bps)

    @staticmethod
    def _decimal(value):
        parsed = Decimal(str(value))
        if not parsed.is_finite():
            raise InvalidOperation
        return parsed

    def _build_order(self, wallet, symbol, price, side):
        try:
            route_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
            futures = getattr(route_wallet, "futures", route_wallet)
            if str(getattr(futures, "margin_mode", "")).lower() != "cross":
                self._warn("account must use cross margin mode")
                return None
            if str(getattr(futures, "position_mode", "")).lower() != "one_way":
                self._warn("account must use one_way position mode")
                return None
            metadata = getattr(futures, "risk_metadata", {}).get(symbol)
            if metadata is None:
                self._warn(f"risk metadata missing for {symbol}")
                return None
            leverage = self._decimal(metadata.configured_leverage)
            if leverage != self.REQUIRED_LEVERAGE:
                self._warn(f"{symbol} must be configured at 10x leverage")
                return None
            step = self._decimal(metadata.step_size)
            balance = self._decimal(route_wallet.get_wallet_balance())
            price_decimal = self._decimal(price)
            if step <= 0 or balance <= 0 or price_decimal <= 0:
                self._warn(f"invalid sizing inputs for {symbol}")
                return None
            notional = balance * self.MARGIN_FRACTION * leverage
            qty = ((notional / price_decimal) / step).to_integral_value(rounding=ROUND_DOWN) * step
            if qty <= 0:
                self._warn(f"rounded quantity is zero for {symbol}")
                return None
        except (AttributeError, InvalidOperation, TypeError, ValueError) as exc:
            self._warn(f"cannot size {symbol} order: {type(exc).__name__}")
            return None
        return OrderDecision(
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            symbol=symbol,
            side=side,
            qty=format(qty.normalize(), "f"),
            order_type=OrderType.MARKET,
            position_side=PositionSide.BOTH,
        )

    def on_market_data(self, data, wallet):
        tick = getattr(data, "trigger", None)
        if tick is None:
            return None
        symbol = str(getattr(tick, "symbol", "")).upper()
        if symbol not in self.SYMBOLS:
            return None
        try:
            price = float(tick.price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None
        reference = self._reference_prices.get(symbol)
        if reference is None:
            self._reference_prices[symbol] = price
            self._set_indicators(price, 0.0)
            return None
        change = (price - reference) / reference
        self._set_indicators(reference, change * 10000.0)
        if change >= self.TRIGGER_PCT - self.EPSILON:
            side = OrderSide.BUY
        elif change <= -self.TRIGGER_PCT + self.EPSILON:
            side = OrderSide.SELL
        else:
            return None
        decision = self._build_order(wallet, symbol, price, side)
        if decision is None:
            return None
        self._reference_prices[symbol] = price
        self._set_indicators(price, change * 10000.0)
        indicators = getattr(self, "indicators", None)
        if indicators is not None:
            color = "#16a34a" if side == OrderSide.BUY else "#dc2626"
            position = "belowBar" if side == OrderSide.BUY else "aboveBar"
            shape = "arrowUp" if side == OrderSide.BUY else "arrowDown"
            indicators.mark(
                "trade_signal",
                text=str(side),
                price=price,
                color=color,
                position=position,
                shape=shape,
            )
        return decision
```

- [ ] **Step 4: Run the focused tests and fix only specification mismatches**

Run:

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest tests/test_btc_eth_zec_cross_momentum.py -q
```

Expected: all tests pass. If a numeric expectation differs only because of Decimal step rounding, retain `ROUND_DOWN` and correct the test to the exact lower step; do not round upward.

- [ ] **Step 5: Run existing neighboring strategy and validator regressions**

Run:

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest \
  tests/test_btc_eth_zec_cross_momentum.py \
  tests/test_zecusdt_two_min_momentum.py \
  tests/test_zecusdt_reconciliation_bollinger_notify.py \
  tests/test_strategy_indicators.py \
  tests/test_strategy_engine.py::test_indicator_sequences_are_contiguous_and_independent_per_stream \
  tests/test_strategy_validator.py -q
```

Expected: all selected tests pass.

- [ ] **Step 6: Run the full strategy-service Python suite**

Run:

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest tests/ -q
```

Expected: the complete suite passes with no new warnings or collection errors.

- [ ] **Step 7: Review the owned diff and commit the implementation**

Run:

```bash
git diff --check
git diff -- strategy_templates/btc_eth_zec_cross_momentum.py tests/test_btc_eth_zec_cross_momentum.py
git status --short
git add strategy_templates/btc_eth_zec_cross_momentum.py tests/test_btc_eth_zec_cross_momentum.py
git commit -m "feat: add three-symbol futures test strategy"
```

Expected: only the new strategy template and its focused test are included in this implementation commit.
