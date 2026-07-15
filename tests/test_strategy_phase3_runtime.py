from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any

import pytest

from strategy_service.strategy.base import BaseStrategy
from strategy_service.strategy_imports import (
    StrategySourceLoadError,
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)
from strategy_service.types import (
    Exchange,
    ExecutionFeedback,
    Market,
    MarketData,
    OrderDecision,
    OrderResponse,
    OrderUpdateEvent,
    OrderUpdateFill,
    OrderSide,
    OrderType,
)
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from tests.helpers.wallet_fixtures import make_backtest_wallet


class RouteWallet:
    def __init__(self) -> None:
        self.market_data: list[tuple[str, str, float]] = []
        self.orders: list[OrderResponse] = []

    def on_market_data(self, symbol: str, symbol_type: str, price: float) -> None:
        self.market_data.append((symbol, symbol_type, price))

    def on_order(self, symbol: str, symbol_type: str, order_resp: OrderResponse) -> None:
        self.orders.append(order_resp)


class StubOrderClient:
    def __init__(self) -> None:
        self.orders: list[tuple[OrderDecision, float, dict[str, Any]]] = []

    def place_order(
        self,
        _portfolio_id: int,
        decision: OrderDecision,
        mark_price: float,
        **kwargs: Any,
    ) -> ExecutionFeedback:
        self.orders.append((decision, mark_price, kwargs))
        return ExecutionFeedback(
            attempt_status="ACCEPTED",
            order=OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id=f"order-{len(self.orders)}",
                orig_qty=float(decision.qty),
                executed_qty=float(decision.qty),
                remaining_qty=0.0,
            ),
            fill_count=1,
            delta_qty=float(decision.qty),
        )


class FailingOrderClient(StubOrderClient):
    def place_order(self, *args: Any, **kwargs: Any) -> ExecutionFeedback:
        raise AssertionError("invalid batch must not place any order")


class LifecycleOrderClient(FailingOrderClient):
    def __init__(self, events: list[OrderUpdateEvent]) -> None:
        super().__init__()
        self.events = events

    def list_order_lifecycle_events(
        self,
        *,
        session_id: str,
        after_event_id: int = 0,
        limit: int = 100,
    ) -> list[OrderUpdateEvent]:
        if limit == 500:
            return []
        return [event for event in self.events if event.event_id > after_event_id]


class RecoveringFirstOrderClient(StubOrderClient):
    def __init__(self) -> None:
        super().__init__()
        self.attempted_routes: list[tuple[str, str, str]] = []

    def place_order(
        self,
        _portfolio_id: int,
        decision: OrderDecision,
        mark_price: float,
        **kwargs: Any,
    ) -> ExecutionFeedback:
        self.orders.append((decision, mark_price, kwargs))
        self.attempted_routes.append((decision.exchange, decision.market, decision.symbol))
        if len(self.orders) == 1:
            return ExecutionFeedback(
                attempt_id="attempt-recovering",
                attempt_status="RECOVERING",
                error_message="fill details pending",
                order=None,
                fill_count=0,
                delta_qty=0.0,
            )
        return ExecutionFeedback(
            attempt_status="ACCEPTED",
            order=OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id=f"order-{len(self.orders)}",
                orig_qty=float(decision.qty),
                executed_qty=float(decision.qty),
                remaining_qty=0.0,
            ),
            fill_count=1,
            delta_qty=float(decision.qty),
        )


class ExpiredNoFillOrderClient(StubOrderClient):
    def place_order(
        self,
        _portfolio_id: int,
        decision: OrderDecision,
        mark_price: float,
        **kwargs: Any,
    ) -> ExecutionFeedback:
        self.orders.append((decision, mark_price, kwargs))
        return ExecutionFeedback(
            attempt_id="attempt-expired",
            attempt_status="ACCEPTED",
            order=OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=0.0,
                fill_price=0.0,
                status="EXPIRED",
                order_id="order-expired",
                orig_qty=float(decision.qty),
                executed_qty=0.0,
                remaining_qty=float(decision.qty),
                price=mark_price,
            ),
            fill_count=0,
            delta_qty=0.0,
        )


class IocPartialExpiredOrderClient(StubOrderClient):
    def place_order(
        self,
        _portfolio_id: int,
        decision: OrderDecision,
        mark_price: float,
        **kwargs: Any,
    ) -> ExecutionFeedback:
        self.orders.append((decision, mark_price, kwargs))
        fill_qty = 0.004
        return ExecutionFeedback(
            attempt_id="attempt-ioc",
            attempt_status="ACCEPTED",
            order=OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=fill_qty,
                fill_price=mark_price,
                status="EXPIRED",
                order_id="order-ioc",
                orig_qty=float(decision.qty),
                executed_qty=fill_qty,
                remaining_qty=float(decision.qty) - fill_qty,
                price=mark_price,
            ),
            fill_count=1,
            delta_qty=fill_qty,
        )


def _tick(
    *,
    exchange: str = Exchange.BINANCE,
    market: str = Market.PERPETUAL_FUTURES,
    symbol: str = "ETHUSDT",
    interval: str = "1m",
    price: float = 2500.0,
) -> MarketData:
    return MarketData(
        exchange=exchange,
        market=market,
        symbol=symbol,
        interval=interval,
        price=price,
        timestamp=datetime.now(timezone.utc),
    )


def _portfolio(*routes: tuple[str, str]) -> PortfolioWalletRuntime:
    wallets = {
        (exchange, market, idx): RouteWallet()
        for idx, (exchange, market) in enumerate(routes, start=10)
    }
    return PortfolioWalletRuntime(1, set(routes), wallets)


def _real_futures_portfolio(symbol: str = "ETHUSDT") -> tuple[PortfolioWalletRuntime, Any]:
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": symbol,
                "position_side": "BOTH",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": 20,
                "initial_balance": 10_000,
                "fee_rate": 0.0,
                "margin_mode": "isolated",
            }
        ],
    )
    portfolio = PortfolioWalletRuntime(
        1,
        {(Exchange.BINANCE, Market.PERPETUAL_FUTURES)},
        {(Exchange.BINANCE, Market.PERPETUAL_FUTURES, 10): wallet},
    )
    return portfolio, wallet


def _base_strategy(
    strategy_path: str,
    wallet: PortfolioWalletRuntime,
    order_client: Any,
    *,
    strategy_code: str,
    **kwargs: Any,
) -> BaseStrategy:
    gate = gate_strategy_source(
        resolve_strategy_source(strategy_path, strategy_code),
        python_invocation_path=sys.executable,
    )
    assert gate.ok, gate.issues
    assert gate.gated_source is not None
    return BaseStrategy(
        prepare_strategy(gate.gated_source),
        wallet,
        order_client,
        **kwargs,
    )


def _strategy(body: str, *, order_targets: str | None = None) -> str:
    if order_targets is None:
        order_targets = (
            '[{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, '
            '"symbol": "ETHUSDT"}]'
        )
    return (
        "from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType\n"
        "\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]\n'
        f"    ORDER_TARGETS = {order_targets}\n"
        "    def on_market_data(self, data, wallet):\n"
        + body
    )


def _strategy_with_order_update_callback() -> str:
    return (
        "from strategy_service.types import Exchange, Market\n"
        "\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]\n'
        '    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT"}]\n'
        "    def __init__(self):\n"
        "        self.last_event_id = None\n"
        "        self.last_event_type = None\n"
        "        self.position_qty_seen = None\n"
        "    def on_order_update(self, event, wallet):\n"
        "        self.last_event_id = event.event_id\n"
        "        self.last_event_type = event.event_type\n"
        "        self.position_qty_seen = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES).futures.positions[(\"ETHUSDT\", 0)].position_qty\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )


def _order_update_event(
    *,
    event_id: int,
    session_id: str,
    event_type: str = "fill",
    side: str = OrderSide.BUY,
) -> OrderUpdateEvent:
    return OrderUpdateEvent(
        event_id=event_id,
        session_id=session_id,
        portfolio_id=1,
        venue_id=10,
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        side=side,
        position_side="both",
        event_type=event_type,
        order_status="FILLED",
        order_id=f"order-{event_id}",
        exchange_order_id=f"exchange-order-{event_id}",
        exchange_trade_id=f"trade-{event_id}",
        fill=OrderUpdateFill(
            symbol="ETHUSDT",
            qty=0.02,
            fill_price=2500.0,
            fee=0.0,
        ),
        orig_qty=0.02,
        executed_qty=0.02,
        remaining_qty=0.0,
        avg_price=2500.0,
        event_source="binance_user_data",
    )


def test_base_strategy_requires_portfolio_wallet_runtime() -> None:
    with pytest.raises(StrategySourceLoadError) as captured:
        _base_strategy(
            "inline.py",
            RouteWallet(),  # type: ignore[arg-type]
            StubOrderClient(),
            portfolio_id=1,
            strategy_code=_strategy("        return None\n", order_targets="[]"),
        )
    assert captured.value.reason == "binding_failed"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_order_targets_empty_but_strategy_returns_order_raises() -> None:
    wallet = _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES))
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET)\n',
            order_targets="[]",
        ),
    )

    with pytest.raises(ValueError, match="ORDER_TARGETS"):
        svc.running_strategy(_tick())
    assert client.orders == []


def test_list_order_decisions_are_all_placed_independently() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            '        return [\n'
            '            OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET),\n'
            '            OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.MARKET, price="2510"),\n'
            "        ]\n",
            order_targets=(
                '[{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT"}, '
                '{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT"}]'
            ),
        ),
    )

    svc.running_strategy(_tick(price=2510.0))

    assert [order[0].market for order in client.orders] == [
        Market.PERPETUAL_FUTURES,
        Market.SPOT,
    ]
    assert [order[1] for order in client.orders] == [2510.0, 2510.0]
    assert [order[2]["market"] for order in client.orders] == [
        Market.PERPETUAL_FUTURES,
        Market.SPOT,
    ]


def test_unresolved_order_blocks_only_same_portfolio_route_symbol() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    client = RecoveringFirstOrderClient()
    code = (
        "from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType\n"
        "\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = [\n"
        '        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT"},\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT"},\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT"},\n'
        "    ]\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return [\n"
        '            OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET),\n'
        '            OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.02", order_type=OrderType.MARKET),\n'
        '            OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.MARKET, price="2500"),\n'
        '            OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="BTCUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET, price="50000"),\n'
        "        ]\n"
    )
    svc = _base_strategy("inline.py", wallet, client, portfolio_id=1, strategy_code=code)

    svc.running_strategy(_tick(price=2500.0))

    assert client.attempted_routes == [
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "ETHUSDT"),
        (Exchange.BINANCE, Market.SPOT, "ETHUSDT"),
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "BTCUSDT"),
    ]
    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "ETHUSDT") in svc._blocked_order_keys
    assert (Exchange.BINANCE, Market.SPOT, "ETHUSDT") not in svc._blocked_order_keys
    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "BTCUSDT") not in svc._blocked_order_keys


def test_terminal_zero_fill_order_is_normal_noop(caplog: pytest.LogCaptureFixture) -> None:
    wallet, route_wallet = _real_futures_portfolio()
    client = ExpiredNoFillOrderClient()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        session_id="session-expired",
        strategy_code=_strategy(
            '        if not hasattr(self, "sent"):\n'
            "            self.sent = True\n"
            '            return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.02", order_type=OrderType.LIMIT, price="2500")\n'
            "        return None\n",
        ),
    )
    responses: list[ExecutionFeedback] = []
    svc._strategy_instance.on_order_response = responses.append

    caplog.set_level(logging.WARNING, logger="strategy_service.strategy.base")

    svc.running_strategy(_tick(price=2500.0))

    assert len(client.orders) == 1
    assert responses and responses[0].status == "EXPIRED"
    assert route_wallet.futures.open_orders == {}
    assert not any(
        "attempt accepted without confirmed fill details" in record.getMessage()
        for record in caplog.records
    )


def test_ioc_partial_expired_settles_filled_qty_without_open_order() -> None:
    wallet, route_wallet = _real_futures_portfolio()
    client = IocPartialExpiredOrderClient()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        session_id="session-ioc",
        strategy_code=_strategy(
            '        if not hasattr(self, "sent"):\n'
            "            self.sent = True\n"
            '            return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.02", order_type=OrderType.LIMIT, price="2500", time_in_force="IOC")\n'
            "        return None\n",
        ),
    )
    responses: list[ExecutionFeedback] = []
    svc._strategy_instance.on_order_response = responses.append

    svc.running_strategy(_tick(price=2500.0))

    assert len(client.orders) == 1
    decision, _mark_price, kwargs = client.orders[0]
    assert decision.time_in_force == "IOC"
    assert kwargs["market"] == Market.PERPETUAL_FUTURES
    assert responses and responses[0].status == "EXPIRED"
    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.004)
    assert route_wallet.futures.open_orders == {}
    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "ETHUSDT") not in svc._blocked_order_keys


def test_on_order_response_exception_is_logged_without_raising(caplog: pytest.LogCaptureFixture) -> None:
    wallet, route_wallet = _real_futures_portfolio()
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        session_id="session-response-callback",
        strategy_code=_strategy(
            '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.02", order_type=OrderType.MARKET)\n',
        ),
    )

    def broken_response(_response: ExecutionFeedback) -> None:
        raise RuntimeError("callback exploded")

    svc._strategy_instance.on_order_response = broken_response
    caplog.set_level(logging.WARNING, logger="strategy_service.strategy.base")

    svc.running_strategy(_tick(price=2500.0))

    assert len(client.orders) == 1
    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.02)
    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "STRATEGY_ORDER_RESPONSE_CALLBACK_FAILED "
        "session=session-response-callback strategy_id=0"
    ]
    assert "callback exploded" not in caplog.text
    assert "Traceback" not in caplog.text


def test_handle_order_update_invokes_callback_without_market_data_tick() -> None:
    wallet, route_wallet = _real_futures_portfolio()
    svc = _base_strategy(
        "inline.py",
        wallet,
        FailingOrderClient(),
        portfolio_id=1,
        session_id="session-push",
        strategy_code=_strategy_with_order_update_callback(),
    )

    updated = svc.handle_order_update(_order_update_event(event_id=88, session_id="session-push"))

    assert updated is True
    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.02)
    assert svc._strategy_instance.last_event_id == 88
    assert svc._strategy_instance.position_qty_seen == pytest.approx(0.02)


def test_handle_order_update_ignores_replayed_event_before_cursor() -> None:
    wallet, route_wallet = _real_futures_portfolio()
    svc = _base_strategy(
        "inline.py",
        wallet,
        FailingOrderClient(),
        portfolio_id=1,
        session_id="session-push",
        strategy_code=_strategy_with_order_update_callback(),
    )
    svc._order_event_cursor = 100

    updated = svc.handle_order_update(_order_update_event(event_id=99, session_id="session-push"))

    assert updated is False
    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.0)
    assert svc._strategy_instance.last_event_id is None


def test_liquidation_order_update_reaches_user_strategy_callback() -> None:
    wallet, route_wallet = _real_futures_portfolio()
    svc = _base_strategy(
        "inline.py",
        wallet,
        FailingOrderClient(),
        portfolio_id=1,
        session_id="session-liquidation",
        strategy_code=_strategy_with_order_update_callback(),
    )

    updated = svc.handle_order_update(_order_update_event(
        event_id=89,
        session_id="session-liquidation",
        event_type="liquidation",
        side=OrderSide.SELL,
    ))

    assert updated is True
    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(-0.02)
    assert svc._strategy_instance.last_event_type == "liquidation"
    assert svc._strategy_instance.position_qty_seen == pytest.approx(-0.02)


def test_batch_decisions_are_validated_before_any_order_is_placed() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    svc = _base_strategy(
        "inline.py",
        wallet,
        FailingOrderClient(),
        portfolio_id=1,
        strategy_code=_strategy(
            '        return [\n'
            '            OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET),\n'
            '            OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.MARKET),\n'
            "        ]\n",
        ),
    )

    with pytest.raises(ValueError, match="ORDER_TARGETS"):
        svc.running_strategy(_tick(price=2500.0))


def test_decision_for_other_route_uses_cached_route_mark_price() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    client = StubOrderClient()
    code = (
        "from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType\n"
        "\n"
        "class MyStrategy:\n"
        '    INPUTS = [\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"},\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT", "interval": "1m"},\n'
        "    ]\n"
        '    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        if data.trigger.market == Market.SPOT:\n"
        "            return None\n"
        '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.MARKET)\n'
    )
    svc = _base_strategy("inline.py", wallet, client, portfolio_id=1, strategy_code=code)

    svc.running_strategy(_tick(market=Market.SPOT, price=101.0))
    svc.running_strategy(_tick(market=Market.PERPETUAL_FUTURES, price=2500.0))

    assert len(client.orders) == 1
    assert client.orders[0][1] == pytest.approx(101.0)
    spot_wallet = wallet.get(Exchange.BINANCE, Market.SPOT)
    assert spot_wallet.market_data[-1] == ("ETHUSDT", "spot", 101.0)
    perp_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
    assert perp_wallet.market_data == [("ETHUSDT", "futures", 2500.0)]


def test_decision_without_route_tick_or_price_fails_before_order() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    svc = _base_strategy(
        "inline.py",
        wallet,
        FailingOrderClient(),
        portfolio_id=1,
        strategy_code=_strategy(
            '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.MARKET)\n',
            order_targets='[{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT"}]',
        ),
    )

    with pytest.raises(ValueError, match="mark price"):
        svc.running_strategy(_tick(market=Market.PERPETUAL_FUTURES, price=2500.0))


def test_strategy_receives_portfolio_wallet_and_uses_get() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            "        assert not hasattr(wallet, 'futures')\n"
            "        wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)\n"
            "        wallet.get(Exchange.BINANCE, Market.SPOT)\n"
            "        return None\n",
            order_targets=(
                '[{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT"}, '
                '{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT"}]'
            ),
        ),
    )

    svc.running_strategy(_tick(price=2520.0))

    perp_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
    assert perp_wallet.market_data == [("ETHUSDT", "futures", 2520.0)]
    assert client.orders == []


def test_invalid_decision_return_type_fails_closed() -> None:
    svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        StubOrderClient(),
        portfolio_id=1,
        strategy_code=_strategy('        return {"side": "BUY"}\n'),
    )

    with pytest.raises(ValueError, match="None, OrderDecision, or list"):
        svc.running_strategy(_tick())


@pytest.mark.parametrize(
    "qty",
    [0.01, "0", "-1"],
)
def test_qty_must_be_positive_decimal_string(qty: object) -> None:
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            f"        qty = {qty!r}\n"
            '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty=qty, order_type=OrderType.MARKET)\n'
        ),
    )

    with pytest.raises(ValueError, match="OrderDecision.qty"):
        svc.running_strategy(_tick())
    assert client.orders == []


@pytest.mark.parametrize(
    "price",
    [2500.0, "0", "-1"],
)
def test_price_when_present_must_be_positive_decimal_string(price: object) -> None:
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            f"        price = {price!r}\n"
            '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.LIMIT, price=price)\n'
        ),
    )

    with pytest.raises(ValueError, match="OrderDecision.price"):
        svc.running_strategy(_tick())
    assert client.orders == []


@pytest.mark.parametrize("value", ["NaN", "Infinity", "1e999999"])
def test_qty_and_price_must_be_finite_decimal_strings(value: str) -> None:
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            f'        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty={value!r}, order_type=OrderType.MARKET)\n'
        ),
    )

    with pytest.raises(ValueError, match="finite"):
        svc.running_strategy(_tick())
    assert client.orders == []

    price_client = StubOrderClient()
    price_svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        price_client,
        portfolio_id=1,
        strategy_code=_strategy(
            f'        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.LIMIT, price={value!r})\n'
        ),
    )

    with pytest.raises(ValueError, match="finite"):
        price_svc.running_strategy(_tick())
    assert price_client.orders == []


@pytest.mark.parametrize("value", ["1e-324", "1e-999999"])
def test_qty_and_price_must_not_underflow_to_zero(value: str) -> None:
    qty_svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        StubOrderClient(),
        portfolio_id=1,
        strategy_code=_strategy(
            f'        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty={value!r}, order_type=OrderType.MARKET)\n'
        ),
    )
    with pytest.raises(ValueError, match="finite"):
        qty_svc.running_strategy(_tick())

    price_svc = _base_strategy(
        "inline.py",
        _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES)),
        StubOrderClient(),
        portfolio_id=1,
        strategy_code=_strategy(
            f'        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.LIMIT, price={value!r})\n'
        ),
    )
    with pytest.raises(ValueError, match="finite"):
        price_svc.running_strategy(_tick())


def test_lifecycle_fill_missing_route_does_not_update_wallet_or_unblock() -> None:
    event = OrderUpdateEvent(
        event_id=1,
        session_id="session-1",
        portfolio_id=1,
        venue_id=10,
        exchange="",
        market="",
        side="BUY",
        position_side="both",
        event_type="fill",
        order_status="FILLED",
        order_id="order-1",
        fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.1, fill_price=2500.0),
        orig_qty=0.1,
        executed_qty=0.1,
        remaining_qty=0.0,
    )
    wallet = _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES))
    svc = _base_strategy(
        "inline.py",
        wallet,
        LifecycleOrderClient([event]),
        portfolio_id=1,
        session_id="session-1",
        strategy_code=_strategy("        return None\n", order_targets="[]"),
    )
    blocked_key = (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "ETHUSDT")
    svc._blocked_order_keys.add(blocked_key)

    svc.running_strategy(_tick())

    assert wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES).orders == []
    assert blocked_key in svc._blocked_order_keys


def test_lifecycle_updates_settle_into_matching_venue_wallets() -> None:
    events = [
        OrderUpdateEvent(
            event_id=1,
            session_id="session-1",
            portfolio_id=1,
            venue_id=10,
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            side="BUY",
            position_side="both",
            event_type="fill",
            order_status="FILLED",
            order_id="perp-order",
            fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.1, fill_price=2500.0),
            orig_qty=0.1,
            executed_qty=0.1,
            remaining_qty=0.0,
        ),
        OrderUpdateEvent(
            event_id=2,
            session_id="session-1",
            portfolio_id=1,
            venue_id=11,
            exchange=Exchange.BINANCE,
            market=Market.SPOT,
            side="BUY",
            position_side="both",
            event_type="fill",
            order_status="FILLED",
            order_id="spot-order",
            fill=OrderUpdateFill(symbol="ETHUSDT", qty=1.0, fill_price=2501.0),
            orig_qty=1.0,
            executed_qty=1.0,
            remaining_qty=0.0,
        ),
    ]
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    svc = _base_strategy(
        "inline.py",
        wallet,
        LifecycleOrderClient(events),
        portfolio_id=1,
        session_id="session-1",
        strategy_code=_strategy("        return None\n", order_targets="[]"),
    )

    svc.running_strategy(_tick())

    perp_wallet = wallet.wallets[(Exchange.BINANCE, Market.PERPETUAL_FUTURES, 10)]
    spot_wallet = wallet.wallets[(Exchange.BINANCE, Market.SPOT, 11)]
    assert [order.order_id for order in perp_wallet.orders] == ["perp-order"]
    assert [order.order_id for order in spot_wallet.orders] == ["spot-order"]
    assert perp_wallet.orders[0].qty == pytest.approx(0.1)
    assert spot_wallet.orders[0].qty == pytest.approx(1.0)


def test_lifecycle_fills_with_same_order_id_are_settled_by_event_id() -> None:
    events = [
        OrderUpdateEvent(
            event_id=1,
            session_id="session-1",
            portfolio_id=1,
            venue_id=10,
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            side="BUY",
            position_side="both",
            event_type="fill",
            order_status="PARTIALLY_FILLED",
            order_id="order-1",
            fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.04, fill_price=2500.0),
            orig_qty=0.1,
            executed_qty=0.04,
            remaining_qty=0.06,
        ),
        OrderUpdateEvent(
            event_id=2,
            session_id="session-1",
            portfolio_id=1,
            venue_id=10,
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            side="BUY",
            position_side="both",
            event_type="fill",
            order_status="FILLED",
            order_id="order-1",
            fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.06, fill_price=2510.0),
            orig_qty=0.1,
            executed_qty=0.1,
            remaining_qty=0.0,
        ),
    ]
    wallet = _portfolio((Exchange.BINANCE, Market.PERPETUAL_FUTURES))
    svc = _base_strategy(
        "inline.py",
        wallet,
        LifecycleOrderClient(events),
        portfolio_id=1,
        session_id="session-1",
        strategy_code=_strategy("        return None\n", order_targets="[]"),
    )

    svc.running_strategy(_tick())

    route_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
    assert [order.qty for order in route_wallet.orders] == pytest.approx([0.04, 0.06])


def test_lifecycle_replay_of_synchronous_fill_is_not_settled_twice() -> None:
    event = OrderUpdateEvent(
        event_id=1,
        session_id="session-1",
        portfolio_id=1,
        venue_id=10,
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        side="BUY",
        position_side="both",
        event_type="fill",
        order_status="FILLED",
        order_id="order-sync",
        fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.1, fill_price=2500.0),
        orig_qty=0.1,
        executed_qty=0.1,
        remaining_qty=0.0,
    )

    class SyncThenLifecycleClient(LifecycleOrderClient):
        def __init__(self) -> None:
            super().__init__([event])
            self.place_calls = 0

        def list_order_lifecycle_events(self, *, session_id: str, after_event_id: int = 0, limit: int = 100) -> list[OrderUpdateEvent]:
            if self.place_calls <= 0:
                return []
            return super().list_order_lifecycle_events(session_id=session_id, after_event_id=after_event_id, limit=limit)

        def place_order(self, _portfolio_id: int, decision: OrderDecision, mark_price: float, **_kwargs: Any) -> ExecutionFeedback:
            self.place_calls += 1
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=OrderResponse(
                    symbol=decision.symbol,
                    side=decision.side,
                    qty=0.1,
                    fill_price=mark_price,
                    status="FILLED",
                    order_id="order-sync",
                    orig_qty=0.1,
                    executed_qty=0.1,
                    remaining_qty=0.0,
                ),
                fill_count=1,
                delta_qty=0.1,
            )

    client = SyncThenLifecycleClient()
    wallet, route_wallet = _real_futures_portfolio()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        session_id="session-1",
        strategy_code=_strategy(
            '        if not hasattr(self, "sent"):\n'
            "            self.sent = True\n"
            '            return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.1", order_type=OrderType.MARKET)\n'
            "        return None\n",
        ),
    )

    svc.running_strategy(_tick())
    svc.running_strategy(_tick(price=2501.0))

    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.1)
    assert route_wallet.futures.open_orders == {}
    assert client.place_calls == 1


def test_lifecycle_after_synchronous_partial_settles_only_new_delta() -> None:
    event = OrderUpdateEvent(
        event_id=1,
        session_id="session-1",
        portfolio_id=1,
        venue_id=10,
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        side="BUY",
        position_side="both",
        event_type="fill",
        order_status="PARTIALLY_FILLED",
        order_id="order-partial",
        fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.2, fill_price=2510.0),
        orig_qty=0.2,
        executed_qty=0.2,
        remaining_qty=0.0,
    )

    class PartialThenLifecycleClient(LifecycleOrderClient):
        def __init__(self) -> None:
            super().__init__([event])
            self.place_calls = 0

        def list_order_lifecycle_events(self, *, session_id: str, after_event_id: int = 0, limit: int = 100) -> list[OrderUpdateEvent]:
            if self.place_calls <= 0:
                return []
            return super().list_order_lifecycle_events(session_id=session_id, after_event_id=after_event_id, limit=limit)

        def place_order(self, _portfolio_id: int, decision: OrderDecision, mark_price: float, **_kwargs: Any) -> ExecutionFeedback:
            self.place_calls += 1
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=OrderResponse(
                    symbol=decision.symbol,
                    side=decision.side,
                    qty=0.1,
                    fill_price=mark_price,
                    status="PARTIALLY_FILLED",
                    order_id="order-partial",
                    orig_qty=0.2,
                    executed_qty=0.1,
                    remaining_qty=0.1,
                ),
                fill_count=1,
                delta_qty=0.1,
            )

    client = PartialThenLifecycleClient()
    wallet, route_wallet = _real_futures_portfolio()
    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        session_id="session-1",
        strategy_code=_strategy(
            '        if not hasattr(self, "sent"):\n'
            "            self.sent = True\n"
            '            return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.2", order_type=OrderType.MARKET)\n'
            "        return None\n",
        ),
    )

    svc.running_strategy(_tick())
    svc.running_strategy(_tick(price=2510.0))

    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.2)
    assert route_wallet.futures.open_orders == {}
    assert client.place_calls == 1


def test_lifecycle_after_partial_close_short_applies_rest_recovery_fill() -> None:
    event = OrderUpdateEvent(
        event_id=1,
        session_id="session-1",
        portfolio_id=1,
        venue_id=10,
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        side="BUY",
        position_side="both",
        event_type="fill",
        event_source="rest_recovery",
        order_status="FILLED",
        order_id="order-close-short",
        fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.016, fill_price=2510.0),
        orig_qty=0.02,
        executed_qty=0.02,
        remaining_qty=0.0,
    )

    class PartialCloseThenRecoveryClient(LifecycleOrderClient):
        def __init__(self) -> None:
            super().__init__([event])
            self.place_calls = 0

        def list_order_lifecycle_events(self, *, session_id: str, after_event_id: int = 0, limit: int = 100) -> list[OrderUpdateEvent]:
            if self.place_calls <= 0:
                return []
            return super().list_order_lifecycle_events(session_id=session_id, after_event_id=after_event_id, limit=limit)

        def place_order(self, _portfolio_id: int, decision: OrderDecision, mark_price: float, **_kwargs: Any) -> ExecutionFeedback:
            self.place_calls += 1
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=OrderResponse(
                    symbol=decision.symbol,
                    side=decision.side,
                    qty=0.004,
                    fill_price=mark_price,
                    status="PARTIALLY_FILLED",
                    order_id="order-close-short",
                    orig_qty=0.02,
                    executed_qty=0.004,
                    remaining_qty=0.016,
                ),
                fill_count=1,
                delta_qty=0.004,
            )

    client = PartialCloseThenRecoveryClient()
    wallet, route_wallet = _real_futures_portfolio()
    pos = route_wallet.futures.positions[("ETHUSDT", 0)]
    pos.position_qty = -2.0
    pos.entry_price = 2500.0
    pos.mark_price = 2500.0
    pos._refresh_derived_fields()

    svc = _base_strategy(
        "inline.py",
        wallet,
        client,
        portfolio_id=1,
        session_id="session-1",
        strategy_code=_strategy(
            '        if not hasattr(self, "sent"):\n'
            "            self.sent = True\n"
            '            return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.02", order_type=OrderType.MARKET)\n'
            "        return None\n",
        ),
    )

    svc.running_strategy(_tick(price=2500.0))
    assert route_wallet.futures.positions[("ETHUSDT", 0)].position_qty == pytest.approx(-1.996)

    svc.running_strategy(_tick(price=2510.0))

    assert route_wallet.futures.positions[("ETHUSDT", 0)].position_qty == pytest.approx(-1.98)
    assert route_wallet.futures.open_orders == {}
    assert client.place_calls == 1


def test_limit_order_with_ambiguous_cached_mark_price_fails_even_with_price() -> None:
    wallet = _portfolio(
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    )
    code = (
        "from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType\n"
        "\n"
        "class MyStrategy:\n"
        '    INPUTS = [\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT", "interval": "1m"},\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT", "interval": "5m"},\n'
        '        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"},\n'
        "    ]\n"
        '    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "ETHUSDT"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        if data.trigger.market == Market.SPOT:\n"
        "            return None\n"
        '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.LIMIT, price="99")\n'
    )
    svc = _base_strategy("inline.py", wallet, FailingOrderClient(), portfolio_id=1, strategy_code=code)
    svc.running_strategy(_tick(market=Market.SPOT, interval="1m", price=100.0))
    svc.running_strategy(_tick(market=Market.SPOT, interval="5m", price=101.0))

    with pytest.raises(ValueError, match="ambiguous mark price"):
        svc.running_strategy(_tick(market=Market.PERPETUAL_FUTURES, interval="1m", price=2500.0))


def test_undeclared_order_target_fails_closed() -> None:
    client = StubOrderClient()
    svc = _base_strategy(
        "inline.py",
        _portfolio(
            (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
            (Exchange.BINANCE, Market.SPOT),
        ),
        client,
        portfolio_id=1,
        strategy_code=_strategy(
            '        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="ETHUSDT", side=OrderSide.BUY, qty="1", order_type=OrderType.MARKET)\n'
        ),
    )

    with pytest.raises(ValueError, match="ORDER_TARGETS"):
        svc.running_strategy(_tick())
    assert client.orders == []
