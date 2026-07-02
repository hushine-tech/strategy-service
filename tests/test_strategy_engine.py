import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from strategy_service import (
    ExecutionFeedback,
    MarketData,
    OrderResponse,
    OrderUpdateEvent,
    OrderUpdateFill,
    StrategyEngine,
)
from strategy_service.strategy.base import _load_strategy_instance
from strategy_service.wallet import SpotAsset
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from tests.helpers.order_client import FilledOrderClient
from tests.helpers.wallet_fixtures import make_backtest_wallet


class _TestPortfolioWalletRuntime(PortfolioWalletRuntime):
    def __init__(self, default_wallet, routes: set[tuple[str, str]] | None = None) -> None:
        route_set = routes or {("binance", "perpetual_futures")}
        wallets = {
            (exchange, market, idx): default_wallet
            for idx, (exchange, market) in enumerate(sorted(route_set), start=10)
        }
        super().__init__(1, route_set, wallets)
        self._default_wallet = default_wallet

    def __getattr__(self, name):
        return getattr(self._default_wallet, name)


def _portfolio_wallet(default_wallet, *routes: tuple[str, str]) -> _TestPortfolioWalletRuntime:
    return _TestPortfolioWalletRuntime(default_wallet, set(routes) if routes else None)


def _md(
    symbol: str = "TESTUSDT",
    price: float = 50_000.0,
    market: str = "perpetual_futures",
    interval: str = "1m",
    exchange: str = "binance",
    timestamp: datetime | None = None,
) -> MarketData:
    return MarketData(
        symbol=symbol,
        price=price,
        timestamp=timestamp or datetime.now(timezone.utc),
        exchange=exchange,
        market=market,
        interval=interval,
    )


def _wallet_with_futures_slot(
    symbol: str = "TESTUSDT",
    *,
    margin_mode: str = "isolated",
    position_mode: str = "one_way",
    initial_balance: float = 10_000.0,
    leverage: float = 20.0,
    fee_rate: float = 0.0004,
):
    """Build a backtest ``BinanceWalletRuntime`` with one (or two in hedge)
    futures position slots preconfigured.

    Post-Phase-C2b this goes through the canonical proto path; the returned
    runtime's ``.futures.positions`` dict exposes the same ``(symbol, dir)``
    keying used by legacy fixtures (``(sym, 0)`` for one-way,
    ``(sym, +1)``/``(sym, -1)`` for hedge).
    """
    sym = symbol.strip().upper()
    account_initial_balance = initial_balance if margin_mode == "cross" else 0.0
    position_initial_balance = initial_balance if margin_mode == "isolated" else 0.0

    if position_mode == "hedge":
        futures_positions = [
            {
                "symbol": sym,
                "position_side": "LONG",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": leverage,
                "initial_balance": position_initial_balance,
                "fee_rate": fee_rate,
                "margin_mode": margin_mode,
            },
            {
                "symbol": sym,
                "position_side": "SHORT",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": leverage,
                "initial_balance": position_initial_balance,
                "fee_rate": fee_rate,
                "margin_mode": margin_mode,
            },
        ]
    else:
        futures_positions = [
            {
                "symbol": sym,
                "position_side": "BOTH",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": leverage,
                "initial_balance": position_initial_balance,
                "fee_rate": fee_rate,
                "margin_mode": margin_mode,
            },
        ]

    wallet = make_backtest_wallet(
        margin_mode=margin_mode,
        position_mode=position_mode,
        wallet_balance=account_initial_balance,
        initial_balance=account_initial_balance,
        futures_positions=futures_positions,
    )
    return _portfolio_wallet(wallet, ("binance", "perpetual_futures"), ("binance", "spot"))


def _wallet_with_spot_slot(symbol: str = "TESTUSDT"):
    """Build a backtest wallet runtime with one spot asset slot preconfigured."""
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        spot_assets=[{"symbol": symbol.strip().upper()}],
    )
    return _portfolio_wallet(wallet, ("binance", "perpetual_futures"), ("binance", "spot"))


# Helper to build inline strategy code with INPUTS auto-inserted.
def _inline(body: str, *, symbol: str = "TESTUSDT", market: str = "perpetual_futures", interval: str = "1m") -> str:
    return (
        "from strategy_service.types import OrderDecision as _OrderDecision\n"
        "\n"
        "def OrderDecision(symbol, side, qty, price=None, market=None, exchange=None, order_type=None, **kwargs):\n"
        f"    raw_market = market or \"{market}\"\n"
        "    resolved_market = {\"futures\": \"perpetual_futures\"}.get(str(raw_market), str(raw_market))\n"
        "    resolved_order_type = order_type or (\"LIMIT\" if price is not None else \"MARKET\")\n"
        "    return _OrderDecision(\n"
        "        exchange=exchange or \"binance\",\n"
        "        market=resolved_market,\n"
        "        symbol=symbol,\n"
        "        side=str(side).upper(),\n"
        "        qty=str(qty),\n"
        "        order_type=resolved_order_type,\n"
        "        price=str(price) if price is not None else None,\n"
        "        **kwargs,\n"
        "    )\n"
        "\n"
        "class MyStrategy:\n"
        f'    INPUTS = [{{"exchange": "binance", "market": "{market}", "symbol": "{symbol}", "interval": "{interval}"}}]\n'
        f'    ORDER_TARGETS = [{{"exchange": "binance", "market": "{market}", "symbol": "{symbol}"}}]\n'
        + body
    )


def test_inline_strategy_code_uses_strategy_path_as_python_filename():
    code = (
        "class MyStrategy:\n"
        "    INPUTS = [{\"exchange\": \"binance\", \"market\": \"perpetual_futures\", \"symbol\": \"TESTUSDT\", \"interval\": \"1m\"}]\n"
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    strategy = _load_strategy_instance("/workspace/self_hosted_strategy.py", strategy_code=code)

    assert strategy.on_market_data.__code__.co_filename == "/workspace/self_hosted_strategy.py"


def _hot_reload_strategy_code(marker: str, *, symbol: str = "TESTUSDT") -> str:
    return (
        "class MyStrategy:\n"
        f'    INPUTS = [{{"exchange": "binance", "market": "perpetual_futures", "symbol": "{symbol}", "interval": "1m"}}]\n'
        f'    ORDER_TARGETS = [{{"exchange": "binance", "market": "perpetual_futures", "symbol": "{symbol}"}}]\n'
        "    def __init__(self):\n"
        f"        self.marker = {marker!r}\n"
        "        self.market_calls = 0\n"
        "        self.order_update_calls = []\n"
        "    def on_market_data(self, data, wallet):\n"
        "        self.market_calls += 1\n"
        "        self.last_market_symbol = data.symbol\n"
        "        return None\n"
        "    def on_order_update(self, event, wallet):\n"
        "        self.order_update_calls.append((self.marker, event.event_id))\n"
    )


def _replace_strategy_file(path: Path, code: str) -> None:
    time.sleep(0.01)
    path.write_text(code, encoding="utf-8")


def test_bare_hot_reload_replaces_user_strategy_when_file_changes(tmp_path: Path):
    strategy_path = tmp_path / "mystrategy.py"
    strategy_path.write_text(_hot_reload_strategy_code("v1"), encoding="utf-8")
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        str(strategy_path),
        wallet,
        hot_reload=True,
    )

    svc.running_strategy(_md())
    assert strat._get_strategy().marker == "v1"
    assert strat._get_strategy().market_calls == 1

    _replace_strategy_file(strategy_path, _hot_reload_strategy_code("v2"))

    svc.running_strategy(_md())

    assert strat._get_strategy().marker == "v2"
    assert strat._get_strategy().market_calls == 1
    assert strat._get_strategy().on_market_data.__code__.co_filename == str(strategy_path)


def test_bare_hot_reload_runs_before_order_update_callback(tmp_path: Path):
    strategy_path = tmp_path / "mystrategy.py"
    strategy_path.write_text(_hot_reload_strategy_code("v1"), encoding="utf-8")
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        str(strategy_path),
        wallet,
        session_id="session-1",
        hot_reload=True,
    )
    _replace_strategy_file(strategy_path, _hot_reload_strategy_code("v2"))

    delivered = svc.handle_order_update(OrderUpdateEvent(
        event_id=8,
        session_id="session-1",
        account_id=1,
        venue_id=10,
        exchange="binance",
        market="perpetual_futures",
        side="BUY",
        position_side="both",
        event_type="accepted",
        order_status="NEW",
        order_id="order-8",
    ))

    assert delivered is True
    assert strat._get_strategy().marker == "v2"
    assert strat._get_strategy().order_update_calls == [("v2", 8)]


def test_bare_hot_reload_rejects_declaration_changes(tmp_path: Path, caplog):
    strategy_path = tmp_path / "mystrategy.py"
    strategy_path.write_text(_hot_reload_strategy_code("v1"), encoding="utf-8")
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        str(strategy_path),
        wallet,
        hot_reload=True,
    )
    svc.running_strategy(_md())
    _replace_strategy_file(strategy_path, _hot_reload_strategy_code("v2", symbol="BTCUSDT"))

    svc.running_strategy(_md())

    assert strat._get_strategy().marker == "v1"
    assert strat._get_strategy().market_calls == 2
    assert "strategy hot reload skipped: declaration changed" in caplog.text


def test_bare_hot_reload_user_code_error_keeps_session_alive_until_file_is_fixed(tmp_path: Path, caplog):
    strategy_path = tmp_path / "mystrategy.py"
    strategy_path.write_text(
        _hot_reload_strategy_code("broken").replace(
            "        self.market_calls += 1\n",
            "        missing_name += 1\n",
        ),
        encoding="utf-8",
    )
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    surfaced_errors: list[str] = []
    recovered: list[bool] = []
    strat = svc.create_strategy(
        "u1",
        str(strategy_path),
        wallet,
        hot_reload=True,
        on_user_code_error=surfaced_errors.append,
        on_user_code_recovered=lambda: recovered.append(True),
    )

    svc.running_strategy(_md())
    assert strat._get_strategy().marker == "broken"
    assert surfaced_errors
    assert "user strategy on_market_data failed: UnboundLocalError" in surfaced_errors[-1]
    assert "user strategy on_market_data failed: UnboundLocalError" in caplog.text

    _replace_strategy_file(strategy_path, _hot_reload_strategy_code("fixed"))
    svc.running_strategy(_md())

    assert strat._get_strategy().marker == "fixed"
    assert strat._get_strategy().market_calls == 1
    assert recovered == [True]


def test_running_strategy_no_signal_does_not_call_on_order():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.noop", wallet)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    svc.running_strategy(_md())
    wallet.on_order.assert_not_called()


def test_user_on_market_data_exception_is_wrapped_as_user_code_error():
    wallet = _wallet_with_futures_slot()
    strategy_code = _inline(
        "    def on_market_data(self, data, wallet):\n"
        "        raise RuntimeError('boom from strategy')\n"
    )
    svc = StrategyEngine()
    svc.create_strategy("u1", "<db:user_boom>", wallet, strategy_code=strategy_code)

    with pytest.raises(RuntimeError, match="user strategy on_market_data failed: RuntimeError: boom from strategy"):
        svc.running_strategy(_md())


def test_running_strategy_with_signal_calls_on_order():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.buy_once", wallet, order_client=FilledOrderClient())
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    svc.running_strategy(_md(price=51_000.0))
    wallet.on_order.assert_called_once()
    args, _kwargs = wallet.on_order.call_args
    assert args[3] == "TESTUSDT"
    assert args[4] == "futures"
    assert args[5].status.upper() == "FILLED"
    assert args[5].fill_price == 51_000.0


def test_on_order_response_called_when_defined():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1", "strategies.buy_with_callback", wallet, order_client=FilledOrderClient()
    )
    svc.running_strategy(_md())
    assert strat._strategy_instance is not None
    assert strat._strategy_instance.last_resp is not None
    assert strat._strategy_instance.last_resp.qty == 0.05


def test_multiple_fill_events_are_applied_sequentially_to_wallet():
    wallet = _wallet_with_futures_slot()

    class FakeOrderClient:
        def place_order(self, *_args, **_kwargs):
            return ExecutionFeedback(
                intent_id="intent-1",
                attempt_id="attempt-1",
                attempt_status="ACCEPTED",
                order=OrderResponse(
                    symbol="TESTUSDT",
                    side="BUY",
                    qty=0.05,
                    fill_price=51200.0,
                    status="FILLED",
                    order_id="order-1",
                    orig_qty=0.05,
                    executed_qty=0.05,
                    remaining_qty=0.0,
                ),
                fill_count=2,
                delta_qty=0.05,
                fill_events=[
                    OrderResponse(
                        symbol="TESTUSDT",
                        side="BUY",
                        qty=0.02,
                        fill_price=50000.0,
                        status="PARTIALLY_FILLED",
                        fee=0.1,
                        order_id="order-1",
                        orig_qty=0.05,
                        executed_qty=0.02,
                        remaining_qty=0.03,
                    ),
                    OrderResponse(
                        symbol="TESTUSDT",
                        side="BUY",
                        qty=0.03,
                        fill_price=52000.0,
                        status="FILLED",
                        fee=0.2,
                        order_id="order-1",
                        orig_qty=0.05,
                        executed_qty=0.05,
                        remaining_qty=0.0,
                    ),
                ],
            )

    svc = StrategyEngine()
    strat = svc.create_strategy("u1", "strategies.buy_once", wallet, order_client=FakeOrderClient())
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    strat.on_order_callback = MagicMock()

    svc.running_strategy(_md(price=51_000.0))

    assert wallet.on_order.call_count == 2
    args1, _ = wallet.on_order.call_args_list[0]
    args2, _ = wallet.on_order.call_args_list[1]
    assert args1[5].status == "PARTIALLY_FILLED"
    assert args1[5].qty == pytest.approx(0.02)
    assert args2[5].status == "FILLED"
    assert args2[5].qty == pytest.approx(0.03)
    assert wallet.futures.positions[("TESTUSDT", 0)].position_qty == pytest.approx(0.05)
    assert wallet.futures.positions[("TESTUSDT", 0)].entry_price == pytest.approx(51200.0)
    strat.on_order_callback.assert_called_once()


def test_order_update_event_updates_wallet_before_callback():
    wallet = _wallet_with_futures_slot()
    code = _inline(
        "    def __init__(self):\n"
        "        self.events = []\n"
        "        self.position_qty_seen = None\n"
        "    def on_order_update(self, event, wallet):\n"
        "        self.events.append(event)\n"
        "        self.position_qty_seen = wallet.futures.positions[(\"TESTUSDT\", 0)].position_qty\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    class FakeOrderClient:
        def __init__(self):
            self.place_calls = 0

        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            assert session_id == "session-1"
            if limit == 500:
                return []
            if after_event_id >= 1:
                return []
            return [
                OrderUpdateEvent(
                    event_id=1,
                    session_id="session-1",
                    account_id=1,
                    venue_id=10,
                    exchange="binance",
                    market="perpetual_futures",
                    side="BUY",
                    position_side="both",
                    event_type="fill",
                    order_status="FILLED",
                    order_id="order-1",
                    fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.1, fill_price=50_000.0),
                )
            ]

        def place_order(self, *_args, **_kwargs):
            self.place_calls += 1
            raise AssertionError("on_order_update return value must not place orders")

    client = FakeOrderClient()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=client,
        session_id="session-1",
        strategy_code=code,
    )
    wallet.on_order = MagicMock(wraps=wallet.on_order)

    svc.running_strategy(_md(price=50_000.0))

    wallet.on_order.assert_called_once()
    assert strat._strategy_instance.events[0].order_id == "order-1"
    assert strat._strategy_instance.position_qty_seen == pytest.approx(0.1)
    assert client.place_calls == 0


def test_strategy_engine_routes_order_update_to_matching_session():
    wallet1 = _wallet_with_futures_slot()
    wallet2 = _wallet_with_futures_slot()
    code = _inline(
        "    def __init__(self):\n"
        "        self.event_ids = []\n"
        "    def on_order_update(self, event, wallet):\n"
        "        self.event_ids.append(event.event_id)\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    svc = StrategyEngine()
    strat1 = svc.create_strategy(
        "u1",
        "/workspace/strategy1.py",
        wallet1,
        session_id="session-1",
        strategy_code=code,
    )
    strat2 = svc.create_strategy(
        "u2",
        "/workspace/strategy2.py",
        wallet2,
        session_id="session-2",
        strategy_code=code,
    )

    delivered = svc.handle_order_update(OrderUpdateEvent(
        event_id=5,
        session_id="session-1",
        account_id=1,
        venue_id=10,
        exchange="binance",
        market="perpetual_futures",
        side="BUY",
        position_side="both",
        event_type="fill",
        order_status="FILLED",
        order_id="order-5",
        fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.1, fill_price=50_000.0),
    ))

    assert delivered is True
    assert strat1._strategy_instance.event_ids == [5]
    assert strat2._strategy_instance.event_ids == []
    assert wallet1.futures.positions[("TESTUSDT", 0)].position_qty == pytest.approx(0.1)
    assert wallet2.futures.positions[("TESTUSDT", 0)].position_qty == pytest.approx(0.0)


def test_async_order_update_unblocks_symbol_and_triggers_snapshot_callback():
    wallet = _wallet_with_futures_slot()
    code = _inline(
        "    def __init__(self):\n"
        "        self.market_calls = 0\n"
        "    def on_order_update(self, event, wallet):\n"
        "        pass\n"
        "    def on_market_data(self, data, wallet):\n"
        "        self.market_calls += 1\n"
        "        return OrderDecision(symbol=\"TESTUSDT\", side=\"BUY\", qty=0.01, market=\"futures\")\n"
    )

    class FakeOrderClient:
        def __init__(self):
            self.place_calls = 0

        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            if limit == 500:
                return []
            if after_event_id >= 1:
                return []
            return [
                OrderUpdateEvent(
                    event_id=1,
                    session_id=session_id,
                    account_id=1,
                    venue_id=10,
                    exchange="binance",
                    market="perpetual_futures",
                    side="BUY",
                    position_side="both",
                    event_type="fill",
                    order_status="FILLED",
                    order_id="order-async",
                    fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.1, fill_price=50_000.0),
                    orig_qty=0.1,
                    executed_qty=0.1,
                    remaining_qty=0.0,
                )
            ]

        def place_order(self, *_args, **_kwargs):
            self.place_calls += 1
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=OrderResponse(
                    symbol="TESTUSDT",
                    side="BUY",
                    qty=0.01,
                    fill_price=50_000.0,
                    status="FILLED",
                    order_id="order-new",
                    orig_qty=0.01,
                    executed_qty=0.01,
                    remaining_qty=0.0,
                ),
                fill_count=1,
                delta_qty=0.01,
            )

    client = FakeOrderClient()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=client,
        session_id="session-1",
        strategy_code=code,
    )
    strat.on_order_callback = MagicMock()
    blocked_key = ("binance", "perpetual_futures", "TESTUSDT")
    strat._blocked_order_keys.add(blocked_key)

    svc.running_strategy(_md(price=50_000.0))

    assert blocked_key not in strat._blocked_order_keys
    strat.on_order_callback.assert_called()
    assert client.place_calls == 1


def test_async_partial_order_update_keeps_symbol_blocked():
    wallet = _wallet_with_futures_slot()
    code = _inline(
        "    def on_order_update(self, event, wallet):\n"
        "        pass\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(symbol=\"TESTUSDT\", side=\"BUY\", qty=0.01, market=\"futures\")\n"
    )

    class FakeOrderClient:
        def __init__(self):
            self.place_calls = 0

        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            if limit == 500 or after_event_id >= 1:
                return []
            return [
                OrderUpdateEvent(
                    event_id=1,
                    session_id=session_id,
                    account_id=1,
                    venue_id=10,
                    exchange="binance",
                    market="perpetual_futures",
                    side="BUY",
                    position_side="both",
                    event_type="fill",
                    order_status="PARTIALLY_FILLED",
                    order_id="order-async",
                    fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.04, fill_price=50_000.0),
                    orig_qty=0.1,
                    executed_qty=0.04,
                    remaining_qty=0.06,
                )
            ]

        def place_order(self, *_args, **_kwargs):
            self.place_calls += 1
            raise AssertionError("blocked symbol should not place a new order")

    client = FakeOrderClient()
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=client,
        session_id="session-1",
        strategy_code=code,
    )
    blocked_key = ("binance", "perpetual_futures", "TESTUSDT")
    strat._blocked_order_keys.add(blocked_key)

    svc.running_strategy(_md(price=50_000.0))

    assert blocked_key in strat._blocked_order_keys
    assert client.place_calls == 0


def test_force_close_terminal_event_unblocks_route_without_wallet_settlement():
    wallet = _wallet_with_futures_slot()
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    code = _inline(
        "    def __init__(self):\n"
        "        self.events = []\n"
        "    def on_order_update(self, event, wallet):\n"
        "        self.events.append(event)\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    class FakeOrderClient:
        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            if limit == 500 or after_event_id >= 7:
                return []
            return [
                OrderUpdateEvent(
                    event_id=7,
                    session_id=session_id,
                    account_id=1,
                    venue_id=10,
                    exchange="binance",
                    market="perpetual_futures",
                    side="BUY",
                    position_side="both",
                    event_type="terminal",
                    order_status="RECOVERY_EXPIRED",
                    symbol="TESTUSDT",
                    order_id="order-expired",
                    exchange_order_id="ex-expired",
                    orig_qty=0.1,
                    executed_qty=0.04,
                    remaining_qty=0.06,
                )
            ]

    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=FakeOrderClient(),
        session_id="session-1",
        strategy_code=code,
    )
    blocked_key = ("binance", "perpetual_futures", "TESTUSDT")
    strat._blocked_order_keys.add(blocked_key)
    before_qty = wallet.futures.positions[("TESTUSDT", 0)].position_qty

    svc.running_strategy(_md(price=50_000.0))

    assert blocked_key not in strat._blocked_order_keys
    assert wallet.futures.positions[("TESTUSDT", 0)].position_qty == before_qty
    wallet.on_order.assert_not_called()
    assert strat._strategy_instance.events[0].order_status == "RECOVERY_EXPIRED"


def test_incremental_fill_event_updates_wallet_once():
    wallet = _wallet_with_futures_slot()
    code = _inline(
        "    def on_order_update(self, event, wallet):\n"
        "        pass\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    event = OrderUpdateEvent(
        event_id=8,
        session_id="session-1",
        account_id=1,
        venue_id=10,
        exchange="binance",
        market="perpetual_futures",
        side="BUY",
        position_side="both",
        event_type="fill",
        order_status="PARTIALLY_FILLED",
        order_id="order-once",
        fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.04, fill_price=50_000.0),
        orig_qty=0.1,
        executed_qty=0.04,
        remaining_qty=0.06,
    )

    class FakeOrderClient:
        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            if limit == 500 or after_event_id >= 8:
                return []
            return [event, event]

    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=FakeOrderClient(),
        session_id="session-1",
        strategy_code=code,
    )

    svc.running_strategy(_md(price=50_000.0))

    assert wallet.futures.positions[("TESTUSDT", 0)].position_qty == pytest.approx(0.04)
    assert 8 in strat._settled_lifecycle_event_ids


def test_order_update_callback_error_does_not_block_market_tick():
    wallet = _wallet_with_futures_slot()
    code = _inline(
        "    def __init__(self):\n"
        "        self.market_calls = 0\n"
        "    def on_order_update(self, event, wallet):\n"
        "        raise RuntimeError('callback failed')\n"
        "    def on_market_data(self, data, wallet):\n"
        "        self.market_calls += 1\n"
        "        return None\n"
    )

    class FakeOrderClient:
        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            if limit == 500:
                return []
            if after_event_id >= 1:
                return []
            return [
                OrderUpdateEvent(
                    event_id=1,
                    session_id=session_id,
                    account_id=1,
                    venue_id=10,
                    exchange="binance",
                    market="perpetual_futures",
                    side="BUY",
                    position_side="both",
                    event_type="fill",
                    order_status="FILLED",
                    order_id="order-1",
                    fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.1, fill_price=50_000.0),
                )
            ]

    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=FakeOrderClient(),
        session_id="session-1",
        strategy_code=code,
    )

    svc.running_strategy(_md(price=50_000.0))

    assert strat._strategy_instance.market_calls == 1


def test_existing_order_events_seed_cursor_without_replaying_wallet():
    wallet = _wallet_with_futures_slot()
    code = _inline(
        "    def __init__(self):\n"
        "        self.events = []\n"
        "    def on_order_update(self, event, wallet):\n"
        "        self.events.append(event)\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    old_event = OrderUpdateEvent(
        event_id=7,
        session_id="session-1",
        account_id=1,
        venue_id=10,
        exchange="binance",
        market="perpetual_futures",
        side="BUY",
        position_side="both",
        event_type="fill",
        order_status="FILLED",
        order_id="order-old",
        fill=OrderUpdateFill(symbol="TESTUSDT", qty=0.1, fill_price=50_000.0),
    )

    class FakeOrderClient:
        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            if limit == 500 and after_event_id == 0:
                return [old_event]
            return []

    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1",
        "/workspace/strategy.py",
        wallet,
        order_client=FakeOrderClient(),
        session_id="session-1",
        strategy_code=code,
    )
    wallet.on_order = MagicMock(wraps=wallet.on_order)

    svc.running_strategy(_md(price=50_000.0))

    wallet.on_order.assert_not_called()
    assert strat._strategy_instance.events == []


def test_unknown_execution_blocks_same_symbol_from_repeat_orders():
    wallet = _wallet_with_futures_slot()

    class FakeOrderClient:
        def __init__(self) -> None:
            self.calls = 0

        def place_order(self, *_args, **_kwargs):
            self.calls += 1
            return ExecutionFeedback(
                intent_id="intent-1",
                attempt_id="attempt-1",
                attempt_status="UNKNOWN",
                error_message="network timeout",
            )

    order_client = FakeOrderClient()
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.buy_once", wallet, order_client=order_client)

    svc.running_strategy(_md(price=50_000.0))
    svc.running_strategy(_md(price=50_100.0))

    assert order_client.calls == 1


def test_fill_pending_execution_does_not_update_wallet_and_blocks_symbol():
    wallet = _wallet_with_futures_slot()

    class FakeOrderClient:
        def __init__(self) -> None:
            self.calls = 0

        def place_order(self, *_args, **_kwargs):
            self.calls += 1
            return ExecutionFeedback(
                intent_id="intent-1",
                attempt_id="attempt-1",
                attempt_status="ACCEPTED",
                error_message="binance trade fee data not available after confirmed execution",
                order=OrderResponse(
                    symbol="TESTUSDT",
                    side="BUY",
                    qty=0.0,
                    fill_price=50000.0,
                    status="FILLED",
                    order_id="order-1",
                    orig_qty=0.1,
                    executed_qty=0.1,
                    remaining_qty=0.0,
                ),
                fill_count=0,
                delta_qty=0.0,
                fill_events=[],
            )

    order_client = FakeOrderClient()
    svc = StrategyEngine()
    strat = svc.create_strategy("u1", "strategies.buy_once", wallet, order_client=order_client)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    strat.on_order_callback = MagicMock()

    svc.running_strategy(_md(price=50_000.0))
    svc.running_strategy(_md(price=50_100.0))

    assert order_client.calls == 1
    wallet.on_order.assert_not_called()
    strat.on_order_callback.assert_not_called()
    assert wallet.futures.positions[("TESTUSDT", 0)].position_qty == pytest.approx(0.0)


def test_unknown_symbol_market_is_dropped_by_router():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.noop", wallet)
    wallet.on_market_data = MagicMock(wraps=wallet.on_market_data)
    # noop declares only TESTUSDT futures 1m; any other key is silently dropped.
    svc.running_strategy(_md(symbol="ETHUSDT"))
    wallet.on_market_data.assert_not_called()


def test_strategy_can_access_wallet_by_exchange_market():
    route_wallet = _wallet_with_futures_slot()._default_wallet
    wallet = PortfolioWalletRuntime(
        1,
        {("binance", "perpetual_futures")},
        {("binance", "perpetual_futures", 10): route_wallet},
    )
    strategy_code = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet):\n"
        '        futures_wallet = wallet.get("binance", "perpetual_futures")\n'
        "        assert futures_wallet is not None\n"
        "        return None\n"
    )
    svc = StrategyEngine()
    svc.create_strategy("u1", "<db:wallet_get>", wallet, strategy_code=strategy_code)

    svc.running_strategy(_md(symbol="TESTUSDT", market="perpetual_futures", interval="1m"))


def test_order_decision_requires_declared_exchange_market_symbol():
    wallet = _wallet_with_futures_slot(symbol="ETHUSDT")
    strategy_code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
        '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(exchange='okx', market='perpetual_futures', symbol='ETHUSDT', side='BUY', qty='0.1', order_type='MARKET')\n"
    )
    svc = StrategyEngine()
    svc.create_strategy("u1", "<db:bad_exchange_target>", wallet, strategy_code=strategy_code)

    with pytest.raises(ValueError, match="ORDER_TARGETS"):
        svc.running_strategy(_md(symbol="ETHUSDT", market="perpetual_futures", interval="1m"))


def test_order_decision_inherits_declared_exchange_before_place_order():
    base_wallet = _wallet_with_futures_slot(symbol="ETHUSDT")._default_wallet
    wallet = _portfolio_wallet(base_wallet, ("okx", "perpetual_futures"))
    strategy_code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "okx", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
        '    ORDER_TARGETS = [{"exchange": "okx", "market": "perpetual_futures", "symbol": "ETHUSDT"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(exchange='okx', market='perpetual_futures', symbol=data.symbol, side='BUY', qty='0.1', order_type='MARKET')\n"
    )

    class CaptureOrderClient:
        def __init__(self) -> None:
            self.decision = None

        def place_order(self, _account_id, decision, _mark_price, **_kwargs):
            self.decision = decision
            return ExecutionFeedback(
                intent_id="intent-okx",
                attempt_id="attempt-okx",
                attempt_status="ACCEPTED",
            )

    order_client = CaptureOrderClient()
    svc = StrategyEngine()
    svc.create_strategy("u1", "<db:okx_inherited_exchange>", wallet, strategy_code=strategy_code, order_client=order_client)

    svc.running_strategy(_md(symbol="ETHUSDT", market="perpetual_futures", interval="1m", exchange="okx"))

    assert order_client.decision is not None
    assert order_client.decision.exchange == "okx"
    assert order_client.decision.market == "perpetual_futures"


def test_strategy_declared_symbols_route_even_without_wallet_slot():
    """Pre_C3 §2.2: wallet can be empty; declaration alone drives routing."""
    wallet = _portfolio_wallet(make_backtest_wallet(
        margin_mode="isolated",
        spot_assets=[{"symbol": "USDC", "qty": 1.0, "price": 1.0}],
    ), ("binance", "perpetual_futures"))
    svc = StrategyEngine()
    strategy_code = _inline(
        body="    def on_market_data(self, data, wallet):\n        return None\n",
        symbol="ETHUSDT",
        market="perpetual_futures",
        interval="1m",
    )

    svc.create_strategy("u1", "<db:declared_symbols>", wallet, strategy_code=strategy_code)
    wallet.on_market_data = MagicMock(wraps=wallet.on_market_data)

    # Router is keyed by the normalized 4-tuple from the declaration.
    assert ("binance", "perpetual_futures", "ETHUSDT", "1m") in svc.strategy_router
    svc.running_strategy(_md(symbol="ETHUSDT", market="perpetual_futures", interval="1m"))

    wallet.on_market_data.assert_called_once_with(
        "binance", "perpetual_futures", "ETHUSDT", "futures", 50_000.0
    )


def test_same_symbol_different_market_routes_correctly():
    """Strategy declaring both markets → both route to the same instance."""
    wallet = _wallet_with_futures_slot(symbol="BTCUSDT")
    wallet.spot.assets["BTCUSDT"] = SpotAsset()
    svc = StrategyEngine()
    strategy_code = (
        "class MyStrategy:\n"
        '    INPUTS = [\n'
        '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"},\n'
        '        {"exchange": "binance", "market": "spot",    "symbol": "BTCUSDT", "interval": "1m"},\n'
        '    ]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    svc.create_strategy("u1", "<db:both_markets>", wallet, strategy_code=strategy_code)
    wallet.on_market_data = MagicMock(wraps=wallet.on_market_data)
    svc.running_strategy(_md(symbol="BTCUSDT", market="perpetual_futures"))
    svc.running_strategy(_md(symbol="BTCUSDT", market="spot"))
    assert wallet.on_market_data.call_count == 2
    wallet.on_market_data.assert_any_call(
        "binance", "perpetual_futures", "BTCUSDT", "futures", 50_000.0
    )
    wallet.on_market_data.assert_any_call(
        "binance", "spot", "BTCUSDT", "spot", 50_000.0
    )


def test_user_strategy_uses_preconfigured_spot_slot():
    wallet = _wallet_with_spot_slot()
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.noop", wallet)
    assert "TESTUSDT" in wallet.spot.assets


def test_import_error_message():
    """Per pre_C3 contract the strategy is loaded during create_strategy —
    so import errors surface there, not on the first tick."""
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    with pytest.raises(ImportError, match="failed to import strategy module"):
        svc.create_strategy("u1", "strategies.does_not_exist_module", wallet)


def test_empty_wallet_can_still_create_strategy():
    """Pre_C3 §2.2: an empty wallet is a valid starting state; strategy creation
    MUST succeed as long as the declaration is valid."""
    wallet = _portfolio_wallet(make_backtest_wallet(margin_mode="isolated"), ("binance", "perpetual_futures"))
    svc = StrategyEngine()
    strategy_code = _inline(
        body="    def on_market_data(self, data, wallet):\n        return None\n",
    )
    strat = svc.create_strategy("u1", "<db:empty_wallet_ok>", wallet, strategy_code=strategy_code)
    # Router is built from declaration, not wallet.
    assert ("binance", "perpetual_futures", "TESTUSDT", "1m") in svc.strategy_router
    assert strat is not None


def test_full_flow_mark_then_order_and_open_position():
    wallet = _wallet_with_futures_slot()
    events: list[str] = []
    _md_impl = wallet.on_market_data
    _oo_impl = wallet.on_order

    def trace_md(*args, **kwargs):
        events.append("on_market_data")
        return _md_impl(*args, **kwargs)

    def trace_oo(*args, **kwargs):
        events.append("on_order")
        return _oo_impl(*args, **kwargs)

    wallet.on_market_data = trace_md
    wallet.on_order = trace_oo

    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.buy_once", wallet, order_client=FilledOrderClient())
    svc.running_strategy(_md(price=51_000.0))

    assert events == ["on_market_data", "on_order"]
    pos = wallet.futures.positions[("TESTUSDT", 0)]
    # Post-Phase-C2b: BinancePosition exposes initial_margin as a plain
    # attribute (sum of position_initial_margin + open_order_initial_margin).
    assert pos.initial_margin > 0
    assert pos.get_unrealized_pnl() == 0.0


def test_declared_tick_refreshes_mark_price_without_new_order():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    strategy_code = _inline(
        "    def __init__(self):\n"
        "        self.done = False\n"
        "    def on_market_data(self, data, wallet):\n"
        "        if self.done:\n"
        "            return None\n"
        "        self.done = True\n"
        "        return OrderDecision(symbol=data.symbol, side='BUY', qty='0.1')\n"
    )
    svc.create_strategy(
        "u1",
        "<db:buy_once_inline>",
        wallet,
        order_client=FilledOrderClient(),
        strategy_code=strategy_code,
    )

    svc.running_strategy(_md(price=50_000.0))
    svc.running_strategy(_md(price=50_100.0))

    pos = wallet.futures.positions[("TESTUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.1)
    assert pos.mark_price == pytest.approx(50_100.0)
    assert pos.get_unrealized_pnl() == pytest.approx(10.0)


def test_running_strategy_records_last_market_time():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    strat = svc.create_strategy("u1", "strategies.noop", wallet)
    ts = datetime(2026, 6, 1, 0, 43, tzinfo=timezone.utc)

    svc.running_strategy(_md(price=50_000.0, timestamp=ts))

    assert strat.last_market_time == ts


def test_futures_short_signal_closes_one_way_position():
    wallet = _wallet_with_futures_slot(symbol="TESTUSDT")
    svc = StrategyEngine()
    strategy_code = _inline(
        body=(
            "    def __init__(self):\n"
            "        self._has_position = False\n"
            "    def on_market_data(self, data, wallet):\n"
            "        price = float(data.klines['close'])\n"
            "        if not self._has_position and price < 120:\n"
            "            self._has_position = True\n"
            "            return OrderDecision(symbol=data.symbol, side='BUY', qty=0.1)\n"
            "        if self._has_position and price > 180:\n"
            "            self._has_position = False\n"
            "            return OrderDecision(symbol=data.symbol, side='SELL', qty=0.1)\n"
            "        return None\n"
        ),
    )
    svc.create_strategy(
        "u1",
        "<db:test_full_flow>",
        wallet,
        order_client=FilledOrderClient(),
        strategy_code=strategy_code,
    )

    svc.running_strategy(MarketData(
        symbol="TESTUSDT",
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        market="perpetual_futures",
        interval="1m",
        klines={"close": 100.0},
    ))
    pos = wallet.futures.positions[("TESTUSDT", 0)]
    assert pos.net_qty == pytest.approx(0.1)
    assert pos.net_direction == 1

    svc.running_strategy(MarketData(
        symbol="TESTUSDT",
        price=190.0,
        timestamp=datetime.now(timezone.utc),
        market="perpetual_futures",
        interval="1m",
        klines={"close": 190.0},
    ))
    assert pos.net_qty == pytest.approx(0.0)
    assert pos.net_direction == 0


def test_spot_market_buy_updates_spot_wallet_only():
    wallet = _wallet_with_spot_slot()
    wallet.spot.free = 1_000.0
    svc = StrategyEngine()
    # buy_once declares (futures, TESTUSDT, 1m); for the spot test we use an
    # inline strategy declaring the spot input.
    strategy_code = _inline(
        body=(
            "    def on_market_data(self, data, wallet):\n"
            "        return OrderDecision(symbol=data.symbol, side='BUY', qty=0.1)\n"
        ),
        market="spot",
    )
    svc.create_strategy(
        "u1",
        "<db:spot_buy>",
        wallet,
        order_client=FilledOrderClient(),
        strategy_code=strategy_code,
    )

    svc.running_strategy(_md(symbol="TESTUSDT", price=100.0, market="spot"))

    asset = wallet.spot.assets["TESTUSDT"]
    assert asset.qty == 0.1
    assert asset.avg_entry_price == 100.0
    assert wallet.spot.free == 990.0
    assert wallet.futures.positions == {}


def test_futures_open_precheck_uses_available_balance_not_wallet_balance():
    wallet = _wallet_with_futures_slot()
    wallet._default_wallet.get_wallet_balance = MagicMock(return_value=10_000.0)
    wallet._default_wallet.get_available_balance = MagicMock(return_value=100.0)
    wallet.on_order = MagicMock(wraps=wallet.on_order)

    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.buy_once", wallet)
    svc.running_strategy(_md(price=50_000.0))

    wallet.on_order.assert_not_called()


def test_limit_order_passes_market_tick_as_mark_price_not_limit_price():
    wallet = _wallet_with_futures_slot()
    captured: dict[str, float] = {}

    class FakeOrderClient:
        def list_order_lifecycle_events(self, *, session_id, after_event_id=0, limit=100):
            return []

        def place_order(self, _account_id, _decision, mark_price, **_kwargs):
            captured["mark_price"] = mark_price
            return ExecutionFeedback(attempt_status="FAILED", error_message="stop after capture")

    strategy_code = _inline(
        "    def on_market_data(self, data, wallet):\n"
        "        tick = data.exchange[\"binance\"].market[\"perpetual_futures\"].symbol[\"TESTUSDT\"].interval[\"1m\"]\n"
        "        return OrderDecision(\n"
        "            symbol=\"TESTUSDT\",\n"
        "            side=\"BUY\",\n"
        "            qty=\"0.01\",\n"
        "            price=str(float(tick.price) * 0.5),\n"
        "            market=\"perpetual_futures\",\n"
        "            exchange=\"binance\",\n"
        "            order_type=\"LIMIT\",\n"
        "            time_in_force=\"GTC\",\n"
        "        )\n"
    )

    svc = StrategyEngine()
    svc.create_strategy(
        "u1",
        "<db:limit_mark_price>",
        wallet,
        strategy_code=strategy_code,
        order_client=FakeOrderClient(),
        session_id="session-limit-mark",
    )
    svc.running_strategy(_md(price=50_000.0))

    assert captured["mark_price"] == pytest.approx(50_000.0)


def test_spot_sell_precheck_uses_unlocked_qty():
    wallet = _wallet_with_spot_slot()
    wallet.spot.assets["TESTUSDT"] = SpotAsset(qty=1.0, locked=0.8, avg_entry_price=90.0, price=100.0)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    strategy_code = _inline(
        body=(
            "    def on_market_data(self, data, wallet):\n"
            "        return OrderDecision(symbol=data.symbol, side='SELL', qty=0.5, price=None)\n"
        ),
        market="spot",
    )

    svc = StrategyEngine()
    svc.create_strategy("u1", "<db:sell_locked>", wallet, strategy_code=strategy_code)
    svc.running_strategy(_md(symbol="TESTUSDT", price=100.0, market="spot"))

    wallet.on_order.assert_not_called()
    assert wallet.spot.assets["TESTUSDT"].qty == pytest.approx(1.0)
    assert wallet.spot.assets["TESTUSDT"].locked == pytest.approx(0.8)


def test_order_callbacks_run_after_wallet_update_in_order():
    wallet = _wallet_with_futures_slot()
    events: list[str] = []
    svc = StrategyEngine()
    strat = svc.create_strategy(
        "u1", "strategies.buy_with_callback", wallet, order_client=FilledOrderClient()
    )

    _wallet_on_order = wallet.on_order

    def trace_wallet_on_order(*args, **kwargs):
        events.append("wallet.on_order")
        return _wallet_on_order(*args, **kwargs)

    wallet.on_order = trace_wallet_on_order

    inst = strat._get_strategy()
    _on_order_response = inst.on_order_response

    def trace_on_order_response(order_resp):
        events.append("user.on_order_response")
        return _on_order_response(order_resp)

    inst.on_order_response = trace_on_order_response
    strat.on_order_callback = lambda: events.append("sync.on_order_callback")

    svc.running_strategy(_md(price=50_000.0))

    assert events == [
        "wallet.on_order",
        "user.on_order_response",
        "sync.on_order_callback",
    ]


def test_spot_order_callbacks_run_after_wallet_update_in_order():
    wallet = _wallet_with_spot_slot()
    wallet.spot.free = 1_000.0
    events: list[str] = []
    svc = StrategyEngine()
    # For the spot variant we need a strategy declaring spot TESTUSDT and
    # emitting a BUY order — reuse buy_with_callback's body but declare spot.
    strategy_code = _inline(
        body=(
            "    def __init__(self):\n"
            "        self.last_resp = None\n"
            "    def on_market_data(self, data, wallet):\n"
            "        return OrderDecision(symbol=data.symbol, side='BUY', qty=0.05)\n"
            "    def on_order_response(self, order_resp):\n"
            "        self.last_resp = order_resp\n"
        ),
        market="spot",
    )
    strat = svc.create_strategy(
        "u1",
        "<db:spot_buy_with_callback>",
        wallet,
        order_client=FilledOrderClient(),
        strategy_code=strategy_code,
    )

    _wallet_on_order = wallet.on_order

    def trace_wallet_on_order(*args, **kwargs):
        events.append("wallet.on_order")
        return _wallet_on_order(*args, **kwargs)

    wallet.on_order = trace_wallet_on_order

    inst = strat._get_strategy()
    _on_order_response = inst.on_order_response

    def trace_on_order_response(order_resp):
        events.append("user.on_order_response")
        return _on_order_response(order_resp)

    inst.on_order_response = trace_on_order_response
    strat.on_order_callback = lambda: events.append("sync.on_order_callback")

    svc.running_strategy(_md(symbol="TESTUSDT", price=100.0, market="spot"))

    assert events == [
        "wallet.on_order",
        "user.on_order_response",
        "sync.on_order_callback",
    ]
    assert wallet.spot.assets["TESTUSDT"].qty == pytest.approx(0.05)


def test_zero_qty_rejected_before_wallet():
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.zero_qty", wallet)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    with pytest.raises(ValueError, match="OrderDecision.qty"):
        svc.running_strategy(_md())
    wallet.on_order.assert_not_called()


def test_invalid_futures_side_rejected_in_hedge_mode():
    wallet = _wallet_with_futures_slot(position_mode="hedge")
    svc = StrategyEngine()
    svc.create_strategy("u1", "strategies.bad_side", wallet)
    with pytest.raises(ValueError, match="OrderDecision.side"):
        svc.running_strategy(_md())


def test_module_without_mystategy_raises():
    """Strategy code without a MyStrategy class fails at create_strategy time
    (pre_C3 contract: strategy is loaded + validated eagerly)."""
    wallet = _wallet_with_futures_slot()
    svc = StrategyEngine()
    bad_code = "class NotMyStrategy:\n    pass\n"
    with pytest.raises(AttributeError, match="MyStrategy"):
        svc.create_strategy("u1", "<db:no_mystrategy>", wallet, strategy_code=bad_code)


def test_multi_symbol_routes_to_same_strategy():
    """Strategy declaring multiple futures symbols → all route to the same instance."""
    wallet = _portfolio_wallet(make_backtest_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": 10,
                "initial_balance": 5000,
                "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
            {
                "symbol": "ETHUSDT",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": 10,
                "initial_balance": 3000,
                "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
        ],
    ), ("binance", "perpetual_futures"))

    svc = StrategyEngine()
    strategy_code = (
        "class MyStrategy:\n"
        '    INPUTS = [\n'
        '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"},\n'
        '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"},\n'
        '    ]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    strat = svc.create_strategy("u1", "<db:multi_symbol>", wallet, strategy_code=strategy_code)

    # Both declared inputs route to the same instance.
    assert svc.strategy_router[("binance", "perpetual_futures", "BTCUSDT", "1m")] is strat
    assert svc.strategy_router[("binance", "perpetual_futures", "ETHUSDT", "1m")] is strat
