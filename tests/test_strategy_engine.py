from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from strategy_service import ExecutionFeedback, MarketData, OrderResponse, StrategyService
from strategy_service.strategy.base import _load_strategy_instance
from strategy_service.wallet import SpotAsset
from tests.helpers.wallet_fixtures import make_backtest_wallet


def _md(
    symbol: str = "TESTUSDT",
    price: float = 50_000.0,
    market: str = "futures",
    interval: str = "1m",
) -> MarketData:
    return MarketData(
        symbol=symbol,
        price=price,
        timestamp=datetime.now(timezone.utc),
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

    return make_backtest_wallet(
        margin_mode=margin_mode,
        position_mode=position_mode,
        wallet_balance=account_initial_balance,
        initial_balance=account_initial_balance,
        futures_positions=futures_positions,
    )


def _wallet_with_spot_slot(symbol: str = "TESTUSDT"):
    """Build a backtest wallet runtime with one spot asset slot preconfigured."""
    return make_backtest_wallet(
        margin_mode="isolated",
        spot_assets=[{"symbol": symbol.strip().upper()}],
    )


# Helper to build inline strategy code with INPUTS auto-inserted.
def _inline(body: str, *, symbol: str = "TESTUSDT", market: str = "futures", interval: str = "1m") -> str:
    return (
        "from strategy_service.types import OrderDecision\n"
        "\n"
        "class MyStrategy:\n"
        f'    INPUTS = [{{"exchange": "binance", "market": "{market}", "symbol": "{symbol}", "interval": "{interval}"}}]\n'
        + body
    )


def test_inline_strategy_code_uses_strategy_path_as_python_filename():
    code = (
        "class MyStrategy:\n"
        "    INPUTS = [{\"exchange\": \"binance\", \"market\": \"futures\", \"symbol\": \"TESTUSDT\", \"interval\": \"1m\"}]\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    strategy = _load_strategy_instance("/workspace/self_hosted_strategy.py", strategy_code=code)

    assert strategy.on_market_data.__code__.co_filename == "/workspace/self_hosted_strategy.py"


def test_running_strategy_no_signal_does_not_call_on_order(capsys):
    wallet = _wallet_with_futures_slot()
    svc = StrategyService()
    svc.create_strategy("u1", "strategies.noop", wallet)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    svc.running_strategy(_md())
    wallet.on_order.assert_not_called()
    out = capsys.readouterr().out
    assert "[Mock Order]" not in out


def test_running_strategy_with_signal_calls_on_order_and_prints(capsys):
    wallet = _wallet_with_futures_slot()
    svc = StrategyService()
    svc.create_strategy("u1", "strategies.buy_once", wallet)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    svc.running_strategy(_md(price=51_000.0))
    wallet.on_order.assert_called_once()
    args, _kwargs = wallet.on_order.call_args
    assert args[0] == "TESTUSDT"
    assert args[1] == "futures"
    assert args[2].status.upper() == "FILLED"
    assert args[2].fill_price == 51_000.0


def test_on_order_response_called_when_defined():
    wallet = _wallet_with_futures_slot()
    svc = StrategyService()
    strat = svc.create_strategy("u1", "strategies.buy_with_callback", wallet)
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

    svc = StrategyService()
    strat = svc.create_strategy("u1", "strategies.buy_once", wallet, order_client=FakeOrderClient())
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    strat.on_order_callback = MagicMock()

    svc.running_strategy(_md(price=51_000.0))

    assert wallet.on_order.call_count == 2
    args1, _ = wallet.on_order.call_args_list[0]
    args2, _ = wallet.on_order.call_args_list[1]
    assert args1[2].status == "PARTIALLY_FILLED"
    assert args1[2].qty == pytest.approx(0.02)
    assert args2[2].status == "FILLED"
    assert args2[2].qty == pytest.approx(0.03)
    assert wallet.futures.positions[("TESTUSDT", 0)].position_qty == pytest.approx(0.05)
    assert wallet.futures.positions[("TESTUSDT", 0)].entry_price == pytest.approx(51200.0)
    strat.on_order_callback.assert_called_once()


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
    svc = StrategyService()
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
    svc = StrategyService()
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
    svc = StrategyService()
    svc.create_strategy("u1", "strategies.noop", wallet)
    wallet.on_market_data = MagicMock(wraps=wallet.on_market_data)
    # noop declares only TESTUSDT futures 1m; any other key is silently dropped.
    svc.running_strategy(_md(symbol="ETHUSDT"))
    wallet.on_market_data.assert_not_called()


def test_strategy_can_access_wallet_by_exchange_market():
    wallet = _wallet_with_futures_slot()
    strategy_code = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        '        futures_wallet = wallet.get("binance", "perpetual_futures")\n'
        "        assert futures_wallet is not None\n"
        "        return None\n"
    )
    svc = StrategyService()
    svc.create_strategy("u1", "<db:wallet_get>", wallet, strategy_code=strategy_code)

    svc.running_strategy(_md(symbol="TESTUSDT", market="futures", interval="1m"))


def test_order_decision_requires_declared_exchange_market_symbol():
    wallet = _wallet_with_futures_slot(symbol="ETHUSDT")
    strategy_code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(symbol='ETHUSDT', side='LONG', qty=0.1, market='futures', exchange='okx')\n"
    )
    svc = StrategyService()
    svc.create_strategy("u1", "<db:bad_exchange_target>", wallet, strategy_code=strategy_code)

    with pytest.raises(ValueError, match="not declared in INPUTS"):
        svc.running_strategy(_md(symbol="ETHUSDT", market="futures", interval="1m"))


def test_strategy_declared_symbols_route_even_without_wallet_slot():
    """Pre_C3 §2.2: wallet can be empty; declaration alone drives routing."""
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        spot_assets=[{"symbol": "USDC", "qty": 1.0, "price": 1.0}],
    )
    svc = StrategyService()
    strategy_code = _inline(
        body="    def on_market_data(self, data, wallet):\n        return None\n",
        symbol="ETHUSDT",
        market="futures",
        interval="1m",
    )

    svc.create_strategy("u1", "<db:declared_symbols>", wallet, strategy_code=strategy_code)
    wallet.on_market_data = MagicMock(wraps=wallet.on_market_data)

    # Router is keyed by the normalized 4-tuple from the declaration.
    assert ("binance", "perpetual_futures", "ETHUSDT", "1m") in svc.strategy_router
    svc.running_strategy(_md(symbol="ETHUSDT", market="futures", interval="1m"))

    wallet.on_market_data.assert_called_once()
    assert wallet.on_market_data.call_args[0][0] == "ETHUSDT"
    assert wallet.on_market_data.call_args[0][1] == "futures"


def test_same_symbol_different_market_routes_correctly():
    """Strategy declaring both markets → both route to the same instance."""
    wallet = _wallet_with_futures_slot(symbol="BTCUSDT")
    wallet.spot.assets["BTCUSDT"] = SpotAsset()
    svc = StrategyService()
    strategy_code = (
        "class MyStrategy:\n"
        '    INPUTS = [\n'
        '        {"exchange": "binance", "market": "futures", "symbol": "BTCUSDT", "interval": "1m"},\n'
        '        {"exchange": "binance", "market": "spot",    "symbol": "BTCUSDT", "interval": "1m"},\n'
        '    ]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    svc.create_strategy("u1", "<db:both_markets>", wallet, strategy_code=strategy_code)
    wallet.on_market_data = MagicMock(wraps=wallet.on_market_data)
    svc.running_strategy(_md(symbol="BTCUSDT", market="futures"))
    svc.running_strategy(_md(symbol="BTCUSDT", market="spot"))
    assert wallet.on_market_data.call_count == 2
    assert wallet.on_market_data.call_args_list[0][0][1] == "futures"
    assert wallet.on_market_data.call_args_list[1][0][1] == "spot"


def test_user_strategy_uses_preconfigured_spot_slot():
    wallet = _wallet_with_spot_slot()
    svc = StrategyService()
    svc.create_strategy("u1", "strategies.noop", wallet)
    assert "TESTUSDT" in wallet.spot.assets


def test_import_error_message():
    """Per pre_C3 contract the strategy is loaded during create_strategy —
    so import errors surface there, not on the first tick."""
    wallet = _wallet_with_futures_slot()
    svc = StrategyService()
    with pytest.raises(ImportError, match="failed to import strategy module"):
        svc.create_strategy("u1", "strategies.does_not_exist_module", wallet)


def test_empty_wallet_can_still_create_strategy():
    """Pre_C3 §2.2: an empty wallet is a valid starting state; strategy creation
    MUST succeed as long as the declaration is valid."""
    wallet = make_backtest_wallet(margin_mode="isolated")
    svc = StrategyService()
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

    svc = StrategyService()
    svc.create_strategy("u1", "strategies.buy_once", wallet)
    svc.running_strategy(_md(price=51_000.0))

    assert events == ["on_market_data", "on_order"]
    pos = wallet.futures.positions[("TESTUSDT", 0)]
    # Post-Phase-C2b: BinancePosition exposes initial_margin as a plain
    # attribute (sum of position_initial_margin + open_order_initial_margin).
    assert pos.initial_margin > 0
    assert pos.get_unrealized_pnl() == 0.0


def test_futures_short_signal_closes_one_way_position():
    wallet = _wallet_with_futures_slot(symbol="TESTUSDT")
    svc = StrategyService()
    strategy_code = _inline(
        body=(
            "    def __init__(self):\n"
            "        self._has_position = False\n"
            "    def on_market_data(self, data, wallet):\n"
            "        price = float(data.klines['close'])\n"
            "        if not self._has_position and price < 120:\n"
            "            self._has_position = True\n"
            "            return OrderDecision(symbol=data.symbol, side='LONG', qty=0.1)\n"
            "        if self._has_position and price > 180:\n"
            "            self._has_position = False\n"
            "            return OrderDecision(symbol=data.symbol, side='SHORT', qty=0.1)\n"
            "        return None\n"
        ),
    )
    svc.create_strategy("u1", "<db:test_full_flow>", wallet, strategy_code=strategy_code)

    svc.running_strategy(MarketData(
        symbol="TESTUSDT",
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        market="futures",
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
        market="futures",
        interval="1m",
        klines={"close": 190.0},
    ))
    assert pos.net_qty == pytest.approx(0.0)
    assert pos.net_direction == 0


def test_spot_market_buy_updates_spot_wallet_only():
    wallet = _wallet_with_spot_slot()
    wallet.spot.free = 1_000.0
    svc = StrategyService()
    # buy_once declares (futures, TESTUSDT, 1m); for the spot test we use an
    # inline strategy declaring the spot input.
    strategy_code = _inline(
        body=(
            "    def on_market_data(self, data, wallet):\n"
            "        return OrderDecision(symbol=data.symbol, side='LONG', qty=0.1)\n"
        ),
        market="spot",
    )
    svc.create_strategy("u1", "<db:spot_buy>", wallet, strategy_code=strategy_code)

    svc.running_strategy(_md(symbol="TESTUSDT", price=100.0, market="spot"))

    asset = wallet.spot.assets["TESTUSDT"]
    assert asset.qty == 0.1
    assert asset.avg_entry_price == 100.0
    assert wallet.spot.free == 990.0
    assert wallet.futures.positions == {}


def test_futures_open_precheck_uses_available_balance_not_wallet_balance():
    wallet = _wallet_with_futures_slot()
    wallet.get_wallet_balance = MagicMock(return_value=10_000.0)
    wallet.get_available_balance = MagicMock(return_value=100.0)
    wallet.on_order = MagicMock(wraps=wallet.on_order)

    svc = StrategyService()
    svc.create_strategy("u1", "strategies.buy_once", wallet)
    svc.running_strategy(_md(price=50_000.0))

    wallet.on_order.assert_not_called()


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

    svc = StrategyService()
    svc.create_strategy("u1", "<db:sell_locked>", wallet, strategy_code=strategy_code)
    svc.running_strategy(_md(symbol="TESTUSDT", price=100.0, market="spot"))

    wallet.on_order.assert_not_called()
    assert wallet.spot.assets["TESTUSDT"].qty == pytest.approx(1.0)
    assert wallet.spot.assets["TESTUSDT"].locked == pytest.approx(0.8)


def test_order_callbacks_run_after_wallet_update_in_order():
    wallet = _wallet_with_futures_slot()
    events: list[str] = []
    svc = StrategyService()
    strat = svc.create_strategy("u1", "strategies.buy_with_callback", wallet)

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
    svc = StrategyService()
    # For the spot variant we need a strategy declaring spot TESTUSDT and
    # emitting LONG (BUY) — reuse buy_with_callback's body but declare spot.
    strategy_code = _inline(
        body=(
            "    def __init__(self):\n"
            "        self.last_resp = None\n"
            "    def on_market_data(self, data, wallet):\n"
            "        return OrderDecision(symbol=data.symbol, side='LONG', qty=0.05)\n"
            "    def on_order_response(self, order_resp):\n"
            "        self.last_resp = order_resp\n"
        ),
        market="spot",
    )
    strat = svc.create_strategy("u1", "<db:spot_buy_with_callback>", wallet, strategy_code=strategy_code)

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
    svc = StrategyService()
    svc.create_strategy("u1", "strategies.zero_qty", wallet)
    wallet.on_order = MagicMock(wraps=wallet.on_order)
    with pytest.raises(ValueError, match="OrderDecision.qty must be != 0"):
        svc.running_strategy(_md())
    wallet.on_order.assert_not_called()


def test_invalid_futures_side_rejected_in_hedge_mode():
    wallet = _wallet_with_futures_slot(position_mode="hedge")
    svc = StrategyService()
    svc.create_strategy("u1", "strategies.bad_side", wallet)
    # Error message changed in BinanceWalletRuntime (_position_key_from_order):
    # "hedge-mode parity orders require explicit position_side".
    with pytest.raises(ValueError, match="explicit position_side"):
        svc.running_strategy(_md())


def test_module_without_mystategy_raises():
    """Strategy code without a MyStrategy class fails at create_strategy time
    (pre_C3 contract: strategy is loaded + validated eagerly)."""
    wallet = _wallet_with_futures_slot()
    svc = StrategyService()
    bad_code = "class NotMyStrategy:\n    pass\n"
    with pytest.raises(AttributeError, match="MyStrategy"):
        svc.create_strategy("u1", "<db:no_mystrategy>", wallet, strategy_code=bad_code)


def test_multi_symbol_routes_to_same_strategy():
    """Strategy declaring multiple futures symbols → all route to the same instance."""
    wallet = make_backtest_wallet(
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
    )

    svc = StrategyService()
    strategy_code = (
        "class MyStrategy:\n"
        '    INPUTS = [\n'
        '        {"exchange": "binance", "market": "futures", "symbol": "BTCUSDT", "interval": "1m"},\n'
        '        {"exchange": "binance", "market": "futures", "symbol": "ETHUSDT", "interval": "1m"},\n'
        '    ]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    strat = svc.create_strategy("u1", "<db:multi_symbol>", wallet, strategy_code=strategy_code)

    # Both declared inputs route to the same instance.
    assert svc.strategy_router[("binance", "perpetual_futures", "BTCUSDT", "1m")] is strat
    assert svc.strategy_router[("binance", "perpetual_futures", "ETHUSDT", "1m")] is strat
