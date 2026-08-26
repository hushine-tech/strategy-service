from __future__ import annotations

from types import SimpleNamespace

import pytest

from strategy_service.types import OrderResponse
from strategy_service.wallet import PortfolioWalletRuntime
from strategy_service.wallet import LedgerEvent
from strategy_service.funding_position_tracker import FundingPositionLegFact


class RecordingWallet:
    def __init__(self) -> None:
        self.market_data_calls: list[tuple[str, str, float]] = []
        self.order_calls: list[tuple[str, str, object]] = []

    def on_market_data(self, symbol: str, symbol_type: str, price: float) -> None:
        self.market_data_calls.append((symbol, symbol_type, price))

    def on_order(self, symbol: str, symbol_type: str, order_resp: object) -> None:
        self.order_calls.append((symbol, symbol_type, order_resp))


def test_get_returns_unique_declared_route_wallet_after_normalization():
    wallet = RecordingWallet()
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )

    assert runtime.get(" BINANCE ", "PERPETUAL_FUTURES") is wallet


def test_get_rejects_undeclared_route_fail_closed():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={},
    )

    with pytest.raises(ValueError, match="not declared"):
        runtime.get("okx", "spot")


def test_get_requires_route_to_be_uniquely_available():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={
            ("binance", "perpetual_futures", 11): RecordingWallet(),
            ("binance", "perpetual_futures", 12): RecordingWallet(),
        },
    )

    with pytest.raises(ValueError, match="ambiguous wallet route.*11.*12"):
        runtime.get("binance", "perpetual_futures")


def test_get_rejects_declared_route_without_wallet():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={},
    )

    with pytest.raises(ValueError, match="missing wallet"):
        runtime.get("binance", "perpetual_futures")


def test_on_market_data_routes_to_unique_declared_wallet():
    wallet = RecordingWallet()
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )

    runtime.on_market_data("binance", "perpetual_futures", "ETHUSDT", "futures", 3210.5)

    assert wallet.market_data_calls == [("ETHUSDT", "futures", 3210.5)]


def test_on_order_routes_by_full_venue_key():
    wallet = RecordingWallet()
    order_resp = object()
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={
            ("binance", "perpetual_futures", 11): RecordingWallet(),
            ("binance", "perpetual_futures", 12): wallet,
        },
    )

    runtime.on_order(
        "binance",
        "perpetual_futures",
        12,
        "ETHUSDT",
        "futures",
        order_resp,
    )

    assert wallet.order_calls == [("ETHUSDT", "futures", order_resp)]


def test_on_order_rejects_undeclared_route_fail_closed():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("okx", "spot", 21): RecordingWallet()},
    )

    with pytest.raises(ValueError, match="not declared"):
        runtime.on_order("okx", "spot", 21, "BTCUSDT", "spot", object())


def test_on_order_rejects_missing_venue_wallet_fail_closed():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): RecordingWallet()},
    )

    with pytest.raises(ValueError, match="missing wallet.*venue 12"):
        runtime.on_order("binance", "perpetual_futures", 12, "ETHUSDT", "futures", object())


def test_portfolio_runtime_has_no_legacy_single_wallet_shortcuts():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): RecordingWallet()},
    )

    assert not hasattr(runtime, "futures")
    assert not hasattr(runtime, "spot")
    assert not hasattr(runtime, "get_wallet_balance")
    assert not hasattr(runtime, "get_available_balance")


def test_ledger_event_rejects_spot_route():
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "spot")},
        wallets={("binance", "spot", 11): RecordingWallet()},
    )

    with pytest.raises(ValueError, match="Futures"):
        runtime.on_ledger_event(
            "binance", "spot", 11,
            LedgerEvent("funding_fee", 0.0, income_entry_id=7, venue_id=11, asset="USDT", amount_decimal="1", margin_mode="cross"),
        )


def test_futures_fill_uses_symbol_isolated_metadata_before_wallet_mutation():
    wallet = RecordingWallet()
    wallet.futures = SimpleNamespace(
        position_mode="hedge",
        margin_mode="cross",
        risk_metadata={
            "BTCUSDT": SimpleNamespace(configured_margin_mode="isolated"),
        },
        positions={},
    )
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    fill = OrderResponse(
        symbol="BTCUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED",
        venue_id=11, exchange_trade_id="trade-1", qty_decimal="0.100000000000000001",
        position_side="LONG", event_type="fill", occurred_at=100,
    )

    runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", fill)

    assert [
        (leg.position_side, leg.margin_mode, leg.signed_qty_decimal)
        for leg in runtime.funding_position_tracker.legs_for(11, "BTCUSDT")
    ] == [("LONG", "isolated", "0.100000000000000001")]
    assert wallet.order_calls == [("BTCUSDT", "futures", fill)]


def test_futures_fill_rejects_ambiguous_or_missing_mode_facts():
    wallet = RecordingWallet()
    wallet.futures = SimpleNamespace(
        position_mode="hedge",
        margin_mode="",
        risk_metadata={},
        positions={},
    )
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    fill = OrderResponse(
        symbol="BTCUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED",
        venue_id=11, exchange_trade_id="trade-1", qty_decimal="0.1", position_side="LONG",
        event_type="fill", occurred_at=100,
    )

    with pytest.raises(ValueError, match="margin_mode"):
        runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", fill)

    assert wallet.order_calls == []


def test_on_order_rejects_mismatched_event_venue_or_symbol_before_wallet_mutation():
    wallet = RecordingWallet()
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    response = OrderResponse(
        symbol="ETHUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED", venue_id=12,
    )

    with pytest.raises(ValueError, match="venue_id"):
        runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", response)

    assert wallet.order_calls == []


def test_lifecycle_fill_requires_exact_event_venue_and_symbol_before_wallet_mutation():
    wallet = RecordingWallet()
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    response = OrderResponse(
        symbol="", side="BUY", qty=0.1, fill_price=100.0, status="FILLED", event_type="fill",
    )

    with pytest.raises(ValueError, match="venue_id"):
        runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", response)

    assert wallet.order_calls == []


def test_futures_fill_rejects_configured_margin_mode_conflicting_with_restored_leg():
    wallet = RecordingWallet()
    wallet.futures = SimpleNamespace(
        position_mode="one_way", margin_mode="cross",
        risk_metadata={"BTCUSDT": SimpleNamespace(configured_margin_mode="isolated")},
        positions={},
    )
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    runtime.funding_position_tracker.restore(
        11, [FundingPositionLegFact("BTCUSDT", "BOTH", "cross", "1")]
    )
    fill = OrderResponse(
        symbol="BTCUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED",
        venue_id=11, exchange_trade_id="trade-1", qty_decimal="0.1", event_type="fill", occurred_at=100,
    )

    with pytest.raises(ValueError, match="margin_mode"):
        runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", fill)

    assert wallet.order_calls == []


def test_futures_fill_rejects_configured_margin_mode_conflicting_with_canonical_position():
    wallet = RecordingWallet()
    wallet.futures = SimpleNamespace(
        position_mode="one_way", margin_mode="cross",
        risk_metadata={"BTCUSDT": SimpleNamespace(configured_margin_mode="isolated")},
        positions={
            ("BTCUSDT", 0): SimpleNamespace(symbol="BTCUSDT", margin_mode="cross"),
        },
    )
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    fill = OrderResponse(
        symbol="BTCUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED",
        venue_id=11, exchange_trade_id="trade-1", qty_decimal="0.1", event_type="fill", occurred_at=100,
    )

    with pytest.raises(ValueError, match="margin_mode conflicts with canonical position"):
        runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", fill)

    assert wallet.order_calls == []
    assert runtime.funding_position_tracker.legs_for(11, "BTCUSDT") == []


def test_futures_fill_accepts_configured_margin_mode_matching_canonical_position():
    wallet = RecordingWallet()
    wallet.futures = SimpleNamespace(
        position_mode="one_way", margin_mode="cross",
        risk_metadata={"BTCUSDT": SimpleNamespace(configured_margin_mode="isolated")},
        positions={
            ("BTCUSDT", 0): SimpleNamespace(symbol="BTCUSDT", margin_mode="isolated"),
        },
    )
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    fill = OrderResponse(
        symbol="BTCUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED",
        venue_id=11, exchange_trade_id="trade-1", qty_decimal="0.1", event_type="fill", occurred_at=100,
    )

    runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", fill)

    assert wallet.order_calls == [("BTCUSDT", "futures", fill)]
    assert runtime.funding_position_tracker.legs_for(11, "BTCUSDT")[0].margin_mode == "isolated"


def test_futures_lifecycle_fill_requires_routed_wallet_futures_state_before_mutation():
    wallet = RecordingWallet()
    runtime = PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    )
    fill = OrderResponse(
        symbol="BTCUSDT", side="BUY", qty=0.1, fill_price=100.0, status="FILLED",
        venue_id=11, exchange_trade_id="trade-1", qty_decimal="0.1", event_type="fill", occurred_at=100,
    )

    with pytest.raises(ValueError, match="Futures wallet"):
        runtime.on_order("binance", "perpetual_futures", 11, "BTCUSDT", "futures", fill)

    assert wallet.order_calls == []
    assert runtime.funding_position_tracker.legs_for(11, "BTCUSDT") == []
