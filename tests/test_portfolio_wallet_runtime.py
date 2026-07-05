from __future__ import annotations

import pytest

from strategy_service.wallet import PortfolioWalletRuntime


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
