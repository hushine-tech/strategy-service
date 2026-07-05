"""Wallet runtime protocol shared by legacy and exchange-parity implementations."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .canonical import CanonicalPortfolioState


@runtime_checkable
class WalletRuntime(Protocol):
    environment_code: int
    futures: object
    spot: object

    def get_total_value(self) -> float: ...

    def get_wallet_balance(self) -> float: ...

    def get_available_balance(self) -> float: ...

    def on_market_data(self, symbol: str, symbol_type: str, price: float) -> None: ...

    def on_order(self, symbol: str, symbol_type: str, order_resp: object) -> None: ...

    def on_ledger_event(self, event: object) -> None: ...

    def to_canonical_state(self) -> CanonicalPortfolioState: ...


@runtime_checkable
class ExchangeWalletRuntime(WalletRuntime, Protocol):
    """Wallet runtime bound to a concrete exchange provider + environment.

    Concrete implementations MUST set the class attributes ``provider`` (e.g.
    ``"binance"``, ``"okx"``) and ``environment`` (e.g. ``"demo"``,
    ``"live"``, ``"backtest"``). These identifiers are the lookup key used by
    ``wallet_factory.RUNTIME_REGISTRY`` to route canonical portfolio state to the
    right runtime class.
    """

    provider: str
    environment: str
