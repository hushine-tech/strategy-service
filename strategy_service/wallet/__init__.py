"""Wallet public API for strategy-service.

After Phase C2b cleanup the wallet package exposes exactly one runtime class
(``BinanceWalletRuntime``), the canonical contract types, the runtime
Protocol types (for future exchanges), and the spot + order/ledger event
types. All wallet construction goes through
``strategy_service.wallet_factory.build_wallet_from_account``.
"""

from .binance import BinanceWalletRuntime
from .canonical import CanonicalAccountState, CanonicalFuturesPositionState, CanonicalFuturesState
from .order_types import ExecutionFeedback, LedgerEvent, OrderResponse
from .runtime import ExchangeWalletRuntime, WalletRuntime
from .spot import SpotAsset, SpotWallet

__all__ = [
    "BinanceWalletRuntime",
    "CanonicalAccountState",
    "CanonicalFuturesPositionState",
    "CanonicalFuturesState",
    "ExecutionFeedback",
    "ExchangeWalletRuntime",
    "LedgerEvent",
    "OrderResponse",
    "SpotAsset",
    "SpotWallet",
    "WalletRuntime",
]
