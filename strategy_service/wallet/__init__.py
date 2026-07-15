"""Wallet public API for strategy-service.

Concrete wallet modules are loaded lazily. Importing a leaf such as
``strategy_service.wallet.order_types`` must not eagerly import portfolio.py,
because portfolio normalization depends on strategy_service.inputs.
"""

from __future__ import annotations

from importlib import import_module


_EXPORT_MODULES = {
    "BinanceWalletRuntime": ".binance",
    "CanonicalPortfolioState": ".canonical",
    "CanonicalFuturesPositionState": ".canonical",
    "CanonicalFuturesState": ".canonical",
    "ExecutionFeedback": ".order_types",
    "ExchangeWalletRuntime": ".runtime",
    "LedgerEvent": ".order_types",
    "OrderResponse": ".order_types",
    "PortfolioWalletRuntime": ".portfolio",
    "SpotAsset": ".spot",
    "SpotWallet": ".spot",
    "WalletRuntime": ".runtime",
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORT_MODULES))


__all__ = list(_EXPORT_MODULES)
