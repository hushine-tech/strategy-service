"""Phase 3 strategy declaration contract.

Strategies MUST declare the `(exchange, market, symbol, interval)` universe
they consume via class-level ``INPUTS`` and the `(exchange, market, symbol)`
universe they can trade via class-level ``ORDER_TARGETS``. Declare
``ORDER_TARGETS = []`` for read-only strategies.

Accepted shape:

    class MyStrategy:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ]
        ORDER_TARGETS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
            },
        ]

Normalization rules:

- ``exchange`` lower-cased; MUST be in ``{"binance", "okx"}``
- ``market`` MUST be in ``{"spot", "perpetual_futures", "delivery_futures"}``
- only canonical market names are accepted
- ``symbol`` upper-cased + stripped; MUST be non-empty
- ``interval`` trimmed; MUST be non-empty
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import hushine_strategy.inputs as _strategy_inputs
from hushine_strategy.inputs import (
    StrategyInput,
    StrategyOrderTarget,
    StrategyRiskControls,
    parse_declared_inputs as _parse_declared_inputs,
    parse_order_targets as _parse_order_targets,
    parse_risk_controls as _parse_risk_controls,
)

from strategy_service.types import MarketData

__all__ = [
    "StrategyInput",
    "StrategyOrderTarget",
    "StrategyRiskControls",
    "StrategyDeclarations",
    "StrategyDeclarationError",
    "InputView",
    "parse_declared_inputs",
    "parse_order_targets",
    "parse_risk_controls",
    "resolve_order_target_leverages",
    "extract_declarations",
    "_normalize_exchange",
    "_normalize_market",
]


SUPPORTED_EXCHANGES = frozenset({"binance", "okx"})
SUPPORTED_MARKETS = frozenset({"spot", "perpetual_futures", "delivery_futures"})


class StrategyDeclarationError(ValueError):
    pass


def _normalize_exchange(value: Any) -> str:
    exchange = str(value or "").strip().lower()
    if exchange not in SUPPORTED_EXCHANGES:
        raise StrategyDeclarationError(
            f"unsupported exchange: {exchange or '<empty>'}"
        )
    return exchange


def _normalize_market(value: Any) -> str:
    market = str(value or "").strip().lower()
    if market not in SUPPORTED_MARKETS:
        raise StrategyDeclarationError(
            f"unsupported market: {market or '<empty>'}"
        )
    return market


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise StrategyDeclarationError("INPUTS entry has empty symbol")
    return symbol


def _normalize_interval(value: Any) -> str:
    interval = str(value or "").strip()
    if not interval:
        raise StrategyDeclarationError("INPUTS entry has empty interval")
    return interval


def _normalize_key(
    exchange: Any,
    market: Any,
    symbol: Any,
    interval: Any,
) -> tuple[str, str, str, str]:
    return (
        _normalize_exchange(exchange),
        _normalize_market(market),
        _normalize_symbol(symbol),
        _normalize_interval(interval),
    )


def parse_declared_inputs(raw: Any) -> list[StrategyInput]:
    try:
        return _parse_declared_inputs(raw)
    except ValueError as exc:
        raise StrategyDeclarationError(str(exc)) from exc


def parse_order_targets(raw: Any) -> list[StrategyOrderTarget]:
    try:
        return _parse_order_targets(raw)
    except ValueError as exc:
        raise StrategyDeclarationError(str(exc)) from exc


def parse_risk_controls(raw: Any) -> StrategyRiskControls:
    try:
        return _parse_risk_controls(raw)
    except ValueError as exc:
        raise StrategyDeclarationError(str(exc)) from exc


def resolve_order_target_leverages(
    order_targets: Iterable[StrategyOrderTarget],
    strategy_leverage: Any,
) -> list[StrategyOrderTarget]:
    resolver = getattr(_strategy_inputs, "resolve_order_target_leverages", None)
    if not callable(resolver):
        raise StrategyDeclarationError(
            "STRATEGY_LIBRARY_LEVERAGE_RESOLVER_UNAVAILABLE: strategy-library "
            "runtime does not support strategy-owned Futures leverage"
        )
    try:
        return resolver(order_targets, strategy_leverage)
    except ValueError as exc:
        raise StrategyDeclarationError(str(exc)) from exc


@dataclass(frozen=True)
class StrategyDeclarations:
    inputs: list[StrategyInput]
    order_targets: list[StrategyOrderTarget]
    risk_controls: StrategyRiskControls

    @property
    def input_keys(self) -> set[tuple[str, str, str, str]]:
        return {entry.key for entry in self.inputs}

    @property
    def order_target_keys(self) -> set[tuple[str, str, str]]:
        return {entry.key for entry in self.order_targets}

    @property
    def required_routes(self) -> set[tuple[str, str]]:
        input_routes = {(entry.exchange, entry.market) for entry in self.inputs}
        order_routes = {
            (entry.exchange, entry.market) for entry in self.order_targets
        }
        return input_routes | order_routes


def extract_declarations(strategy_instance: Any) -> StrategyDeclarations:
    inputs = parse_declared_inputs(getattr(strategy_instance, "INPUTS", None))
    order_targets = resolve_order_target_leverages(
        parse_order_targets(getattr(strategy_instance, "ORDER_TARGETS", None)),
        getattr(strategy_instance, "LEVERAGE", None),
    )
    risk_controls = parse_risk_controls(getattr(strategy_instance, "RISK_CONTROLS", None))
    return StrategyDeclarations(
        inputs=inputs,
        order_targets=order_targets,
        risk_controls=risk_controls,
    )


class InputView:
    """Declaration-bound view handed to ``on_market_data(data, wallet)``."""

    def __init__(self, declared: Iterable[StrategyInput]) -> None:
        decl: dict[str, dict[str, dict[str, set[str]]]] = {}
        for d in declared:
            exchange, market, symbol, interval = _normalize_key(
                d.exchange, d.market, d.symbol, d.interval
            )
            decl.setdefault(exchange, {}).setdefault(market, {}).setdefault(symbol, set()).add(interval)
        if not decl:
            raise StrategyDeclarationError(
                "InputView cannot be constructed from an empty declaration."
            )
        self._declared = decl
        self._cache: dict[tuple[str, str, str, str], MarketData] = {}
        self._trigger: MarketData | None = None

    def update(self, data: MarketData) -> bool:
        """Cache a tick only if its normalized route key is declared."""
        key = _normalize_key(
            getattr(data, "exchange", "binance"),
            data.market,
            data.symbol,
            getattr(data, "interval", ""),
        )
        exchange, market, symbol, interval = key
        if not (
            exchange in self._declared
            and market in self._declared[exchange]
            and symbol in self._declared[exchange][market]
            and interval in self._declared[exchange][market][symbol]
        ):
            return False
        self._cache[key] = data
        self._trigger = data
        return True

    @property
    def trigger(self) -> MarketData | None:
        return self._trigger

    def _require_trigger(self) -> MarketData:
        if self._trigger is None:
            raise RuntimeError(
                "InputView has no trigger; on_market_data should only be "
                "invoked after update() set a declared tick."
            )
        return self._trigger

    @property
    def price(self) -> float:
        return float(self._require_trigger().price)

    @property
    def symbol(self) -> str:
        return self._require_trigger().symbol

    @property
    def interval(self) -> str:
        return str(getattr(self._require_trigger(), "interval", ""))

    @property
    def timestamp(self) -> Any:
        return self._require_trigger().timestamp

    @property
    def klines(self) -> Any:
        return self._require_trigger().klines

    @property
    def orderbook(self) -> Any:
        return self._require_trigger().orderbook

    @property
    def oi(self) -> Any:
        return self._require_trigger().oi

    @property
    def funding_rate(self) -> Any:
        return self._require_trigger().funding_rate

    def is_declared(self, exchange: str, market: str, symbol: str, interval: str) -> bool:
        exchange_key, market_key, symbol_key, interval_key = _normalize_key(
            exchange, market, symbol, interval
        )
        return (
            exchange_key in self._declared
            and market_key in self._declared[exchange_key]
            and symbol_key in self._declared[exchange_key][market_key]
            and interval_key in self._declared[exchange_key][market_key][symbol_key]
        )

    def declared_keys(self) -> list[tuple[str, str, str, str]]:
        out: list[tuple[str, str, str, str]] = []
        for exchange, market_map in self._declared.items():
            for market, symbol_map in market_map.items():
                for symbol, intervals in symbol_map.items():
                    for interval in intervals:
                        out.append((exchange, market, symbol, interval))
        return out

    @property
    def exchange(self) -> "_ExchangeAccessor":
        return _ExchangeAccessor(self)

    @property
    def market(self) -> "_MarketAccessor":
        return _MarketAccessor(self, "binance")


class _ExchangeAccessor:
    __slots__ = ("_view",)

    def __init__(self, view: InputView) -> None:
        self._view = view

    def __getitem__(self, exchange: str) -> "_ExchangeSlice":
        exchange_key = _normalize_exchange(exchange)
        if exchange_key not in self._view._declared:
            raise KeyError(
                f"exchange {exchange!r} is not in the declared strategy universe"
            )
        return _ExchangeSlice(self._view, exchange_key)

    def __contains__(self, exchange: str) -> bool:
        try:
            exchange_key = _normalize_exchange(exchange)
        except StrategyDeclarationError:
            return False
        return exchange_key in self._view._declared

    def keys(self) -> list[str]:
        return list(self._view._declared.keys())


class _ExchangeSlice:
    __slots__ = ("_view", "_exchange")

    def __init__(self, view: InputView, exchange: str) -> None:
        self._view = view
        self._exchange = exchange

    @property
    def market(self) -> "_MarketAccessor":
        return _MarketAccessor(self._view, self._exchange)

    def __getitem__(self, market: str) -> "_MarketSlice":
        return self.market[market]


class _MarketAccessor:
    __slots__ = ("_view", "_exchange")

    def __init__(self, view: InputView, exchange: str) -> None:
        self._view = view
        self._exchange = exchange

    def __getitem__(self, market: str) -> "_MarketSlice":
        market_key = _normalize_market(market)
        market_map = self._view._declared.get(self._exchange, {})
        if market_key not in market_map:
            raise KeyError(
                f"market {market!r} is not declared under exchange {self._exchange!r}"
            )
        return _MarketSlice(self._view, self._exchange, market_key)

    def __contains__(self, market: str) -> bool:
        try:
            market_key = _normalize_market(market)
        except StrategyDeclarationError:
            return False
        return market_key in self._view._declared.get(self._exchange, {})

    def keys(self) -> list[str]:
        return list(self._view._declared.get(self._exchange, {}).keys())


class _MarketSlice:
    __slots__ = ("_view", "_exchange", "_market")

    def __init__(self, view: InputView, exchange: str, market: str) -> None:
        self._view = view
        self._exchange = exchange
        self._market = market

    @property
    def symbol(self) -> "_SymbolAccessor":
        return _SymbolAccessor(self._view, self._exchange, self._market)


class _SymbolAccessor:
    __slots__ = ("_view", "_exchange", "_market")

    def __init__(self, view: InputView, exchange: str, market: str) -> None:
        self._view = view
        self._exchange = exchange
        self._market = market

    def __getitem__(self, symbol: str) -> "_SymbolSlice":
        symbol_key = _normalize_symbol(symbol)
        symbol_map = self._view._declared.get(self._exchange, {}).get(self._market, {})
        if symbol_key not in symbol_map:
            raise KeyError(
                f"symbol {symbol!r} is not declared under "
                f"{self._exchange}/{self._market}"
            )
        return _SymbolSlice(self._view, self._exchange, self._market, symbol_key)

    def __contains__(self, symbol: str) -> bool:
        try:
            symbol_key = _normalize_symbol(symbol)
        except StrategyDeclarationError:
            return False
        return symbol_key in self._view._declared.get(self._exchange, {}).get(self._market, {})

    def keys(self) -> list[str]:
        return list(self._view._declared.get(self._exchange, {}).get(self._market, {}).keys())


class _SymbolSlice:
    __slots__ = ("_view", "_exchange", "_market", "_symbol")

    def __init__(self, view: InputView, exchange: str, market: str, symbol: str) -> None:
        self._view = view
        self._exchange = exchange
        self._market = market
        self._symbol = symbol

    @property
    def interval(self) -> "_IntervalAccessor":
        return _IntervalAccessor(self._view, self._exchange, self._market, self._symbol)


class _IntervalAccessor:
    __slots__ = ("_view", "_exchange", "_market", "_symbol")

    def __init__(self, view: InputView, exchange: str, market: str, symbol: str) -> None:
        self._view = view
        self._exchange = exchange
        self._market = market
        self._symbol = symbol

    def __getitem__(self, interval: str) -> MarketData | None:
        interval_key = _normalize_interval(interval)
        declared = (
            self._view._declared
            .get(self._exchange, {})
            .get(self._market, {})
            .get(self._symbol, set())
        )
        if interval_key not in declared:
            raise KeyError(
                f"interval {interval!r} is not declared for "
                f"{self._exchange}/{self._market}/{self._symbol}"
            )
        return self._view._cache.get(
            (self._exchange, self._market, self._symbol, interval_key)
        )

    def __contains__(self, interval: str) -> bool:
        try:
            interval_key = _normalize_interval(interval)
        except StrategyDeclarationError:
            return False
        return interval_key in (
            self._view._declared
            .get(self._exchange, {})
            .get(self._market, {})
            .get(self._symbol, set())
        )

    def keys(self) -> list[str]:
        return list(
            self._view._declared
            .get(self._exchange, {})
            .get(self._market, {})
            .get(self._symbol, set())
        )
