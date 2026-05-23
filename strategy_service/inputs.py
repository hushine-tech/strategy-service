"""Strategy input declaration contract (pre-C3).

Strategies MUST declare the `(market, symbol, interval)` universe they consume
via a class-level ``INPUTS`` attribute. The runtime parses and validates this
declaration at strategy load time, then uses it as the authoritative input set
for routing, wallet on-market-data updates, and the ``on_market_data`` callback
view.

The declaration replaces the previous wallet-positions / wallet-assets /
``data.symbol`` reverse-inference rules described in ``docs/pre_C3.md``.

Shape accepted by ``parse_declared_inputs``:

    class MyStrategy:
        INPUTS = [
            {"market": "futures", "symbol": "ETHUSDT", "interval": "1m"},
            {"market": "spot",    "symbol": "BTCUSDT", "interval": "5m"},
        ]

Also tolerated for authoring convenience:

    INPUTS = [("futures", "ETHUSDT", "1m"), ...]            # 3-tuple
    INPUTS = ["futures:ETHUSDT:1m", ...]                    # colon-joined string

Normalization rules:

- ``market`` lower-cased; MUST be in ``{"spot", "futures"}``
- ``symbol`` upper-cased + stripped; MUST be non-empty
- ``interval`` trimmed; MUST be non-empty (e.g. ``1m`` / ``5m`` / ``1h``)
- Duplicates are de-duplicated by the 3-tuple
- The normalized universe MUST NOT be empty
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from strategy_service.types import MarketData

__all__ = [
    "StrategyInput",
    "StrategyDeclarationError",
    "InputView",
    "parse_declared_inputs",
]


SUPPORTED_MARKETS = frozenset({"spot", "futures"})


class StrategyDeclarationError(ValueError):
    """Raised when a strategy's ``INPUTS`` declaration is missing or invalid."""


@dataclass(frozen=True)
class StrategyInput:
    """Normalized ``(market, symbol, interval)`` entry declared by a strategy."""

    market: str
    symbol: str
    interval: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.market, self.symbol, self.interval)


# ── Parsing ─────────────────────────────────────────────────────────────────


def _coerce_entry(entry: Any) -> tuple[str, str, str]:
    if isinstance(entry, StrategyInput):
        return entry.key
    if isinstance(entry, Mapping):
        market = entry.get("market") or entry.get("symbol_type")
        symbol = entry.get("symbol") or entry.get("sym")
        interval = entry.get("interval") or entry.get("period")
        if not (market and symbol and interval):
            raise StrategyDeclarationError(
                f"INPUTS entry missing required field(s): {entry!r}"
            )
        return (str(market), str(symbol), str(interval))
    if isinstance(entry, tuple) and len(entry) == 3:
        market, symbol, interval = entry
        return (str(market), str(symbol), str(interval))
    if isinstance(entry, str) and entry.count(":") == 2:
        market, symbol, interval = entry.split(":", 2)
        return (market, symbol, interval)
    raise StrategyDeclarationError(
        f"INPUTS entry has unsupported shape: {entry!r}. "
        f"Expected dict, (market, symbol, interval) tuple, or 'market:symbol:interval' string."
    )


def parse_declared_inputs(raw: Any) -> list[StrategyInput]:
    """Parse and normalize a strategy's ``INPUTS`` declaration.

    Raises ``StrategyDeclarationError`` when the declaration is missing,
    malformed, or normalizes to an empty universe.
    """
    if raw is None:
        raise StrategyDeclarationError(
            "strategy has no INPUTS declaration; "
            "strategies must declare their (market, symbol, interval) universe."
        )
    if isinstance(raw, (str, bytes)) or isinstance(raw, Mapping):
        # A bare string / dict at the top level is almost certainly a typo —
        # the declaration is always a list/tuple of entries.
        raise StrategyDeclarationError(
            "INPUTS must be an iterable of entries, not a single object."
        )
    if not isinstance(raw, Iterable):
        raise StrategyDeclarationError(
            f"INPUTS must be iterable, got {type(raw).__name__}."
        )

    seen: set[tuple[str, str, str]] = set()
    out: list[StrategyInput] = []
    for entry in raw:
        market, symbol, interval = _coerce_entry(entry)
        market = str(market).strip().lower()
        symbol = str(symbol).strip().upper()
        interval = str(interval).strip()
        if market not in SUPPORTED_MARKETS:
            raise StrategyDeclarationError(
                f"INPUTS entry has unsupported market {market!r}; "
                f"expected one of {sorted(SUPPORTED_MARKETS)}."
            )
        if not symbol:
            raise StrategyDeclarationError(
                f"INPUTS entry has empty symbol: {entry!r}"
            )
        if not interval:
            raise StrategyDeclarationError(
                f"INPUTS entry has empty interval: {entry!r}"
            )
        key = (market, symbol, interval)
        if key in seen:
            continue
        seen.add(key)
        out.append(StrategyInput(market=market, symbol=symbol, interval=interval))

    if not out:
        raise StrategyDeclarationError(
            "INPUTS declaration resolved to an empty universe; "
            "strategies must declare at least one (market, symbol, interval) entry."
        )
    return out


# ── Declaration-bound input view ────────────────────────────────────────────
#
# Gives user callbacks the accessor shape:
#
#     data.market["futures"].symbol["ETHUSDT"].interval["1m"]
#
# - Attribute access (``.market`` / ``.symbol`` / ``.interval``) always returns
#   a dict-like helper; its ``__getitem__`` raises ``KeyError`` when the key
#   is not in the declared universe.
# - The terminal ``interval[...]`` returns the latest ``MarketData`` cached for
#   that input, or ``None`` if the strategy has not yet received a tick for it.


class InputView:
    """Declaration-bound view handed to ``on_market_data(data, wallet)``.

    The view is long-lived for a strategy instance — individual ticks update
    its cache in place rather than replacing the view object. User code can
    therefore safely capture a reference (e.g. during ``__init__``) if it
    wants, though the normal pattern is to re-access on each callback.

    Attribute surface (exact semantics — important for migrating legacy code):

    - ``data.market`` is the **declared-universe accessor** (NOT a string).
      Use it as ``data.market["futures"].symbol["ETHUSDT"].interval["1m"]``
      to reach the latest cached ``MarketData`` for that declared input.
      Old code like ``str(data.market) == "futures"`` will NOT work —
      ``data.market`` is an accessor object, not the trigger's market string.
      For the triggering tick's market string, use ``data.trigger.market``.

    - ``data.trigger`` returns the ``MarketData`` object that caused the
      current callback invocation, or ``None`` if no tick has been delivered
      yet.

    - ``data.price`` / ``data.symbol`` / ``data.interval`` / ``data.timestamp``
      / ``data.klines`` / ``data.orderbook`` / ``data.oi`` /
      ``data.funding_rate`` are **trigger proxies** — they return the
      matching field on ``data.trigger``. These are the single-input
      convenience accessors; the accessor path (``data.market[...]``) is the
      canonical form for multi-input strategies.
    """

    def __init__(self, declared: Iterable[StrategyInput]) -> None:
        # market -> {symbol -> {interval}}  (declared keys only)
        decl: dict[str, dict[str, set[str]]] = {}
        for d in declared:
            decl.setdefault(d.market, {}).setdefault(d.symbol, set()).add(d.interval)
        if not decl:
            raise StrategyDeclarationError(
                "InputView cannot be constructed from an empty declaration."
            )
        self._declared = decl
        self._cache: dict[tuple[str, str, str], MarketData] = {}
        self._trigger: MarketData | None = None

    # ── mutation (runtime internal) ────────────────────────────────────

    def update(self, data: MarketData) -> bool:
        """Store a new tick for its declared key and mark it as the trigger.
        Returns True if the key is declared (and was cached); False otherwise.
        Callers must not invoke ``on_market_data`` when this returns False.
        """
        market = str(data.market).strip().lower()
        symbol = str(data.symbol).strip().upper()
        interval = str(getattr(data, "interval", "")).strip()
        if not (
            market in self._declared
            and symbol in self._declared[market]
            and interval in self._declared[market][symbol]
        ):
            return False
        self._cache[(market, symbol, interval)] = data
        self._trigger = data
        return True

    # ── convenience proxies to the triggering tick ────────────────────
    # These make single-input strategies portable without rewriting every
    # callback to index through the nested accessor. Multi-input strategies
    # should use the explicit ``market[...].symbol[...].interval[...]`` path.

    @property
    def trigger(self) -> MarketData | None:
        return self._trigger

    def _require_trigger(self) -> MarketData:
        if self._trigger is None:
            raise RuntimeError(
                "InputView has no trigger — on_market_data should only be "
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
        # Name collision with the declared-key accessor path: that path only
        # fires through ``market[...].symbol[...].interval`` (attribute access
        # on a ``_SymbolSlice``). Plain ``view.interval`` here returns the
        # trigger's interval string, which is what simple single-input
        # strategies want.
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

    def is_declared(self, market: str, symbol: str, interval: str) -> bool:
        m = str(market).strip().lower()
        s = str(symbol).strip().upper()
        i = str(interval).strip()
        return (
            m in self._declared
            and s in self._declared[m]
            and i in self._declared[m][s]
        )

    def declared_keys(self) -> list[tuple[str, str, str]]:
        out: list[tuple[str, str, str]] = []
        for m, sym_map in self._declared.items():
            for s, ivs in sym_map.items():
                for i in ivs:
                    out.append((m, s, i))
        return out

    # ── reader surface ──────────────────────────────────────────────────

    @property
    def market(self) -> "_MarketAccessor":
        return _MarketAccessor(self)


class _MarketAccessor:
    __slots__ = ("_view",)

    def __init__(self, view: InputView) -> None:
        self._view = view

    def __getitem__(self, market: str) -> "_MarketSlice":
        m = str(market).strip().lower()
        if m not in self._view._declared:
            raise KeyError(
                f"market {market!r} is not in the declared strategy universe"
            )
        return _MarketSlice(self._view, m)

    def __contains__(self, market: str) -> bool:
        return str(market).strip().lower() in self._view._declared

    def keys(self) -> list[str]:
        return list(self._view._declared.keys())


class _MarketSlice:
    __slots__ = ("_view", "_market")

    def __init__(self, view: InputView, market: str) -> None:
        self._view = view
        self._market = market

    @property
    def symbol(self) -> "_SymbolAccessor":
        return _SymbolAccessor(self._view, self._market)


class _SymbolAccessor:
    __slots__ = ("_view", "_market")

    def __init__(self, view: InputView, market: str) -> None:
        self._view = view
        self._market = market

    def __getitem__(self, symbol: str) -> "_SymbolSlice":
        s = str(symbol).strip().upper()
        sym_map = self._view._declared.get(self._market, {})
        if s not in sym_map:
            raise KeyError(
                f"symbol {symbol!r} is not declared under market {self._market!r}"
            )
        return _SymbolSlice(self._view, self._market, s)

    def __contains__(self, symbol: str) -> bool:
        s = str(symbol).strip().upper()
        return s in self._view._declared.get(self._market, {})

    def keys(self) -> list[str]:
        return list(self._view._declared.get(self._market, {}).keys())


class _SymbolSlice:
    __slots__ = ("_view", "_market", "_symbol")

    def __init__(self, view: InputView, market: str, symbol: str) -> None:
        self._view = view
        self._market = market
        self._symbol = symbol

    @property
    def interval(self) -> "_IntervalAccessor":
        return _IntervalAccessor(self._view, self._market, self._symbol)


class _IntervalAccessor:
    __slots__ = ("_view", "_market", "_symbol")

    def __init__(self, view: InputView, market: str, symbol: str) -> None:
        self._view = view
        self._market = market
        self._symbol = symbol

    def __getitem__(self, interval: str) -> MarketData | None:
        i = str(interval).strip()
        declared = self._view._declared.get(self._market, {}).get(self._symbol, set())
        if i not in declared:
            raise KeyError(
                f"interval {interval!r} is not declared for "
                f"{self._market}/{self._symbol}"
            )
        return self._view._cache.get((self._market, self._symbol, i))

    def __contains__(self, interval: str) -> bool:
        i = str(interval).strip()
        return i in self._view._declared.get(self._market, {}).get(self._symbol, set())

    def keys(self) -> list[str]:
        return list(self._view._declared.get(self._market, {}).get(self._symbol, set()))
