from __future__ import annotations

import importlib
import logging
import math
import re
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

from strategy_service.indicators import IndicatorDefinition, IndicatorFrame, IndicatorWriter, parse_indicator_definitions
from strategy_service.inputs import (
    InputView,
    StrategyDeclarations,
    StrategyDeclarationError,
    StrategyInput,
    _normalize_exchange,
    _normalize_market,
    extract_declarations,
    parse_declared_inputs,
)
from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.types import MarketData, OrderDecision, OrderSide, OrderType, OrderUpdateEvent
from strategy_service.wallet.order_types import ExecutionFeedback, OrderResponse
from strategy_service.wallet.portfolio import PortfolioWalletRuntime

logger = logging.getLogger(__name__)

_TERMINAL_ORDER_STATUSES = {
    "FILLED",
    "CANCELED",
    "EXPIRED",
    "REJECTED",
    "RECOVERY_EXPIRED",
    "FORCE_CLOSED",
}


class StrategyUserCodeError(RuntimeError):
    """Raised when user strategy code fails inside a runtime callback."""


USER_STRATEGY_ON_MARKET_DATA_ERROR_PREFIX = "user strategy on_market_data failed:"
_INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "1h": 3_600_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}
_INTERVAL_RE = re.compile(r"^(\d+)([smhdw])$")


def _norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def _norm_exchange(exchange: str) -> str:
    return _normalize_exchange(exchange)


def _norm_market(market: str) -> str:
    return _normalize_market(market)


def _wallet_market(market: str) -> str:
    market_key = _norm_market(market)
    if market_key in {"perpetual_futures", "delivery_futures"}:
        return "futures"
    return market_key


def _is_futures_market(market: str) -> bool:
    return _norm_market(market) in {"perpetual_futures", "delivery_futures"}


def _norm_interval(interval: str) -> str:
    return str(interval).strip()


def _interval_to_ms(interval: str) -> int:
    raw = _norm_interval(interval).lower()
    if raw in _INTERVAL_MS:
        return _INTERVAL_MS[raw]
    match = _INTERVAL_RE.match(raw)
    if not match:
        return 60_000
    qty = int(match.group(1))
    unit = match.group(2)
    unit_ms = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }[unit]
    return qty * unit_ms


def _stream_key(exchange: str, market: str, symbol: str, interval: str) -> str:
    return f"{_norm_exchange(exchange)}:{_norm_market(market)}:{_norm_symbol(symbol)}:{_norm_interval(interval)}"


def _value_to_epoch_ms(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _market_time_ms(market_data: MarketData) -> int:
    for attr in ("open_time_ms", "open_time", "time_ms"):
        raw = getattr(market_data, attr, None)
        if raw is not None:
            return _value_to_epoch_ms(raw)
    klines = getattr(market_data, "klines", None)
    if isinstance(klines, dict):
        for key in ("open_time_ms", "open_time", "timestamp"):
            raw = klines.get(key)
            if raw is not None:
                return _value_to_epoch_ms(raw)
    return _value_to_epoch_ms(getattr(market_data, "timestamp", None))


def _normalize_decisions(signal: object) -> list[OrderDecision]:
    if signal is None:
        return []
    if isinstance(signal, OrderDecision):
        return [signal]
    if isinstance(signal, list) and all(isinstance(item, OrderDecision) for item in signal):
        return list(signal)
    raise ValueError("on_market_data must return None, OrderDecision, or list[OrderDecision]")


def _parse_positive_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, str):
        raise ValueError(f"OrderDecision.{field} must be a string")
    try:
        parsed = Decimal(value.strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"OrderDecision.{field} must be a decimal string") from exc
    if not parsed.is_finite():
        raise ValueError(f"OrderDecision.{field} must be finite")
    try:
        parsed_float = float(parsed)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"OrderDecision.{field} must be finite") from exc
    if not math.isfinite(parsed_float):
        raise ValueError(f"OrderDecision.{field} must be finite")
    if parsed_float <= 0.0:
        raise ValueError(f"OrderDecision.{field} must be finite")
    try:
        if parsed <= 0:
            raise ValueError(f"OrderDecision.{field} must be > 0")
    except InvalidOperation as exc:
        raise ValueError(f"OrderDecision.{field} must be finite") from exc
    return parsed


def _normalize_order_side(side: object) -> str:
    side_key = str(side or "").strip().upper()
    if side_key not in {OrderSide.BUY, OrderSide.SELL}:
        raise ValueError("OrderDecision.side must be BUY or SELL")
    return side_key


def _normalize_order_type(order_type: object) -> str:
    order_type_key = str(order_type or "").strip().upper()
    if order_type_key not in {OrderType.MARKET, OrderType.LIMIT}:
        raise ValueError("OrderDecision.order_type must be MARKET or LIMIT")
    return order_type_key


@dataclass(frozen=True)
class _PreparedOrderDecision:
    signal: OrderDecision
    exchange: str
    market: str
    symbol: str
    qty: Decimal
    price: Decimal | None
    side: str
    order_type: str
    mark_price: float
    market_time: Any
    mark_price_refreshed: bool
    venue_id: int
    route_wallet: Any


def _load_strategy_instance_from_code(filename: str, source: str) -> Any:
    ns: dict = {}
    try:
        code = compile(source, filename, "exec")
        exec(code, ns)  # noqa: S102
    except Exception as e:
        raise ImportError(
            f"failed to exec strategy code: {e}"
        ) from e
    if "MyStrategy" not in ns:
        raise AttributeError(
            "strategy code has no 'MyStrategy' class"
        )
    return ns["MyStrategy"]()


def _load_strategy_instance(strategy_path: str, strategy_code: str | None = None) -> Any:
    if strategy_code is not None:
        # Dynamic exec for DB-backed strategies.
        return _load_strategy_instance_from_code(strategy_path, strategy_code)

    source_path = Path(strategy_path)
    if source_path.is_file():
        try:
            source = source_path.read_text(encoding="utf-8")
        except OSError as e:
            raise ImportError(f"failed to read strategy source {strategy_path!r}: {e}") from e
        return _load_strategy_instance_from_code(str(source_path), source)

    try:
        module = importlib.import_module(strategy_path)
    except ImportError as e:
        raise ImportError(
            f"failed to import strategy module {strategy_path!r}: {e}"
        ) from e
    if not hasattr(module, "MyStrategy"):
        raise AttributeError(
            f"module {strategy_path!r} has no 'MyStrategy' class"
        )
    cls = getattr(module, "MyStrategy")
    return cls()


def _read_declared_inputs(strategy_instance: Any) -> list[StrategyInput]:
    """Extract + normalize ``MyStrategy.INPUTS`` from an already-loaded strategy.

    Accepted forms (see ``inputs.parse_declared_inputs``):
      - class attribute ``INPUTS``
      - callable ``inputs()`` / ``declared_inputs()`` returning the same shape
    """
    raw = getattr(strategy_instance, "INPUTS", None)
    if raw is None:
        # Also accept a method variant for future compatibility.
        for attr in ("inputs", "declared_inputs"):
            fn = getattr(strategy_instance, attr, None)
            if callable(fn):
                try:
                    raw = fn()
                except Exception as e:
                    raise StrategyDeclarationError(
                        f"strategy.{attr}() raised: {e}"
                    ) from e
                break
    return parse_declared_inputs(raw)


def _read_indicator_definitions(strategy_instance: Any) -> list[IndicatorDefinition]:
    try:
        return parse_indicator_definitions(getattr(strategy_instance, "INDICATORS", None))
    except ValueError as exc:
        raise StrategyDeclarationError(f"invalid INDICATORS: {exc}") from exc


def extract_strategy_inputs(
    strategy_path: str,
    strategy_code: str | None = None,
) -> list[StrategyInput]:
    """Introspection helper — loads the strategy and returns its declared inputs.

    Callers that want a best-effort symbol preview (e.g. gateway / preflight)
    can use this; it will raise ``StrategyDeclarationError`` if the strategy
    has no valid declaration, surfacing the same contract violation the
    runtime would see.
    """
    strategy = _load_strategy_instance(strategy_path, strategy_code)
    return _read_declared_inputs(strategy)


def extract_strategy_declarations(
    strategy_path: str,
    strategy_code: str | None = None,
) -> StrategyDeclarations:
    """Load a strategy and return normalized INPUTS + ORDER_TARGETS."""
    strategy = _load_strategy_instance(strategy_path, strategy_code)
    return extract_declarations(strategy)


def flatten_declared_inputs_to_symbols(
    declared: list[StrategyInput],
) -> list[tuple[str, str]]:
    """Collapse a list of declared inputs to ``[(symbol, market), ...]`` for
    the preflight / live-subscription code that still operates at that
    granularity. Deduplicates by ``(symbol, market)``.

    Callers MUST call ``extract_strategy_inputs`` (which raises on declaration
    errors) first and surface those errors themselves; this helper deliberately
    does not swallow anything.
    """
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for inp in declared:
        key = (inp.symbol, inp.market)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


class BaseStrategy:
    """Dispatcher from declared-input ticks to wallet + user signal + order.

    Contract (pre_C3):
      - Strategy declares its ``(market, symbol, interval)`` universe via
        ``INPUTS`` on the class.
      - The router only binds to that declaration; wallet positions / assets
        do NOT expand the strategy universe.
      - ``on_market_data(view, wallet)`` receives a declaration-bound
        ``InputView`` — never a raw ``MarketData``.
    """

    def __init__(
        self,
        strategy_path: str,
        wallet: PortfolioWalletRuntime,
        order_client: OrderClient | None = None,
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        strategy_code: str | None = None,
        notifier: StrategyNotifier | None = None,
        hot_reload: bool = False,
        on_user_code_error: Callable[[str], None] | None = None,
        on_user_code_recovered: Callable[[], None] | None = None,
    ) -> None:
        if not isinstance(wallet, PortfolioWalletRuntime):
            raise TypeError("BaseStrategy wallet must be PortfolioWalletRuntime")
        self.strategy_path = strategy_path
        self.wallet = wallet
        self.on_order_callback: Any | None = None
        self.on_indicator_frame: Callable[[str, int, int, IndicatorFrame], None] | None = None
        self._order_client: OrderClient = order_client or OrderClient()
        self._portfolio_id: int = portfolio_id
        self._strategy_id: int = strategy_id
        self._session_id: str = session_id
        self._strategy_code: str | None = strategy_code
        self._hot_reload_enabled = bool(hot_reload and strategy_code is None)
        self._hot_reload_source_path: Path | None = self._resolve_hot_reload_source_path(strategy_path)
        self._hot_reload_signature: tuple[int, int] | None = None
        self._on_user_code_error = on_user_code_error
        self._on_user_code_recovered = on_user_code_recovered
        self._notifier = (notifier or StrategyNotifier()).bind_context(
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            session_id=session_id,
        )

        # Load + validate the user strategy at construction time so any
        # declaration error fails fast (before any tick is routed).
        self._strategy_instance: Any = _load_strategy_instance(
            strategy_path, strategy_code=strategy_code,
        )
        setattr(self._strategy_instance, "notify", self._notifier)
        self._decl = extract_declarations(self._strategy_instance)
        self._indicator_definitions = _read_indicator_definitions(self._strategy_instance)
        self._indicator_writer = self._install_indicator_writer(
            self._strategy_instance,
            self._indicator_definitions,
        )
        self._inputs: list[StrategyInput] = self._decl.inputs
        self._order_targets = self._decl.order_targets
        self._input_keys: set[tuple[str, str, str, str]] = self._decl.input_keys
        self._order_target_keys: set[tuple[str, str, str]] = self._decl.order_target_keys
        self._required_routes: set[tuple[str, str]] = self._decl.required_routes
        self._view: InputView = InputView(self._inputs)
        self._blocked_order_keys: set[tuple[str, str, str]] = set()
        self._order_event_cursor: int = 0
        self._settled_lifecycle_event_ids: set[int] = set()
        self._sync_settled_order_quantities: dict[str, float] = {}
        self._last_market_time: Any | None = None
        if self._hot_reload_source_path is not None:
            self._hot_reload_signature = self._strategy_file_signature(self._hot_reload_source_path)
        self._initialize_order_event_cursor()

    @property
    def declared_inputs(self) -> list[StrategyInput]:
        """Read-only snapshot of the strategy's declared universe."""
        return list(self._inputs)

    @property
    def last_market_time(self) -> Any | None:
        return self._last_market_time

    def _get_strategy(self) -> Any:
        return self._strategy_instance

    @property
    def indicator_definitions(self) -> list[IndicatorDefinition]:
        return list(self._indicator_definitions)

    def _install_indicator_writer(
        self,
        strategy_instance: Any,
        definitions: list[IndicatorDefinition],
    ) -> IndicatorWriter:
        writer = IndicatorWriter(definitions)
        setattr(strategy_instance, "indicators", writer)
        return writer

    def _resolve_hot_reload_source_path(self, strategy_path: str) -> Path | None:
        if not self._hot_reload_enabled:
            return None
        source_path = Path(strategy_path)
        if source_path.is_file():
            return source_path
        logger.warning(
            "strategy hot reload disabled: source file does not exist: session=%s path=%s",
            self._session_id,
            strategy_path,
        )
        return None

    @staticmethod
    def _strategy_file_signature(path: Path) -> tuple[int, int]:
        stat = path.stat()
        return int(stat.st_mtime_ns), int(stat.st_size)

    def _maybe_reload_strategy(self) -> None:
        source_path = self._hot_reload_source_path
        if source_path is None:
            return
        try:
            signature = self._strategy_file_signature(source_path)
        except OSError:
            logger.warning(
                "strategy hot reload skipped: source file unavailable: session=%s path=%s",
                self._session_id,
                source_path,
                exc_info=True,
            )
            return
        if signature == self._hot_reload_signature:
            return

        try:
            candidate = _load_strategy_instance(str(source_path))
            candidate_decl = extract_declarations(candidate)
            candidate_indicator_definitions = _read_indicator_definitions(candidate)
        except Exception:  # noqa: BLE001
            self._hot_reload_signature = signature
            logger.warning(
                "strategy hot reload failed: session=%s path=%s",
                self._session_id,
                source_path,
                exc_info=True,
            )
            return

        if (
            candidate_decl.input_keys != self._input_keys
            or candidate_decl.order_target_keys != self._order_target_keys
            or candidate_decl.required_routes != self._required_routes
        ):
            self._hot_reload_signature = signature
            logger.warning(
                "strategy hot reload skipped: declaration changed; restart session required: "
                "session=%s path=%s old_inputs=%s new_inputs=%s old_order_targets=%s new_order_targets=%s",
                self._session_id,
                source_path,
                sorted(self._input_keys),
                sorted(candidate_decl.input_keys),
                sorted(self._order_target_keys),
                sorted(candidate_decl.order_target_keys),
            )
            return

        setattr(candidate, "notify", self._notifier)
        self._indicator_definitions = candidate_indicator_definitions
        self._indicator_writer = self._install_indicator_writer(candidate, self._indicator_definitions)
        self._strategy_instance = candidate
        self._hot_reload_signature = signature
        logger.info(
            "strategy hot reloaded: session=%s path=%s",
            self._session_id,
            source_path,
        )

    def _prepare_indicator_frame(self) -> None:
        writer = getattr(self._strategy_instance, "indicators", None)
        if writer is not None:
            writer.reset_bar()

    def _drain_indicator_frame(self, stream_key: str, market_time_ms: int, interval_ms: int) -> None:
        writer = getattr(self._strategy_instance, "indicators", None)
        if writer is None or not self._indicator_definitions:
            return
        frame = writer.drain()
        for warning in frame.warnings:
            logger.warning(
                "strategy indicator warning: session=%s strategy_id=%s %s",
                self._session_id,
                self._strategy_id,
                warning,
            )
        if self.on_indicator_frame is None:
            return
        try:
            self.on_indicator_frame(stream_key, market_time_ms, interval_ms, frame)
        except Exception:  # noqa: BLE001
            logger.warning(
                "strategy indicator callback failed: session=%s strategy_id=%s stream_key=%s",
                self._session_id,
                self._strategy_id,
                stream_key,
                exc_info=True,
            )

    def _emit_user_code_error(self, message: str) -> None:
        if self._on_user_code_error is None:
            return
        try:
            self._on_user_code_error(message)
        except Exception:  # noqa: BLE001
            logger.warning("user code error callback failed: session=%s", self._session_id, exc_info=True)

    def _emit_user_code_recovered(self) -> None:
        if self._on_user_code_recovered is None:
            return
        try:
            self._on_user_code_recovered()
        except Exception:  # noqa: BLE001
            logger.warning("user code recovery callback failed: session=%s", self._session_id, exc_info=True)

    def _notify_order_response(self, order_resp: Any) -> None:
        self._maybe_reload_strategy()
        fn = getattr(self._strategy_instance, "on_order_response", None)
        if callable(fn):
            try:
                fn(order_resp)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "order response callback failed: session=%s",
                    self._session_id,
                    exc_info=True,
                )

    def _notify_order_update(self, event: OrderUpdateEvent) -> None:
        self._maybe_reload_strategy()
        fn = getattr(self._strategy_instance, "on_order_update", None)
        if callable(fn):
            try:
                fn(event, self.wallet)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "order lifecycle callback failed: session=%s event_id=%s",
                    self._session_id,
                    int(getattr(event, "event_id", 0) or 0),
                    exc_info=True,
                )

    def _venue_id_for_route(self, exchange: str, market: str) -> int:
        route = (_norm_exchange(exchange), _norm_market(market))
        matches = [
            venue_id
            for wallet_exchange, wallet_market, venue_id in self.wallet.wallets
            if (wallet_exchange, wallet_market) == route
        ]
        if not matches:
            raise ValueError(f"missing wallet for route {route[0]}/{route[1]}")
        if len(matches) > 1:
            venue_ids = ", ".join(str(venue_id) for venue_id in matches)
            raise ValueError(
                f"ambiguous wallet route {route[0]}/{route[1]} matched venue ids: {venue_ids}"
            )
        return matches[0]

    def _apply_order_to_wallet(
        self,
        exchange: str,
        market: str,
        symbol: str,
        order_resp: OrderResponse,
        *,
        venue_id: int | None = None,
    ) -> None:
        symbol_type = _wallet_market(market)
        route_venue_id = self._venue_id_for_route(exchange, market) if venue_id is None else int(venue_id)
        self.wallet.on_order(exchange, market, route_venue_id, symbol, symbol_type, order_resp)

    def _initialize_order_event_cursor(self) -> None:
        if not self._session_id or not hasattr(self._order_client, "list_order_lifecycle_events"):
            return
        cursor = 0
        for _ in range(100):
            try:
                events = self._order_client.list_order_lifecycle_events(
                    session_id=self._session_id,
                    after_event_id=cursor,
                    limit=500,
                )
            except Exception:
                logger.warning("order lifecycle cursor initialization failed", exc_info=True)
                break
            if not events:
                break
            cursor = max(cursor, *(int(getattr(event, "event_id", 0) or 0) for event in events))
            if len(events) < 500:
                break
        self._order_event_cursor = cursor

    def _consume_order_updates(self) -> None:
        if not self._session_id or not hasattr(self._order_client, "list_order_lifecycle_events"):
            return
        try:
            events = self._order_client.list_order_lifecycle_events(
                session_id=self._session_id,
                after_event_id=self._order_event_cursor,
                limit=100,
            )
        except Exception:
            logger.warning("order lifecycle event fetch failed", exc_info=True)
            return
        for event in events:
            self.handle_order_update(event)

    def handle_order_update(self, event: OrderUpdateEvent) -> bool:
        """Apply one order lifecycle event and notify the user strategy.

        This method is intentionally shared by the historical polling path and
        RuntimeChannel push delivery so wallet settlement has a single source of
        truth.
        """
        event_id = int(getattr(event, "event_id", 0) or 0)
        if event_id > 0 and event_id <= self._order_event_cursor:
            return False
        order_resp = None
        wallet_updated = False
        try:
            order_resp = OrderClient.order_response_from_update(event)
            if order_resp is not None:
                if event_id <= 0 or event_id not in self._settled_lifecycle_event_ids:
                    event_exchange = _norm_exchange(getattr(event, "exchange", ""))
                    event_market = _norm_market(getattr(event, "market", ""))
                    route_wallet = self.wallet.get(event_exchange, event_market)
                    order_resp = self._adjust_lifecycle_order_response(order_resp, route_wallet)
                    if order_resp is None:
                        if event_id > 0:
                            self._settled_lifecycle_event_ids.add(event_id)
                        if self._is_order_update_terminal(event, None):
                            self._blocked_order_keys.discard(self._blocked_key_for_event(event, None))
                        if event_id > self._order_event_cursor:
                            self._order_event_cursor = event_id
                        return False
                    self._apply_order_to_wallet(
                        event_exchange,
                        event_market,
                        order_resp.symbol,
                        order_resp,
                        venue_id=getattr(event, "venue_id", None),
                    )
                    self._record_order_settlement(order_resp)
                    if event_id > 0:
                        self._settled_lifecycle_event_ids.add(event_id)
                    wallet_updated = True
                if self._is_order_update_terminal(event, order_resp):
                    self._blocked_order_keys.discard(self._blocked_key_for_event(event, order_resp))
            elif self._is_order_update_terminal(event, None):
                self._blocked_order_keys.discard(self._blocked_key_for_event(event, None))
        except Exception:
            logger.warning(
                "order lifecycle event handling failed: session=%s event_id=%s",
                self._session_id,
                event_id,
                exc_info=True,
            )
        self._notify_order_update(event)
        if wallet_updated and self.on_order_callback is not None:
            try:
                self.on_order_callback()
            except Exception:
                logger.warning("on_order_callback failed after lifecycle update", exc_info=True)
        if event_id > self._order_event_cursor:
            self._order_event_cursor = event_id
        return wallet_updated

    @staticmethod
    def _is_order_update_terminal(event: OrderUpdateEvent, order_resp: OrderResponse | None) -> bool:
        status = str(getattr(order_resp, "status", "") or getattr(event, "order_status", "") or "").strip().upper()
        if status == "PARTIALLY_FILLED":
            return False
        if status in _TERMINAL_ORDER_STATUSES:
            return True
        if order_resp is not None and float(getattr(order_resp, "remaining_qty", 0.0) or 0.0) <= 0.0:
            return True
        return False

    @staticmethod
    def _blocked_key_for_event(event: OrderUpdateEvent, order_resp: OrderResponse | None) -> tuple[str, str, str]:
        symbol = str(
            getattr(order_resp, "symbol", "")
            or getattr(getattr(event, "fill", None), "symbol", "")
            or getattr(event, "symbol", "")
            or ""
        )
        return (
            _norm_exchange(getattr(event, "exchange", "")),
            _norm_market(getattr(event, "market", "")),
            _norm_symbol(symbol),
        )

    @staticmethod
    def _order_cumulative_executed_qty(order_resp: OrderResponse) -> float | None:
        try:
            executed_qty = abs(float(getattr(order_resp, "executed_qty", 0.0) or 0.0))
        except (TypeError, ValueError):
            return None
        if executed_qty > 0.0:
            return executed_qty
        return None

    @staticmethod
    def _order_delta_qty(order_resp: OrderResponse) -> float:
        try:
            return abs(float(getattr(order_resp, "qty", 0.0) or 0.0))
        except (TypeError, ValueError):
            return 0.0

    def _record_order_settlement(self, order_resp: OrderResponse) -> None:
        order_id = str(getattr(order_resp, "order_id", "") or "").strip()
        if not order_id:
            return
        current = self._sync_settled_order_quantities.get(order_id, 0.0)
        cumulative = self._order_cumulative_executed_qty(order_resp)
        if cumulative is None:
            delta = self._order_delta_qty(order_resp)
            if delta <= 0.0:
                return
            cumulative = current + delta
        if cumulative > current:
            self._sync_settled_order_quantities[order_id] = cumulative

    @staticmethod
    def _route_wallet_has_open_order(route_wallet: Any, order_id: str) -> bool:
        if not order_id:
            return False
        candidates = [
            route_wallet,
            getattr(route_wallet, "futures", None),
            getattr(route_wallet, "spot", None),
        ]
        for wallet_part in candidates:
            open_orders = getattr(wallet_part, "open_orders", None)
            if isinstance(open_orders, dict) and order_id in open_orders:
                return True
        return False

    def _adjust_lifecycle_order_response(
        self,
        order_resp: OrderResponse,
        route_wallet: Any,
    ) -> OrderResponse | None:
        order_id = str(getattr(order_resp, "order_id", "") or "").strip()
        cumulative = self._order_cumulative_executed_qty(order_resp)
        if not order_id or cumulative is None:
            return order_resp
        settled = self._sync_settled_order_quantities.get(order_id)
        if settled is None:
            return order_resp
        if cumulative <= settled:
            self._sync_settled_order_quantities[order_id] = max(settled, cumulative)
            return None
        if self._route_wallet_has_open_order(route_wallet, order_id):
            return order_resp
        delta = cumulative - settled
        sign = -1.0 if float(getattr(order_resp, "qty", 0.0) or 0.0) < 0.0 else 1.0
        adjusted = replace(
            order_resp,
            qty=sign * delta,
            orig_qty=delta,
            executed_qty=delta,
            remaining_qty=max(0.0, float(getattr(order_resp, "remaining_qty", 0.0) or 0.0)),
        )
        self._sync_settled_order_quantities[order_id] = cumulative
        return adjusted

    @staticmethod
    def _coerce_execution_feedback(payload: Any) -> ExecutionFeedback:
        if isinstance(payload, ExecutionFeedback):
            return payload
        if isinstance(payload, OrderResponse):
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=payload,
                fill_count=1 if str(payload.status).upper() == "FILLED" else 0,
                delta_qty=float(getattr(payload, "qty", 0.0) or 0.0),
            )
        status = str(getattr(payload, "attempt_status", "") or "").strip().upper()
        if status:
            return payload
        if hasattr(payload, "status"):
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=payload,
                fill_count=1 if str(getattr(payload, "status", "")).upper() == "FILLED" else 0,
                delta_qty=float(getattr(payload, "qty", 0.0) or 0.0),
            )
        raise TypeError(f"unsupported execution feedback payload: {type(payload)!r}")

    def running_strategy(self, market_data: MarketData) -> None:
        exchange = _norm_exchange(getattr(market_data, "exchange", "binance"))
        market = _norm_market(market_data.market)
        sym = _norm_symbol(market_data.symbol)
        interval = _norm_interval(getattr(market_data, "interval", ""))
        key = (exchange, market, sym, interval)

        # Declaration gate: only declared (market, symbol, interval) keys reach
        # the strategy. Wallet state is irrelevant here per pre_C3.
        if key not in self._input_keys:
            return

        # Refresh the bound view with this tick.
        if not self._view.update(market_data):
            # Defensive: update() also enforces the declaration. Stay silent
            # rather than raise — the router gate above already screened.
            return

        self._last_market_time = getattr(market_data, "timestamp", None)
        self.wallet.on_market_data(
            exchange,
            market,
            sym,
            _wallet_market(market),
            float(market_data.price),
        )
        self._consume_order_updates()

        # Call user strategy with the view, not the raw tick.
        self._maybe_reload_strategy()
        try:
            self._prepare_indicator_frame()
            raw_signals = self._strategy_instance.on_market_data(self._view, self.wallet)
        except Exception as exc:  # noqa: BLE001
            message = (
                f"{USER_STRATEGY_ON_MARKET_DATA_ERROR_PREFIX} "
                f"{type(exc).__name__}: {exc}"
            )
            logger.warning(
                "%s session=%s strategy_id=%s",
                message,
                self._session_id,
                self._strategy_id,
                exc_info=True,
            )
            if self._hot_reload_source_path is not None:
                self._emit_user_code_error(message)
                self._notifier.error(
                    message,
                    title="Strategy code error",
                    dedupe_key=f"strategy-user-code-error:{self._session_id}:on_market_data",
                )
                return
            raise StrategyUserCodeError(message) from exc
        self._drain_indicator_frame(
            _stream_key(exchange, market, sym, interval),
            _market_time_ms(market_data),
            _interval_to_ms(interval),
        )
        self._emit_user_code_recovered()
        signals = _normalize_decisions(raw_signals)
        prepared = [
            self._prepare_order_decision(signal, market_data)
            for signal in signals
        ]
        for item in prepared:
            self._execute_prepared_order(item)

    def _prepare_order_decision(
        self,
        signal: OrderDecision,
        market_data: MarketData,
    ) -> _PreparedOrderDecision:
        sig_exchange = _norm_exchange(signal.exchange)
        sig_market = _norm_market(signal.market)
        sig_sym = _norm_symbol(signal.symbol)
        qty_dec = _parse_positive_decimal(signal.qty, "qty")
        price_dec = (
            _parse_positive_decimal(signal.price, "price")
            if signal.price is not None
            else None
        )
        side = _normalize_order_side(signal.side)
        order_type = _normalize_order_type(signal.order_type)

        if (sig_exchange, sig_market, sig_sym) not in self._order_target_keys:
            raise ValueError(
                "strategy attempted to place order outside ORDER_TARGETS: "
                f"target=({sig_exchange}, {sig_market}, {sig_sym}), "
                f"ORDER_TARGETS={sorted(self._order_target_keys)}"
            )
        venue_id = self._venue_id_for_route(sig_exchange, sig_market)
        route_wallet = self.wallet.get(sig_exchange, sig_market)
        mark_price = self._resolve_mark_price(
            exchange=sig_exchange,
            market=sig_market,
            symbol=sig_sym,
            explicit_price=price_dec,
            trigger=market_data,
        )
        normalized_signal = replace(
            signal,
            exchange=sig_exchange,
            market=sig_market,
            symbol=sig_sym,
            side=side,
            qty=str(signal.qty).strip(),
            order_type=order_type,
            price=str(signal.price).strip() if signal.price is not None else None,
        )
        trigger_key = (
            _norm_exchange(getattr(market_data, "exchange", "binance")),
            _norm_market(market_data.market),
            _norm_symbol(market_data.symbol),
        )
        return _PreparedOrderDecision(
            signal=normalized_signal,
            exchange=sig_exchange,
            market=sig_market,
            symbol=sig_sym,
            qty=qty_dec,
            price=price_dec,
            side=side,
            order_type=order_type,
            mark_price=mark_price,
            market_time=getattr(market_data, "timestamp", None),
            mark_price_refreshed=(sig_exchange, sig_market, sig_sym) == trigger_key,
            venue_id=venue_id,
            route_wallet=route_wallet,
        )

    def _resolve_mark_price(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        explicit_price: Decimal | None,
        trigger: MarketData,
    ) -> float:
        trigger_key = (
            _norm_exchange(getattr(trigger, "exchange", "binance")),
            _norm_market(trigger.market),
            _norm_symbol(trigger.symbol),
        )
        target_key = (exchange, market, symbol)
        if target_key == trigger_key:
            return float(trigger.price)

        matches = [
            tick
            for tick_key, tick in self._view._cache.items()
            if tick_key[:3] == target_key
        ]
        if not matches:
            if explicit_price is not None:
                return float(explicit_price)
            raise ValueError(
                f"missing mark price for ORDER_TARGETS route: {exchange}/{market}/{symbol}"
            )
        if len(matches) == 1:
            return float(matches[0].price)
        raise ValueError(
            f"ambiguous mark price for ORDER_TARGETS route: {exchange}/{market}/{symbol}"
        )

    def _execute_prepared_order(self, item: _PreparedOrderDecision) -> None:
        sig_exchange = item.exchange
        sig_market = item.market
        sig_sym = item.symbol
        signal = item.signal
        blocked_key = (sig_exchange, sig_market, sig_sym)
        if blocked_key in self._blocked_order_keys:
            logger.warning(
                "skip order on blocked symbol while execution state unresolved: exchange=%s symbol=%s market=%s",
                sig_exchange, sig_sym, sig_market,
            )
            return

        balance_check_price = float(item.price) if item.price is not None else item.mark_price
        route_wallet = item.route_wallet

        # Look up a position/asset to get leverage for margin math.
        fw = getattr(route_wallet, "futures", None)
        pos = None
        if fw is not None and _is_futures_market(sig_market):
            matched = fw._get_positions_for_symbol(sig_sym)
            if matched:
                pos = matched[0][1]
        if pos is None and sig_market == "spot":
            sw = getattr(route_wallet, "spot", None)
            pos = sw.assets.get(sig_sym) if sw is not None else None
        leverage = float(getattr(pos, "leverage", 1.0)) if pos else 1.0
        qty = float(item.qty)
        side_upper = item.side

        # Balance guard: distinguish open/close, spot buy/sell.
        if sig_market == "spot" and hasattr(route_wallet, "spot"):
            sw = getattr(route_wallet, "spot", None)
            if side_upper == OrderSide.SELL:
                asset = sw.assets.get(sig_sym) if sw is not None else None
                available_qty = (
                    float(getattr(asset, "qty", 0.0)) - float(getattr(asset, "locked", 0.0))
                    if asset else 0.0
                )
                if available_qty < qty:
                    logger.debug(
                        "[Skip] insufficient spot qty to SELL: need %.6f, have %.6f %s",
                        qty, available_qty, sig_sym,
                    )
                    return
            else:
                need = qty * balance_check_price
                free = float(sw.free) if sw is not None else 0.0
                if free < need:
                    logger.debug(
                        "[Skip] insufficient spot free USDT to BUY: need %.2f, have %.2f",
                        need, free,
                    )
                    return
        elif sig_market != "spot" and (
            hasattr(route_wallet, "get_available_balance") or pos is not None
        ):
            current_qty = float(getattr(pos, "net_qty", 0.0)) if pos else 0.0
            is_closing = False
            if pos is not None and current_qty != 0.0:
                if (current_qty > 0 and side_upper == OrderSide.SELL) or \
                   (current_qty < 0 and side_upper == OrderSide.BUY):
                    if qty <= abs(current_qty):
                        is_closing = True
            if not is_closing:
                margin_needed = qty * balance_check_price / leverage
                if hasattr(route_wallet, "get_available_balance"):
                    available_balance = float(route_wallet.get_available_balance())
                else:
                    available_balance = 0.0
                if available_balance < margin_needed:
                    logger.debug(
                        "[Skip] insufficient available balance to open: need %.2f, have %.2f",
                        margin_needed, available_balance,
                    )
                    return

        if not item.mark_price_refreshed:
            self.wallet.on_market_data(
                sig_exchange,
                sig_market,
                sig_sym,
                _wallet_market(sig_market),
                item.mark_price,
            )
        intent_id = uuid.uuid4().hex
        feedback = self._coerce_execution_feedback(self._order_client.place_order(
            self._portfolio_id, signal, item.mark_price,
            portfolio_symbol=sig_sym,
            strategy_id=self._strategy_id,
            market=sig_market,
            session_id=self._session_id,
            intent_id=intent_id,
            market_time=item.market_time,
        ))
        has_settleable_fill = bool(feedback.fill_events) or (
            int(getattr(feedback, "fill_count", 0) or 0) > 0
            and feedback.order is not None
            and abs(float(getattr(feedback.order, "qty", 0.0) or 0.0)) > 0.0
        )
        order_status = str(getattr(feedback.order, "status", "") or "").strip().upper() if feedback.order is not None else ""
        terminal_without_fill = (
            feedback.order is not None
            and order_status in _TERMINAL_ORDER_STATUSES
            and not has_settleable_fill
            and float(getattr(feedback.order, "executed_qty", 0.0) or 0.0) <= 0.0
        )
        pending_fill_confirmation = (
            feedback.attempt_status in {"ACCEPTED", "RECOVERED"}
            and feedback.order is not None
            and float(getattr(feedback.order, "executed_qty", 0.0) or 0.0) > 0.0
            and not has_settleable_fill
        )
        if feedback.attempt_status in {"UNKNOWN", "RECOVERING", "RECOVERY_FAILED"} or pending_fill_confirmation:
            self._blocked_order_keys.add(blocked_key)
        elif feedback.attempt_status in {"FAILED", "ACCEPTED", "RECOVERED"}:
            self._blocked_order_keys.discard(blocked_key)
        if feedback.attempt_status not in {"ACCEPTED", "RECOVERED"} or pending_fill_confirmation:
            self._notify_order_response(feedback)
            logger.warning(
                "order attempt unresolved or failed: symbol=%s market=%s attempt=%s status=%s error=%s",
                sig_sym, sig_market, feedback.attempt_id, feedback.attempt_status, feedback.error_message,
            )
            return

        if feedback.order is None:
            self._notify_order_response(feedback)
            logger.warning(
                "attempt accepted without order payload: symbol=%s market=%s attempt=%s",
                sig_sym, sig_market, feedback.attempt_id,
            )
            return

        if feedback.fill_events:
            for fill_event in feedback.fill_events:
                self._apply_order_to_wallet(sig_exchange, sig_market, sig_sym, fill_event, venue_id=item.venue_id)
                self._record_order_settlement(fill_event)
        elif has_settleable_fill:
            self._apply_order_to_wallet(sig_exchange, sig_market, sig_sym, feedback.order, venue_id=item.venue_id)
            self._record_order_settlement(feedback.order)
        elif terminal_without_fill:
            self._apply_order_to_wallet(sig_exchange, sig_market, sig_sym, feedback.order, venue_id=item.venue_id)
        else:
            self._notify_order_response(feedback)
            logger.warning(
                "attempt accepted without confirmed fill details: symbol=%s market=%s attempt=%s",
                sig_sym, sig_market, feedback.attempt_id,
            )
            return
        self._notify_order_response(feedback)
        if self.on_order_callback is not None:
            try:
                self.on_order_callback()
            except Exception:
                logger.warning("on_order_callback failed", exc_info=True)
