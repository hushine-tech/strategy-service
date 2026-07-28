from __future__ import annotations

import logging
import math
import re
import threading
import uuid
from dataclasses import FrozenInstanceError, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Literal

from strategy_service.indicators import IndicatorDefinition, IndicatorFrame, IndicatorWriter
from strategy_service.inputs import (
    InputView,
    StrategyDeclarations,
    StrategyInput,
    _normalize_exchange,
    _normalize_market,
)
from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.types import MarketData, OrderDecision, OrderSide, OrderType, OrderUpdateEvent
from strategy_service.wallet.order_types import ExecutionFeedback, OrderResponse
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from strategy_service.strategy_imports import (
    CapturedFileSignature,
    GatedStrategySource,
    PreparedStrategy,
    StrategySourceLoadError,
    _claim_prepared_strategy,
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)

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


@dataclass(frozen=True, slots=True)
class StrategyUserCodeFatalError(BaseException):
    stage: Literal[
        "attribute",
        "callback",
        "result_iteration",
        "decision_normalization",
    ]

    def __str__(self) -> str:
        return "strategy user code terminated"


@dataclass(frozen=True, slots=True)
class StrategyActivationError(Exception):
    reason: Literal["order_cursor_failed"]

    def __str__(self) -> str:
        return "strategy activation failed"


def _exception_setattr(self: BaseException, name: str, value: object) -> None:
    if name in {"__traceback__", "__cause__", "__context__", "__suppress_context__"}:
        BaseException.__setattr__(self, name, value)
        return
    raise FrozenInstanceError(f"cannot assign to field {name!r}")


StrategyActivationError.__setattr__ = _exception_setattr  # type: ignore[method-assign]
StrategyUserCodeFatalError.__setattr__ = _exception_setattr  # type: ignore[method-assign]


def _user_code_fatal(stage: str) -> StrategyUserCodeFatalError:
    error = StrategyUserCodeFatalError(stage=stage)  # type: ignore[arg-type]
    error.__cause__ = None
    error.__context__ = None
    return error


USER_STRATEGY_ON_MARKET_DATA_ERROR_PREFIX = "user strategy on_market_data failed:"
_INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "1h": 3_600_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}
_INTERVAL_RE = re.compile(r"^(\d+)([smhdwM])$")


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
    raw = _norm_interval(interval)
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
        "M": 2_592_000_000,
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
    spot_risk_snapshot_id: str = ""


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
        prepared_strategy: PreparedStrategy,
        wallet: PortfolioWalletRuntime,
        order_client: OrderClient | None = None,
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        notifier: StrategyNotifier | None = None,
        on_user_code_error: Callable[[str], None] | None = None,
        on_user_code_recovered: Callable[[], None] | None = None,
        on_user_code_fatal: Callable[[StrategyUserCodeFatalError], None] | None = None,
    ) -> None:
        def bind_candidate(
            strategy_instance: object,
            declarations: StrategyDeclarations,
            indicator_definitions: tuple[IndicatorDefinition, ...],
            gated_source: GatedStrategySource,
        ) -> dict[str, object]:
            failed = False
            candidate: dict[str, object] = {}
            try:
                if not isinstance(wallet, PortfolioWalletRuntime):
                    raise TypeError("BaseStrategy wallet must be PortfolioWalletRuntime")
                resolved_order_client = order_client or OrderClient()
                resolved_notifier = notifier or StrategyNotifier()
                bound_notifier = resolved_notifier.bind_context(
                    portfolio_id=portfolio_id,
                    strategy_id=strategy_id,
                    session_id=session_id,
                )
                writer = IndicatorWriter(list(indicator_definitions))
                setattr(strategy_instance, "notify", bound_notifier)
                setattr(strategy_instance, "indicators", writer)
                resolved = gated_source.resolved
                inputs = list(declarations.inputs)
                candidate = {
                    "strategy_path": resolved.filename,
                    "wallet": wallet,
                    "on_order_callback": None,
                    "on_indicator_frame": None,
                    "_order_client": resolved_order_client,
                    "_portfolio_id": portfolio_id,
                    "_strategy_id": strategy_id,
                    "_session_id": session_id,
                    "_hot_reload_enabled": resolved.hot_reload_path is not None,
                    "_hot_reload_source_path": (
                        Path(resolved.hot_reload_path)
                        if resolved.hot_reload_path is not None
                        else None
                    ),
                    "_hot_reload_signature": resolved.hot_reload_signature,
                    "_python_invocation_path": gated_source.python_invocation_path,
                    "_on_user_code_error": on_user_code_error,
                    "_on_user_code_recovered": on_user_code_recovered,
                    "_on_user_code_fatal": on_user_code_fatal,
                    "_notifier": bound_notifier,
                    "_strategy_instance": strategy_instance,
                    "_decl": declarations,
                    "_indicator_definitions": list(indicator_definitions),
                    "_indicator_writer": writer,
                    "_next_indicator_sequence": {},
                    "_inputs": inputs,
                    "_order_targets": list(declarations.order_targets),
                    "_input_keys": declarations.input_keys,
                    "_order_target_keys": declarations.order_target_keys,
                    "_required_routes": declarations.required_routes,
                    "_view": InputView(inputs),
                    "_blocked_order_keys": set(),
                    "_order_event_cursor": 0,
                    "_order_cursor_activated": False,
                    "_order_cursor_lock": threading.Lock(),
                    "_strategy_swap_lock": threading.Lock(),
                    "_reload_lock": threading.Lock(),
                    "_fatal_lock": threading.Lock(),
                    "_callback_execution_lock": threading.RLock(),
                    "_callback_generation_depth": 0,
                    "_fatal_error": None,
                    "_callbacks_disarmed": False,
                    "_fatal_event": threading.Event(),
                    "_settled_lifecycle_event_ids": set(),
                    "_sync_settled_order_quantities": {},
                    "_last_market_time": None,
                }
            except BaseException:
                failed = True
            if failed:
                error = StrategySourceLoadError(reason="binding_failed")
                error.__cause__ = None
                error.__context__ = None
                raise error
            return candidate

        candidate = _claim_prepared_strategy(prepared_strategy, bind_candidate)
        self.__dict__.update(candidate)

    @property
    def declared_inputs(self) -> list[StrategyInput]:
        """Read-only snapshot of the strategy's declared universe."""
        return list(self._inputs)

    @property
    def last_market_time(self) -> Any | None:
        return self._last_market_time

    def _get_strategy(self) -> Any:
        return self._strategy_instance

    def _callbacks_are_armed(self) -> bool:
        with self._fatal_lock:
            return not self._callbacks_disarmed

    def _latch_user_code_fatal(self, stage: str) -> None:
        self._record_user_code_fatal(_user_code_fatal(stage))

    def _record_user_code_fatal(self, fatal: StrategyUserCodeFatalError) -> None:
        callback: Callable[[StrategyUserCodeFatalError], None] | None = None
        with self._callback_execution_lock:
            with self._fatal_lock:
                if self._fatal_error is not None:
                    return
                self._fatal_error = fatal
                self._callbacks_disarmed = True
                self._fatal_event.set()
                callback = self._on_user_code_fatal
        if callback is None:
            return
        try:
            callback(fatal)
        except BaseException:
            logger.error(
                "STRATEGY_USER_CODE_FATAL_WAKE_FAILED session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )

    def has_user_code_fatal(self) -> bool:
        with self._fatal_lock:
            return self._fatal_error is not None

    def raise_if_user_code_fatal(self) -> None:
        with self._fatal_lock:
            fatal = self._fatal_error
        if fatal is not None:
            raise fatal

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
            "STRATEGY_HOT_RELOAD_SOURCE_UNAVAILABLE session=%s strategy_id=%s",
            self._session_id,
            self._strategy_id,
        )
        return None

    @staticmethod
    def _strategy_file_signature(path: Path) -> CapturedFileSignature:
        stat = path.stat()
        return CapturedFileSignature(
            device=int(stat.st_dev),
            inode=int(stat.st_ino),
            mtime_ns=int(stat.st_mtime_ns),
            ctime_ns=int(stat.st_ctime_ns),
            size=int(stat.st_size),
        )

    def _maybe_reload_strategy(self) -> None:
        with self._callback_execution_lock:
            if self._callback_generation_depth > 1:
                return
            with self._reload_lock:
                self._maybe_reload_strategy_locked()

    def _begin_callback_generation(self) -> None:
        self._callback_generation_depth += 1

    def _finish_callback_generation(self) -> None:
        self._callback_generation_depth -= 1

    def _maybe_reload_strategy_locked(self) -> None:
        source_path = self._hot_reload_source_path
        if source_path is None:
            return
        try:
            signature = self._strategy_file_signature(source_path)
        except OSError:
            logger.warning("STRATEGY_HOT_RELOAD_SOURCE_UNAVAILABLE session=%s", self._session_id)
            return
        if signature == self._hot_reload_signature:
            return

        try:
            resolved = resolve_strategy_source(str(source_path), None, hot_reload=True)
            gate = gate_strategy_source(
                resolved,
                python_invocation_path=self._python_invocation_path,
            )
            if not gate.ok or gate.gated_source is None:
                self._hot_reload_signature = signature
                logger.warning("STRATEGY_HOT_RELOAD_GATE_FAILED session=%s", self._session_id)
                return
            prepared = prepare_strategy(gate.gated_source)
            candidate_decl = prepared.declarations
            candidate_indicator_definitions = list(
                prepared.indicator_definitions
            )
        except BaseException:
            self._hot_reload_signature = signature
            logger.warning("STRATEGY_HOT_RELOAD_PREPARE_FAILED session=%s", self._session_id)
            return

        if candidate_decl != self._decl:
            self._hot_reload_signature = signature
            logger.warning(
                "STRATEGY_HOT_RELOAD_DECLARATION_CHANGED session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )
            return
        if candidate_indicator_definitions != self._indicator_definitions:
            self._hot_reload_signature = signature
            logger.warning(
                "STRATEGY_HOT_RELOAD_INDICATOR_DECLARATION_CHANGED "
                "session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )
            return

        def bind_candidate(
            candidate_instance: object,
            declarations: StrategyDeclarations,
            definitions: tuple[IndicatorDefinition, ...],
            gated_source: GatedStrategySource,
        ) -> tuple[object, StrategyDeclarations, list[IndicatorDefinition], IndicatorWriter, CapturedFileSignature | None]:
            del gated_source
            failed = False
            try:
                candidate_definitions = list(definitions)
                candidate_writer = IndicatorWriter(candidate_definitions)
                setattr(candidate_instance, "notify", self._notifier)
                setattr(candidate_instance, "indicators", candidate_writer)
            except BaseException:
                failed = True
            if failed:
                error = StrategySourceLoadError(reason="binding_failed")
                error.__cause__ = None
                error.__context__ = None
                raise error
            return (
                candidate_instance,
                declarations,
                candidate_definitions,
                candidate_writer,
                resolved.hot_reload_signature,
            )

        try:
            candidate = _claim_prepared_strategy(prepared, bind_candidate)
        except BaseException:
            self._hot_reload_signature = signature
            logger.warning("STRATEGY_HOT_RELOAD_BIND_FAILED session=%s", self._session_id)
            return
        with self._strategy_swap_lock:
            (
                self._strategy_instance,
                self._decl,
                self._indicator_definitions,
                self._indicator_writer,
                self._hot_reload_signature,
            ) = candidate
        logger.info("STRATEGY_HOT_RELOADED session=%s", self._session_id)

    def _prepare_indicator_frame(self) -> None:
        attribute_fatal = False
        try:
            writer = getattr(self._strategy_instance, "indicators", None)
        except Exception:
            raise
        except BaseException:
            writer = None
            attribute_fatal = True
        if attribute_fatal:
            raise _user_code_fatal("attribute")
        if writer is not None:
            callback_fatal = False
            try:
                writer.reset_bar()
            except Exception:
                raise
            except BaseException:
                callback_fatal = True
            if callback_fatal:
                raise _user_code_fatal("callback")

    def _drain_indicator_frame(self, stream_key: str, market_time_ms: int, interval_ms: int) -> None:
        attribute_fatal = False
        try:
            writer = getattr(self._strategy_instance, "indicators", None)
        except Exception:
            raise
        except BaseException:
            writer = None
            attribute_fatal = True
        if attribute_fatal:
            raise _user_code_fatal("attribute")
        if writer is None or not self._indicator_definitions:
            return
        callback_fatal = False
        try:
            frame = writer.drain()
        except Exception:
            raise
        except BaseException:
            frame = None
            callback_fatal = True
        if callback_fatal:
            raise _user_code_fatal("callback")
        attribute_fatal = False
        try:
            raw_warnings = frame.warnings
        except Exception:
            raise
        except BaseException:
            raw_warnings = ()
            attribute_fatal = True
        if attribute_fatal:
            raise _user_code_fatal("attribute")
        iteration_fatal = False
        try:
            warnings = list(raw_warnings)
        except Exception:
            raise
        except BaseException:
            warnings = []
            iteration_fatal = True
        if iteration_fatal:
            raise _user_code_fatal("result_iteration")
        for _warning in warnings:
            logger.warning(
                "STRATEGY_INDICATOR_WARNING session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )
        sequence = self._next_indicator_sequence.get(stream_key, 0)
        self._next_indicator_sequence[stream_key] = sequence + 1
        if self.on_indicator_frame is None:
            return
        callback_fatal = False
        try:
            self.on_indicator_frame(
                stream_key,
                sequence,
                market_time_ms,
                interval_ms,
                frame,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"indicator V2 transport failed: {exc}"
            ) from exc
        except BaseException:
            callback_fatal = True
        if callback_fatal:
            raise _user_code_fatal("callback")

    def _emit_user_code_error(self, message: str) -> None:
        if self._on_user_code_error is None:
            return
        callback_fatal = False
        try:
            self._on_user_code_error(message)
        except Exception:  # noqa: BLE001
            logger.warning(
                "STRATEGY_USER_CODE_ERROR_CALLBACK_FAILED session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )
        except BaseException:
            callback_fatal = True
        if callback_fatal:
            raise _user_code_fatal("callback")

    def _emit_user_code_recovered(self) -> None:
        if self._on_user_code_recovered is None:
            return
        callback_fatal = False
        try:
            self._on_user_code_recovered()
        except Exception:  # noqa: BLE001
            logger.warning(
                "STRATEGY_USER_CODE_RECOVERY_CALLBACK_FAILED session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )
        except BaseException:
            callback_fatal = True
        if callback_fatal:
            raise _user_code_fatal("callback")

    def _notify_order_response(self, order_resp: Any) -> None:
        with self._callback_execution_lock:
            try:
                self._begin_callback_generation()
                if not self._callbacks_are_armed():
                    return
                self._maybe_reload_strategy()
                attribute_fatal = False
                try:
                    fn = getattr(self._strategy_instance, "on_order_response", None)
                except Exception:
                    raise
                except BaseException:
                    fn = None
                    attribute_fatal = True
                if attribute_fatal:
                    self._latch_user_code_fatal("attribute")
                    return
                if callable(fn):
                    callback_fatal = False
                    try:
                        fn(order_resp)
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "STRATEGY_ORDER_RESPONSE_CALLBACK_FAILED session=%s strategy_id=%s",
                            self._session_id,
                            self._strategy_id,
                        )
                    except BaseException:
                        callback_fatal = True
                    if callback_fatal:
                        self._latch_user_code_fatal("callback")
            finally:
                self._finish_callback_generation()

    def _notify_order_update(self, event: OrderUpdateEvent) -> None:
        with self._callback_execution_lock:
            try:
                self._begin_callback_generation()
                if not self._callbacks_are_armed():
                    return
                self._maybe_reload_strategy()
                attribute_fatal = False
                try:
                    fn = getattr(self._strategy_instance, "on_order_update", None)
                except Exception:
                    raise
                except BaseException:
                    fn = None
                    attribute_fatal = True
                if attribute_fatal:
                    self._latch_user_code_fatal("attribute")
                    return
                if callable(fn):
                    callback_fatal = False
                    try:
                        fn(event, self.wallet)
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "STRATEGY_ORDER_UPDATE_CALLBACK_FAILED session=%s strategy_id=%s event_id=%s",
                            self._session_id,
                            self._strategy_id,
                            int(getattr(event, "event_id", 0) or 0),
                        )
                    except BaseException:
                        callback_fatal = True
                    if callback_fatal:
                        self._latch_user_code_fatal("callback")
            finally:
                self._finish_callback_generation()

    def _notify_order_snapshot(self) -> None:
        with self._callback_execution_lock:
            try:
                self._begin_callback_generation()
                if not self._callbacks_are_armed() or self.on_order_callback is None:
                    return
                callback_fatal = False
                try:
                    self.on_order_callback()
                except Exception:
                    logger.warning(
                        "STRATEGY_ORDER_SNAPSHOT_CALLBACK_FAILED session=%s strategy_id=%s",
                        self._session_id,
                        self._strategy_id,
                    )
                except BaseException:
                    callback_fatal = True
                if callback_fatal:
                    self._latch_user_code_fatal("callback")
            finally:
                self._finish_callback_generation()

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
    ) -> OrderResponse:
        symbol_type = _wallet_market(market)
        route_venue_id = self._venue_id_for_route(exchange, market) if venue_id is None else int(venue_id)
        normalized = replace(
            order_resp,
            venue_id=(
                int(getattr(order_resp, "venue_id", 0) or 0)
                or route_venue_id
            ),
            exchange=(
                str(getattr(order_resp, "exchange", "") or "").strip()
                or _norm_exchange(exchange)
            ),
            market=(
                str(getattr(order_resp, "market", "") or "").strip()
                or _norm_market(market)
            ),
        )
        self.wallet.on_order(exchange, market, route_venue_id, symbol, symbol_type, normalized)
        return normalized

    def activate_order_event_cursor(self) -> None:
        failed = False
        with self._order_cursor_lock:
            if self._order_cursor_activated:
                return
            cursor = 0
            if self._session_id and hasattr(self._order_client, "list_order_lifecycle_events"):
                try:
                    for _ in range(100):
                        events = self._order_client.list_order_lifecycle_events(
                            session_id=self._session_id,
                            after_event_id=cursor,
                            limit=500,
                        )
                        if not events:
                            break
                        cursor = max(
                            cursor,
                            *(int(getattr(event, "event_id", 0) or 0) for event in events),
                        )
                        if len(events) < 500:
                            break
                except BaseException:
                    failed = True
            if not failed:
                self._order_event_cursor = cursor
                self._order_cursor_activated = True
        if failed:
            logger.warning(
                "STRATEGY_ORDER_CURSOR_ACTIVATION_FAILED session=%s",
                self._session_id,
            )
            error = StrategyActivationError(reason="order_cursor_failed")
            error.__cause__ = None
            error.__context__ = None
            raise error

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
            logger.warning(
                "STRATEGY_ORDER_LIFECYCLE_FETCH_FAILED session=%s strategy_id=%s",
                self._session_id,
                self._strategy_id,
            )
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
                    order_resp = self._apply_order_to_wallet(
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
                "STRATEGY_ORDER_LIFECYCLE_HANDLE_FAILED session=%s strategy_id=%s event_id=%s",
                self._session_id,
                self._strategy_id,
                event_id,
            )
        self._notify_order_update(event)
        if self.has_user_code_fatal():
            return wallet_updated
        if wallet_updated:
            self._notify_order_snapshot()
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

    @staticmethod
    def _order_settlement_key(order_resp: OrderResponse) -> tuple[int, str, str, str]:
        identity = str(
            getattr(order_resp, "exchange_order_id", "")
            or getattr(order_resp, "order_id", "")
            or ""
        ).strip()
        raw_exchange = str(getattr(order_resp, "exchange", "") or "").strip()
        raw_market = str(getattr(order_resp, "market", "") or "").strip()
        return (
            int(getattr(order_resp, "venue_id", 0) or 0),
            _norm_exchange(raw_exchange) if raw_exchange else "",
            _norm_market(raw_market) if raw_market else "",
            identity,
        )

    def _record_order_settlement(self, order_resp: OrderResponse) -> None:
        settlement_key = self._order_settlement_key(order_resp)
        if not settlement_key[3]:
            return
        current = self._sync_settled_order_quantities.get(settlement_key, 0.0)
        cumulative = self._order_cumulative_executed_qty(order_resp)
        if cumulative is None:
            delta = self._order_delta_qty(order_resp)
            if delta <= 0.0:
                return
            cumulative = current + delta
        if cumulative > current:
            self._sync_settled_order_quantities[settlement_key] = cumulative

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
            if isinstance(open_orders, dict):
                if order_id in open_orders:
                    return True
                if any(
                    isinstance(key, tuple) and key and str(key[-1]) == order_id
                    for key in open_orders
                ):
                    return True
        return False

    def _adjust_lifecycle_order_response(
        self,
        order_resp: OrderResponse,
        route_wallet: Any,
    ) -> OrderResponse | None:
        order_id = str(
            getattr(order_resp, "exchange_order_id", "")
            or getattr(order_resp, "order_id", "")
            or ""
        ).strip()
        settlement_key = self._order_settlement_key(order_resp)
        cumulative = self._order_cumulative_executed_qty(order_resp)
        if not order_id or cumulative is None:
            return order_resp
        settled = self._sync_settled_order_quantities.get(settlement_key)
        if settled is None:
            return order_resp
        if cumulative <= settled:
            self._sync_settled_order_quantities[settlement_key] = max(settled, cumulative)
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
        self._sync_settled_order_quantities[settlement_key] = cumulative
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
        self.raise_if_user_code_fatal()
        exchange = _norm_exchange(getattr(market_data, "exchange", "binance"))
        market = _norm_market(market_data.market)
        sym = _norm_symbol(market_data.symbol)
        interval = _norm_interval(getattr(market_data, "interval", ""))
        with self._callback_execution_lock:
            try:
                self._begin_callback_generation()
                self.raise_if_user_code_fatal()
                self._run_admitted_strategy(
                    market_data,
                    exchange=exchange,
                    market=market,
                    symbol=sym,
                    interval=interval,
                )
                self.raise_if_user_code_fatal()
            except StrategyUserCodeFatalError as fatal:
                self._record_user_code_fatal(fatal)
                raise
            finally:
                self._finish_callback_generation()

    def _run_admitted_strategy(
        self,
        market_data: MarketData,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> None:
        key = (exchange, market, symbol, interval)
        stream_key = _stream_key(exchange, market, symbol, interval)
        market_time_ms = _market_time_ms(market_data)
        interval_ms = _interval_to_ms(interval)

        # Declaration gate: only declared (market, symbol, interval) keys reach
        # the strategy. Wallet state is irrelevant here per pre_C3.
        if key not in self._input_keys:
            self.raise_if_user_code_fatal()
            return

        # Refresh the bound view with this tick.
        if not self._view.update(market_data):
            # Defensive: update() also enforces the declaration. Stay silent
            # rather than raise — the router gate above already screened.
            self.raise_if_user_code_fatal()
            return

        self.raise_if_user_code_fatal()
        self._last_market_time = getattr(market_data, "timestamp", None)
        self.wallet.on_market_data(
            exchange,
            market,
            symbol,
            _wallet_market(market),
            float(market_data.price),
        )
        self._consume_order_updates()
        self.raise_if_user_code_fatal()

        # Call user strategy with the view, not the raw tick.
        self._maybe_reload_strategy()
        self.raise_if_user_code_fatal()
        attribute_fatal = False
        try:
            self._prepare_indicator_frame()
            self.raise_if_user_code_fatal()
            callback = getattr(self._strategy_instance, "on_market_data")
        except Exception as exc:  # noqa: BLE001
            self._prepare_indicator_frame()
            self._drain_indicator_frame(
                stream_key,
                market_time_ms,
                interval_ms,
            )
            self._handle_on_market_data_exception(exc)
            self.raise_if_user_code_fatal()
            return
        except StrategyUserCodeFatalError:
            raise
        except BaseException:
            attribute_fatal = True
            callback = None
        if attribute_fatal:
            raise _user_code_fatal("attribute")

        self.raise_if_user_code_fatal()
        callback_fatal = False
        try:
            raw_signals = callback(self._view, self.wallet)
        except Exception as exc:  # noqa: BLE001
            self._prepare_indicator_frame()
            self._drain_indicator_frame(
                stream_key,
                market_time_ms,
                interval_ms,
            )
            self._handle_on_market_data_exception(exc)
            self.raise_if_user_code_fatal()
            return
        except BaseException:
            self._prepare_indicator_frame()
            self._drain_indicator_frame(
                stream_key,
                market_time_ms,
                interval_ms,
            )
            callback_fatal = True
            raw_signals = None
        if callback_fatal:
            raise _user_code_fatal("callback")
        self.raise_if_user_code_fatal()
        self._drain_indicator_frame(
            stream_key,
            market_time_ms,
            interval_ms,
        )
        self.raise_if_user_code_fatal()
        self._emit_user_code_recovered()
        self.raise_if_user_code_fatal()
        result_fatal = False
        try:
            signals = _normalize_decisions(raw_signals)
        except Exception:
            raise
        except BaseException:
            signals = []
            result_fatal = True
        if result_fatal:
            raise _user_code_fatal("result_iteration")
        decision_fatal = False
        try:
            prepared = [
                self._prepare_order_decision(signal, market_data)
                for signal in signals
            ]
        except Exception:
            raise
        except BaseException:
            prepared = []
            decision_fatal = True
        if decision_fatal:
            raise _user_code_fatal("decision_normalization")
        for item in prepared:
            self.raise_if_user_code_fatal()
            self._execute_prepared_order(item)
        self.raise_if_user_code_fatal()

    def _handle_on_market_data_exception(self, exc: Exception) -> None:
        message = (
            f"{USER_STRATEGY_ON_MARKET_DATA_ERROR_PREFIX} "
            f"{type(exc).__name__}: {exc}"
        )
        logger.warning(
            "STRATEGY_USER_CODE_ERROR session=%s strategy_id=%s",
            self._session_id,
            self._strategy_id,
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
            spot_risk_snapshot_id="",
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
            pos = sw.asset_for_symbol(sig_sym) if sw is not None else None
        leverage = float(getattr(pos, "leverage", 1.0)) if pos else 1.0
        qty = float(item.qty)
        side_upper = item.side

        # Balance guard: distinguish open/close, spot buy/sell.
        if sig_market == "spot" and hasattr(route_wallet, "spot"):
            sw = getattr(route_wallet, "spot", None)
            if side_upper == OrderSide.SELL:
                asset = sw.asset_for_symbol(sig_sym) if sw is not None else None
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

        spot_risk_snapshot_id = item.spot_risk_snapshot_id
        if sig_market == "spot":
            spot_wallet = getattr(route_wallet, "spot", None)
            reviewer = getattr(spot_wallet, "review_order", None)
            if callable(reviewer):
                spot_risk_snapshot_id = reviewer(
                    symbol=sig_sym,
                    side=side_upper,
                    order_type=item.order_type,
                    qty_decimal=str(signal.qty).strip(),
                    price_decimal=(
                        str(signal.price).strip() if signal.price is not None else None
                    ),
                    reduce_only=bool(getattr(signal, "reduce_only", False)),
                )

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
            spot_risk_snapshot_id=spot_risk_snapshot_id,
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
            self.raise_if_user_code_fatal()
            logger.warning(
                "order attempt unresolved or failed: symbol=%s market=%s attempt=%s status=%s error=%s",
                sig_sym, sig_market, feedback.attempt_id, feedback.attempt_status, feedback.error_message,
            )
            return

        if feedback.order is None:
            self._notify_order_response(feedback)
            self.raise_if_user_code_fatal()
            logger.warning(
                "attempt accepted without order payload: symbol=%s market=%s attempt=%s",
                sig_sym, sig_market, feedback.attempt_id,
            )
            return

        if feedback.fill_events:
            for fill_event in feedback.fill_events:
                fill_event = self._apply_order_to_wallet(
                    sig_exchange,
                    sig_market,
                    sig_sym,
                    fill_event,
                    venue_id=item.venue_id,
                )
                self._record_order_settlement(fill_event)
        elif has_settleable_fill:
            settled_order = self._apply_order_to_wallet(
                sig_exchange,
                sig_market,
                sig_sym,
                feedback.order,
                venue_id=item.venue_id,
            )
            self._record_order_settlement(settled_order)
        elif terminal_without_fill:
            self._apply_order_to_wallet(
                sig_exchange,
                sig_market,
                sig_sym,
                feedback.order,
                venue_id=item.venue_id,
            )
        else:
            self._notify_order_response(feedback)
            self.raise_if_user_code_fatal()
            logger.warning(
                "attempt accepted without confirmed fill details: symbol=%s market=%s attempt=%s",
                sig_sym, sig_market, feedback.attempt_id,
            )
            return
        self._notify_order_response(feedback)
        self.raise_if_user_code_fatal()
        self._notify_order_snapshot()
        self.raise_if_user_code_fatal()
