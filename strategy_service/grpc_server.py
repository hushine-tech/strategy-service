"""strategy-service gRPC servicer：统一 RunStrategy 入口，按 portfolio environment 路由数据源。"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_FLOOR
from types import SimpleNamespace
from typing import Any, Callable

import grpc

try:  # OpenTelemetry is optional in unit/local contexts.
    from opentelemetry import context as _otel_context
    from opentelemetry import trace as _otel_trace

    _OTEL_AVAILABLE = True
except Exception:  # noqa: BLE001
    _otel_context = None
    _otel_trace = None
    _OTEL_AVAILABLE = False

from strategy_service.gen import strategy_service_pb2 as pb2
from strategy_service.gen import strategy_service_pb2_grpc as pb2_grpc

from strategy_service.indicators import IndicatorChunkBuffer
from strategy_service.notification import StrategyNotifier
from strategy_service.service import StrategyEngine
from strategy_service.session import SessionManager, SessionState, StreamBinding
from strategy_service.inputs import (
    StrategyDeclarationError,
    StrategyInput,
    StrategyOrderTarget,
    _normalize_exchange,
    _normalize_market,
)
from strategy_service.preflight import (
    SUPPORTED_PROFILES,
    PreflightResult,
    RuntimeSourceProfile,
    backtest_preflight,
    check_profile_supported,
    live_stream_preflight,
    _marketdata_market,
    resolve_profile,
)
from strategy_service.debug_strategy_sources import (
    DebugStrategySourceError,
    ensure_bare_strategy_source,
)
from strategy_service.strategy.base import (
    USER_STRATEGY_ON_MARKET_DATA_ERROR_PREFIX,
    extract_strategy_declarations,
)
from strategy_service.strategy_validator import validate_strategy_code
from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot
from strategy_service.wallet.portfolio import PortfolioWalletRuntime

logger = logging.getLogger(__name__)

# Phase C hybrid PeriodicSample trigger defaults (K-line-driven).
# Fire reconciliation when bars_since_last_compare reaches this
# OR wall-clock idle seconds since last compare reaches the max idle threshold,
# whichever fires first. Both counters reset on fire.
DEFAULT_PERIODIC_SAMPLE_EVERY_BARS = 20
DEFAULT_PERIODIC_SAMPLE_MAX_IDLE_SECONDS = 300
SNAPSHOT_REASON_EVENT = 1
SNAPSHOT_REASON_STRATEGY_START = 2
SNAPSHOT_REASON_STRATEGY_END = 3
SNAPSHOT_REASON_PERIODIC_SAMPLE = 6
DEFAULT_LEASE_HEARTBEAT_SECONDS = 30
DEFAULT_LEASE_TTL_SECONDS = 90
DEFAULT_FRESHNESS_GRACE_SECONDS = 30
DEFAULT_STOP_AND_CLOSE_TIMEOUT_SECONDS = 30
DEFAULT_MAX_LOSS_CLOSE_PCT = 0.30
DEFAULT_SESSION_LEVERAGE = 1.0


def _strategy_validation_error(code: str | None) -> str:
    """Return stable validation details for saved strategy source code."""
    if code is None:
        return ""
    result = validate_strategy_code(code)
    if result.ok:
        return ""
    issues = [
        {
            "code": issue.code,
            "message": issue.message,
            "module": issue.module,
            "line": issue.line,
        }
        for issue in result.issues
    ]
    return "strategy code validation failed: " + json.dumps(
        issues,
        separators=(",", ":"),
        sort_keys=True,
    )
RESTORE_RUNNING_SESSIONS_RETRIES = 5
RESTORE_RUNNING_SESSIONS_RETRY_SECONDS = 1.0
PLATFORM_ACCESS_PROXY_ONLY = "proxy_only"


def _capture_otel_context():
    if not _OTEL_AVAILABLE or _otel_context is None:
        return None
    return _otel_context.get_current()


def _run_in_otel_context(parent_context, span_name: str, fn):
    if not _OTEL_AVAILABLE or _otel_context is None or _otel_trace is None or parent_context is None:
        return fn()
    tracer = _otel_trace.get_tracer("strategy-service/session")
    token = _otel_context.attach(parent_context)
    try:
        with tracer.start_as_current_span(span_name):
            return fn()
    finally:
        _otel_context.detach(token)


@dataclass(frozen=True)
class _StopOrder:
    exchange: str
    venue_id: int
    symbol: str
    portfolio_symbol: str
    market: str
    side: str
    qty: float
    mark_price: float


@dataclass(frozen=True)
class _EffectiveRiskControls:
    max_loss_close_pct: float
    max_loss_close_source: str
    leverage: float
    leverage_source: str


def _sync_strategy_snapshot(
    portfolio_client: Any,
    *,
    portfolio_id: int,
    user_id: int,
    environment: int,
    wallet: Any,
    snapshot_reason: int,
    strategy_id: int,
    session_id: str,
    snapshot_time: object | None = None,
) -> Any:
    kwargs = {
        "portfolio_id": portfolio_id,
        "snapshot_reason": snapshot_reason,
        "strategy_id": strategy_id,
        "session_id": session_id,
    }
    if _snapshot_time_present(snapshot_time):
        kwargs["snapshot_time"] = snapshot_time
    future_wallet, spot_wallet = _wallet_parts_for_portfolio_sync(wallet)
    kwargs["user_id"] = user_id
    kwargs["future_wallet"] = future_wallet
    kwargs["spot_wallet"] = spot_wallet
    result = portfolio_client.update_portfolio_wallet_state(
        **kwargs,
    )
    if result is None:
        raise RuntimeError(
            f"UpdatePortfolioWalletState returned no response for portfolio_id={portfolio_id} session_id={session_id}"
        )
    return result


def _safe_send_session_status_patch(
    platform_proxy: Any | None,
    *,
    session_id: str,
    status: str,
    bars_processed: int,
    error: str,
    runtime_id: str,
) -> None:
    send = getattr(platform_proxy, "send_session_status_patch", None)
    if not callable(send):
        return
    try:
        send(
            session_id=session_id,
            status=status,
            bars_processed=bars_processed,
            error=error,
            runtime_id=runtime_id,
        )
    except Exception:
        logger.warning("session %s: failed to send final status patch", session_id, exc_info=True)


def _wallet_parts_for_portfolio_sync(wallet: Any) -> tuple[Any | None, Any | None]:
    futures_wallet = None
    spot_wallet = None
    if isinstance(wallet, PortfolioWalletRuntime):
        for (_exchange, market, _venue_id), route_wallet in wallet.wallets.items():
            if market == "spot" and spot_wallet is None:
                spot_wallet = getattr(route_wallet, "spot", None)
            elif market in {"perpetual_futures", "delivery_futures"} and futures_wallet is None:
                futures_wallet = getattr(route_wallet, "futures", None)
        return futures_wallet, spot_wallet if _spot_wallet_has_state(spot_wallet) else None
    spot_wallet = getattr(wallet, "spot", None)
    return getattr(wallet, "futures", None), spot_wallet if _spot_wallet_has_state(spot_wallet) else None


def _spot_wallet_has_state(spot_wallet: Any) -> bool:
    if spot_wallet is None:
        return False
    if float(getattr(spot_wallet, "free", 0.0) or 0.0) != 0.0:
        return True
    if float(getattr(spot_wallet, "locked", 0.0) or 0.0) != 0.0:
        return True
    return bool(getattr(spot_wallet, "assets", {}) or {})


def _snapshot_time_present(value: object | None) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return float(value) > 0.0
    return True


def _normalize_target_key(exchange: Any, market: Any, symbol: Any) -> tuple[str, str, str]:
    return (
        _normalize_exchange(exchange),
        _normalize_market(market),
        str(symbol or "").strip().upper(),
    )


def _target_is_allowed(state: SessionState, exchange: Any, market: Any, symbol: Any) -> bool:
    symbol_text = str(symbol or "").strip().upper()
    if not symbol_text:
        return False
    try:
        key = _normalize_target_key(exchange, market, symbol_text)
    except StrategyDeclarationError:
        return False
    return key in set(state.order_target_keys or set())


def _spot_asset_target_is_allowed(state: SessionState, exchange: Any, market: Any, asset_symbol: Any) -> bool:
    sym = str(asset_symbol or "").strip().upper()
    if not sym:
        return False
    return _target_is_allowed(state, exchange, market, sym) or _target_is_allowed(
        state, exchange, market, f"{sym}USDT"
    )


def _futures_stop_metadata(futures_wallet: Any, symbol: str) -> Any | None:
    metadata = getattr(futures_wallet, "risk_metadata", None) or {}
    if isinstance(metadata, dict):
        return metadata.get(str(symbol or "").strip().upper())
    for item in metadata:
        if str(getattr(item, "symbol", "") or "").strip().upper() == str(symbol or "").strip().upper():
            return item
    return None


def _quantize_stop_futures_qty(qty: float, metadata: Any | None) -> float:
    qty_value = abs(float(qty or 0.0))
    if qty_value <= 0.0 or metadata is None:
        return qty_value

    qty_dec = Decimal(str(qty_value))
    step = float(getattr(metadata, "step_size", 0.0) or 0.0)
    if step > 0.0:
        step_dec = Decimal(str(step))
        units = qty_dec / step_dec
        nearest_units = units.to_integral_value()
        nearest = nearest_units * step_dec
        tolerance = max(Decimal("1e-12"), step_dec * Decimal("1e-9"))
        if abs(nearest - qty_dec) <= tolerance:
            return float(nearest)
        floor_units = units.to_integral_value(rounding=ROUND_FLOOR)
        return float(floor_units * step_dec)

    precision = int(getattr(metadata, "quantity_precision", 0) or 0)
    if precision > 0:
        quantum = Decimal(1).scaleb(-precision)
        return float(qty_dec.quantize(quantum, rounding=ROUND_FLOOR))
    return qty_value


def _effective_risk_controls_from_request(
    declarations: Any,
    request_value: Any,
    leverage_value: Any = 0.0,
) -> _EffectiveRiskControls:
    declared_value = getattr(getattr(declarations, "risk_controls", None), "max_loss_close_pct", None)
    if declared_value is not None:
        max_loss_close_pct = float(declared_value)
        max_loss_close_source = "strategy"
    else:
        try:
            raw = float(request_value or 0.0)
        except (TypeError, ValueError) as exc:
            raise StrategyDeclarationError("max_loss_close_pct must be a number") from exc
        if raw < 0.0 or raw > 1.0:
            raise StrategyDeclarationError("max_loss_close_pct must satisfy 0 < value <= 1")
        if raw > 0.0:
            max_loss_close_pct = raw
            max_loss_close_source = "request_default"
        else:
            max_loss_close_pct = DEFAULT_MAX_LOSS_CLOSE_PCT
            max_loss_close_source = "platform_default"

    try:
        raw_leverage = float(leverage_value or 0.0)
    except (TypeError, ValueError) as exc:
        raise StrategyDeclarationError("leverage must be a number") from exc
    if not math.isfinite(raw_leverage) or raw_leverage < 0.0 or math.trunc(raw_leverage) != raw_leverage:
        raise StrategyDeclarationError("leverage must be a positive whole number")
    if raw_leverage > 0.0:
        leverage = raw_leverage
        leverage_source = "request_default"
    else:
        leverage = DEFAULT_SESSION_LEVERAGE
        leverage_source = "platform_default"
    return _EffectiveRiskControls(
        max_loss_close_pct=max_loss_close_pct,
        max_loss_close_source=max_loss_close_source,
        leverage=leverage,
        leverage_source=leverage_source,
    )


def _wallet_margin_balance(wallet: Any) -> float:
    if isinstance(wallet, PortfolioWalletRuntime):
        total = 0.0
        found_futures = False
        for (_exchange, market, _venue_id), route_wallet in wallet.wallets.items():
            if market not in {"perpetual_futures", "delivery_futures"}:
                continue
            futures = getattr(route_wallet, "futures", None)
            getter = getattr(futures, "get_margin_balance", None)
            if not callable(getter):
                continue
            total += float(getter())
            found_futures = True
        if found_futures:
            return total

    getter = getattr(wallet, "get_margin_balance", None)
    if callable(getter):
        return float(getter())
    futures = getattr(wallet, "futures", None)
    getter = getattr(futures, "get_margin_balance", None)
    if callable(getter):
        return float(getter())
    total_value = getattr(wallet, "get_total_value", None)
    if callable(total_value):
        return float(total_value())
    raise RuntimeError("margin_balance unavailable")


def _target_close_margin_balance(
    wallet: Any,
    order_target_keys: set[tuple[str, str, str]],
) -> float:
    normalized_targets: set[tuple[str, str, str]] = set()
    for exchange, market, symbol in order_target_keys or set():
        try:
            normalized_targets.add(_normalize_target_key(exchange, market, symbol))
        except StrategyDeclarationError:
            continue
    if not normalized_targets:
        return _wallet_margin_balance(wallet)

    if isinstance(wallet, PortfolioWalletRuntime):
        total = 0.0
        found_target_route = False
        for (exchange, market, _venue_id), route_wallet in wallet.wallets.items():
            if market not in {"perpetual_futures", "delivery_futures"}:
                continue
            try:
                route_exchange = _normalize_exchange(exchange)
                route_market = _normalize_market(market)
            except StrategyDeclarationError:
                continue
            if not any(key[0] == route_exchange and key[1] == route_market for key in normalized_targets):
                continue
            futures = getattr(route_wallet, "futures", None)
            getter = getattr(futures, "get_wallet_balance", None)
            if not callable(getter):
                continue
            route_value = float(getter())
            positions = getattr(futures, "positions", {}) or {}
            for pos in positions.values():
                symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
                if (route_exchange, route_market, symbol) not in normalized_targets:
                    continue
                upnl_getter = getattr(pos, "get_unrealized_pnl", None)
                if callable(upnl_getter):
                    route_value += float(upnl_getter())
                    continue
                qty = float(getattr(pos, "position_qty", 0.0) or 0.0)
                entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
                mark = float(getattr(pos, "mark_price", 0.0) or 0.0)
                if entry > 0.0 and mark > 0.0:
                    route_value += qty * (mark - entry)
            total += route_value
            found_target_route = True
        if found_target_route:
            return total

    return _wallet_margin_balance(wallet)


# Note: wallet-derived symbol inference was intentionally removed (pre_C3 §2.1/§2.2).
# The authoritative universe is the strategy's ``INPUTS`` declaration, which is
# resolved via ``extract_strategy_declarations`` at RPC entry.


def _live_consumer_group(strategy_id: int, session_id: str) -> str:
    return f"strategy-session-{int(strategy_id)}-{session_id.strip()}"


def _stream_label(binding: StreamBinding) -> str:
    return f"{binding.symbol} {binding.market} {binding.interval}"


def _stream_key(market: Any, symbol: Any, interval: Any) -> tuple[str, str, str]:
    return (
        str(market or "").strip().lower(),
        str(symbol or "").strip().upper(),
        str(interval or "").strip() or "1m",
    )


def _canonical_market_for_kline(
    kline: Any,
    canonical_by_stream: dict[tuple[str, str, str], str],
) -> str | None:
    key = _stream_key(
        getattr(kline, "market", None),
        getattr(kline, "symbol", None),
        getattr(kline, "interval", None),
    )
    canonical = canonical_by_stream.get(key)
    if canonical:
        return canonical
    raw_market = getattr(kline, "market", None)
    if isinstance(raw_market, str) and raw_market.strip():
        return raw_market.strip().lower()
    return None


def _interval_ms(interval: str) -> int:
    raw = str(interval or "1m").strip()
    if len(raw) < 2:
        return 60_000
    try:
        value = int(raw[:-1])
    except ValueError:
        return 60_000
    unit = raw[-1]
    multipliers = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 7 * 86_400_000,
        "M": 30 * 86_400_000,
    }
    return max(1, value) * multipliers.get(unit, 60_000)


def _resolve_stop_action(request: Any) -> int:
    action = int(getattr(request, "stop_action", pb2.STOP_ACTION_UNSPECIFIED) or pb2.STOP_ACTION_UNSPECIFIED)
    if action != pb2.STOP_ACTION_UNSPECIFIED:
        return action
    return pb2.STOP_ACTION_STOP_ONLY


def _message_has_field(message: Any, field_name: str) -> bool:
    has_field = getattr(message, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field(field_name))
        except ValueError:
            return False
    return getattr(message, field_name, None) is not None


def _portfolio_snapshot_environment(snapshot: Any) -> int:
    wallet = getattr(snapshot, "wallet", None)
    if wallet is not None and _message_has_field(snapshot, "wallet"):
        environment = int(getattr(wallet, "environment", 0) or 0)
        if environment != 0:
            return environment
    for venue in getattr(snapshot, "venues", []) or []:
        venue_wallet = getattr(venue, "wallet", None)
        if venue_wallet is None or not _message_has_field(venue, "wallet"):
            continue
        environment = int(getattr(venue_wallet, "environment", 0) or 0)
        if environment != 0:
            return environment
    return int(getattr(wallet, "environment", 0) or 0) if wallet is not None else 0


def _get_portfolio_snapshot(
    acct_client: Any,
    portfolio_id: int,
    user_id: int,
    required_symbols: set[tuple[str, str, str]] | None = None,
):
    getter = getattr(acct_client, "get_portfolio_snapshot")
    if required_symbols:
        try:
            return getter(portfolio_id, user_id, required_symbols=sorted(required_symbols))
        except TypeError:
            logger.debug("portfolio client does not accept required_symbols; using legacy snapshot call")
    return getter(portfolio_id, user_id)


class StrategyServiceServicer(pb2_grpc.StrategyServiceServicer):

    def __init__(
        self,
        portfolio_service_addr: str,
        order_service_addr: str,
        timescale_config: dict[str, Any],
        kafka_brokers: str,
        market_data_policy: dict[str, Any] | None = None,
        bound_user_id: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
        platform_access_mode: str = PLATFORM_ACCESS_PROXY_ONLY,
        market_data_control_panel_addr: str = "",
        restore_running_sessions: bool = True,
        platform_proxy: Any | None = None,
        notification_client: Any | None = None,
    ) -> None:
        self._portfolio_addr = portfolio_service_addr
        self._market_data_addr = market_data_control_panel_addr
        self._order_addr = order_service_addr
        self._ts_config = timescale_config
        self._kafka_brokers = kafka_brokers
        self._market_data_policy = market_data_policy or {}
        # Phase D1 hosted runtime: when registered with control-panel,
        # the runtime is bound to one user_id at registration time. All
        # inbound strategy RPCs MUST carry that user_id; mismatch is a
        # PermissionDenied. 0 = unregistered runtime (skip check).
        self._bound_user_id = int(bound_user_id or 0)
        self._runtime_id = str(runtime_id or "").strip()
        self._runtime_source = str(runtime_source or "").strip()
        self._runtime_name = str(runtime_name or "").strip()
        if str(platform_access_mode or "").strip().lower() not in ("", PLATFORM_ACCESS_PROXY_ONLY):
            logger.warning("unsupported platform_access_mode=%s ignored; using RuntimeChannel proxy", platform_access_mode)
        self._platform_access_mode = PLATFORM_ACCESS_PROXY_ONLY
        self._platform_proxy = platform_proxy
        self._notification_client = notification_client
        self._runtime_data_source = None
        self._indicator_frame_sink: Callable[..., None] | None = None
        self._preflight_enabled = bool(self._market_data_policy.get("preflight_enabled", True))
        self._lease_management_enabled = bool(self._market_data_policy.get("lease_management_enabled", True))
        self._lease_heartbeat_seconds = int(
            self._market_data_policy.get("lease_heartbeat_seconds", DEFAULT_LEASE_HEARTBEAT_SECONDS)
            or DEFAULT_LEASE_HEARTBEAT_SECONDS
        )
        self._lease_ttl_seconds = int(
            self._market_data_policy.get("lease_ttl_seconds", DEFAULT_LEASE_TTL_SECONDS)
            or DEFAULT_LEASE_TTL_SECONDS
        )
        self._freshness_grace_seconds = int(
            self._market_data_policy.get("freshness_grace_seconds", DEFAULT_FRESHNESS_GRACE_SECONDS)
            or DEFAULT_FRESHNESS_GRACE_SECONDS
        )
        self._sessions = SessionManager()
        if restore_running_sessions:
            self._restore_running_sessions()

    def set_platform_proxy(self, platform_proxy: Any) -> None:
        self._platform_proxy = platform_proxy

    def set_notification_client(self, notification_client: Any) -> None:
        self._notification_client = notification_client

    def set_runtime_data_source(self, runtime_data_source: Any) -> None:
        self._runtime_data_source = runtime_data_source

    def set_indicator_frame_sink(self, sink: Callable[..., None] | None) -> None:
        self._indicator_frame_sink = sink

    def _portfolio_client(self):
        if self._platform_proxy is None:
            raise RuntimeError("RuntimeChannel platform proxy client is not configured")
        return self._platform_proxy.portfolio_client()

    def _order_client(self):
        if self._platform_proxy is None:
            raise RuntimeError("RuntimeChannel platform proxy client is not configured")
        return self._platform_proxy.order_client()

    def _marketdata_client(self):
        if self._platform_proxy is None:
            raise RuntimeError("RuntimeChannel platform proxy client is not configured")
        return self._platform_proxy.marketdata_client()

    def _debug_strategy_path_for_db_code(
        self,
        *,
        user_id: int,
        strategy_id: int,
        strategy_name: str,
        strategy_version: str,
        strategy_path: str,
        strategy_code: str | None,
    ) -> str:
        strategy_path, _strategy_code, _hot_reload = self._debug_strategy_source_for_db_code(
            user_id=user_id,
            strategy_id=strategy_id,
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            strategy_path=strategy_path,
            strategy_code=strategy_code,
        )
        return strategy_path

    def _debug_strategy_source_for_db_code(
        self,
        *,
        user_id: int,
        strategy_id: int,
        strategy_name: str,
        strategy_version: str,
        strategy_path: str,
        strategy_code: str | None,
    ) -> tuple[str, str | None, bool]:
        if self._runtime_source != "bare" or not strategy_code:
            return strategy_path, strategy_code, False
        path = ensure_bare_strategy_source(
            user_id=user_id,
            strategy_id=strategy_id,
            name=strategy_name,
            version=strategy_version,
            strategy_code=strategy_code,
        )
        logger.info(
            "bare runtime using local strategy source: user_id=%s strategy_id=%s path=%s",
            user_id,
            strategy_id,
            path,
        )
        return str(path), None, True

    def _restore_running_sessions(self) -> None:
        if not self._runtime_id:
            logger.info(
                "startup session recovery skipped: runtime_id is empty; "
                "unfiltered recovery is disabled"
            )
            return
        acct_client = self._portfolio_client()
        sessions = None
        last_error: Exception | None = None
        for attempt in range(1, RESTORE_RUNNING_SESSIONS_RETRIES + 1):
            try:
                sessions = self._list_running_sessions_for_restore(acct_client)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "startup session recovery failed to list running sessions "
                    "(attempt %d/%d)",
                    attempt,
                    RESTORE_RUNNING_SESSIONS_RETRIES,
                    exc_info=True,
                )
                if attempt < RESTORE_RUNNING_SESSIONS_RETRIES:
                    time.sleep(RESTORE_RUNNING_SESSIONS_RETRY_SECONDS)
        if sessions is None:
            raise RuntimeError(
                f"startup session recovery failed: cannot list running sessions "
                f"from core-service at {self._portfolio_addr}"
            ) from last_error

        orphaned = 0
        for session in sessions:
            session_id = getattr(session, "session_id", "")
            if not session_id:
                continue
            previous = str(getattr(session, "status", "running") or "running").strip().lower()
            if previous not in ("running", "stopping"):
                continue
            target = "stop_failed" if previous == "stopping" else "stopped"
            reason = (
                "session orphaned after strategy-service restart; runtime recovery is not implemented"
            )
            if previous == "stopping":
                reason = (
                    "session stop interrupted by strategy-service restart; runtime recovery is not implemented"
                )
            if not acct_client.update_session(
                session_id=session_id,
                status=target,
                bars_processed=int(getattr(session, "bars_processed", 0)),
                error=reason,
                runtime_id=self._runtime_id,
            ):
                logger.warning("failed to mark orphaned session terminal on restore: %s", session_id)
                continue
            orphaned += 1
            logger.warning(
                "marked orphaned session terminal on restore: session_id=%s previous=%s target=%s",
                session_id,
                previous,
                target,
            )
        if orphaned:
            logger.warning(
                "marked %d orphaned running/stopping sessions terminal during startup recovery",
                orphaned,
            )

    def _list_running_sessions_for_restore(self, acct_client: Any):
        strict = getattr(acct_client, "require_running_sessions", None)
        if callable(strict):
            return strict(runtime_id=self._runtime_id)
        return acct_client.list_running_sessions(runtime_id=self._runtime_id)

    def _enforce_user_binding(self, request_user_id: int, context) -> bool:
        """Phase D1 section 6.5 cross-check.

        Bare debug runtimes can be pinned to one user at startup. All
        strategy RPCs MUST carry that user_id in their request so a
        mismatched platform route fails closed.

        Returns True if the call should continue, False if it was
        rejected. Caller MUST return immediately on False.
        """
        if self._bound_user_id <= 0:
            return True
        if int(request_user_id) != self._bound_user_id:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(
                f"user_id mismatch: request={int(request_user_id)} bound={self._bound_user_id}"
            )
            return False
        return True

    def _enforce_request_runtime(self, request: Any, context) -> bool:
        requested = str(getattr(request, "runtime_id", "") or "").strip()
        if self._runtime_id and requested and requested != self._runtime_id:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(
                f"runtime_id mismatch: request={requested} bound={self._runtime_id}"
            )
            return False
        return True

    def _runtime_binding_for_request(self, request: Any) -> tuple[str, str, str]:
        requested = str(getattr(request, "runtime_id", "") or "").strip()
        runtime_id = self._runtime_id or requested
        if not runtime_id:
            return "", "", ""
        return runtime_id, self._runtime_source, self._runtime_name

    @staticmethod
    def _error_details(exc: BaseException) -> str:
        details = getattr(exc, "details", None)
        if callable(details):
            try:
                value = details()
                if value:
                    return str(value)
            except Exception:  # noqa: BLE001
                pass
        return str(exc)

    @staticmethod
    def _is_save_session_conflict(exc: BaseException) -> bool:
        code_getter = getattr(exc, "code", None)
        if callable(code_getter):
            try:
                code = code_getter()
                if code in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.ALREADY_EXISTS):
                    return True
            except Exception:  # noqa: BLE001
                pass
        text = str(exc).lower()
        return (
            "failedprecondition" in text
            or "failed_precondition" in text
            or "alreadyexists" in text
            or "already_exists" in text
            or "active session" in text
        )

    def _persist_session_or_set_error(self, acct_client: Any, context, **kwargs: Any) -> bool:
        strict = getattr(acct_client, "require_save_session", None)
        try:
            if callable(strict):
                strict(**kwargs)
            else:
                ok = acct_client.save_session(**kwargs)
                if not ok:
                    raise RuntimeError("SaveSession returned false")
            return True
        except Exception as exc:  # noqa: BLE001
            if self._is_save_session_conflict(exc):
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                detail = self._error_details(exc)
                context.set_details(
                    "portfolio already has an active session; stop or recover the existing "
                    f"session before starting a new one ({detail})"
                )
            else:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"failed to persist session to core-service: {self._error_details(exc)}")
            return False

    def _enforce_session_runtime(self, request: Any, state: SessionState, context) -> bool:
        requested = str(getattr(request, "runtime_id", "") or "").strip()
        if state.runtime_id and requested and requested != state.runtime_id:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"session {getattr(request, 'session_id', '')} not found")
            return False
        if self._runtime_id and state.runtime_id and self._runtime_id != state.runtime_id:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"session {getattr(request, 'session_id', '')} not found")
            return False
        return True

    def _require_platform_proxy(self, context, operation: str) -> bool:
        if self._platform_proxy is not None:
            return True
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
        context.set_details(
            f"{operation} unavailable in RuntimeChannel runtime: "
            "platform proxy client is not configured"
        )
        return False

    def _require_market_data_execution_path(self, context, operation: str, profile: RuntimeSourceProfile) -> bool:
        if profile in (RuntimeSourceProfile.DEMO, RuntimeSourceProfile.LIVE):
            if callable(getattr(self._runtime_data_source, "iter_live_klines", None)):
                return True
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(
                f"{operation} unavailable in RuntimeChannel runtime for "
                f"profile={profile.value}: platform live delivery is not configured; "
                "FetchKlines fallback is disabled for demo/live execution"
            )
            return False
        if profile is RuntimeSourceProfile.BACKTEST:
            try:
                client = self._marketdata_client()
            except Exception as exc:  # noqa: BLE001
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    f"{operation} unavailable in RuntimeChannel runtime for "
                    f"profile={profile.value}: market-data proxy client failed to initialize: {exc}"
                )
                return False
            if callable(getattr(client, "fetch_backtest_page", None)):
                return True
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(
                f"{operation} unavailable in RuntimeChannel runtime for "
                f"profile={profile.value}: paged backtest data proxy is not configured; "
                "FetchKlines fallback is disabled for backtest execution"
            )
            return False
        if self._platform_proxy is not None:
            try:
                client = self._platform_proxy.marketdata_client()
            except Exception as exc:  # noqa: BLE001
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    f"{operation} unavailable in RuntimeChannel runtime: "
                    f"market-data proxy client failed to initialize: {exc}"
                )
                return False
            if callable(getattr(client, "fetch_klines", None)):
                return True
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
        context.set_details(
            f"{operation} unavailable in RuntimeChannel runtime for "
            f"profile={profile.value}: portfolio/order/control-plane proxy is wired, "
            "but strategy market-data execution still needs an approved platform "
            "data proxy"
        )
        return False

    # ── RunStrategy ──────────────────────────────────────────────────────────

    def RunStrategy(self, request, context):
        user_id = int(request.user_id)
        if user_id <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("user_id is required")
            return pb2.RunStrategyResponse()
        if not self._enforce_user_binding(user_id, context):
            return pb2.RunStrategyResponse()
        if not self._enforce_request_runtime(request, context):
            return pb2.RunStrategyResponse()
        if not self._require_platform_proxy(context, "RunStrategy"):
            return pb2.RunStrategyResponse()
        portfolio_id = request.portfolio_id
        if portfolio_id == 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("portfolio_id is required")
            return pb2.RunStrategyResponse()

        # 1. 从 core-service 获取组合快照（environment + 多 venue 钱包）
        acct_client = self._portfolio_client()
        snapshot = _get_portfolio_snapshot(acct_client, portfolio_id, user_id)
        if snapshot is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"portfolio {portfolio_id} not found or core-service unreachable")
            return pb2.RunStrategyResponse()

        environment = _portfolio_snapshot_environment(snapshot)

        # 2. Resolve the runtime source profile FIRST (pre_C3 gate 2 §4).
        # This is an internal runtime-source mapping, not a strategy/portfolio
        # compatibility signal. Unsupported profiles (today: live environment)
        # fail-fast here with a structured PROFILE failure, *before* we try
        # to build a wallet or load a strategy — so the error surfaces the
        # actual reason (profile not wired up) instead of a downstream
        # wallet-registry miss or strategy-mismatch message.
        profile = resolve_profile(environment)
        profile_gate = check_profile_supported(profile)
        if not profile_gate.ok:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(profile_gate.error_message())
            return pb2.RunStrategyResponse()
        if not self._require_market_data_execution_path(context, "RunStrategy", profile):
            return pb2.RunStrategyResponse()

        # 3. 确定策略来源：优先 GetActiveStrategy（DB 存储），fallback strategy_path（开发/测试）
        strategy_id = 0
        strategy_code: str | None = None
        strategy_name = ""
        strategy_version = ""
        strategy_path = request.strategy_path  # may be empty in production
        strategy_hot_reload = False

        active = acct_client.get_active_strategy(portfolio_id)
        if active is not None and active.strategy_id != 0:
            strategy_id = active.strategy_id
            strategy_code = active.code
            strategy_name = str(getattr(active, "name", "") or "")
            strategy_version = str(getattr(active, "version", "") or "")
            strategy_path = f"<db:{strategy_name}@{strategy_version}>"
        elif not strategy_path:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("portfolio has no active strategy; mount and activate one first")
            return pb2.RunStrategyResponse()
        try:
            strategy_path, strategy_code, strategy_hot_reload = self._debug_strategy_source_for_db_code(
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                strategy_path=strategy_path,
                strategy_code=strategy_code,
            )
        except DebugStrategySourceError as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"failed to materialize bare debug strategy source: {e}")
            return pb2.RunStrategyResponse()

        validation_error = _strategy_validation_error(strategy_code)
        if validation_error:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(validation_error)
            return pb2.RunStrategyResponse()

        # Resolve the strategy's declared input universe. Per pre_C3
        # gate 1 this is the ONLY authoritative source for what a strategy
        # consumes — wallet positions / spot assets MUST NOT contribute. Any
        # declaration error surfaces as FAILED_PRECONDITION on the RPC itself,
        # never as a background-thread session failure.
        try:
            declarations = extract_strategy_declarations(strategy_path, strategy_code)
        except StrategyDeclarationError as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"strategy input declaration invalid: {e}")
            return pb2.RunStrategyResponse()
        except (ImportError, AttributeError) as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"strategy could not be loaded: {e}")
            return pb2.RunStrategyResponse()
        try:
            effective_risk = _effective_risk_controls_from_request(
                declarations,
                getattr(request, "max_loss_close_pct", 0.0),
                getattr(request, "leverage", 0.0),
            )
        except StrategyDeclarationError as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"strategy risk control invalid: {e}")
            return pb2.RunStrategyResponse()
        declared_inputs = list(declarations.inputs)
        required_routes = set(declarations.required_routes)
        required_symbols = {
            (entry.exchange, entry.market, entry.symbol)
            for entry in declarations.inputs
        } | set(declarations.order_target_keys)
        preflight_session_id = uuid.uuid4().hex

        portfolio_preflight = self._run_portfolio_preflight(
            acct_client=acct_client,
            portfolio_id=portfolio_id,
            user_id=user_id,
            required_routes=required_routes,
            required_symbols=required_symbols,
            session_id=preflight_session_id,
            strategy_id=strategy_id,
            leverage=effective_risk.leverage,
        )
        if portfolio_preflight is not None:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            try:
                context.set_trailing_metadata((("preflight-session-id", preflight_session_id),))
            except Exception:  # noqa: BLE001
                logger.debug("context does not support trailing metadata for preflight failure")
            context.set_details(f"{portfolio_preflight}; preflight_session_id={preflight_session_id}")
            return pb2.RunStrategyResponse()

        snapshot = _get_portfolio_snapshot(
            acct_client,
            portfolio_id,
            user_id,
            required_symbols=required_symbols,
        )
        if snapshot is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"portfolio {portfolio_id} not found or core-service unreachable")
            return pb2.RunStrategyResponse()

        try:
            wallet = build_portfolio_wallet_from_snapshot(
                snapshot,
                allowed_routes=required_routes,
            )
            backtest_restore_wallet = None
            if environment == 0:
                backtest_restore_wallet = build_portfolio_wallet_from_snapshot(
                    snapshot,
                    allowed_routes=required_routes,
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"failed to build wallet: {e}")
            return pb2.RunStrategyResponse()
        try:
            initial_margin_balance = _target_close_margin_balance(
                wallet,
                set(declarations.order_target_keys),
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"failed to initialize risk controls: {e}")
            return pb2.RunStrategyResponse()
        if initial_margin_balance <= 0.0:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("failed to initialize risk controls: initial margin_balance must be positive")
            return pb2.RunStrategyResponse()

        # The declared 3-tuple universe is threaded straight through to the
        # backtest / live runtime now — the (symbol, market) flattening was
        # removed after multi-interval support landed, because collapsing to
        # 2-tuples silently dropped declared intervals on the replay/subscribe
        # paths. Declared (exchange, market, symbol, interval) inputs remain
        # distinct and are never flattened to a single interval per symbol.

        # Preflight is split into two concerns:
        #
        # 1. Stream binding (always runs for demo/live profiles) — resolves
        #    declared inputs → ``StreamBinding`` list so lease management can
        #    identify which market-data streams this session depends on.
        #    Without these bindings the control plane has no idea the session
        #    exists and may stop the underlying stream. A missing stream is
        #    ALWAYS a startup failure even when readiness gating is disabled.
        #
        # 2. Readiness gating (optional via ``market_data_policy.preflight_enabled``)
        #    — state/delivery/freshness checks. Disabling this is an operator
        #    bypass for testing; it must NOT disable binding resolution.
        # Build the control-panel market-data proxy only after profile/path
        # guards so RuntimeChannel sessions cannot accidentally use local
        # data sources.
        marketdata_client = self._marketdata_client()
        preflight = self._run_profile_preflight(
            profile=profile,
            declared_inputs=declared_inputs,
            marketdata_client=marketdata_client,
            start_ms=int(getattr(request, "start_time_ms", 0) or 0),
            end_ms=int(getattr(request, "end_time_ms", 0) or 0),
            require_readiness=self._preflight_enabled,
        )
        if not preflight.ok:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(preflight.error_message())
            return pb2.RunStrategyResponse()
        required_streams = list(preflight.required_streams)

        # 4. 创建 session（内存 + DB 持久化）
        runtime_id, runtime_source, runtime_name = self._runtime_binding_for_request(request)
        if not runtime_id:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("runtime_id is required to start a strategy session")
            return pb2.RunStrategyResponse()
        session_id, state = self._sessions.create(
            environment=environment,
            user_id=user_id,
            portfolio_id=portfolio_id,
            runtime_id=runtime_id,
            runtime_source=runtime_source,
            runtime_name=runtime_name,
        )
        if environment == 1:
            state.configure_live_runtime(
                portfolio_id=portfolio_id,
                strategy_id=strategy_id,
                required_streams=required_streams,
                consumer_group=_live_consumer_group(strategy_id, session_id),
            )
            if not self._create_session_market_data_subscriptions(
                session_id=session_id,
                state=state,
                user_id=user_id,
            ):
                self._sessions.discard(session_id)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    "failed to create required live delivery subscriptions for demo session"
                )
                return pb2.RunStrategyResponse()
        else:
            state.portfolio_id = portfolio_id
            state.strategy_id = strategy_id
        state.configure_risk_runtime(
            order_target_keys=set(declarations.order_target_keys),
            max_loss_close_pct=effective_risk.max_loss_close_pct,
            max_loss_close_source=effective_risk.max_loss_close_source,
            leverage=effective_risk.leverage,
            leverage_source=effective_risk.leverage_source,
            initial_margin_balance=initial_margin_balance,
        )

        if not self._persist_session_or_set_error(
            acct_client,
            context,
            session_id=session_id,
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            environment=environment,
            interval=request.interval or "1m",
            start_time_ms=request.start_time_ms,
            end_time_ms=request.end_time_ms,
            runtime_id=runtime_id,
            runtime_source=runtime_source,
            runtime_name=runtime_name,
            leverage=effective_risk.leverage,
        ):
            if environment == 1:
                self._release_session_market_data_subscriptions(session_id, state)
            self._sessions.discard(session_id)
            return pb2.RunStrategyResponse()

        # 写 strategy_start 组合快照。启动快照写不进去时直接拒绝启动，
        # 否则 backtest 会继续产生成交但没有钱包/PnL 审计链路。
        try:
            _sync_strategy_snapshot(
                acct_client,
                portfolio_id=portfolio_id,
                user_id=user_id,
                environment=environment,
                wallet=wallet,
                snapshot_reason=SNAPSHOT_REASON_STRATEGY_START,
                strategy_id=strategy_id,
                session_id=session_id,
                snapshot_time=getattr(request, "start_time_ms", None),
            )
        except Exception as e:
            logger.warning("session %s: failed to persist strategy_start snapshot", session_id, exc_info=True)
            error = f"failed to persist strategy_start snapshot: {e}"
            state.transition("failed", error=error)
            try:
                if not acct_client.update_session(
                    session_id=session_id,
                    status=state.status,
                    bars_processed=state.bars_processed,
                    error=state.error,
                    runtime_id=state.runtime_id,
                ):
                    logger.warning("session %s: failed to persist startup failure status", session_id)
            except Exception:
                logger.warning("session %s: failed to persist startup failure status", session_id, exc_info=True)
            if environment == 1:
                self._release_session_market_data_subscriptions(session_id, state)
            self._sessions.discard(session_id)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(error)
            return pb2.RunStrategyResponse()

        # 5. 启动后台线程
        otel_parent_context = _capture_otel_context()

        def _run_session_with_context() -> None:
            _run_in_otel_context(
                otel_parent_context,
                f"StrategySession/{session_id}",
                lambda: self._run_session(
                    session_id, state, request, wallet, environment, portfolio_id, user_id,
                    declared_inputs, strategy_path, strategy_id, strategy_code,
                    strategy_hot_reload=strategy_hot_reload,
                    backtest_restore_wallet=backtest_restore_wallet,
                ),
            )

        t = threading.Thread(
            target=_run_session_with_context,
            daemon=True,
        )
        t.start()
        self._sessions.set_thread(session_id, t)

        return pb2.RunStrategyResponse(session_id=session_id)

    def _run_session(
        self,
        session_id: str,
        state: SessionState,
        request: Any,
        wallet: Any,
        environment: int,
        portfolio_id: int,
        user_id: int,
        declared_inputs: list[StrategyInput],
        strategy_path: str,
        strategy_id: int,
        strategy_code: str | None,
        strategy_hot_reload: bool = False,
        backtest_restore_wallet: Any | None = None,
    ) -> None:
        user_strategy: Any | None = None
        try:
            order_client = self._order_client()
            acct_client = self._portfolio_client()
            engine = StrategyEngine()

            def _persist_runtime_error(message: str) -> None:
                if not state.record_runtime_error(message):
                    return
                self._persist_session_status(session_id, state)

            def _clear_runtime_error() -> None:
                if not state.clear_runtime_error(USER_STRATEGY_ON_MARKET_DATA_ERROR_PREFIX):
                    return
                self._persist_session_status(session_id, state)

            user_strategy = engine.create_strategy(
                user_id=f"user:{user_id}:session:{session_id}",
                strategy_path=strategy_path,
                wallet=wallet,
                order_client=order_client,
                portfolio_id=portfolio_id,
                strategy_id=strategy_id,
                session_id=session_id,
                strategy_code=strategy_code,
                notifier=StrategyNotifier(self._notification_client),
                hot_reload=strategy_hot_reload,
                on_user_code_error=_persist_runtime_error if strategy_hot_reload else None,
                on_user_code_recovered=_clear_runtime_error if strategy_hot_reload else None,
            )

            # 注册 on_order 回调：同步钱包到 core-service（带 session_id 审计追溯）

            def _on_order_sync() -> None:
                _sync_strategy_snapshot(
                    acct_client,
                    portfolio_id=portfolio_id,
                    user_id=user_id,
                    environment=environment,
                    wallet=wallet,
                    snapshot_reason=SNAPSHOT_REASON_EVENT,
                    strategy_id=strategy_id,
                    session_id=session_id,
                    snapshot_time=getattr(user_strategy, "last_market_time", None),
                )

            user_strategy.on_order_callback = _on_order_sync
            state.configure_stop_runtime(wallet=wallet, order_client=order_client)

            # Phase C PeriodicSample hybrid trigger — only for demo sessions.
            # Backtest has no external oracle and live remains fail-closed here.
            if environment == 1:
                self._install_periodic_sample_trigger(
                    engine=engine,
                    portfolio_id=portfolio_id,
                    user_id=user_id,
                    strategy_id=strategy_id,
                    session_id=session_id,
                    wallet=wallet,
                    portfolio_client=acct_client,
                    every_n_bars=DEFAULT_PERIODIC_SAMPLE_EVERY_BARS,
                    max_idle_seconds=float(DEFAULT_PERIODIC_SAMPLE_MAX_IDLE_SECONDS),
                )
            self._install_max_loss_close_guard(
                engine=engine,
                session_id=session_id,
                state=state,
                wallet=wallet,
            )

            if environment == 0:
                self._run_backtest(session_id, state, engine, request, declared_inputs)
            elif environment == 1:
                self._run_live(session_id, state, engine, declared_inputs, strategy_id)
            else:
                raise ValueError(f"unsupported portfolio environment: {environment}")

        except Exception as e:
            logger.exception("session %s failed", session_id)
            state.transition("failed", error=str(e))

        # session 结束（finished/stopped/failed）：写 strategy_end 快照 + 更新 DB
        finally:
            lease_stop_event = state.lease_stop_event
            if lease_stop_event is not None:
                lease_stop_event.set()
            self._release_stream_leases(session_id, state)
            self._release_session_market_data_subscriptions(session_id, state)
            finalization_errors: list[str] = []
            try:
                acct_client = self._portfolio_client()
                try:
                    _sync_strategy_snapshot(
                        acct_client,
                        portfolio_id=portfolio_id,
                        user_id=user_id,
                        environment=environment,
                        wallet=wallet,
                        snapshot_reason=SNAPSHOT_REASON_STRATEGY_END,
                        strategy_id=strategy_id,
                        session_id=session_id,
                        snapshot_time=(
                            (getattr(user_strategy, "last_market_time", None) if user_strategy is not None else None)
                            or getattr(request, "end_time_ms", None)
                        ),
                    )
                except Exception as e:
                    logger.warning("session %s: failed to persist strategy_end snapshot", session_id, exc_info=True)
                    finalization_errors.append(f"failed to persist strategy_end snapshot: {e}")
                if environment == 0 and backtest_restore_wallet is not None:
                    try:
                        # 回测过程中的 wallet 写入只服务本 session 审计；结束后必须
                        # 恢复启动前账户状态，避免下一次回测继承本次 PnL/仓位。
                        _sync_strategy_snapshot(
                            acct_client,
                            portfolio_id=portfolio_id,
                            user_id=user_id,
                            environment=environment,
                            wallet=backtest_restore_wallet,
                            snapshot_reason=0,
                            strategy_id=strategy_id,
                            session_id=session_id,
                        )
                    except Exception as e:
                        logger.warning(
                            "session %s: failed to restore backtest portfolio wallet state",
                            session_id,
                            exc_info=True,
                        )
                        finalization_errors.append(f"failed to restore backtest portfolio wallet state: {e}")
                if finalization_errors:
                    with state._lock:
                        if state.status not in {"failed", "stop_failed"}:
                            state.status = "recoverable"
                            state.error = "; ".join(finalization_errors)
                        elif not state.error:
                            state.error = "; ".join(finalization_errors)
                if not acct_client.update_session(
                    session_id=session_id,
                    status=state.status,
                    bars_processed=state.bars_processed,
                    error=state.error,
                    runtime_id=state.runtime_id,
                ):
                    logger.warning("session %s: failed to persist final session status", session_id)
                    _safe_send_session_status_patch(
                        self._platform_proxy,
                        session_id=session_id,
                        status=state.status,
                        bars_processed=state.bars_processed,
                        error=state.error,
                        runtime_id=state.runtime_id,
                    )
                self._sessions.mark_terminal(session_id)
            except Exception:
                logger.warning("session %s: failed to finalize", session_id, exc_info=True)

    @staticmethod
    def _run_portfolio_preflight(
        *,
        acct_client: Any,
        portfolio_id: int,
        user_id: int,
        required_routes: set[tuple[str, str]],
        required_symbols: set[tuple[str, str, str]],
        session_id: str = "",
        strategy_id: int = 0,
        leverage: float = 0.0,
    ) -> str | None:
        preflight = getattr(acct_client, "preflight_strategy_session", None)
        if not callable(preflight):
            return "portfolio preflight unavailable: client does not support PreflightStrategySession"
        resp = preflight(
            portfolio_id=portfolio_id,
            user_id=user_id,
            required_routes=sorted(required_routes),
            required_symbols=sorted(required_symbols),
            session_id=str(session_id or ""),
            strategy_id=int(strategy_id),
            leverage=float(leverage or 0.0),
        )
        if resp is None:
            return "portfolio preflight unavailable: core-service did not return a result"
        if bool(getattr(resp, "ok", False)):
            return None
        issue_messages: list[str] = []
        for issue in getattr(resp, "issues", []) or []:
            code = str(getattr(issue, "code", "") or "preflight_failed")
            message = str(getattr(issue, "message", "") or "").strip()
            exchange = getattr(issue, "exchange", 0)
            market = getattr(issue, "market", 0)
            symbol = str(getattr(issue, "symbol", "") or "").strip()
            route = f" exchange={exchange} market={market}"
            if symbol:
                route = f"{route} symbol={symbol}"
            issue_messages.append(f"{code}: {message}{route}".strip())
        if issue_messages:
            return "portfolio preflight failed: " + "; ".join(issue_messages)
        return "portfolio preflight failed"

    def _run_profile_preflight(
        self,
        *,
        profile: RuntimeSourceProfile,
        declared_inputs: list[StrategyInput],
        marketdata_client: Any,
        start_ms: int,
        end_ms: int,
        require_readiness: bool = True,
    ) -> PreflightResult:
        """Dispatch to the profile-specific preflight evaluator.

        Backtest profile → historical-data availability for each declared input.
        Demo / live profile → stream binding for each declared input, with
        optional readiness gating. ``require_readiness=False`` disables the
        state/delivery/freshness checks but still resolves bindings — this is
        essential for demo lease management when ``preflight_enabled=False``.
        Unsupported profiles SHOULD have been caught earlier via
        ``check_profile_supported``; if we still land here for one, surface it
        as a profile failure rather than silently running the wrong evaluator.
        """
        if profile is RuntimeSourceProfile.BACKTEST:
            if not require_readiness:
                # Backtest has no streams — the readiness bypass collapses to
                # an unconditional pass. Return an empty ok result rather than
                # burning a DB query on an evaluator whose result we'd ignore.
                return PreflightResult(profile=profile)
            return backtest_preflight(
                declared_inputs,
                start_ms=start_ms,
                end_ms=end_ms,
                availability_fn=self._proxy_backtest_availability(marketdata_client),
            )
        if profile in (RuntimeSourceProfile.DEMO, RuntimeSourceProfile.LIVE):
            return live_stream_preflight(
                declared_inputs,
                profile=profile,
                lookup_stream=self._live_stream_lookup(marketdata_client),
                freshness_grace_seconds=self._freshness_grace_seconds,
                require_readiness=require_readiness,
            )
        # Defense in depth — should be unreachable post check_profile_supported.
        return check_profile_supported(profile)

    @staticmethod
    def _proxy_backtest_availability(marketdata_client: Any):
        def _check(inp: StrategyInput, start_ms: int, end_ms: int) -> bool:
            fetch = getattr(marketdata_client, "fetch_klines", None)
            if not callable(fetch):
                raise RuntimeError("market-data proxy client does not support fetch_klines")
            return bool(fetch(
                exchange="binance",
                market=_marketdata_market(inp.market),
                symbol=inp.symbol,
                interval=inp.interval,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                limit=1,
            ))

        return _check

    @staticmethod
    def _live_stream_lookup(marketdata_client: Any):
        """Return a callable that looks up control-plane stream status by key.

        Kept as a separate method so tests can patch it cleanly when they
        want to assert per-declared-input lookups without a real platform
        proxy. The per-declared-input lookup contract is unchanged.
        """
        def _lookup(market: str, symbol: str, interval: str):
            return marketdata_client.get_market_data_stream_status(
                exchange="binance",
                market=market,
                kind="kline",
                symbol=symbol,
                interval=interval,
            )
        return _lookup

    def _install_periodic_sample_trigger(
        self,
        *,
        engine: StrategyEngine,
        portfolio_id: int,
        user_id: int,
        strategy_id: int,
        session_id: str,
        wallet: Any,
        portfolio_client: PortfolioClient,
        every_n_bars: int,
        max_idle_seconds: float,
        now_fn: Callable[[], float] | None = None,
    ) -> None:
        """Wrap engine.running_strategy to fire a PeriodicSample snapshot after each bar
        when either the bar count OR the wall-clock idle threshold is reached.

        The trigger is session-scoped: both counters live in the closure, so they reset
        independently for each session and do not leak across sessions.
        """
        clock = now_fn if now_fn is not None else time.monotonic
        original = engine.running_strategy

        state = {
            "bars_since_last_compare": 0,
            "last_compare_at": clock(),
        }

        def _maybe_fire() -> None:
            state["bars_since_last_compare"] += 1
            now = clock()
            idle = now - state["last_compare_at"]
            if (state["bars_since_last_compare"] < every_n_bars
                    and idle < max_idle_seconds):
                return
            # Reset BOTH counters before the push — failures must still reset the window
            # so the trigger cannot fire storm-of-pushes against a broken transport.
            state["bars_since_last_compare"] = 0
            state["last_compare_at"] = now
            try:
                _sync_strategy_snapshot(
                    portfolio_client,
                    portfolio_id=portfolio_id,
                    user_id=user_id,
                    environment=1,
                    wallet=wallet,
                    snapshot_reason=SNAPSHOT_REASON_PERIODIC_SAMPLE,
                    strategy_id=strategy_id,
                    session_id=session_id,
                )
            except Exception:
                logger.warning(
                    "session %s: PeriodicSample push failed (non-fatal)",
                    session_id, exc_info=True,
                )

        def wrapped(market_data: Any) -> None:
            original(market_data)
            _maybe_fire()

        engine.running_strategy = wrapped  # type: ignore[assignment]

    def _install_max_loss_close_guard(
        self,
        *,
        engine: StrategyEngine,
        session_id: str,
        state: SessionState,
        wallet: Any,
    ) -> None:
        if float(getattr(state, "initial_margin_balance", 0.0) or 0.0) <= 0.0:
            return
        original = getattr(engine, "running_strategy", None)
        if not callable(original):
            return

        def wrapped(market_data: Any) -> None:
            original(market_data)
            if state.status != "running":
                return
            try:
                self._maybe_trigger_max_loss_close(
                    session_id=session_id,
                    state=state,
                    wallet=wallet,
                )
            except Exception:
                logger.warning("session %s: max-loss guard failed", session_id, exc_info=True)

        engine.running_strategy = wrapped  # type: ignore[assignment]

    def _maybe_trigger_max_loss_close(
        self,
        *,
        session_id: str,
        state: SessionState,
        wallet: Any,
    ) -> None:
        initial = float(state.initial_margin_balance or 0.0)
        threshold = float(state.max_loss_close_pct or DEFAULT_MAX_LOSS_CLOSE_PCT)
        if initial <= 0.0 or threshold <= 0.0:
            return
        current = _target_close_margin_balance(wallet, set(state.order_target_keys or set()))
        loss_pct = max(0.0, initial - current) / initial
        if loss_pct < threshold:
            return
        if not state.mark_max_loss_close_triggered():
            return

        reason = (
            "max_loss_close_triggered:"
            f"loss_pct={loss_pct:.8f}:threshold={threshold:.8f}:"
            f"source={state.max_loss_close_source}:"
            f"initial_margin_balance={initial:.8f}:"
            f"current_margin_balance={current:.8f}"
        )
        logger.warning("session %s: %s", session_id, reason)
        if not state.transition("stopping", error=reason):
            return
        self._persist_session_status(session_id, state)
        self._halt_session_runtime(state, finalize=False)

        ok, close_reason = self._stop_and_close_portfolio(session_id, state)
        if ok:
            state.transition("stopped", error=reason)
        else:
            state.transition("stop_failed", error=f"{reason}; {close_reason}")
        self._persist_session_status(session_id, state)
        self._halt_session_runtime(state, finalize=True)

    def _install_indicator_collection(
        self,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
        request: Any | None = None,
    ) -> Callable[[], None]:
        strategies = [
            strategy
            for strategy in getattr(engine, "strategies", {}).values()
            if getattr(strategy, "indicator_definitions", None)
        ]
        if not strategies:
            return lambda: None

        try:
            portfolio_client = self._portfolio_client()
        except Exception:  # noqa: BLE001
            logger.warning("strategy indicator collection disabled: portfolio client unavailable", exc_info=True)
            return lambda: None

        user_id = int(getattr(request, "user_id", 0) or getattr(state, "user_id", 0) or 0)
        sink = self._indicator_frame_sink
        if callable(sink):
            flushers: list[Callable[[], None]] = []
            for strategy in strategies:
                definitions = list(getattr(strategy, "indicator_definitions", []) or [])
                definition_streams_sent: set[str] = set()

                def on_frame(
                    stream_key: str,
                    market_time_ms: int,
                    interval_ms: int,
                    frame,
                    *,
                    definitions=definitions,
                    definition_streams_sent=definition_streams_sent,
                ) -> None:
                    definition_payload = []
                    if stream_key not in definition_streams_sent:
                        definition_payload = [replace(definition, stream_key=stream_key) for definition in definitions]
                        definition_streams_sent.add(stream_key)
                    try:
                        sink(
                            session_id=session_id,
                            user_id=user_id,
                            strategy_id=int(getattr(state, "strategy_id", 0) or 0),
                            stream_key=stream_key,
                            market_time_ms=market_time_ms,
                            interval_ms=interval_ms,
                            definitions=definition_payload,
                            frame=frame,
                        )
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "strategy indicator sink failed: session=%s stream_key=%s",
                            session_id,
                            stream_key,
                            exc_info=True,
                        )

                strategy.on_indicator_frame = on_frame
            return lambda: None

        flushers: list[Callable[[], None]] = []

        def save_payload(*, definitions: list[Any] | None = None, chunks: list[Any] | None = None) -> None:
            if not definitions and not chunks:
                return
            save = getattr(portfolio_client, "save_strategy_indicators", None)
            if not callable(save):
                logger.warning("strategy indicator collection disabled: portfolio client cannot save indicators")
                return
            try:
                save(
                    session_id=session_id,
                    user_id=user_id,
                    definitions=definitions or [],
                    chunks=chunks or [],
                )
            except Exception:  # noqa: BLE001
                logger.warning(
                    "strategy indicator save failed: session=%s definitions=%d chunks=%d",
                    session_id,
                    len(definitions or []),
                    len(chunks or []),
                    exc_info=True,
                )

        for strategy in strategies:
            definitions = list(getattr(strategy, "indicator_definitions", []) or [])
            buffer = IndicatorChunkBuffer(definitions)
            definition_streams_saved: set[str] = set()

            def on_frame(
                stream_key: str,
                market_time_ms: int,
                interval_ms: int,
                frame,
                *,
                definitions=definitions,
                buffer=buffer,
                definition_streams_saved=definition_streams_saved,
            ) -> None:
                if stream_key not in definition_streams_saved:
                    save_payload(
                        definitions=[replace(definition, stream_key=stream_key) for definition in definitions],
                    )
                    definition_streams_saved.add(stream_key)
                chunks = buffer.record_bar(stream_key, market_time_ms, interval_ms, frame)
                if chunks:
                    save_payload(chunks=chunks)

            strategy.on_indicator_frame = on_frame

            def flush(*, buffer=buffer) -> None:
                chunks = buffer.flush_open()
                if chunks:
                    save_payload(chunks=chunks)

            flushers.append(flush)

        def flush_all() -> None:
            for flush in flushers:
                flush()

        return flush_all

    def _run_backtest(
        self,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
        request: Any,
        declared_inputs: list[StrategyInput],
    ) -> None:
        self._run_backtest_via_platform_proxy(
            session_id=session_id,
            state=state,
            engine=engine,
            request=request,
            declared_inputs=declared_inputs,
        )

    def _run_backtest_via_platform_proxy(
        self,
        *,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
        request: Any,
        declared_inputs: list[StrategyInput],
    ) -> None:
        start = int(getattr(request, "start_time_ms", 0) or 0)
        end = int(getattr(request, "end_time_ms", 0) or 0)
        if start == 0 or end == 0:
            state.transition("failed", error="start_time_ms and end_time_ms are required for backtest portfolios")
            return

        from strategy_service.marketdata_adapter import _adapt_kline

        required_streams = [
            StreamBinding(
                stream_id=0,
                exchange="binance",
                market=_marketdata_market(inp.market),
                kind="kline",
                symbol=inp.symbol,
                interval=inp.interval,
                canonical_market=inp.market,
            )
            for inp in declared_inputs
        ]
        canonical_by_stream = {
            _stream_key(stream.market, stream.symbol, stream.interval): (
                stream.canonical_market or stream.market
            )
            for stream in required_streams
        }
        from strategy_service.backtest_pages import BACKTEST_PAGE_SIZE, PagedBacktestDataSource

        marketdata_client = self._marketdata_client()
        fetch_backtest_page = getattr(marketdata_client, "fetch_backtest_page", None)
        if not callable(fetch_backtest_page):
            state.transition(
                "failed",
                error=(
                    "paged backtest data proxy is not configured; "
                    "FetchKlines fallback is disabled for backtest execution"
                ),
            )
            return
        n = 0
        data_source = PagedBacktestDataSource(
            marketdata_client,
            start_time_ms=start,
            end_time_ms=end,
            streams=required_streams,
        )
        flush_indicators = self._install_indicator_collection(session_id, state, engine, request)
        try:
            for kline in data_source.iter_klines():
                if state.status != "running":
                    break
                engine.running_strategy(_adapt_kline(kline, _canonical_market_for_kline(kline, canonical_by_stream)))
                n += 1
                state.bars_processed = n
                if n % BACKTEST_PAGE_SIZE == 0:
                    self._persist_session_status(session_id, state)
        finally:
            flush_indicators()

        if state.status == "running":
            state.transition("finished", bars=n)
        else:
            state.bars_processed = n
        logger.info("session %s finished via platform market-data proxy: %d bars", session_id, n)

    def _run_live(
        self,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
        declared_inputs: list[StrategyInput],
        strategy_id: int,
    ) -> None:
        del declared_inputs, strategy_id
        self._run_live_via_platform_proxy(session_id, state, engine)

    def _run_live_via_platform_proxy(
        self,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
    ) -> None:
        import threading as _threading
        from strategy_service.marketdata_adapter import _adapt_kline

        if state.environment == 1 and self._lease_management_enabled:
            if not self._renew_stream_leases_once(session_id, state):
                raise RuntimeError("failed to create required market-data leases for demo session")
            lease_stop_event = _threading.Event()
            lease_thread = _threading.Thread(
                target=self._lease_heartbeat_loop,
                args=(session_id, state, lease_stop_event),
                daemon=True,
            )
            state.set_lease_runtime(stop_event=lease_stop_event, lease_thread=lease_thread)
            lease_thread.start()

        stop_event = _threading.Event()
        state._stop_event = stop_event  # type: ignore[attr-defined]
        data_source = self._runtime_data_source
        iter_session_events = getattr(data_source, "iter_session_events", None)
        iter_live = getattr(data_source, "iter_live_klines", None)
        if not callable(iter_session_events) and not callable(iter_live):
            raise RuntimeError(
                "platform live delivery is not configured; "
                "FetchKlines fallback is disabled for demo/live execution"
            )
        acct_client = self._portfolio_client()
        canonical_by_stream = {
            _stream_key(stream.market, stream.symbol, stream.interval): (
                stream.canonical_market or stream.market
            )
            for stream in state.required_streams
        }

        if callable(iter_session_events):
            live_events = iter_session_events(
                session_id=session_id,
                required_streams=state.required_streams,
                stop_event=stop_event,
            )
        else:
            live_events = (
                SimpleNamespace(kind="kline", payload=kline)
                for kline in iter_live(
                    session_id=session_id,
                    required_streams=state.required_streams,
                    stop_event=stop_event,
                )
            )

        flush_indicators = self._install_indicator_collection(session_id, state, engine)
        try:
            for event in live_events:
                if state.status != "running" or stop_event.is_set():
                    break
                event_kind = str(getattr(event, "kind", "") or "").strip().lower()
                if event_kind == "order_update":
                    handler = getattr(engine, "handle_order_update", None)
                    if callable(handler):
                        handler(getattr(event, "payload", None))
                    continue
                kline = getattr(event, "payload", event)
                routed = engine.running_strategy(_adapt_kline(kline, _canonical_market_for_kline(kline, canonical_by_stream)))
                if routed is False:
                    self._record_unroutable_live_kline(session_id, state, kline)
                with state._lock:
                    state.bars_processed += 1
                    bars_processed = state.bars_processed
                    status = state.status
                    error = state.error
                    runtime_id = state.runtime_id
                try:
                    ok = acct_client.update_session(
                        session_id=session_id,
                        status=status,
                        bars_processed=bars_processed,
                        error=error,
                        runtime_id=runtime_id,
                    )
                    if not ok:
                        logger.warning("session %s: failed to update live progress", session_id)
                except Exception:  # noqa: BLE001
                    logger.warning("session %s: failed to update live progress", session_id, exc_info=True)
        finally:
            flush_indicators()

    def _renew_stream_leases_once(self, session_id: str, state: SessionState) -> bool:
        if state.environment != 1 or not state.required_streams:
            return True
        marketdata_client = self._marketdata_client()
        for binding in state.required_streams:
            ok = marketdata_client.create_or_renew_market_data_lease(
                session_id=session_id,
                strategy_id=state.strategy_id,
                portfolio_id=state.portfolio_id,
                stream_id=binding.stream_id,
                ttl_seconds=self._lease_ttl_seconds,
            )
            if not ok:
                logger.warning(
                    "session %s: failed to renew lease for %s",
                    session_id,
                    _stream_label(binding),
                )
                return False
        state.note_lease_heartbeat()
        return True

    def _create_session_market_data_subscriptions(
        self,
        *,
        session_id: str,
        state: SessionState,
        user_id: int,
    ) -> bool:
        if state.environment != 1:
            return True
        if not state.required_streams:
            logger.warning("session %s: no required live streams to subscribe", session_id)
            return False
        marketdata_client = self._marketdata_client()
        create = getattr(marketdata_client, "create_session_market_data_subscriptions", None)
        if not callable(create):
            logger.warning("session %s: market-data client does not support session subscriptions", session_id)
            return False
        ok = create(
            user_id=user_id,
            session_id=session_id,
            runtime_id=state.runtime_id,
            environment=state.environment,
            streams=state.required_streams,
        )
        if not ok:
            logger.warning("session %s: failed to create session market-data subscriptions", session_id)
        return bool(ok)

    def _release_session_market_data_subscriptions(self, session_id: str, state: SessionState) -> None:
        if state.environment != 1:
            return
        marketdata_client = self._marketdata_client()
        release = getattr(marketdata_client, "release_session_market_data_subscriptions", None)
        if not callable(release):
            return
        if not release(session_id=session_id, runtime_id=state.runtime_id):
            logger.warning("session %s: failed to release session market-data subscriptions", session_id)

    def _lease_heartbeat_loop(
        self,
        session_id: str,
        state: SessionState,
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(timeout=self._lease_heartbeat_seconds):
            if state.status != "running":
                return
            self._renew_stream_leases_once(session_id, state)

    def _release_stream_leases(self, session_id: str, state: SessionState) -> None:
        if not self._lease_management_enabled:
            return
        if state.environment != 1 or not state.required_streams:
            return
        marketdata_client = self._marketdata_client()
        for binding in state.required_streams:
            if not marketdata_client.release_market_data_lease(
                session_id=session_id,
                stream_id=binding.stream_id,
            ):
                logger.warning(
                    "session %s: failed to release lease for %s",
                    session_id,
                    _stream_label(binding),
                )

    def _record_unroutable_live_kline(self, session_id: str, state: SessionState, kline: Any) -> None:
        reason = (
            f"unroutable live kline: {getattr(kline, 'symbol', '?')} "
            f"{getattr(kline, 'market', '?')} {getattr(kline, 'interval', '?')}"
        )
        state.record_unroutable(reason)
        logger.warning("session %s: %s", session_id, reason)

    # ── GetStrategyStatus ────────────────────────────────────────────────────

    def GetStrategyStatus(self, request, context):
        user_id = int(request.user_id)
        if user_id <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("user_id is required")
            return pb2.GetStrategyStatusResponse()
        if not self._enforce_user_binding(user_id, context):
            return pb2.GetStrategyStatusResponse()
        if not self._enforce_request_runtime(request, context):
            return pb2.GetStrategyStatusResponse()
        state = self._sessions.get(request.session_id)
        if state is None or state.user_id != user_id:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"session {request.session_id} not found")
            return pb2.GetStrategyStatusResponse()
        if not self._enforce_session_runtime(request, state, context):
            return pb2.GetStrategyStatusResponse()

        return pb2.GetStrategyStatusResponse(
            status=state.status,
            bars_processed=state.bars_processed,
            error=state.error,
        )

    # ── StopStrategy ─────────────────────────────────────────────────────────

    def StopStrategy(self, request, context):
        user_id = int(request.user_id)
        if user_id <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("user_id is required")
            return pb2.StopStrategyResponse(stopped=False)
        if not self._enforce_user_binding(user_id, context):
            return pb2.StopStrategyResponse(stopped=False)
        if not self._enforce_request_runtime(request, context):
            return pb2.StopStrategyResponse(stopped=False)
        state = self._sessions.get(request.session_id)
        if state is None or state.user_id != user_id:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"session {request.session_id} not found")
            return pb2.StopStrategyResponse(stopped=False)
        if not self._enforce_session_runtime(request, state, context):
            return pb2.StopStrategyResponse(stopped=False)

        action = _resolve_stop_action(request)
        if action == pb2.STOP_ACTION_CANCEL:
            return pb2.StopStrategyResponse(stopped=False)
        if state.is_terminal():
            self._persist_session_status(request.session_id, state)
            self._halt_session_runtime(state, finalize=True)
            return pb2.StopStrategyResponse(stopped=True)
        if action == pb2.STOP_ACTION_FINISH:
            if not state.transition("stopping"):
                return pb2.StopStrategyResponse(stopped=False)
            self._persist_session_status(request.session_id, state)
            self._halt_session_runtime(state, finalize=False)
            state.transition("finished")
            self._persist_session_status(request.session_id, state)
            self._halt_session_runtime(state, finalize=True)
            return pb2.StopStrategyResponse(stopped=True)
        if action == pb2.STOP_ACTION_STOP_ONLY:
            if not state.transition("stopped"):
                return pb2.StopStrategyResponse(stopped=False)
            self._persist_session_status(request.session_id, state)
            self._halt_session_runtime(state, finalize=True)
            return pb2.StopStrategyResponse(stopped=True)
        if action != pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"unsupported stop action: {action}")
            return pb2.StopStrategyResponse(stopped=False)

        if not state.transition("stopping"):
            return pb2.StopStrategyResponse(stopped=False)
        self._persist_session_status(request.session_id, state)
        self._halt_session_runtime(state, finalize=False)

        ok, reason = self._stop_and_close_portfolio(request.session_id, state)
        if not ok:
            state.transition("stop_failed", error=reason)
            self._persist_session_status(request.session_id, state)
            self._halt_session_runtime(state, finalize=True)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(reason)
            return pb2.StopStrategyResponse(stopped=False)

        state.transition("stopped")
        self._persist_session_status(request.session_id, state)
        self._halt_session_runtime(state, finalize=True)
        return pb2.StopStrategyResponse(stopped=True)

    def _persist_session_status(self, session_id: str, state: SessionState) -> None:
        acct_client = self._portfolio_client()
        if not acct_client.update_session(
            session_id=session_id,
            status=state.status,
            bars_processed=state.bars_processed,
            error=state.error,
            runtime_id=state.runtime_id,
        ):
            logger.warning("session %s: failed to persist status=%s", session_id, state.status)

    @staticmethod
    def _halt_session_runtime(state: SessionState, *, finalize: bool) -> None:
        if state.lease_stop_event is not None:
            state.lease_stop_event.set()
        if finalize:
            stop_event = getattr(state, "_stop_event", None)
            if stop_event is not None:
                stop_event.set()

    def _stop_and_close_portfolio(self, session_id: str, state: SessionState) -> tuple[bool, str]:
        wallet = state.wallet
        order_client = state.order_client
        if wallet is None or order_client is None:
            return False, "stop_and_close_failed:runtime_not_available"

        started_at = time.monotonic()
        orders, reason = self._build_stop_and_close_orders(wallet, state)
        if reason:
            return False, reason

        acct_client = self._portfolio_client()
        for index, order in enumerate(orders, start=1):
            if time.monotonic() - started_at > DEFAULT_STOP_AND_CLOSE_TIMEOUT_SECONDS:
                return False, (
                    "stop_and_close_failed:timeout:"
                    f"processed={index-1}:total={len(orders)}"
                )
            decision = self._build_stop_order_decision(order)
            order_resp = order_client.place_order(
                state.portfolio_id,
                decision,
                order.mark_price,
                portfolio_symbol=order.portfolio_symbol,
                strategy_id=state.strategy_id,
                market=order.market,
                session_id=session_id,
            )
            if str(getattr(order_resp, "status", "")).upper() != "FILLED":
                return False, (
                    "stop_and_close_failed:order_rejected:"
                    f"{order.market}:{order.symbol}:{getattr(order_resp, 'status', '')}:"
                    f"{getattr(order_resp, 'error_message', '')}"
                )
            wallet.on_order(
                order.exchange,
                order.market,
                order.venue_id,
                order.portfolio_symbol,
                _marketdata_market(order.market),
                order_resp,
            )
            _sync_strategy_snapshot(
                acct_client,
                portfolio_id=state.portfolio_id,
                user_id=state.user_id,
                environment=state.environment,
                wallet=wallet,
                snapshot_reason=SNAPSHOT_REASON_EVENT,
                strategy_id=state.strategy_id,
                session_id=session_id,
            )

        flat, flat_reason = self._portfolio_is_flat(wallet, state)
        if not flat:
            return False, flat_reason
        return True, ""

    @staticmethod
    def _build_stop_order_decision(order: _StopOrder):
        from strategy_service.types import OrderDecision

        return OrderDecision(
            exchange=order.exchange,
            market=order.market,
            symbol=order.symbol,
            side=order.side,
            qty=str(order.qty),
            order_type="MARKET",
            reduce_only=True,
        )

    def _build_stop_and_close_orders(self, wallet: Any, state: SessionState) -> tuple[list[_StopOrder], str]:
        if not isinstance(wallet, PortfolioWalletRuntime):
            return [], "stop_and_close_failed:portfolio_wallet_required"

        futures_open_orders = 0
        spot_open_orders = 0
        for (exchange, market, _venue_id), route_wallet in wallet.wallets.items():
            if market in {"perpetual_futures", "delivery_futures"}:
                for open_order in (getattr(route_wallet.futures, "open_orders", {}) or {}).values():
                    symbol = getattr(open_order, "symbol", "")
                    if _target_is_allowed(state, exchange, market, symbol):
                        futures_open_orders += 1
            elif market == "spot":
                for open_order in (getattr(route_wallet.spot, "open_orders", {}) or {}).values():
                    symbol = getattr(open_order, "symbol", "")
                    if _target_is_allowed(state, exchange, market, symbol):
                        spot_open_orders += 1
        if futures_open_orders or spot_open_orders:
            return [], (
                "stop_and_close_failed:open_orders_present:"
                f"futures={futures_open_orders}:spot={spot_open_orders}"
            )

        orders: list[_StopOrder] = []
        for (exchange, market, venue_id), route_wallet in sorted(wallet.wallets.items()):
            if market in {"perpetual_futures", "delivery_futures"}:
                for pos in list(getattr(route_wallet.futures, "positions", {}).values()):
                    qty = abs(float(getattr(pos, "position_qty", 0.0) or 0.0))
                    if qty <= 1e-12:
                        continue
                    symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
                    if not symbol:
                        return [], "stop_and_close_failed:invalid_futures_symbol"
                    if not _target_is_allowed(state, exchange, market, symbol):
                        continue
                    metadata = _futures_stop_metadata(route_wallet.futures, symbol)
                    qty = _quantize_stop_futures_qty(qty, metadata)
                    if qty <= 1e-12:
                        return [], f"stop_and_close_failed:futures_qty_below_step_size:{symbol}"
                    side = "SELL" if float(getattr(pos, "position_qty", 0.0) or 0.0) > 0 else "BUY"
                    mark_price = float(
                        getattr(pos, "mark_price", 0.0)
                        or getattr(pos, "entry_price", 0.0)
                        or 0.0
                    )
                    orders.append(_StopOrder(
                        exchange=exchange,
                        venue_id=venue_id,
                        symbol=symbol,
                        portfolio_symbol=symbol,
                        market=market,
                        side=side,
                        qty=qty,
                        mark_price=mark_price,
                    ))
            elif market == "spot":
                for asset_symbol, asset in getattr(route_wallet.spot, "assets", {}).items():
                    sym = str(asset_symbol).strip().upper()
                    if sym == "USDT":
                        continue
                    if not _spot_asset_target_is_allowed(state, exchange, market, sym):
                        continue
                    qty = float(getattr(asset, "qty", 0.0) or 0.0) - float(getattr(asset, "locked", 0.0) or 0.0)
                    if qty <= 1e-12:
                        continue
                    if state.environment != 0:
                        return [], f"stop_and_close_failed:spot_liquidation_not_supported:{sym}"
                    mark_price = float(getattr(asset, "price", 0.0) or getattr(asset, "avg_entry_price", 0.0) or 0.0)
                    if mark_price <= 0.0:
                        return [], f"stop_and_close_failed:missing_spot_mark_price:{sym}"
                    orders.append(_StopOrder(
                        exchange=exchange,
                        venue_id=venue_id,
                        symbol=f"{sym}USDT",
                        portfolio_symbol=sym,
                        market=market,
                        side="SELL",
                        qty=qty,
                        mark_price=mark_price,
                    ))

        return orders, ""

    @staticmethod
    def _portfolio_is_flat(wallet: Any, state: SessionState) -> tuple[bool, str]:
        if not isinstance(wallet, PortfolioWalletRuntime):
            return False, "stop_and_close_failed:portfolio_wallet_required"
        for (exchange, market, _venue_id), route_wallet in sorted(wallet.wallets.items()):
            if market in {"perpetual_futures", "delivery_futures"}:
                for pos in getattr(route_wallet.futures, "positions", {}).values():
                    symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
                    if not _target_is_allowed(state, exchange, market, symbol):
                        continue
                    if abs(float(getattr(pos, "position_qty", 0.0) or 0.0)) > 1e-12:
                        return False, f"stop_and_close_failed:futures_not_flat:{symbol}"
                for open_order in (getattr(route_wallet.futures, "open_orders", {}) or {}).values():
                    symbol = getattr(open_order, "symbol", "")
                    if _target_is_allowed(state, exchange, market, symbol):
                        return False, "stop_and_close_failed:futures_open_orders_remaining"
            elif market == "spot":
                for asset_symbol, asset in getattr(route_wallet.spot, "assets", {}).items():
                    sym = str(asset_symbol).strip().upper()
                    if sym == "USDT":
                        continue
                    if not _spot_asset_target_is_allowed(state, exchange, market, sym):
                        continue
                    qty = float(getattr(asset, "qty", 0.0) or 0.0)
                    locked = float(getattr(asset, "locked", 0.0) or 0.0)
                    if qty > 1e-12 or locked > 1e-12:
                        if state.environment == 0:
                            return False, f"stop_and_close_failed:spot_not_flat:{sym}"
                        return False, f"stop_and_close_failed:spot_exit_unsupported:{sym}"
                for open_order in (getattr(route_wallet.spot, "open_orders", {}) or {}).values():
                    symbol = getattr(open_order, "symbol", "")
                    if _target_is_allowed(state, exchange, market, symbol):
                        return False, "stop_and_close_failed:spot_open_orders_remaining"
        return True, ""

    # ── ValidateStrategyCode ─────────────────────────────────────────────────

    def ValidateStrategyCode(self, request, context):
        user_id = int(getattr(request, "user_id", 0) or 0)
        if user_id <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("user_id is required")
            return pb2.ValidateStrategyCodeResponse(ok=False)
        if not self._enforce_user_binding(user_id, context):
            return pb2.ValidateStrategyCodeResponse(ok=False)

        result = validate_strategy_code(request.code or "")
        return pb2.ValidateStrategyCodeResponse(
            ok=result.ok,
            issues=[
                pb2.StrategyValidationIssue(
                    code=issue.code,
                    message=issue.message,
                    module=issue.module,
                    line=issue.line,
                )
                for issue in result.issues
            ],
            runtime_version=result.runtime_version,
            runtime_profile=result.runtime_profile,
            allowed_third_party_modules=result.allowed_third_party_modules,
        )

    # ── PreviewRunStrategy ───────────────────────────────────────────────────
    #
    # Same code path as RunStrategy up through preflight, but no session
    # creation. Returns a structured readiness report keyed by declared input.
    # This is what gateway / UI calls to show "will this strategy start?".

    def PreviewRunStrategy(self, request, context):
        """Dry-run preflight that mirrors ``RunStrategy`` exactly except for
        session creation. The whole startup chain runs in the same order,
        so UI readiness never drifts from actual-start behaviour:

          1. validate request args
          2. fetch portfolio snapshot → environment
          3. resolve profile + gate support
          4. build wallet (same failure modes as RunStrategy — demo
             metadata rejection, unsupported margin mode, etc.)
          5. resolve active strategy source
          6. parse declared inputs
          7. run profile-specific preflight — UNLESS
             ``market_data_policy.preflight_enabled=false`` matches the
             bypass RunStrategy also honours, in which case we report
             ``ok=true, failures=[]`` (explicit opt-out, not a silent pass)

        Every RPC error code RunStrategy returns is mirrored here so a
        caller can trust that ``PreviewRunStrategy ok=true`` implies
        ``RunStrategy`` will pass every prior gate.
        """
        user_id = int(getattr(request, "user_id", 0) or 0)
        if user_id <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("user_id is required")
            return pb2.PreviewRunStrategyResponse()
        if not self._enforce_user_binding(user_id, context):
            return pb2.PreviewRunStrategyResponse()
        if not self._enforce_request_runtime(request, context):
            return pb2.PreviewRunStrategyResponse()
        if not self._require_platform_proxy(context, "PreviewRunStrategy"):
            return pb2.PreviewRunStrategyResponse()
        portfolio_id = int(getattr(request, "portfolio_id", 0) or 0)
        if portfolio_id == 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("portfolio_id is required")
            return pb2.PreviewRunStrategyResponse()

        acct_client = self._portfolio_client()
        snapshot = _get_portfolio_snapshot(acct_client, portfolio_id, user_id)
        if snapshot is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"portfolio {portfolio_id} not found or core-service unreachable")
            return pb2.PreviewRunStrategyResponse()

        environment = _portfolio_snapshot_environment(snapshot)
        profile = resolve_profile(environment)

        # Helper to shape a PreflightResult into the proto response.
        def _shape(
            preflight_result,
            declared: list[StrategyInput] | None = None,
            order_targets: list[StrategyOrderTarget] | None = None,
            required_routes: set[tuple[str, str]] | None = None,
            risk_controls: _EffectiveRiskControls | None = None,
        ) -> pb2.PreviewRunStrategyResponse:
            effective = risk_controls or _EffectiveRiskControls(
                DEFAULT_MAX_LOSS_CLOSE_PCT,
                "platform_default",
                DEFAULT_SESSION_LEVERAGE,
                "platform_default",
            )
            failures_proto: list[pb2.PreflightFailureProto] = []
            for f in preflight_result.failures:
                kw: dict[str, Any] = {"kind": f.kind.value, "reason": f.reason}
                if f.input_key is not None:
                    m, s, i = f.input_key
                    kw["input_key"] = pb2.PreflightInputKey(
                        market=m, symbol=s, interval=i,
                    )
                failures_proto.append(pb2.PreflightFailureProto(**kw))
            required_streams_proto = [
                pb2.LiveStreamBinding(
                    stream_id=b.stream_id,
                    exchange=b.exchange,
                    market=b.market,
                    kind=b.kind,
                    symbol=b.symbol,
                    interval=b.interval,
                )
                for b in preflight_result.required_streams
            ]
            declared_inputs_proto = [
                pb2.LiveStreamBinding(
                    exchange=inp.exchange,
                    market=inp.market,
                    kind="kline",
                    symbol=inp.symbol,
                    interval=inp.interval,
                )
                for inp in (declared or [])
            ]
            declared_order_targets_proto = [
                pb2.StrategyOrderTargetBinding(
                    exchange=target.exchange,
                    market=target.market,
                    symbol=target.symbol,
                )
                for target in (order_targets or [])
            ]
            required_routes_proto = [
                pb2.StrategyRouteBinding(exchange=exchange, market=market)
                for exchange, market in sorted(required_routes or set())
            ]
            return pb2.PreviewRunStrategyResponse(
                profile=profile.value,
                supported=(profile in SUPPORTED_PROFILES),
                ok=preflight_result.ok,
                failures=failures_proto,
                required_streams=required_streams_proto,
                declared_inputs=declared_inputs_proto,
                declared_order_targets=declared_order_targets_proto,
                required_routes=required_routes_proto,
                risk_controls=pb2.RiskControls(
                    max_loss_close_pct=effective.max_loss_close_pct,
                    max_loss_close_source=effective.max_loss_close_source,
                    leverage=effective.leverage,
                    leverage_source=effective.leverage_source,
                ),
            )

        # Profile support gate — same as RunStrategy's early check.
        profile_gate = check_profile_supported(profile)
        if not profile_gate.ok:
            return _shape(profile_gate)
        if not self._require_market_data_execution_path(context, "PreviewRunStrategy", profile):
            return pb2.PreviewRunStrategyResponse()

        # Strategy source resolution (same as RunStrategy).
        strategy_id = 0
        strategy_code: str | None = None
        strategy_name = ""
        strategy_version = ""
        strategy_path = getattr(request, "strategy_path", "") or ""

        active = acct_client.get_active_strategy(portfolio_id)
        if active is not None and int(getattr(active, "strategy_id", 0) or 0) != 0:
            strategy_id = int(getattr(active, "strategy_id", 0) or 0)
            strategy_code = active.code
            strategy_name = str(getattr(active, "name", "") or "")
            strategy_version = str(getattr(active, "version", "") or "")
            strategy_path = f"<db:{strategy_name}@{strategy_version}>"
        elif not strategy_path:
            # No active strategy and no explicit strategy_path.
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("portfolio has no active strategy; mount and activate one first")
            return pb2.PreviewRunStrategyResponse()
        try:
            strategy_path, strategy_code, _strategy_hot_reload = self._debug_strategy_source_for_db_code(
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                strategy_path=strategy_path,
                strategy_code=strategy_code,
            )
        except DebugStrategySourceError as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"failed to materialize bare debug strategy source: {e}")
            return pb2.PreviewRunStrategyResponse()

        validation_error = _strategy_validation_error(strategy_code)
        if validation_error:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(validation_error)
            return pb2.PreviewRunStrategyResponse()

        try:
            declarations = extract_strategy_declarations(strategy_path, strategy_code)
        except StrategyDeclarationError as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"strategy input declaration invalid: {e}")
            return pb2.PreviewRunStrategyResponse()
        except (ImportError, AttributeError) as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"strategy could not be loaded: {e}")
            return pb2.PreviewRunStrategyResponse()
        try:
            effective_risk = _effective_risk_controls_from_request(
                declarations,
                getattr(request, "max_loss_close_pct", 0.0),
                getattr(request, "leverage", 0.0),
            )
        except StrategyDeclarationError as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"strategy risk control invalid: {e}")
            return pb2.PreviewRunStrategyResponse()
        declared_inputs = list(declarations.inputs)
        required_routes = set(declarations.required_routes)
        required_symbols = {
            (entry.exchange, entry.market, entry.symbol)
            for entry in declarations.inputs
        } | set(declarations.order_target_keys)

        portfolio_preflight = self._run_portfolio_preflight(
            acct_client=acct_client,
            portfolio_id=portfolio_id,
            user_id=user_id,
            required_routes=required_routes,
            required_symbols=required_symbols,
            strategy_id=int(getattr(active, "strategy_id", 0) or 0) if active is not None else 0,
            leverage=effective_risk.leverage,
        )
        if portfolio_preflight is not None:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(portfolio_preflight)
            return pb2.PreviewRunStrategyResponse()

        snapshot = _get_portfolio_snapshot(
            acct_client,
            portfolio_id,
            user_id,
            required_symbols=required_symbols,
        )
        if snapshot is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"portfolio {portfolio_id} not found or core-service unreachable")
            return pb2.PreviewRunStrategyResponse()

        try:
            build_portfolio_wallet_from_snapshot(
                snapshot,
                allowed_routes=required_routes,
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"failed to build wallet: {e}")
            return pb2.PreviewRunStrategyResponse()

        # Mirror RunStrategy's preflight call: readiness gating follows
        # ``market_data_policy.preflight_enabled``, but structural binding
        # resolution (stream presence) always runs — so Preview and Run agree
        # on when the start would fail for "stream missing" even under the
        # operator bypass.
        if not self._preflight_enabled:
            logger.info(
                "PreviewRunStrategy: readiness gating bypassed via "
                "market_data_policy.preflight_enabled=false; stream bindings "
                "are still resolved to match RunStrategy semantics"
            )
        # Phase D2: marketdata RPCs come from control-panel-service. Build the
        # client after proxy-only execution guards for the same reason as
        # RunStrategy.
        marketdata_client = self._marketdata_client()
        preflight = self._run_profile_preflight(
            profile=profile,
            declared_inputs=declared_inputs,
            marketdata_client=marketdata_client,
            start_ms=int(getattr(request, "start_time_ms", 0) or 0),
            end_ms=int(getattr(request, "end_time_ms", 0) or 0),
            require_readiness=self._preflight_enabled,
        )
        return _shape(
            preflight,
            declared_inputs,
            list(declarations.order_targets),
            required_routes,
            effective_risk,
        )

    def GetLiveConsumptionDiagnostics(self, request, context):
        del request, context
        sessions: list[pb2.LiveSessionDiagnostic] = []
        for session_id, state in self._sessions.list_active_live_sessions():
            sessions.append(
                pb2.LiveSessionDiagnostic(
                    session_id=session_id,
                    user_id=state.user_id,
                    portfolio_id=state.portfolio_id,
                    strategy_id=state.strategy_id,
                    portfolio_environment=state.environment,
                    status=state.status,
                    bars_processed=state.bars_processed,
                    error=state.error,
                    consumer_group=state.live_consumer_group,
                    streams=[
                        pb2.LiveStreamBinding(
                            stream_id=binding.stream_id,
                            exchange=binding.exchange,
                            market=binding.market,
                            kind=binding.kind,
                            symbol=binding.symbol,
                            interval=binding.interval,
                        )
                        for binding in state.required_streams
                    ],
                    unroutable_events=state.unroutable_events,
                    last_unroutable_at_ms=state.last_unroutable_at_ms,
                    last_unroutable_reason=state.last_unroutable_reason,
                    last_lease_heartbeat_at_ms=state.lease_heartbeat_at_ms,
                )
            )
        return pb2.GetLiveConsumptionDiagnosticsResponse(sessions=sessions)
