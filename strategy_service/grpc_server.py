"""strategy-service gRPC servicer：统一 RunStrategy 入口，按 account mode 路由数据源。"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
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

from strategy_service.account_client import AccountClient
from strategy_service.marketdata_client import MarketDataClient
from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.service import StrategyEngine
from strategy_service.session import SessionManager, SessionState, StreamBinding
from strategy_service.inputs import StrategyDeclarationError, StrategyInput
from strategy_service.preflight import (
    SUPPORTED_PROFILES,
    PreflightResult,
    RuntimeSourceProfile,
    backtest_preflight,
    check_profile_supported,
    default_backtest_availability,
    live_stream_preflight,
    _marketdata_market,
    resolve_profile,
)
from strategy_service.strategy.base import extract_strategy_declarations
from strategy_service.strategy_validator import validate_strategy_code
from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot

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
RESTORE_RUNNING_SESSIONS_RETRIES = 5
RESTORE_RUNNING_SESSIONS_RETRY_SECONDS = 1.0
PLATFORM_ACCESS_DIRECT = "direct"
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
    symbol: str
    account_symbol: str
    market: str
    side: str
    qty: float
    mark_price: float


def _periodic_sample_every_bars(request: Any) -> int:
    """Return bar-count threshold, falling back to default when not configured or <=0.

    Extension hook — NOT yet reachable from a real gRPC client.
    `reconcile_every_n_bars` is NOT currently a field on `RunStrategyRequest`
    in `proto/strategy_service.proto`, so for real traffic this always falls
    through to `DEFAULT_PERIODIC_SAMPLE_EVERY_BARS`. The authoritative source
    of truth for thresholds is `core-service`'s `reconciliation.*` config
    (see `core-service/internal/config/config.go` → `ReconciliationConfig`).
    The hook stays so a future proto extension (per-session override) can
    light up without touching this code path, and so unit tests can inject
    test doubles via SimpleNamespace.
    """
    n = getattr(request, "reconcile_every_n_bars", 0) or 0
    try:
        n = int(n)
    except (TypeError, ValueError):
        n = 0
    return n if n > 0 else DEFAULT_PERIODIC_SAMPLE_EVERY_BARS


def _periodic_sample_max_idle_seconds(request: Any) -> float:
    """Return wall-clock idle threshold, falling back to default when not configured or <=0.

    Extension hook — see ``_periodic_sample_every_bars`` for the proto-field
    gap explanation. Real clients always get `DEFAULT_PERIODIC_SAMPLE_MAX_IDLE_SECONDS`.
    """
    t = getattr(request, "reconcile_max_idle_seconds", 0) or 0
    try:
        t = float(t)
    except (TypeError, ValueError):
        t = 0.0
    return t if t > 0 else float(DEFAULT_PERIODIC_SAMPLE_MAX_IDLE_SECONDS)


# Note: wallet-derived symbol inference was intentionally removed (pre_C3 §2.1/§2.2).
# The authoritative universe is the strategy's ``INPUTS`` declaration, which is
# resolved via ``extract_strategy_declarations`` at RPC entry.


def _live_consumer_group(strategy_id: int, session_id: str) -> str:
    return f"strategy-session-{int(strategy_id)}-{session_id.strip()}"


def _stream_label(binding: StreamBinding) -> str:
    return f"{binding.symbol} {binding.market} {binding.interval}"


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
    if bool(getattr(request, "close_positions", False)):
        return pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS
    return pb2.STOP_ACTION_STOP_ONLY


def _message_has_field(message: Any, field_name: str) -> bool:
    has_field = getattr(message, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field(field_name))
        except ValueError:
            return False
    return getattr(message, field_name, None) is not None


def _portfolio_snapshot_mode(snapshot: Any) -> int:
    wallet = getattr(snapshot, "wallet", None)
    if wallet is not None and _message_has_field(snapshot, "wallet"):
        mode = int(getattr(wallet, "mode", 0) or 0)
        if mode != 0:
            return mode
    for venue in getattr(snapshot, "venues", []) or []:
        venue_wallet = getattr(venue, "wallet", None)
        if venue_wallet is None or not _message_has_field(venue, "wallet"):
            continue
        mode = int(getattr(venue_wallet, "mode", 0) or 0)
        if mode != 0:
            return mode
    return int(getattr(wallet, "mode", 0) or 0) if wallet is not None else 0


class StrategyServiceServicer(pb2_grpc.StrategyServiceServicer):

    def __init__(
        self,
        account_service_addr: str,
        order_service_addr: str,
        timescale_config: dict[str, Any],
        kafka_brokers: str,
        market_data_policy: dict[str, Any] | None = None,
        bound_user_id: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
        platform_access_mode: str = PLATFORM_ACCESS_DIRECT,
        market_data_control_panel_addr: str = "",
        restore_running_sessions: bool = True,
        platform_proxy: Any | None = None,
        notification_client: Any | None = None,
    ) -> None:
        self._account_addr = account_service_addr
        # Phase D2: market-data control plane lives in control-panel-service.
        # Empty string ⇒ MarketDataClient runs in noop mode (dev / partial env).
        self._market_data_addr = market_data_control_panel_addr
        self._order_addr = order_service_addr
        self._ts_config = timescale_config
        self._kafka_brokers = kafka_brokers
        self._market_data_policy = market_data_policy or {}
        # Phase D1 hosted runtime: when registered with control-panel,
        # the runtime is bound to one user_id at registration time. All
        # inbound strategy RPCs MUST carry that user_id; mismatch is a
        # PermissionDenied. 0 = legacy / unregistered mode (skip check).
        self._bound_user_id = int(bound_user_id or 0)
        self._runtime_id = str(runtime_id or "").strip()
        self._runtime_source = str(runtime_source or "").strip()
        self._runtime_name = str(runtime_name or "").strip()
        self._platform_access_mode = (
            PLATFORM_ACCESS_PROXY_ONLY
            if str(platform_access_mode or "").strip().lower() == PLATFORM_ACCESS_PROXY_ONLY
            else PLATFORM_ACCESS_DIRECT
        )
        self._platform_proxy = platform_proxy
        self._notification_client = notification_client
        self._runtime_data_source = None
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

    def _account_client(self):
        if self._platform_access_mode == PLATFORM_ACCESS_PROXY_ONLY:
            if self._platform_proxy is None:
                raise RuntimeError("self-hosted platform proxy client is not configured")
            return self._platform_proxy.account_client()
        return AccountClient(self._account_addr)

    def _order_client(self):
        if self._platform_access_mode == PLATFORM_ACCESS_PROXY_ONLY:
            if self._platform_proxy is None:
                raise RuntimeError("self-hosted platform proxy client is not configured")
            return self._platform_proxy.order_client()
        return OrderClient(self._order_addr)

    def _marketdata_client(self):
        if self._platform_access_mode == PLATFORM_ACCESS_PROXY_ONLY:
            if self._platform_proxy is None:
                raise RuntimeError("self-hosted platform proxy client is not configured")
            return self._platform_proxy.marketdata_client()
        return MarketDataClient(self._market_data_addr)

    def _restore_running_sessions(self) -> None:
        if not self._runtime_id:
            logger.info(
                "startup session recovery skipped: runtime_id is empty; "
                "unfiltered recovery is disabled"
            )
            return
        acct_client = self._account_client()
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
                f"from core-service at {self._account_addr}"
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

    def _list_running_sessions_for_restore(self, acct_client: AccountClient):
        strict = getattr(acct_client, "require_running_sessions", None)
        if callable(strict):
            return strict(runtime_id=self._runtime_id)
        return acct_client.list_running_sessions(runtime_id=self._runtime_id)

    def _enforce_user_binding(self, request_user_id: int, context) -> bool:
        """Phase D1 section 6.5 cross-check.

        When the runtime is registered with control-panel-service it is
        bound to a single user (the bind_user_id at registration time).
        All strategy RPCs MUST carry that user_id in their request. A
        mismatch means either (a) a caller_token issued for user A is
        being replayed against user B's runtime, or (b) the handler is
        wired wrong.

        Returns True if the call should continue, False if it was
        rejected. Caller MUST return immediately on False.
        """
        if self._bound_user_id <= 0:
            # Legacy / unregistered mode — control-plane attestation is
            # absent so cross-check has no anchor. Accept and rely on
            # the legacy direct-dial trust model.
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
                    "account already has an active session; stop or recover the existing "
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

    def _require_direct_platform_access(self, context, operation: str) -> bool:
        if self._platform_access_mode != PLATFORM_ACCESS_PROXY_ONLY:
            return True
        if self._platform_proxy is not None:
            return True
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
        context.set_details(
            f"{operation} unavailable in self-hosted proxy-only runtime: "
            "platform proxy client is not configured"
        )
        return False

    def _require_market_data_execution_path(self, context, operation: str, profile: RuntimeSourceProfile) -> bool:
        if self._platform_access_mode != PLATFORM_ACCESS_PROXY_ONLY:
            return True
        if profile in (RuntimeSourceProfile.TESTNET, RuntimeSourceProfile.LIVE):
            if callable(getattr(self._runtime_data_source, "iter_live_klines", None)):
                return True
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(
                f"{operation} unavailable in self-hosted proxy-only runtime for "
                f"profile={profile.value}: platform live delivery is not configured; "
                "FetchKlines fallback is disabled for mode=2 live execution"
            )
            return False
        if profile is RuntimeSourceProfile.BACKTEST:
            if callable(getattr(self._runtime_data_source, "iter_dataset_klines", None)):
                return True
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(
                f"{operation} unavailable in self-hosted proxy-only runtime for "
                f"profile={profile.value}: chunked dataset delivery is not configured; "
                "FetchKlines fallback is disabled for mode=0 backtest execution"
            )
            return False
        if self._platform_proxy is not None:
            try:
                client = self._platform_proxy.marketdata_client()
            except Exception as exc:  # noqa: BLE001
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    f"{operation} unavailable in self-hosted proxy-only runtime: "
                    f"market-data proxy client failed to initialize: {exc}"
                )
                return False
            if callable(getattr(client, "fetch_klines", None)):
                return True
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
        context.set_details(
            f"{operation} unavailable in self-hosted proxy-only runtime for "
            f"profile={profile.value}: account/order/control-plane proxy is wired, "
            "but strategy market-data execution still needs an approved platform "
            "data proxy; direct TimescaleDB/Kafka access is disabled"
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
        if not self._require_direct_platform_access(context, "RunStrategy"):
            return pb2.RunStrategyResponse()
        account_id = request.account_id
        if account_id == 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("account_id is required")
            return pb2.RunStrategyResponse()

        # 1. 从 core-service 获取组合快照（mode + 多 venue 钱包）
        acct_client = self._account_client()
        snapshot = acct_client.get_portfolio_snapshot(account_id, user_id)
        if snapshot is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"account {account_id} not found or core-service unreachable")
            return pb2.RunStrategyResponse()

        mode = _portfolio_snapshot_mode(snapshot)

        # 2. Resolve the runtime source profile FIRST (pre_C3 gate 2 §4).
        # This is an internal runtime-source mapping, not a strategy/account
        # compatibility signal. Unsupported profiles (today: live=mode 1)
        # fail-fast here with a structured PROFILE failure, *before* we try
        # to build a wallet or load a strategy — so the error surfaces the
        # actual reason (profile not wired up) instead of a downstream
        # wallet-registry miss or strategy-mismatch message.
        profile = resolve_profile(mode)
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
        strategy_path = request.strategy_path  # may be empty in production

        active = acct_client.get_active_strategy(account_id)
        if active is not None and active.strategy_id != 0:
            strategy_id = active.strategy_id
            strategy_code = active.code
            strategy_path = f"<db:{active.name}@{active.version}>"
        elif not strategy_path:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("account has no active strategy; mount and activate one first")
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
        declared_inputs = list(declarations.inputs)
        required_routes = set(declarations.required_routes)
        required_symbols = {
            (entry.exchange, entry.market, entry.symbol)
            for entry in declarations.inputs
        } | set(declarations.order_target_keys)

        account_preflight = self._run_account_preflight(
            acct_client=acct_client,
            account_id=account_id,
            user_id=user_id,
            required_routes=required_routes,
            required_symbols=required_symbols,
        )
        if account_preflight is not None:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(account_preflight)
            return pb2.RunStrategyResponse()

        try:
            wallet = build_portfolio_wallet_from_snapshot(
                snapshot,
                allowed_routes=required_routes,
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"failed to build wallet: {e}")
            return pb2.RunStrategyResponse()

        # The declared 3-tuple universe is threaded straight through to the
        # backtest / live runtime now — the (symbol, market) flattening was
        # removed after multi-interval support landed, because collapsing to
        # 2-tuples silently dropped declared intervals on the replay/subscribe
        # paths (see ``data_loop.BacktestDataLoop.run_declared`` +
        # ``LiveKlineSubscription.from_declared_inputs``).

        # Preflight is split into two concerns:
        #
        # 1. Stream binding (always runs for live/testnet profiles) — resolves
        #    declared inputs → ``StreamBinding`` list so lease management can
        #    identify which market-data streams this session depends on.
        #    Without these bindings the control plane has no idea the session
        #    exists and may stop the underlying stream. A missing stream is
        #    ALWAYS a startup failure even when readiness gating is disabled.
        #
        # 2. Readiness gating (optional via ``market_data_policy.preflight_enabled``)
        #    — state/delivery/freshness checks. Disabling this is an operator
        #    bypass for testing; it must NOT disable binding resolution.
        # Phase D2: market-data RPCs split off into MarketDataClient
        # (control-panel-service). Build it only after profile/path guards so
        # proxy-only runtimes cannot accidentally touch direct market-data deps.
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
            account_mode=mode,
            user_id=user_id,
            account_id=account_id,
            runtime_id=runtime_id,
            runtime_source=runtime_source,
            runtime_name=runtime_name,
        )
        if mode == 2:
            state.configure_live_runtime(
                account_id=account_id,
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
                    "failed to create required live delivery subscriptions for mode=2 session"
                )
                return pb2.RunStrategyResponse()
        else:
            state.account_id = account_id
            state.strategy_id = strategy_id

        if not self._persist_session_or_set_error(
            acct_client,
            context,
            session_id=session_id,
            account_id=account_id,
            strategy_id=strategy_id,
            mode=mode,
            interval=request.interval or "1m",
            start_time_ms=request.start_time_ms,
            end_time_ms=request.end_time_ms,
            runtime_id=runtime_id,
            runtime_source=runtime_source,
            runtime_name=runtime_name,
        ):
            if mode == 2:
                self._release_session_market_data_subscriptions(session_id, state)
            self._sessions.discard(session_id)
            return pb2.RunStrategyResponse()

        # 写 strategy_start 组合快照
        acct_client.update_portfolio_snapshot(
            account_id=account_id,
            user_id=user_id,
            snapshot_reason=SNAPSHOT_REASON_STRATEGY_START,
            strategy_id=strategy_id,
            session_id=session_id,
        )

        # 5. 启动后台线程
        otel_parent_context = _capture_otel_context()

        def _run_session_with_context() -> None:
            _run_in_otel_context(
                otel_parent_context,
                f"StrategySession/{session_id}",
                lambda: self._run_session(
                    session_id, state, request, wallet, mode, account_id, user_id,
                    declared_inputs, strategy_path, strategy_id, strategy_code,
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
        mode: int,
        account_id: int,
        user_id: int,
        declared_inputs: list[StrategyInput],
        strategy_path: str,
        strategy_id: int,
        strategy_code: str | None,
    ) -> None:
        try:
            order_client = self._order_client()
            engine = StrategyEngine()

            user_strategy = engine.create_strategy(
                user_id=f"user:{user_id}:session:{session_id}",
                strategy_path=strategy_path,
                wallet=wallet,
                order_client=order_client,
                account_id=account_id,
                strategy_id=strategy_id,
                session_id=session_id,
                strategy_code=strategy_code,
                notifier=StrategyNotifier(self._notification_client),
            )

            # 注册 on_order 回调：同步钱包到 core-service（带 session_id 审计追溯）
            acct_client = self._account_client()

            def _on_order_sync() -> None:
                acct_client.update_portfolio_snapshot(
                    account_id=account_id,
                    user_id=user_id,
                    snapshot_reason=SNAPSHOT_REASON_EVENT,
                    strategy_id=strategy_id,
                    session_id=session_id,
                )

            user_strategy.on_order_callback = _on_order_sync
            state.configure_stop_runtime(wallet=wallet, order_client=order_client)

            # Phase C PeriodicSample hybrid trigger — only for mode=2 testnet.
            # Wrapping the engine keeps this out of BaseStrategy. mode=0 has no
            # external oracle and mode=1 remains fail-closed in Phase C.
            if mode == 2:
                self._install_periodic_sample_trigger(
                    engine=engine,
                    account_id=account_id,
                    user_id=user_id,
                    strategy_id=strategy_id,
                    session_id=session_id,
                    account_client=acct_client,
                    every_n_bars=_periodic_sample_every_bars(request),
                    max_idle_seconds=_periodic_sample_max_idle_seconds(request),
                )

            if mode == 0:
                self._run_backtest(session_id, state, engine, request, declared_inputs)
            elif mode in (1, 2):
                # mode=1 is Phase C fail-closed via the profile gate above, so
                # we never actually reach here. Explicit allowlist instead of `else`
                # prevents new/unknown modes from silently borrowing the
                # live path as defense-in-depth.
                self._run_live(session_id, state, engine, declared_inputs, strategy_id)
            else:
                raise ValueError(f"unsupported account mode: {mode}")

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
            try:
                acct_client = self._account_client()
                acct_client.update_portfolio_snapshot(
                    account_id=account_id,
                    user_id=user_id,
                    snapshot_reason=SNAPSHOT_REASON_STRATEGY_END,
                    strategy_id=strategy_id,
                    session_id=session_id,
                )
                acct_client.update_session(
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
    def _run_account_preflight(
        *,
        acct_client: Any,
        account_id: int,
        user_id: int,
        required_routes: set[tuple[str, str]],
        required_symbols: set[tuple[str, str, str]],
    ) -> str | None:
        preflight = getattr(acct_client, "preflight_strategy_session", None)
        if not callable(preflight):
            return "account preflight unavailable: client does not support PreflightStrategySession"
        resp = preflight(
            account_id=account_id,
            user_id=user_id,
            required_routes=sorted(required_routes),
            required_symbols=sorted(required_symbols),
        )
        if resp is None:
            return "account preflight unavailable: core-service did not return a result"
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
            return "account preflight failed: " + "; ".join(issue_messages)
        return "account preflight failed"

    def _run_profile_preflight(
        self,
        *,
        profile: RuntimeSourceProfile,
        declared_inputs: list[StrategyInput],
        marketdata_client: MarketDataClient,
        start_ms: int,
        end_ms: int,
        require_readiness: bool = True,
    ) -> PreflightResult:
        """Dispatch to the profile-specific preflight evaluator.

        Backtest profile → historical-data availability for each declared input.
        Live / testnet profile → stream binding for each declared input, with
        optional readiness gating. ``require_readiness=False`` disables the
        state/delivery/freshness checks but still resolves bindings — this is
        essential for mode=2 lease management when ``preflight_enabled=False``.
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
            if self._platform_access_mode == PLATFORM_ACCESS_PROXY_ONLY:
                return backtest_preflight(
                    declared_inputs,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    availability_fn=self._proxy_backtest_availability(marketdata_client),
                )
            return backtest_preflight(
                declared_inputs,
                start_ms=start_ms,
                end_ms=end_ms,
                availability_fn=default_backtest_availability(self._ts_config),
            )
        if profile in (RuntimeSourceProfile.TESTNET, RuntimeSourceProfile.LIVE):
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
    def _live_stream_lookup(marketdata_client: MarketDataClient):
        """Return a callable that looks up control-plane stream status by key.

        Kept as a separate method so tests can patch it cleanly when they
        want to assert per-declared-input lookups without spinning up a
        ``MarketDataClient``. (Phase D2 moved this RPC from core-service
        into control-panel-service; the per-declared-input lookup contract
        is unchanged.)
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
        account_id: int,
        user_id: int,
        strategy_id: int,
        session_id: str,
        account_client: AccountClient,
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
                account_client.update_portfolio_snapshot(
                    account_id=account_id,
                    user_id=user_id,
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

    def _run_backtest(
        self,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
        request: Any,
        declared_inputs: list[StrategyInput],
    ) -> None:
        if self._platform_access_mode == PLATFORM_ACCESS_PROXY_ONLY:
            self._run_backtest_via_platform_proxy(
                session_id=session_id,
                state=state,
                engine=engine,
                request=request,
                declared_inputs=declared_inputs,
            )
            return

        from market_data.config import TimescaleConfig
        from strategy_service.data_loop import BacktestDataLoop

        start = request.start_time_ms
        end = request.end_time_ms

        if start == 0 or end == 0:
            state.transition("failed", error="start_time_ms and end_time_ms are required for backtest accounts")
            return

        ts_config = TimescaleConfig.from_dict(self._ts_config)
        loop = BacktestDataLoop(service=engine, config=ts_config)

        # Multi-input replay — each declared (market, symbol, interval)
        # gets its own iterator so distinct intervals (e.g. BTCUSDT 1m + 5m)
        # both reach the strategy.
        n = loop.run_declared(
            declared_inputs,
            start,
            end,
            should_stop=lambda: state.status != "running",
        )
        if state.status == "running":
            state.transition("finished", bars=n)
        else:
            state.bars_processed = n
        logger.info("session %s finished: %d bars", session_id, n)

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
            state.transition("failed", error="start_time_ms and end_time_ms are required for backtest accounts")
            return

        from strategy_service.data_loop import _adapt_kline

        required_streams = [
            StreamBinding(
                stream_id=0,
                exchange="binance",
                market=_marketdata_market(inp.market),
                kind="kline",
                symbol=inp.symbol,
                interval=inp.interval,
            )
            for inp in declared_inputs
        ]
        data_source = self._runtime_data_source
        iter_dataset = getattr(data_source, "iter_dataset_klines", None)
        if not callable(iter_dataset):
            state.transition(
                "failed",
                error=(
                    "chunked dataset delivery is not configured; "
                    "FetchKlines fallback is disabled for mode=0 backtest execution"
                ),
            )
            return
        marketdata_client = self._marketdata_client()
        deliver_dataset = getattr(marketdata_client, "deliver_dataset_klines", None)
        if not callable(deliver_dataset):
            state.transition(
                "failed",
                error=(
                    "chunked dataset delivery is not configured; "
                    "platform dataset delivery request is unavailable"
                ),
            )
            return
        if not deliver_dataset(
            session_id=session_id,
            runtime_id=state.runtime_id,
            start_time_ms=start,
            end_time_ms=end,
            streams=required_streams,
        ):
            state.transition(
                "failed",
                error="failed to request platform dataset delivery for mode=0 backtest execution",
            )
            return
        stop_event = threading.Event()
        n = 0
        for kline in iter_dataset(
            session_id=session_id,
            required_streams=required_streams,
            stop_event=stop_event,
        ):
            if state.status != "running" or stop_event.is_set():
                break
            engine.running_strategy(_adapt_kline(kline, getattr(kline, "market", None)))
            n += 1

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
        if self._platform_access_mode == PLATFORM_ACCESS_PROXY_ONLY:
            self._run_live_via_platform_proxy(session_id, state, engine)
            return

        unsupported_exchanges = sorted({inp.exchange for inp in declared_inputs if inp.exchange != "binance"})
        if unsupported_exchanges:
            state.transition(
                "failed",
                error=(
                    "unsupported live market-data exchange(s): "
                    + ", ".join(unsupported_exchanges)
                ),
            )
            return

        import threading as _threading
        from market_data.config import KafkaBrokerConfig, KafkaConfig, LiveKlineSubscription
        from strategy_service.data_loop import LiveDataLoop

        # KAFKA_BROKERS 环境变量是逗号分隔的 "host:port" 字符串，
        # 这里先只负责解析 brokers；topic family / filtering 语义交给 strategy-library.
        broker_list = []
        for entry in str(self._kafka_brokers).split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                host, port_str = entry.rsplit(":", 1)
                try:
                    port = int(port_str)
                except ValueError:
                    host, port = entry, 9092
            else:
                host, port = entry, 9092
            broker_list.append(KafkaBrokerConfig(host=host, port=port))
        consumer_group = state.live_consumer_group or _live_consumer_group(strategy_id, session_id)
        # Multi-interval subscription — one Kafka topic per distinct
        # (market, interval) pair declared by the strategy. Ensures a
        # BTCUSDT 1m + 5m strategy consumes both topics, not just one.
        marketdata_inputs = [
            SimpleNamespace(
                exchange=inp.exchange,
                market=_marketdata_market(inp.market),
                symbol=inp.symbol,
                interval=inp.interval,
            )
            for inp in declared_inputs
        ]
        subscription = LiveKlineSubscription.from_declared_inputs(
            marketdata_inputs,
            consumer_group=consumer_group,
            exchange="binance",
        )
        kafka_cfg = KafkaConfig.for_live_kline_subscription(
            subscription,
            brokers=broker_list,
        )
        if state.account_mode == 2 and self._lease_management_enabled:
            if not self._renew_stream_leases_once(session_id, state):
                raise RuntimeError("failed to create required market-data leases for mode=2 session")
            lease_stop_event = _threading.Event()
            lease_thread = _threading.Thread(
                target=self._lease_heartbeat_loop,
                args=(session_id, state, lease_stop_event),
                daemon=True,
            )
            state.set_lease_runtime(stop_event=lease_stop_event, lease_thread=lease_thread)
            lease_thread.start()
        live_loop = LiveDataLoop(
            service=engine,
            config=kafka_cfg,
            on_unroutable=lambda kline: self._record_unroutable_live_kline(session_id, state, kline),
        )
        self._sessions.set_live_loop(session_id, live_loop)

        stop_event = _threading.Event()
        state._stop_event = stop_event  # type: ignore[attr-defined]
        live_loop.start()
        # 每 60 秒唤醒一次检查 session 是否仍为 running（防止孤儿 hang）
        while not stop_event.wait(timeout=60):
            if state.status != "running":
                break

    def _run_live_via_platform_proxy(
        self,
        session_id: str,
        state: SessionState,
        engine: StrategyEngine,
    ) -> None:
        import threading as _threading
        from strategy_service.data_loop import _adapt_kline

        if state.account_mode == 2 and self._lease_management_enabled:
            if not self._renew_stream_leases_once(session_id, state):
                raise RuntimeError("failed to create required market-data leases for mode=2 session")
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
        iter_live = getattr(data_source, "iter_live_klines", None)
        if not callable(iter_live):
            raise RuntimeError(
                "platform live delivery is not configured; "
                "FetchKlines fallback is disabled for mode=2 live execution"
            )
        acct_client = self._account_client()

        for kline in iter_live(
            session_id=session_id,
            required_streams=state.required_streams,
            stop_event=stop_event,
        ):
            if state.status != "running" or stop_event.is_set():
                break
            routed = engine.running_strategy(_adapt_kline(kline, getattr(kline, "market", None)))
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

    def _renew_stream_leases_once(self, session_id: str, state: SessionState) -> bool:
        if state.account_mode != 2 or not state.required_streams:
            return True
        marketdata_client = self._marketdata_client()
        for binding in state.required_streams:
            ok = marketdata_client.create_or_renew_market_data_lease(
                session_id=session_id,
                strategy_id=state.strategy_id,
                account_id=state.account_id,
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
        if state.account_mode != 2:
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
            mode=state.account_mode,
            streams=state.required_streams,
        )
        if not ok:
            logger.warning("session %s: failed to create session market-data subscriptions", session_id)
        return bool(ok)

    def _release_session_market_data_subscriptions(self, session_id: str, state: SessionState) -> None:
        if state.account_mode != 2:
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
        if state.account_mode != 2 or not state.required_streams:
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

        ok, reason = self._stop_and_close_account(request.session_id, state)
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
        acct_client = self._account_client()
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
        if state.live_loop is not None:
            state.live_loop.stop()
        if state.lease_stop_event is not None:
            state.lease_stop_event.set()
        if finalize:
            stop_event = getattr(state, "_stop_event", None)
            if stop_event is not None:
                stop_event.set()

    def _stop_and_close_account(self, session_id: str, state: SessionState) -> tuple[bool, str]:
        wallet = state.wallet
        order_client = state.order_client
        if wallet is None or order_client is None:
            return False, "stop_and_close_failed:runtime_not_available"

        started_at = time.monotonic()
        orders, reason = self._build_stop_and_close_orders(wallet, state)
        if reason:
            return False, reason

        acct_client = self._account_client()
        for index, order in enumerate(orders, start=1):
            if time.monotonic() - started_at > DEFAULT_STOP_AND_CLOSE_TIMEOUT_SECONDS:
                return False, (
                    "stop_and_close_failed:timeout:"
                    f"processed={index-1}:total={len(orders)}"
                )
            decision = self._build_stop_order_decision(order)
            order_resp = order_client.place_order(
                state.account_id,
                decision,
                order.mark_price,
                account_symbol=order.account_symbol,
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
            wallet.on_order(order.account_symbol, order.market, order_resp)
            acct_client.update_portfolio_snapshot(
                account_id=state.account_id,
                user_id=state.user_id,
                snapshot_reason=SNAPSHOT_REASON_EVENT,
                strategy_id=state.strategy_id,
                session_id=session_id,
            )

        flat, flat_reason = self._account_is_flat(wallet, state)
        if not flat:
            return False, flat_reason
        return True, ""

    @staticmethod
    def _build_stop_order_decision(order: _StopOrder):
        from strategy_service.types import OrderDecision

        return OrderDecision(
            exchange="binance",
            market=order.market,
            symbol=order.symbol,
            side=order.side,
            qty=str(order.qty),
            order_type="MARKET",
        )

    def _build_stop_and_close_orders(self, wallet: Any, state: SessionState) -> tuple[list[_StopOrder], str]:
        futures_open_orders = len(getattr(wallet.futures, "open_orders", {}) or {})
        spot_open_orders = len(getattr(wallet.spot, "open_orders", {}) or {})
        if futures_open_orders or spot_open_orders:
            return [], (
                "stop_and_close_failed:open_orders_present:"
                f"futures={futures_open_orders}:spot={spot_open_orders}"
            )

        orders: list[_StopOrder] = []
        for pos in list(getattr(wallet.futures, "positions", {}).values()):
            qty = abs(float(getattr(pos, "position_qty", 0.0) or 0.0))
            if qty <= 1e-12:
                continue
            symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
            if not symbol:
                return [], "stop_and_close_failed:invalid_futures_symbol"
            side = "SHORT" if float(getattr(pos, "position_qty", 0.0) or 0.0) > 0 else "LONG"
            mark_price = float(
                getattr(pos, "mark_price", 0.0)
                or getattr(pos, "entry_price", 0.0)
                or 0.0
            )
            orders.append(_StopOrder(
                symbol=symbol,
                account_symbol=symbol,
                market="futures",
                side=side,
                qty=qty,
                mark_price=mark_price,
            ))

        for asset_symbol, asset in getattr(wallet.spot, "assets", {}).items():
            sym = str(asset_symbol).strip().upper()
            if sym == "USDT":
                continue
            qty = float(getattr(asset, "qty", 0.0) or 0.0) - float(getattr(asset, "locked", 0.0) or 0.0)
            if qty <= 1e-12:
                continue
            if state.account_mode != 0:
                return [], f"stop_and_close_failed:spot_liquidation_not_supported:{sym}"
            mark_price = float(getattr(asset, "price", 0.0) or getattr(asset, "avg_entry_price", 0.0) or 0.0)
            if mark_price <= 0.0:
                return [], f"stop_and_close_failed:missing_spot_mark_price:{sym}"
            orders.append(_StopOrder(
                symbol=f"{sym}USDT",
                account_symbol=sym,
                market="spot",
                side="SELL",
                qty=qty,
                mark_price=mark_price,
            ))

        return orders, ""

    @staticmethod
    def _account_is_flat(wallet: Any, state: SessionState) -> tuple[bool, str]:
        for pos in getattr(wallet.futures, "positions", {}).values():
            if abs(float(getattr(pos, "position_qty", 0.0) or 0.0)) > 1e-12:
                return False, f"stop_and_close_failed:futures_not_flat:{getattr(pos, 'symbol', '')}"
        for asset_symbol, asset in getattr(wallet.spot, "assets", {}).items():
            sym = str(asset_symbol).strip().upper()
            if sym == "USDT":
                continue
            qty = float(getattr(asset, "qty", 0.0) or 0.0)
            locked = float(getattr(asset, "locked", 0.0) or 0.0)
            if qty > 1e-12 or locked > 1e-12:
                if state.account_mode == 0:
                    return False, f"stop_and_close_failed:spot_not_flat:{sym}"
                return False, f"stop_and_close_failed:spot_exit_unsupported:{sym}"
        if getattr(wallet.futures, "open_orders", {}):
            return False, "stop_and_close_failed:futures_open_orders_remaining"
        if getattr(wallet.spot, "open_orders", {}):
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
          2. fetch portfolio snapshot → mode
          3. resolve profile + gate support
          4. build wallet (same failure modes as RunStrategy — mode=2
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
        if not self._require_direct_platform_access(context, "PreviewRunStrategy"):
            return pb2.PreviewRunStrategyResponse()
        account_id = int(getattr(request, "account_id", 0) or 0)
        if account_id == 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("account_id is required")
            return pb2.PreviewRunStrategyResponse()

        acct_client = self._account_client()
        snapshot = acct_client.get_portfolio_snapshot(account_id, user_id)
        if snapshot is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"account {account_id} not found or core-service unreachable")
            return pb2.PreviewRunStrategyResponse()

        mode = _portfolio_snapshot_mode(snapshot)
        profile = resolve_profile(mode)

        # Helper to shape a PreflightResult into the proto response.
        def _shape(
            preflight_result,
            declared: list[StrategyInput] | None = None,
        ) -> pb2.PreviewRunStrategyResponse:
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
                    exchange="binance",
                    market=_marketdata_market(inp.market),
                    kind="kline",
                    symbol=inp.symbol,
                    interval=inp.interval,
                )
                for inp in (declared or [])
            ]
            return pb2.PreviewRunStrategyResponse(
                profile=profile.value,
                supported=(profile in SUPPORTED_PROFILES),
                ok=preflight_result.ok,
                failures=failures_proto,
                required_streams=required_streams_proto,
                declared_inputs=declared_inputs_proto,
            )

        # Profile support gate — same as RunStrategy's early check.
        profile_gate = check_profile_supported(profile)
        if not profile_gate.ok:
            return _shape(profile_gate)
        if not self._require_market_data_execution_path(context, "PreviewRunStrategy", profile):
            return pb2.PreviewRunStrategyResponse()

        # Strategy source resolution (same as RunStrategy).
        strategy_code: str | None = None
        strategy_path = getattr(request, "strategy_path", "") or ""

        active = acct_client.get_active_strategy(account_id)
        if active is not None and int(getattr(active, "strategy_id", 0) or 0) != 0:
            strategy_code = active.code
            strategy_path = f"<db:{active.name}@{active.version}>"
        elif not strategy_path:
            # No active strategy and no explicit strategy_path.
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("account has no active strategy; mount and activate one first")
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
        declared_inputs = list(declarations.inputs)
        required_routes = set(declarations.required_routes)
        required_symbols = {
            (entry.exchange, entry.market, entry.symbol)
            for entry in declarations.inputs
        } | set(declarations.order_target_keys)

        account_preflight = self._run_account_preflight(
            acct_client=acct_client,
            account_id=account_id,
            user_id=user_id,
            required_routes=required_routes,
            required_symbols=required_symbols,
        )
        if account_preflight is not None:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(account_preflight)
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
        return _shape(preflight, declared_inputs)

    def GetLiveConsumptionDiagnostics(self, request, context):
        del request, context
        sessions: list[pb2.LiveSessionDiagnostic] = []
        for session_id, state in self._sessions.list_active_live_sessions():
            sessions.append(
                pb2.LiveSessionDiagnostic(
                    session_id=session_id,
                    user_id=state.user_id,
                    account_id=state.account_id,
                    strategy_id=state.strategy_id,
                    account_mode=state.account_mode,
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
