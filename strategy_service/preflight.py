"""Runtime source profile resolver + profile-specific startup preflight.

pre_C3 gate 2: every strategy startup MUST resolve a runtime source profile
from the account context, then evaluate a profile-specific preflight whose
authoritative input set is the strategy's declared ``(market, symbol, interval)``
universe — wallet positions / spot assets / account-balance assets MUST NOT
extend that set.

This module is the single source of truth for:

- ``RuntimeSourceProfile`` — conceptual runtime source (backtest / live /
  testnet). This is **not** a strategy-vs-account compatibility condition;
  it only decides which data sources and which preflight to run.
- ``SUPPORTED_PROFILES`` — profiles currently wired up in strategy-service.
  Mirrors the ``wallet_factory.RUNTIME_REGISTRY`` allowlist. LIVE is
  intentionally excluded until Phase C+ (fail-closed).
- ``PreflightFailure`` / ``PreflightResult`` — typed failure surface the
  RPC boundary converts into ``FAILED_PRECONDITION`` details.
- ``backtest_preflight`` — per-declared-input historical-data availability
  check for the backtest profile. Does NOT query stream readiness.
- ``live_stream_preflight`` — per-declared-input stream readiness check for
  the live/testnet profile. Uses each declared input's own interval, not
  a single request-level interval.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable

from strategy_service.inputs import StrategyInput
from strategy_service.session import StreamBinding

logger = logging.getLogger(__name__)


# ── Runtime source profile ──────────────────────────────────────────────────


class RuntimeSourceProfile(Enum):
    """Conceptual source of truth for a strategy session's data/order/wallet wiring.

    - ``BACKTEST`` — historical TimescaleDB replay; wallet mutates locally
    - ``LIVE`` — real Binance (or other exchange) live data + real order flow
    - ``TESTNET`` — testnet data + testnet order flow (exchange-parity wallet)
    - ``UNKNOWN`` — unrecognised ``account.mode`` (guardrail)
    """

    BACKTEST = "backtest"
    LIVE = "live"
    TESTNET = "testnet"
    UNKNOWN = "unknown"


# Profiles that have a wired runtime in strategy-service today. Mirror
# ``strategy_service.wallet_factory.RUNTIME_REGISTRY``: both ``BACKTEST`` and
# ``TESTNET`` route through ``BinanceWalletRuntime``; ``LIVE`` stays excluded
# until Phase C+ (fail-closed, no silent fallback to testnet).
SUPPORTED_PROFILES: frozenset[RuntimeSourceProfile] = frozenset({
    RuntimeSourceProfile.BACKTEST,
    RuntimeSourceProfile.TESTNET,
})


def resolve_profile(mode: int) -> RuntimeSourceProfile:
    """Map the numeric ``account.mode`` to a runtime source profile.

    This is a pure translation — it does NOT decide whether the profile is
    supported. Use ``SUPPORTED_PROFILES`` for that check.
    """
    if mode == 0:
        return RuntimeSourceProfile.BACKTEST
    if mode == 1:
        return RuntimeSourceProfile.LIVE
    if mode == 2:
        return RuntimeSourceProfile.TESTNET
    return RuntimeSourceProfile.UNKNOWN


# ── Preflight result model ─────────────────────────────────────────────────


class PreflightFailureKind(Enum):
    """Typed reason a preflight evaluator rejected a startup attempt."""

    DECLARATION = "declaration"       # (reserved — RunStrategy surfaces declaration errors directly)
    PROFILE = "profile"               # unsupported runtime profile (e.g. live mode=1 today)
    INVALID_REQUEST = "invalid_request"  # backtest missing time range etc.
    HISTORICAL_DATA = "historical_data"  # backtest: no usable rows in requested range
    STREAM = "stream"                 # live/testnet: stream missing / not running / stale / delivery off


@dataclass(frozen=True)
class PreflightFailure:
    """One reason the startup preflight rejected a session.

    ``input_key`` is the declared ``(market, symbol, interval)`` 3-tuple when
    the failure is per-input (historical data / stream readiness), or ``None``
    when the failure is profile-level (unsupported profile, invalid request).
    """

    kind: PreflightFailureKind
    reason: str
    input_key: tuple[str, str, str] | None = None

    def format(self) -> str:
        """One-line human-readable rendering, safe to put in an RPC details string."""
        if self.input_key is None:
            return f"[{self.kind.value}] {self.reason}"
        market, symbol, interval = self.input_key
        return f"[{self.kind.value}] {symbol} {market} {interval}: {self.reason}"


@dataclass
class PreflightResult:
    """Outcome of a profile-specific preflight evaluation.

    ``required_streams`` is populated by the live/testnet evaluator so the
    caller can set up Kafka subscriptions + market-data leases on success;
    backtest preflights leave it empty.
    """

    profile: RuntimeSourceProfile
    failures: list[PreflightFailure] = field(default_factory=list)
    required_streams: list[StreamBinding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures

    def error_message(self) -> str:
        """Combine failures into a single RPC details string."""
        if not self.failures:
            return ""
        header = f"{self.profile.value} profile preflight failed"
        return header + ": " + "; ".join(f.format() for f in self.failures)


# ── Profile gate ──────────────────────────────────────────────────────────


def check_profile_supported(profile: RuntimeSourceProfile) -> PreflightResult:
    """Return an ``ok`` result for supported profiles, or a PROFILE failure otherwise.

    Callers typically short-circuit on this before dispatching to backtest or
    live preflight: an unsupported profile must NOT fall back to a neighbouring
    preflight (e.g. live must not silently run against testnet data).
    """
    result = PreflightResult(profile=profile)
    if profile in SUPPORTED_PROFILES:
        return result
    if profile is RuntimeSourceProfile.UNKNOWN:
        reason = "account mode does not map to a known runtime source profile"
    else:
        reason = (
            f"runtime source profile {profile.value!r} is not yet wired up in "
            f"strategy-service; see wallet_factory.RUNTIME_REGISTRY"
        )
    result.failures.append(
        PreflightFailure(kind=PreflightFailureKind.PROFILE, reason=reason)
    )
    return result


# ── Backtest profile: historical-data availability ─────────────────────────


# Callable signature used by ``backtest_preflight`` to ask whether a declared
# ``(market, symbol, interval)`` key has at least one row in ``[start_ms, end_ms]``.
# Tests inject a fake; production passes ``default_backtest_availability``.
BacktestAvailabilityFn = Callable[[StrategyInput, int, int], bool]


def _marketdata_market(market: str) -> str:
    market_key = str(market or "").strip().lower()
    if market_key == "perpetual_futures":
        return "futures"
    return market_key


def _input_key(inp: StrategyInput) -> tuple[str, str, str]:
    return (_marketdata_market(inp.market), inp.symbol, inp.interval)


def _is_time_range_valid(start_ms: int, end_ms: int) -> tuple[bool, str]:
    if start_ms <= 0 or end_ms <= 0:
        return False, "start_time_ms and end_time_ms must be positive"
    if end_ms < start_ms:
        return False, "end_time_ms must be >= start_time_ms"
    return True, ""


def backtest_preflight(
    declared: Iterable[StrategyInput],
    start_ms: int,
    end_ms: int,
    availability_fn: BacktestAvailabilityFn,
) -> PreflightResult:
    """Check historical-data availability for every declared input.

    The ``availability_fn`` is the injection point: production wires it to a
    TimescaleDB ``EXISTS`` query, tests wire it to a deterministic dict.

    Returns a result with one ``HISTORICAL_DATA`` failure per declared input
    that has no usable data, or one ``INVALID_REQUEST`` failure if the
    ``[start_ms, end_ms]`` range is invalid. This evaluator MUST NOT consult
    stream readiness or live-delivery state.
    """
    result = PreflightResult(profile=RuntimeSourceProfile.BACKTEST)
    ok, reason = _is_time_range_valid(start_ms, end_ms)
    if not ok:
        result.failures.append(
            PreflightFailure(kind=PreflightFailureKind.INVALID_REQUEST, reason=reason)
        )
        return result

    for inp in declared:
        try:
            has_data = bool(availability_fn(inp, start_ms, end_ms))
        except Exception as e:  # noqa: BLE001 — surface as preflight failure, never raise
            logger.warning(
                "backtest availability check raised for %s: %s",
                inp.key, e, exc_info=True,
            )
            has_data = False
            err_reason = f"historical-data lookup failed: {e}"
        else:
            err_reason = "no historical kline data in requested range"
        if not has_data:
            result.failures.append(
                PreflightFailure(
                    kind=PreflightFailureKind.HISTORICAL_DATA,
                    reason=err_reason,
                    input_key=_input_key(inp),
                )
            )
    return result


def default_backtest_availability(
    ts_config: Any,
) -> BacktestAvailabilityFn:
    """Return a production availability function backed by TimescaleDB.

    ``ts_config`` accepts both raw ``dict`` (strategy-service stores its
    TimescaleDB config that way) and a pre-built ``TimescaleConfig``. The
    conversion happens once per evaluator build, not once per declared
    input, so constructing the evaluator is cheap even when called per-RPC.

    Imports ``BacktestDataSource`` + ``TimescaleConfig`` lazily — unit
    tests that never touch backtest preflight don't pull psycopg2 /
    market_data into scope.
    """
    from market_data.backtest import BacktestDataSource
    from market_data.config import TimescaleConfig

    if isinstance(ts_config, TimescaleConfig):
        resolved = ts_config
    elif isinstance(ts_config, dict):
        resolved = TimescaleConfig.from_dict(ts_config)
    elif ts_config is None:
        resolved = TimescaleConfig()
    else:
        # Something unexpected (already-typed custom object?) — let
        # BacktestDataSource raise its own clear error rather than swallow.
        resolved = ts_config

    def _check(inp: StrategyInput, start_ms: int, end_ms: int) -> bool:
        with BacktestDataSource(resolved) as ds:
            return bool(ds.has_kline_coverage(
                inp.symbol, inp.interval, start_ms, end_ms, market=_marketdata_market(inp.market),
            ))

    return _check


# ── Live / testnet profile: stream readiness per declared input ────────────


def _interval_seconds(interval: str) -> int:
    raw = str(interval or "1m").strip()
    if not raw:
        return 60
    unit = raw[-1]
    try:
        value = int(raw[:-1])
    except ValueError:
        return 60
    if value <= 0:
        return 60
    multipliers = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 7 * 86400,
        "M": 30 * 86400,
    }
    return value * multipliers.get(unit, 60)


def _stream_to_binding(stream_proto: Any) -> StreamBinding:
    key = stream_proto.key
    return StreamBinding(
        stream_id=int(stream_proto.stream_id),
        exchange=str(key.exchange or "").strip().lower(),
        market=str(key.market or "").strip().lower(),
        kind=str(key.kind or "kline").strip().lower(),
        symbol=str(key.symbol or "").strip().upper(),
        interval=str(key.interval or "").strip() or "1m",
    )


def live_stream_preflight(
    declared: Iterable[StrategyInput],
    *,
    profile: RuntimeSourceProfile,
    lookup_stream: Callable[[str, str, str], Any | None],
    freshness_grace_seconds: int,
    now_ms: Callable[[], int] | None = None,
    exchange: str = "binance",
    require_readiness: bool = True,
) -> PreflightResult:
    """Per-declared-input stream discovery + optional readiness gating.

    ``lookup_stream(market, symbol, interval)`` returns the control-plane
    stream proto or ``None`` if it doesn't exist / the lookup failed. The
    evaluator MUST use each declared input's own interval — it must not
    collapse the declared universe to a single ``request.interval``.

    **Two independent concerns:**

    1. **Stream binding** — every declared input needs a ``stream_id`` so
       downstream code can create market-data leases against the control
       plane. A missing stream (``None`` lookup) is ALWAYS a structural
       failure regardless of ``require_readiness``: we can't bind without it.

    2. **Readiness gating** (only when ``require_readiness=True``) — state,
       delivery flag, freshness. When ``require_readiness=False`` these
       checks are skipped and the binding is still emitted; this is how
       ``market_data_policy.preflight_enabled=false`` disables readiness
       without breaking lease management on mode=2.

    For ``require_readiness=True`` each declared input must additionally pass:
    - ``actual_state == "running"``
    - ``effective_live_delivery`` is True
    - ``last_data_at`` present and fresher than ``(2 * interval) + grace`` seconds
    """

    result = PreflightResult(profile=profile)
    clock = now_ms if now_ms is not None else (lambda: int(time.time() * 1000))
    now = clock()

    def _record_readiness_failure(reason: str, input_key: tuple[str, str, str]) -> None:
        """Record a readiness failure ONLY when readiness gating is on."""
        if require_readiness:
            result.failures.append(
                PreflightFailure(
                    kind=PreflightFailureKind.STREAM,
                    reason=reason,
                    input_key=input_key,
                )
            )

    for inp in declared:
        if inp.exchange != exchange:
            result.failures.append(
                PreflightFailure(
                    kind=PreflightFailureKind.STREAM,
                    reason=(
                        f"market-data exchange {inp.exchange!r} is not supported "
                        f"for {profile.value} profile; supported exchange: {exchange}"
                    ),
                    input_key=_input_key(inp),
                )
            )
            continue
        max_age_seconds = (_interval_seconds(inp.interval) * 2) + freshness_grace_seconds
        stream = None
        try:
            stream = lookup_stream(_marketdata_market(inp.market), inp.symbol, inp.interval)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "stream status lookup raised for %s: %s", inp.key, e, exc_info=True,
            )
            # Lookup failure is a structural failure — we have no binding.
            # Surface it even when readiness gating is off, because lease
            # management downstream would fail silently otherwise.
            result.failures.append(
                PreflightFailure(
                    kind=PreflightFailureKind.STREAM,
                    reason=f"stream lookup failed: {e}",
                    input_key=_input_key(inp),
                )
            )
            continue

        if stream is None:
            # Structural failure — no stream_id to bind against.
            result.failures.append(
                PreflightFailure(
                    kind=PreflightFailureKind.STREAM,
                    reason="stream missing or control-plane lookup returned nothing",
                    input_key=_input_key(inp),
                )
            )
            continue

        # Stream exists → emit the binding BEFORE the readiness checks, so
        # downstream lease management can always rely on having bindings for
        # every declared input that passed structural validation. Readiness
        # checks below only affect ``result.failures``, not ``required_streams``.
        result.required_streams.append(_stream_to_binding(stream))

        actual_state = str(getattr(stream, "actual_state", "") or "").strip().lower()
        if actual_state != "running":
            detail = f"stream state is {actual_state or 'unknown'}"
            last_error = getattr(stream, "last_error", "")
            if last_error:
                detail += f" ({last_error})"
            _record_readiness_failure(detail, _input_key(inp))
            continue

        if not bool(getattr(stream, "effective_live_delivery", False)):
            _record_readiness_failure("live delivery is disabled", _input_key(inp))
            continue

        last_data_at = getattr(stream, "last_data_at", None)
        has_last_data_at = last_data_at is not None
        has_field = getattr(stream, "HasField", None)
        if callable(has_field):
            try:
                has_last_data_at = bool(has_field("last_data_at"))
            except ValueError:
                has_last_data_at = last_data_at is not None

        if not has_last_data_at or last_data_at is None:
            _record_readiness_failure(
                "stream has no freshness timestamp yet", _input_key(inp),
            )
            continue

        try:
            last_ms = int(last_data_at.ToMilliseconds())
        except Exception as e:  # noqa: BLE001
            _record_readiness_failure(
                f"invalid freshness timestamp: {e}", _input_key(inp),
            )
            continue

        age_ms = now - last_ms
        if age_ms > max_age_seconds * 1000:
            _record_readiness_failure(
                (
                    f"stream is stale ({age_ms // 1000}s old, "
                    f"max {max_age_seconds}s for interval {inp.interval})"
                ),
                _input_key(inp),
            )
            continue

    return result


# ── Public re-exports ──────────────────────────────────────────────────────

__all__ = [
    "RuntimeSourceProfile",
    "SUPPORTED_PROFILES",
    "resolve_profile",
    "PreflightFailureKind",
    "PreflightFailure",
    "PreflightResult",
    "check_profile_supported",
    "BacktestAvailabilityFn",
    "backtest_preflight",
    "default_backtest_availability",
    "live_stream_preflight",
]
