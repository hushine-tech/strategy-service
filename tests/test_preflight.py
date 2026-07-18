"""Unit tests for ``strategy_service.preflight`` (pre_C3 gate 2).

These tests cover the profile resolver + the two profile-specific preflight
evaluators at module level — end-to-end RunStrategy wiring is covered in
``tests/test_grpc_server.py``.
"""

from __future__ import annotations

from types import SimpleNamespace

from google.protobuf.timestamp_pb2 import Timestamp

from strategy_service.gen import portfolio_service_pb2
from strategy_service.inputs import StrategyInput
from strategy_service.portfolio_client import _required_symbol_protos
from strategy_service.preflight import (
    SUPPORTED_PROFILES,
    PreflightFailureKind,
    RuntimeSourceProfile,
    backtest_preflight,
    check_profile_supported,
    live_stream_preflight,
    resolve_profile,
)


def test_required_symbol_descriptor_field_numbers():
    """The worker must consume the additive Task 5 preflight contract verbatim."""
    fields = portfolio_service_pb2.RequiredSymbol.DESCRIPTOR.fields_by_name

    assert fields["exchange"].number == 1
    assert fields["market"].number == 2
    assert fields["symbol"].number == 3
    assert fields["order_target"].number == 4
    assert fields["required_order_types"].number == 5


def test_required_symbols_mark_order_targets_and_market_limit_capabilities():
    inputs_and_targets = {
        ("binance", "spot", "BTCUSDT"),
        ("binance", "spot", "ETHUSDT"),
    }
    items = _required_symbol_protos(
        portfolio_service_pb2,
        inputs_and_targets,
        order_target_symbols={("binance", "spot", "ETHUSDT")},
    )
    by_symbol = {item.symbol: item for item in items}

    assert by_symbol["BTCUSDT"].order_target is False
    assert list(by_symbol["BTCUSDT"].required_order_types) == []
    assert by_symbol["ETHUSDT"].order_target is True
    assert list(by_symbol["ETHUSDT"].required_order_types) == ["MARKET", "LIMIT"]


# ── Profile resolver ───────────────────────────────────────────────────────


def test_resolve_profile_maps_modes_to_profiles():
    assert resolve_profile(0) is RuntimeSourceProfile.BACKTEST
    assert resolve_profile(1) is RuntimeSourceProfile.DEMO
    assert resolve_profile(2) is RuntimeSourceProfile.LIVE


def test_resolve_profile_returns_unknown_for_unexpected_environments():
    assert resolve_profile(99) is RuntimeSourceProfile.UNKNOWN
    assert resolve_profile(-1) is RuntimeSourceProfile.UNKNOWN


def test_supported_profiles_include_backtest_and_demo_only():
    # Gate 2: live (environment=2) must remain unsupported until Phase C+ wiring lands.
    assert RuntimeSourceProfile.BACKTEST in SUPPORTED_PROFILES
    assert RuntimeSourceProfile.DEMO in SUPPORTED_PROFILES
    assert RuntimeSourceProfile.LIVE not in SUPPORTED_PROFILES
    assert RuntimeSourceProfile.UNKNOWN not in SUPPORTED_PROFILES


def test_check_profile_supported_returns_ok_for_supported():
    result = check_profile_supported(RuntimeSourceProfile.BACKTEST)
    assert result.ok
    assert result.failures == []


def test_check_profile_supported_fails_live_with_profile_reason():
    result = check_profile_supported(RuntimeSourceProfile.LIVE)
    assert not result.ok
    assert len(result.failures) == 1
    failure = result.failures[0]
    assert failure.kind is PreflightFailureKind.PROFILE
    assert failure.input_key is None
    # Message explicitly says profile/runtime wiring, NOT "missing holding".
    assert "profile" in failure.reason.lower()
    assert "live" in failure.reason.lower()


def test_check_profile_supported_fails_unknown_environment():
    result = check_profile_supported(RuntimeSourceProfile.UNKNOWN)
    assert not result.ok
    assert result.failures[0].kind is PreflightFailureKind.PROFILE


# ── Backtest preflight ────────────────────────────────────────────────────


def test_backtest_preflight_passes_when_all_declared_inputs_have_data():
    declared = [
        StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m"),
        StrategyInput("binance", "spot", "ETHUSDT", "5m"),
    ]
    seen: list[tuple[str, str, str, int, int]] = []

    def has_data(inp, start, end):
        seen.append((inp.market, inp.symbol, inp.interval, start, end))
        return True

    result = backtest_preflight(declared, 1_700_000_000_000, 1_700_000_060_000, has_data)

    assert result.ok
    assert result.profile is RuntimeSourceProfile.BACKTEST
    # One lookup per declared input, honouring declared interval (NOT a single
    # request-level interval).
    assert seen == [
        ("perpetual_futures", "BTCUSDT", "1m", 1_700_000_000_000, 1_700_000_060_000),
        ("spot", "ETHUSDT", "5m", 1_700_000_000_000, 1_700_000_060_000),
    ]


def test_backtest_preflight_reports_per_input_missing_data():
    declared = [
        StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m"),
        StrategyInput("binance", "perpetual_futures", "ETHUSDT", "1m"),
    ]

    def has_data(inp, _start, _end):
        return inp.symbol == "BTCUSDT"

    result = backtest_preflight(declared, 1_700_000_000_000, 1_700_000_060_000, has_data)

    assert not result.ok
    assert len(result.failures) == 1
    failure = result.failures[0]
    assert failure.kind is PreflightFailureKind.HISTORICAL_DATA
    assert failure.input_key == ("futures", "ETHUSDT", "1m")
    # Error message should be human-readable and identify the missing input.
    assert "ETHUSDT" in result.error_message()
    assert "1m" in result.error_message()


def test_backtest_preflight_rejects_declared_input_when_availability_false():
    declared = [StrategyInput(exchange="binance", market="perpetual_futures", symbol="ETHUSDT", interval="1m")]

    result = backtest_preflight(
        declared,
        1_779_033_600_000,
        1_779_037_200_000,
        lambda *_: False,
    )

    assert not result.ok
    assert result.failures[0].kind is PreflightFailureKind.HISTORICAL_DATA
    assert result.failures[0].input_key == ("futures", "ETHUSDT", "1m")


def test_backtest_preflight_fails_with_invalid_time_range():
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]

    def has_data(_inp, _start, _end):
        raise AssertionError("availability_fn must not be called when time range is invalid")

    result = backtest_preflight(declared, 0, 0, has_data)
    assert not result.ok
    assert result.failures[0].kind is PreflightFailureKind.INVALID_REQUEST

    result = backtest_preflight(declared, 1_700_000_100_000, 1_700_000_000_000, has_data)
    assert not result.ok
    assert result.failures[0].kind is PreflightFailureKind.INVALID_REQUEST


def test_backtest_preflight_surfaces_availability_exception_as_failure():
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]

    def has_data(_inp, _start, _end):
        raise RuntimeError("table missing")

    result = backtest_preflight(declared, 1, 2, has_data)
    assert not result.ok
    assert result.failures[0].kind is PreflightFailureKind.HISTORICAL_DATA
    # Underlying error bubbles into the reason string.
    assert "table missing" in result.failures[0].reason


def test_default_backtest_availability_accepts_dict_ts_config(monkeypatch):
    """Regression: strategy-service stores Timescale config as dict.

    ``default_backtest_availability(dict)`` must convert to TimescaleConfig
    internally — otherwise ``BacktestDataSource`` (which expects
    TimescaleConfig) blows up with AttributeError at first row fetch,
    making every backtest falsely report historical-data failure.
    """
    import types

    captured_configs: list = []

    class FakeTimescaleConfig:
        def __init__(self, **kwargs):
            self.fields = kwargs

        @classmethod
        def from_dict(cls, data):
            return cls(**dict(data))

        def database_for_year(self, year):  # sanity: must not raise
            return "binance_{}".format(year)

    class FakeBacktestDataSource:
        def __init__(self, resolved):
            captured_configs.append(resolved)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def has_kline_coverage(self, *_a, **_kw):
            return False

    fake_market_data_backtest = types.SimpleNamespace(
        BacktestDataSource=FakeBacktestDataSource
    )
    fake_market_data_config = types.SimpleNamespace(
        TimescaleConfig=FakeTimescaleConfig
    )
    import sys
    monkeypatch.setitem(sys.modules, "market_data.backtest", fake_market_data_backtest)
    monkeypatch.setitem(sys.modules, "market_data.config", fake_market_data_config)

    from strategy_service.preflight import default_backtest_availability

    # Feed the exact shape strategy-service uses: a dict from TimescaleDB config.
    dict_cfg = {"host": "pg.local", "port": 5432, "database": "binance_{year}"}
    evaluator = default_backtest_availability(dict_cfg)
    # One call to actually materialise BacktestDataSource.
    evaluator(StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m"), 1, 2)

    assert len(captured_configs) == 1
    # Must be the TimescaleConfig instance, NOT the raw dict.
    assert isinstance(captured_configs[0], FakeTimescaleConfig)
    assert captured_configs[0].fields["host"] == "pg.local"


def test_backtest_preflight_ignores_undeclared_symbols():
    # Per pre_C3 gate 2: only declared inputs feed the evaluator. The evaluator
    # never receives wallet positions / spot assets, so this is effectively a
    # contract test — with zero declared inputs we'd raise at parse time.
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]
    queried: list[str] = []

    def has_data(inp, _start, _end):
        queried.append(inp.symbol)
        return True

    backtest_preflight(declared, 1, 2, has_data)
    # Only the declared BTCUSDT is consulted — no ETHUSDT from wallet assets.
    assert queried == ["BTCUSDT"]


# ── Live / testnet preflight ──────────────────────────────────────────────


def _make_stream(
    *,
    stream_id: int = 1,
    actual_state: str = "running",
    effective_live_delivery: bool = True,
    last_data_at: Timestamp | None = None,
    last_error: str = "",
    market: str = "futures",
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    has_last_data_at: bool = True,
):
    if last_data_at is None:
        last_data_at = Timestamp()
        last_data_at.GetCurrentTime()
    return SimpleNamespace(
        stream_id=stream_id,
        actual_state=actual_state,
        effective_live_delivery=effective_live_delivery,
        last_data_at=last_data_at,
        last_error=last_error,
        key=SimpleNamespace(
            exchange="binance",
            market=market,
            kind="kline",
            symbol=symbol,
            interval=interval,
        ),
        HasField=lambda field: field == "last_data_at" and has_last_data_at,
    )


def test_live_stream_preflight_passes_on_running_fresh_streams():
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]
    streams = {("futures", "BTCUSDT", "1m"): _make_stream()}

    def lookup(m, s, i):
        return streams.get((m, s, i))

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )
    assert result.ok
    assert [(b.symbol, b.market, b.interval) for b in result.required_streams] == [
        ("BTCUSDT", "futures", "1m"),
    ]


def test_live_stream_preflight_fails_closed_for_unsupported_exchange():
    declared = [StrategyInput("okx", "perpetual_futures", "ETHUSDT", "1m")]
    calls: list[tuple] = []

    def lookup(*args):
        calls.append(args)
        return _make_stream(symbol="ETHUSDT")

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )

    assert not result.ok
    assert calls == []
    failure = result.failures[0]
    assert failure.kind is PreflightFailureKind.STREAM
    assert failure.input_key == ("futures", "ETHUSDT", "1m")
    assert "okx" in failure.reason.lower()


def test_live_stream_preflight_fails_when_stream_missing():
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]

    def lookup(*_args):
        return None

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )
    assert not result.ok
    failure = result.failures[0]
    assert failure.kind is PreflightFailureKind.STREAM
    assert failure.input_key == ("futures", "BTCUSDT", "1m")


def test_live_stream_preflight_fails_when_state_not_running():
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]
    stream = _make_stream(actual_state="error", last_error="kafka down")

    def lookup(*_args):
        return stream

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )
    assert not result.ok
    assert "error" in result.failures[0].reason
    assert "kafka down" in result.failures[0].reason


def test_live_stream_preflight_fails_when_delivery_disabled():
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]

    def lookup(*_args):
        return _make_stream(effective_live_delivery=False)

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )
    assert not result.ok
    assert "live delivery is disabled" in result.failures[0].reason


def test_live_stream_preflight_fails_when_stream_stale():
    # Declared interval is 1m → max_age = 2 * 60 + 30 = 150s. Inject a stream
    # whose last_data_at is 10 minutes old, freshness check must reject.
    declared = [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")]
    now_ms = 1_700_000_000_000
    stale_ts = Timestamp()
    stale_ts.FromMilliseconds(now_ms - 600_000)  # 10 min old

    def lookup(*_args):
        return _make_stream(last_data_at=stale_ts)

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
        now_ms=lambda: now_ms,
    )
    assert not result.ok
    assert "stale" in result.failures[0].reason
    assert "1m" in result.failures[0].reason


def test_live_stream_preflight_honours_per_input_interval():
    # Two declared inputs on same symbol, DIFFERENT intervals. Lookup must
    # receive each declared interval individually — not a single shared one.
    declared = [
        StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m"),
        StrategyInput("binance", "perpetual_futures", "BTCUSDT", "5m"),
    ]
    queried: list[tuple[str, str, str]] = []

    def lookup(m, s, i):
        queried.append((m, s, i))
        return _make_stream(interval=i)

    live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )
    assert queried == [
        ("futures", "BTCUSDT", "1m"),
        ("futures", "BTCUSDT", "5m"),
    ]


def test_live_stream_preflight_aggregates_multiple_failures():
    declared = [
        StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m"),
        StrategyInput("binance", "perpetual_futures", "ETHUSDT", "1m"),
    ]

    def lookup(_m, symbol, _i):
        if symbol == "BTCUSDT":
            return None
        return _make_stream(symbol=symbol, effective_live_delivery=False)

    result = live_stream_preflight(
        declared,
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )
    assert not result.ok
    # Both declared inputs fail — one missing, one delivery-off.
    assert len(result.failures) == 2
    reasons = [f.input_key for f in result.failures]
    assert ("futures", "BTCUSDT", "1m") in reasons
    assert ("futures", "ETHUSDT", "1m") in reasons
