import copy
import importlib
import pickle
import sys
import threading
from contextlib import contextmanager
from dataclasses import replace

import pytest

import strategy_service.strategy_imports as strategy_imports
from strategy_service.service import StrategyEngine
from strategy_service.strategy_imports import (
    CapturedFileSignature,
    StrategyDependencyError,
    StrategySourceLoadError,
    StrategySourceResolutionError,
    _probe_strategy_imports_for_test,
    gate_strategy_source,
    prepare_strategy,
    probe_strategy_imports,
    resolve_strategy_source,
)
from strategy_service.wallet.portfolio import PortfolioWalletRuntime


_VALID_SOURCE = '''
import sys
sys._hushine_strategy_import_execs += 1

class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def on_market_data(self, data, wallet):
        return None
'''


class _EqualString(str):
    pass


class _EqualTuple(tuple):
    pass


class _EqualInteger(int):
    pass


class _DecodeHookBytes(bytes):
    calls = 0

    def decode(self, *args, **kwargs):
        type(self).calls += 1
        return bytes.decode(self, *args, **kwargs)


def _install_successful_probe(monkeypatch):
    def successful_probe(
        imports,
        *,
        python_invocation_path,
        expected_profile,
        timeout_seconds=30.0,
    ):
        del imports, python_invocation_path, timeout_seconds
        return strategy_imports.ImportProbeResult(
            ok=True,
            code="",
            requested_module="",
            profile_name=expected_profile.name,
            profile_version=expected_profile.version,
            contract_sha256=expected_profile.contract_sha256,
        )

    monkeypatch.setattr(strategy_imports, "probe_import_records", successful_probe)


def _gate_valid_source(monkeypatch):
    _install_successful_probe(monkeypatch)
    resolved = resolve_strategy_source("<db:capability>", _VALID_SOURCE)
    gate = gate_strategy_source(
        resolved,
        python_invocation_path=sys.executable,
    )
    assert gate.ok and gate.gated_source is not None
    return resolved, gate.gated_source


def _prepare_valid_source(monkeypatch):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _, gated = _gate_valid_source(monkeypatch)
    prepared = prepare_strategy(gated)
    assert sys._hushine_strategy_import_execs == 1
    return gated, prepared


def _wallet_runtime():
    return PortfolioWalletRuntime(
        1,
        {("binance", "perpetual_futures")},
        {("binance", "perpetual_futures", 1): object()},
    )


def _gate_source_code(monkeypatch, source, *, strategy_path="<db:marker>"):
    _install_successful_probe(monkeypatch)
    resolved = resolve_strategy_source(strategy_path, source)
    gate = gate_strategy_source(resolved, python_invocation_path=sys.executable)
    assert gate.ok and gate.gated_source is not None
    return resolved, gate.gated_source


def _retarget_resolved(target, replacement):
    for field in (
        "filename",
        "source_bytes",
        "source_sha256",
        "module_name",
        "package_name",
        "is_package",
        "package_search_locations",
        "source_kind",
        "hot_reload_path",
        "hot_reload_signature",
    ):
        object.__setattr__(target, field, getattr(replacement, field))


def _retarget_snapshot(target, replacement):
    for field in (
        "filename",
        "source_bytes",
        "source_sha256",
        "module_name",
        "package_name",
        "is_package",
        "package_search_locations",
        "source_kind",
        "hot_reload_path",
        "hot_reload_signature_fields",
    ):
        object.__setattr__(target, field, getattr(replacement, field))


def _marker_mutation_source(action, *, raise_after=False):
    suffix = "raise SystemExit('marker-cleanup-canary')" if raise_after else ""
    return f'''
import sys
sys._hushine_marker_execs += 1
active = sys.modules["strategy_service.strategy_imports"]._ACTIVE_MODULE_NAMES
{action}

class MyStrategy:
    INPUTS = [{{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}}]
    ORDER_TARGETS = []
    def on_market_data(self, data, wallet):
        return None

{suffix}
'''


def test_strategy_imports_surface_is_importable():
    module = importlib.import_module("strategy_service.strategy_imports")
    assert tuple(
        name
        for name in (
            "ResolvedStrategySource",
            "GatedStrategySource",
            "StrategyDependencyError",
            "StrategySourceResolutionError",
            "StrategySourceLoadError",
            "StrategySourceGateResult",
            "resolve_strategy_source",
            "probe_strategy_imports",
            "gate_strategy_source",
        )
        if not hasattr(module, name)
    ) == ()


def test_prepare_sets_source_backed_spec_and_immutable_package_path(
    monkeypatch,
    tmp_path,
):
    package = tmp_path / "sealed_package"
    package.mkdir()
    (package / "__init__.py").write_text(
        """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    captured_name = __name__
    captured_package = __package__
    captured_file = __file__
    captured_loader_type = type(__spec__.loader).__name__
    captured_path = __path__
    def on_market_data(self, data, wallet):
        return None
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    gate = gate_strategy_source(
        resolve_strategy_source("sealed_package", None),
        python_invocation_path=sys.executable,
    )
    assert gate.ok and gate.gated_source is not None
    strategy = StrategyEngine().create_strategy(
        "package-metadata",
        prepare_strategy(gate.gated_source),
        PortfolioWalletRuntime(
            1,
            {("binance", "perpetual_futures")},
            {("binance", "perpetual_futures", 1): object()},
        ),
    )._get_strategy()

    assert strategy.captured_name == "sealed_package"
    assert strategy.captured_package == "sealed_package"
    assert strategy.captured_file == str(package / "__init__.py")
    assert strategy.captured_loader_type == "SourceFileLoader"
    assert strategy.captured_path == (str(package),)
    assert type(strategy.captured_path) is tuple


def test_dependency_error_allows_traceback_reraise_without_mutating_payload():
    @contextmanager
    def reraiser():
        try:
            yield
        except BaseException as error:
            raise error.with_traceback(error.__traceback__)

    dependency_error = StrategyDependencyError(
        code="STRATEGY_DEPENDENCY_UNAVAILABLE",
        module="missing.child",
        runtime_profile="hosted",
        runtime_profile_version="v1",
        image_build_id="image-1",
        message="strategy dependency missing.child is unavailable in runtime profile hosted",
    )

    with pytest.raises(StrategyDependencyError) as captured:
        with reraiser():
            raise dependency_error

    assert captured.value is dependency_error
    assert captured.value.module == "missing.child"


def test_missing_allowed_submodule_is_unavailable():
    resolved = resolve_strategy_source(
        "<db>",
        "import google.hushine_missing\nraise AssertionError('must not execute')",
    )

    error = probe_strategy_imports(
        resolved,
        python_invocation_path=sys.executable,
    )

    assert error is not None
    assert error.code == "STRATEGY_DEPENDENCY_UNAVAILABLE"
    assert error.module == "google.hushine_missing"


def test_import_initialization_failure_is_distinct(tmp_path):
    package = tmp_path / "requests"
    package.mkdir()
    (package / "__init__.py").write_text(
        "import private_transitive_canary\n",
        encoding="utf-8",
    )
    resolved = resolve_strategy_source(
        "<db>",
        "import requests\nraise AssertionError('user body executed')",
    )

    error = _probe_strategy_imports_for_test(
        resolved,
        python_invocation_path=sys.executable,
        extra_python_path=(str(tmp_path),),
    )

    assert error is not None
    assert error.code == "STRATEGY_IMPORT_FAILED"
    assert error.module == "requests"
    assert "private_transitive_canary" not in str(error)
    assert "user body executed" not in str(error)


def test_resolution_error_drops_original_unicode_exception():
    with pytest.raises(StrategySourceResolutionError) as captured:
        resolve_strategy_source("<db:unicode-canary>", "\ud800")

    assert captured.value.reason == "invalid_utf8"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_declaration_load_error_drops_original_baseexception():
    source = '''
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def __getattribute__(self, name):
        if name == "INPUTS":
            raise KeyboardInterrupt("declaration-secret")
        return object.__getattribute__(self, name)
    def on_market_data(self, data, wallet):
        return None
'''
    gate = gate_strategy_source(
        resolve_strategy_source("<db:declaration-canary>", source),
        python_invocation_path=sys.executable,
    )
    assert gate.ok and gate.gated_source is not None

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gate.gated_source)

    assert captured.value.reason == "declaration_failed"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_post_gate_bytes_subclass_cannot_override_the_executed_source(
    monkeypatch,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    resolved, gated = _gate_valid_source(monkeypatch)
    _DecodeHookBytes.calls = 0
    object.__setattr__(
        resolved,
        "source_bytes",
        _DecodeHookBytes(resolved.source_bytes),
    )

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "gated_source_invalid"
    assert _DecodeHookBytes.calls == 0
    assert sys._hushine_strategy_import_execs == 0


def test_public_probe_rejects_bytes_subclass_without_calling_decode(
    monkeypatch,
):
    _install_successful_probe(monkeypatch)
    resolved = resolve_strategy_source("<db:probe-capability>", _VALID_SOURCE)
    _DecodeHookBytes.calls = 0
    object.__setattr__(
        resolved,
        "source_bytes",
        _DecodeHookBytes(resolved.source_bytes),
    )

    with pytest.raises(TypeError):
        probe_strategy_imports(
            resolved,
            python_invocation_path=sys.executable,
        )

    assert _DecodeHookBytes.calls == 0


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("filename", lambda value: _EqualString(value)),
        ("source_sha256", lambda value: _EqualString(value)),
        ("module_name", lambda value: _EqualString(value)),
        ("package_name", lambda value: _EqualString(value)),
        ("is_package", lambda value: int(value)),
        ("package_search_locations", lambda value: _EqualTuple(value)),
        ("source_kind", lambda value: _EqualString(value)),
    ),
)
def test_post_gate_equal_value_metadata_subclasses_execute_nothing(
    monkeypatch,
    field,
    replacement,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    resolved, gated = _gate_valid_source(monkeypatch)
    object.__setattr__(resolved, field, replacement(getattr(resolved, field)))

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


def test_post_gate_equal_value_hot_reload_path_subclass_executes_nothing(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _install_successful_probe(monkeypatch)
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(_VALID_SOURCE, encoding="utf-8")
    resolved = resolve_strategy_source(str(strategy_file), None, hot_reload=True)
    gate = gate_strategy_source(resolved, python_invocation_path=sys.executable)
    assert gate.ok and gate.gated_source is not None
    assert resolved.hot_reload_path is not None
    object.__setattr__(
        resolved,
        "hot_reload_path",
        _EqualString(resolved.hot_reload_path),
    )

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gate.gated_source)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


@pytest.mark.parametrize(
    "field",
    ("device", "inode", "mtime_ns", "ctime_ns", "size"),
)
def test_post_gate_equal_value_signature_field_subclasses_execute_nothing(
    monkeypatch,
    tmp_path,
    field,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _install_successful_probe(monkeypatch)
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text(_VALID_SOURCE, encoding="utf-8")
    resolved = resolve_strategy_source(str(strategy_file), None, hot_reload=True)
    gate = gate_strategy_source(resolved, python_invocation_path=sys.executable)
    assert gate.ok and gate.gated_source is not None
    signature = resolved.hot_reload_signature
    assert type(signature) is CapturedFileSignature
    object.__setattr__(
        signature,
        field,
        _EqualInteger(getattr(signature, field)),
    )

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gate.gated_source)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


@pytest.mark.parametrize(
    "replacement",
    (
        lambda value: _EqualTuple(value),
        lambda value: (_EqualInteger(value[0]), value[1]),
        lambda value: (value[0], _EqualInteger(value[1])),
    ),
)
def test_gated_interpreter_identity_requires_exact_builtin_fields(
    monkeypatch,
    replacement,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _, gated = _gate_valid_source(monkeypatch)
    interpreter_identity = gated._interpreter_identity
    assert type(interpreter_identity) is tuple
    object.__setattr__(
        gated,
        "_interpreter_identity",
        replacement(interpreter_identity),
    )

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


@pytest.mark.parametrize(
    "field",
    ("_runtime_contract_sha256", "_python_invocation_path"),
)
def test_gated_private_text_metadata_requires_exact_builtin_strings(
    monkeypatch,
    field,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _, gated = _gate_valid_source(monkeypatch)
    object.__setattr__(gated, field, _EqualString(getattr(gated, field)))

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


def test_gated_equal_value_private_snapshot_clone_executes_nothing(monkeypatch):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _, gated = _gate_valid_source(monkeypatch)
    object.__setattr__(gated, "_fingerprint", replace(gated._fingerprint))

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


def test_coordinated_public_and_gated_snapshot_retarget_executes_nothing(
    monkeypatch,
):
    resolved, gated = _gate_valid_source(monkeypatch)
    monkeypatch.setattr(sys, "_hushine_canonical_baseline_execs", 0, raising=False)
    replacement = resolve_strategy_source(
        "<db:retargeted>",
        _VALID_SOURCE.replace(
            "sys._hushine_strategy_import_execs += 1",
            "sys._hushine_canonical_baseline_execs += 1",
        ),
    )
    replacement_snapshot = strategy_imports._resolved_fingerprint(replacement)

    _retarget_resolved(resolved, replacement)
    _retarget_snapshot(gated._fingerprint, replacement_snapshot)

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_canonical_baseline_execs == 0


def test_coordinated_resolved_gated_and_prepared_retarget_cannot_bind(
    monkeypatch,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    resolved, gated = _gate_valid_source(monkeypatch)
    prepared = prepare_strategy(gated)
    monkeypatch.setattr(sys, "_hushine_canonical_baseline_execs", 0, raising=False)
    replacement = resolve_strategy_source(
        "<db:retargeted-prepared>",
        _VALID_SOURCE.replace(
            "sys._hushine_strategy_import_execs += 1",
            "sys._hushine_canonical_baseline_execs += 1",
        ),
    )
    replacement_snapshot = strategy_imports._resolved_fingerprint(replacement)
    bind_calls = 0

    _retarget_resolved(resolved, replacement)
    _retarget_snapshot(gated._fingerprint, replacement_snapshot)
    _retarget_snapshot(prepared._gated_fingerprint, replacement_snapshot)

    def binder(*_args):
        nonlocal bind_calls
        bind_calls += 1
        return object()

    with pytest.raises(StrategySourceLoadError) as captured:
        strategy_imports._claim_prepared_strategy(prepared, binder)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_canonical_baseline_execs == 0
    assert bind_calls == 0


def test_prepared_declaration_objects_are_fresh_from_primitive_authority(
    monkeypatch,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    source = _VALID_SOURCE.replace(
        "    ORDER_TARGETS = []",
        "    ORDER_TARGETS = [{\"exchange\": \"binance\", "
        "\"market\": \"perpetual_futures\", \"symbol\": \"BTCUSDT\"}]\n"
        "    RISK_CONTROLS = {\"max_loss_close_pct\": 0.25}",
    )
    _, gated = _gate_source_code(monkeypatch, source)
    prepared = prepare_strategy(gated)
    exposed = prepared.declarations

    object.__setattr__(exposed.inputs[0], "symbol", "ETHUSDT")
    object.__setattr__(exposed.order_targets[0], "symbol", "ETHUSDT")
    object.__setattr__(exposed.risk_controls, "max_loss_close_pct", 0.9)

    fresh = prepared.declarations
    assert fresh.inputs[0].symbol == "BTCUSDT"
    assert fresh.order_targets[0].symbol == "BTCUSDT"
    assert fresh.risk_controls.max_loss_close_pct == 0.25
    assert fresh.inputs[0] is not exposed.inputs[0]
    assert fresh.order_targets[0] is not exposed.order_targets[0]
    assert fresh.risk_controls is not exposed.risk_controls

    observed = {}

    def binder(_instance, declarations, _indicators, _gated_source):
        observed["input"] = declarations.inputs[0].symbol
        observed["target"] = declarations.order_targets[0].symbol
        observed["risk"] = declarations.risk_controls.max_loss_close_pct
        return object()

    strategy_imports._claim_prepared_strategy(prepared, binder)
    assert observed == {
        "input": "BTCUSDT",
        "target": "BTCUSDT",
        "risk": 0.25,
    }


def test_prepared_declarations_preserve_full_stream_identity(monkeypatch):
    source = '''
class MyStrategy:
    INPUTS = [
        {"stream_id": "btc-kline", "exchange": "binance", "market": "perpetual_futures", "kind": "kline", "symbol": "BTCUSDT", "interval": "1m"},
        {"stream_id": "btc-mark", "exchange": "binance", "market": "perpetual_futures", "kind": "mark_price", "symbol": "BTCUSDT", "interval": "1m"},
    ]
    ORDER_TARGETS = []
    def on_market_data(self, data, wallet):
        return None
'''
    _, gated = _gate_source_code(monkeypatch, source)

    prepared = prepare_strategy(gated)

    assert [
        (item.stream_id, item.exchange, item.market, item.kind, item.symbol, item.interval)
        for item in prepared.declarations.inputs
    ] == [
        ("btc-kline", "binance", "perpetual_futures", "kline", "BTCUSDT", "1m"),
        ("btc-mark", "binance", "perpetual_futures", "mark_price", "BTCUSDT", "1m"),
    ]


def test_gated_capability_rejects_direct_constructor(monkeypatch):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    resolved, _ = _gate_valid_source(monkeypatch)
    gated_type = strategy_imports._SealedGatedStrategySource
    profile = strategy_imports.current_runtime_profile()

    with pytest.raises(TypeError):
        gated_type(
            resolved,
            runtime_contract_sha256=profile.contract_sha256,
            python_invocation_path=sys.executable,
        )

    assert sys._hushine_strategy_import_execs == 0


def test_object_new_gated_forge_with_correlated_fields_executes_nothing(
    monkeypatch,
):
    monkeypatch.setattr(sys, "_hushine_strategy_import_execs", 0, raising=False)
    _, gated = _gate_valid_source(monkeypatch)
    forged = object.__new__(type(gated))
    for field in type(gated).__slots__:
        if field != "__weakref__":
            object.__setattr__(forged, field, getattr(gated, field))

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(forged)

    assert captured.value.reason == "gated_source_invalid"
    assert sys._hushine_strategy_import_execs == 0


def test_prepared_capability_rejects_direct_constructor(monkeypatch):
    _, prepared = _prepare_valid_source(monkeypatch)
    prepared_type = type(prepared)

    with pytest.raises(TypeError):
        prepared_type(
            prepared._gated_source,
            prepared._instance,
            prepared.declarations,
            prepared._indicators,
        )


def test_object_new_prepared_forge_with_correlated_fields_cannot_bind(
    monkeypatch,
):
    _, prepared = _prepare_valid_source(monkeypatch)
    forged = object.__new__(type(prepared))
    for field in type(prepared).__slots__:
        if field != "__weakref__":
            object.__setattr__(forged, field, getattr(prepared, field))
    engine = StrategyEngine()

    with pytest.raises(StrategySourceLoadError) as captured:
        engine.create_strategy("forged", forged, _wallet_runtime())

    assert captured.value.reason == "gated_source_invalid"
    assert engine.strategies == {}
    assert engine.strategy_router == {}


def test_first_claim_attempt_invalidates_a_mutated_prepared_seal(
    monkeypatch,
):
    _, prepared = _prepare_valid_source(monkeypatch)
    original_seal = prepared._seal
    object.__setattr__(prepared, "_seal", object())

    with pytest.raises(StrategySourceLoadError) as first:
        StrategyEngine().create_strategy("first", prepared, _wallet_runtime())
    assert first.value.reason == "gated_source_invalid"

    object.__setattr__(prepared, "_seal", original_seal)
    with pytest.raises(StrategySourceLoadError) as retry:
        StrategyEngine().create_strategy("retry", prepared, _wallet_runtime())
    assert retry.value.reason == "gated_source_invalid"


def test_first_claim_attempt_invalidates_equal_value_fingerprint_clone(
    monkeypatch,
):
    _, prepared = _prepare_valid_source(monkeypatch)
    original_fingerprint = prepared._gated_fingerprint
    object.__setattr__(
        prepared,
        "_gated_fingerprint",
        replace(original_fingerprint),
    )

    with pytest.raises(StrategySourceLoadError) as first:
        StrategyEngine().create_strategy("first", prepared, _wallet_runtime())
    assert first.value.reason == "gated_source_invalid"

    object.__setattr__(prepared, "_gated_fingerprint", original_fingerprint)
    with pytest.raises(StrategySourceLoadError) as retry:
        StrategyEngine().create_strategy("retry", prepared, _wallet_runtime())
    assert retry.value.reason == "gated_source_invalid"


def test_object_setattr_state_reset_cannot_reuse_a_claimed_prepared_capability(
    monkeypatch,
):
    _, prepared = _prepare_valid_source(monkeypatch)
    StrategyEngine().create_strategy("first", prepared, _wallet_runtime())
    with pytest.raises(AttributeError):
        prepared._state = "UNCLAIMED"
    with pytest.raises(AttributeError):
        object.__setattr__(prepared, "_state", "UNCLAIMED")

    with pytest.raises(StrategySourceLoadError) as retry:
        StrategyEngine().create_strategy("retry", prepared, _wallet_runtime())

    assert retry.value.reason == "gated_source_invalid"


def test_capability_authority_registry_and_issuers_are_not_module_reachable(
    monkeypatch,
):
    _, prepared = _prepare_valid_source(monkeypatch)
    StrategyEngine().create_strategy("first", prepared, _wallet_runtime())

    for authority_name in (
        "_GATED_ISSUANCES",
        "_GATED_ISSUANCE_LOCK",
        "_PREPARED_ISSUANCES",
        "_PREPARED_ISSUANCE_LOCK",
        "_lookup_gated_issuance",
        "_lookup_prepared_issuance",
        "_issue_gated_source",
        "_issue_prepared_strategy",
        "_validate_gated",
    ):
        assert not hasattr(strategy_imports, authority_name)

    with pytest.raises(StrategySourceLoadError) as retry:
        StrategyEngine().create_strategy("retry", prepared, _wallet_runtime())
    assert retry.value.reason == "gated_source_invalid"


@pytest.mark.parametrize(
    "copier",
    (
        copy.copy,
        copy.deepcopy,
        lambda value: pickle.loads(pickle.dumps(value)),
    ),
)
def test_capabilities_reject_copy_deepcopy_and_pickle(monkeypatch, copier):
    gated, prepared = _prepare_valid_source(monkeypatch)

    with pytest.raises(TypeError):
        copier(gated)
    with pytest.raises(TypeError):
        copier(prepared)


def test_concurrent_prepared_reuse_has_one_claim_winner(monkeypatch):
    _, prepared = _prepare_valid_source(monkeypatch)
    barrier = threading.Barrier(3)
    outcomes = []

    def claim(name):
        barrier.wait()
        try:
            StrategyEngine().create_strategy(name, prepared, _wallet_runtime())
        except StrategySourceLoadError as error:
            outcomes.append(("error", error.reason))
        else:
            outcomes.append(("claimed", name))

    threads = [
        threading.Thread(target=claim, args=(f"claim-{index}",))
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sum(kind == "claimed" for kind, _ in outcomes) == 1
    assert outcomes.count(("error", "gated_source_invalid")) == 1
    assert sys._hushine_strategy_import_execs == 1


def test_active_module_marker_removal_fails_and_restores_prior_module(
    monkeypatch,
):
    monkeypatch.setattr(sys, "_hushine_marker_execs", 0, raising=False)
    source = _marker_mutation_source(
        '''
if isinstance(active, set):
    active.discard(__name__)
else:
    active.pop(__name__, None)
'''
    )
    resolved, gated = _gate_source_code(monkeypatch, source)
    prior = object()
    monkeypatch.setitem(sys.modules, resolved.module_name, prior)

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "compile_or_exec_failed"
    assert sys._hushine_marker_execs == 1
    assert sys.modules[resolved.module_name] is prior
    assert resolved.module_name not in strategy_imports._ACTIVE_MODULE_NAMES


def test_active_module_marker_equal_name_replacement_fails_closed(
    monkeypatch,
):
    monkeypatch.setattr(sys, "_hushine_marker_execs", 0, raising=False)
    source = _marker_mutation_source(
        '''
if isinstance(active, set):
    active.discard(__name__)
    active.add(__name__)
else:
    active[__name__] = sys
'''
    )
    resolved, gated = _gate_source_code(monkeypatch, source)

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "compile_or_exec_failed"
    assert sys._hushine_marker_execs == 1
    assert resolved.module_name not in sys.modules
    assert resolved.module_name not in strategy_imports._ACTIVE_MODULE_NAMES


def test_removed_marker_cannot_enable_nested_same_name_execution(monkeypatch):
    monkeypatch.setattr(sys, "_hushine_marker_execs", 0, raising=False)
    source = _marker_mutation_source(
        '''
if isinstance(active, set):
    active.discard(__name__)
else:
    active.pop(__name__, None)
sys._hushine_nested_prepare()
'''
    )
    resolved, outer_gated = _gate_source_code(monkeypatch, source)
    second_gate = gate_strategy_source(
        resolved,
        python_invocation_path=sys.executable,
    )
    assert second_gate.ok and second_gate.gated_source is not None
    nested_results = []
    nested_calls = 0

    def nested_prepare():
        nonlocal nested_calls
        nested_calls += 1
        if nested_calls != 1:
            return
        try:
            prepare_strategy(second_gate.gated_source)
        except StrategySourceLoadError as error:
            nested_results.append(error.reason)
        else:
            nested_results.append("succeeded")

    monkeypatch.setattr(sys, "_hushine_nested_prepare", nested_prepare, raising=False)

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(outer_gated)

    assert captured.value.reason == "compile_or_exec_failed"
    assert nested_results == ["gated_source_invalid"]
    assert nested_calls == 1
    assert sys._hushine_marker_execs == 1
    assert resolved.module_name not in sys.modules
    assert resolved.module_name not in strategy_imports._ACTIVE_MODULE_NAMES


def test_marker_and_module_cleanup_survives_user_baseexception(monkeypatch):
    monkeypatch.setattr(sys, "_hushine_marker_execs", 0, raising=False)
    source = _marker_mutation_source(
        '''
if isinstance(active, set):
    active.discard(__name__)
    active.add(__name__)
else:
    active[__name__] = sys
''',
        raise_after=True,
    )
    resolved, gated = _gate_source_code(monkeypatch, source)
    prior = object()
    monkeypatch.setitem(sys.modules, resolved.module_name, prior)

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "compile_or_exec_failed"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert sys.modules[resolved.module_name] is prior
    assert resolved.module_name not in strategy_imports._ACTIVE_MODULE_NAMES


@pytest.mark.parametrize(
    ("fatal", "prior_present"),
    [
        (False, False),
        (True, False),
        (True, True),
    ],
)
def test_sys_modules_mapping_rebind_restores_original_mapping_and_prior_entry(
    monkeypatch,
    fatal,
    prior_present,
):
    source = (
        "import sys\n"
        "sys.modules = {}\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
        + ("raise SystemExit('sys-modules-fatal-canary')\n" if fatal else "")
    )
    resolved, gated = _gate_source_code(
        monkeypatch,
        source,
        strategy_path="<db:sys-modules-rebind>",
    )
    module_name = resolved.module_name
    original_mapping = sys.modules
    prior = object()
    if prior_present:
        original_mapping[module_name] = prior
    else:
        original_mapping.pop(module_name, None)

    try:
        with pytest.raises(StrategySourceLoadError) as captured:
            prepare_strategy(gated)

        assert captured.value.reason == "compile_or_exec_failed"
        assert captured.value.__cause__ is None
        assert captured.value.__context__ is None
        assert sys.modules is original_mapping
        if prior_present:
            assert original_mapping[module_name] is prior
        else:
            assert module_name not in original_mapping
    finally:
        sys.modules = original_mapping
        if prior_present:
            original_mapping[module_name] = prior
        else:
            original_mapping.pop(module_name, None)


def test_marker_registry_replacement_cannot_break_final_cleanup(monkeypatch):
    monkeypatch.setattr(sys, "_hushine_marker_execs", 0, raising=False)
    original_markers = strategy_imports._ACTIVE_MODULE_NAMES
    original_state = strategy_imports._MODULE_EXECUTION_STATE
    monkeypatch.setattr(strategy_imports, "_ACTIVE_MODULE_NAMES", original_markers)
    monkeypatch.setattr(strategy_imports, "_MODULE_EXECUTION_STATE", original_state)
    source = _marker_mutation_source(
        '''
owner = sys.modules["strategy_service.strategy_imports"]
owner._ACTIVE_MODULE_NAMES = {}
owner._MODULE_EXECUTION_STATE = None
'''
    )
    resolved, gated = _gate_source_code(monkeypatch, source)
    prior = object()
    monkeypatch.setitem(sys.modules, resolved.module_name, prior)

    with pytest.raises(StrategySourceLoadError) as captured:
        prepare_strategy(gated)

    assert captured.value.reason == "compile_or_exec_failed"
    assert sys.modules[resolved.module_name] is prior
    assert strategy_imports._ACTIVE_MODULE_NAMES is original_markers
    assert strategy_imports._MODULE_EXECUTION_STATE is original_state
    assert resolved.module_name not in original_markers


def test_resolver_clean_lookup_miss_remains_an_ordinary_missing_source():
    with pytest.raises(StrategySourceResolutionError) as captured:
        resolve_strategy_source("hushine_package_that_does_not_exist", None)

    assert captured.value.reason == "missing"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.parametrize(
    "failure",
    (
        RuntimeError("finder-runtime-canary"),
        SystemExit("finder-system-exit-canary"),
        KeyboardInterrupt("finder-keyboard-canary"),
        GeneratorExit("finder-generator-canary"),
    ),
)
def test_resolver_finder_baseexception_is_a_distinct_closed_internal_failure(
    monkeypatch,
    failure,
):
    def fail_finder(fullname, path=None, target=None):
        del fullname, path, target
        raise failure

    monkeypatch.setattr(
        importlib.machinery.PathFinder,
        "find_spec",
        fail_finder,
    )

    with pytest.raises(BaseException) as captured:
        resolve_strategy_source("hushine_finder_failure_canary", None)

    assert not isinstance(captured.value, StrategySourceResolutionError)
    assert captured.value.__class__.__name__ == "_StrategySourceFinderError"
    assert str(captured.value) == "strategy source resolution failed"
    assert "canary" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
