from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, dataclass
import hashlib
import importlib.machinery
import importlib.util
import json
import math
import os
from pathlib import Path
import sys
import threading
import weakref
from types import ModuleType
from typing import Callable, Literal, Protocol, TypeVar, runtime_checkable

from hushine_runtime_import_probe import (
    ExpectedProfile,
    ImportProbeResult,
    ImportRecord,
    collect_import_records,
    probe_import_records,
)

from strategy_service.indicators import IndicatorDefinition, parse_indicator_definitions
from strategy_service.inputs import (
    StrategyDeclarations,
    StrategyInput,
    StrategyOrderTarget,
    StrategyRiskControls,
    extract_declarations,
)
from strategy_service.runtime_profile import RuntimeProfile, current_runtime_profile
from strategy_service.strategy_validator import StrategyValidationIssue, validate_strategy_code


_MAX_SOURCE_BYTES = 1_048_576
_MAX_INDICATOR_DEPTH = 16
_MAX_INDICATOR_NODES = 4_096
_MAX_INDICATOR_JSON_BYTES = 65_536
_SEALED_GATE = object()
_SEALED_PREPARED = object()
_MODULE_EXECUTION_LOCK = threading.RLock()
_ACTIVE_MODULE_NAMES: dict[str, object] = {}
_MODULE_EXECUTION_STATE = threading.local()
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class CapturedFileSignature:
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    size: int


@dataclass(frozen=True)
class ResolvedStrategySource:
    filename: str
    source_bytes: bytes
    source_sha256: str
    module_name: str
    package_name: str
    is_package: bool
    package_search_locations: tuple[str, ...]
    source_kind: Literal["db", "file", "module"]
    hot_reload_path: str | None = None
    hot_reload_signature: CapturedFileSignature | None = None


@dataclass(frozen=True, slots=True)
class _ResolvedSnapshot:
    filename: str
    source_bytes: bytes
    source_sha256: str
    module_name: str
    package_name: str
    is_package: bool
    package_search_locations: tuple[str, ...]
    source_kind: str
    hot_reload_path: str | None
    hot_reload_signature_fields: tuple[int, int, int, int, int] | None


@runtime_checkable
class GatedStrategySource(Protocol):
    @property
    def resolved(self) -> ResolvedStrategySource: ...

    @property
    def runtime_contract_sha256(self) -> str: ...

    @property
    def python_invocation_path(self) -> str: ...


@runtime_checkable
class PreparedStrategy(Protocol):
    @property
    def gated_source(self) -> GatedStrategySource: ...

    @property
    def declarations(self) -> StrategyDeclarations: ...

    @property
    def indicator_definitions(self) -> tuple[IndicatorDefinition, ...]: ...


@dataclass(frozen=True)
class StrategyDependencyError(Exception):
    code: str
    module: str
    runtime_profile: str
    runtime_profile_version: str
    image_build_id: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class StrategySourceResolutionError(Exception):
    reason: Literal[
        "missing",
        "unreadable",
        "too_large",
        "invalid_utf8",
        "unsupported_source",
    ]

    def __str__(self) -> str:
        return {
            "missing": "strategy source was not found",
            "unreadable": "strategy source could not be read",
            "too_large": "strategy source exceeds the 1048576-byte limit",
            "invalid_utf8": "strategy source must be valid UTF-8",
            "unsupported_source": "strategy source format is unsupported",
        }[self.reason]


@dataclass(frozen=True, slots=True)
class StrategySourceLoadError(Exception):
    reason: Literal[
        "compile_or_exec_failed",
        "strategy_class_missing",
        "strategy_construction_failed",
        "declaration_failed",
        "binding_failed",
        "gated_source_invalid",
    ]

    def __str__(self) -> str:
        return "strategy could not be loaded"


class _StrategySourceFinderError(RuntimeError):
    __slots__ = ()

    def __str__(self) -> str:
        return "strategy source resolution failed"


def _exception_setattr(self: BaseException, name: str, value: object) -> None:
    # Traceback machinery must remain writable even though the public payload is frozen.
    if name in {"__traceback__", "__cause__", "__context__", "__suppress_context__"}:
        BaseException.__setattr__(self, name, value)
        return
    raise FrozenInstanceError(f"cannot assign to field {name!r}")


StrategyDependencyError.__setattr__ = _exception_setattr  # type: ignore[method-assign]
StrategySourceResolutionError.__setattr__ = _exception_setattr  # type: ignore[method-assign]
StrategySourceLoadError.__setattr__ = _exception_setattr  # type: ignore[method-assign]


@dataclass(frozen=True)
class StrategySourceGateResult:
    ok: bool
    issues: tuple[StrategyValidationIssue, ...]
    runtime_profile: str
    runtime_profile_version: str
    contract_sha256: str
    image_build_id: str
    dependency_error: StrategyDependencyError | None = None
    gated_source: GatedStrategySource | None = None


def _signature(stat_result: os.stat_result) -> CapturedFileSignature:
    return CapturedFileSignature(
        device=int(stat_result.st_dev),
        inode=int(stat_result.st_ino),
        mtime_ns=int(stat_result.st_mtime_ns),
        ctime_ns=int(stat_result.st_ctime_ns),
        size=int(stat_result.st_size),
    )


def _safe_resolution_error(reason: str) -> StrategySourceResolutionError:
    error = StrategySourceResolutionError(reason=reason)  # type: ignore[arg-type]
    error.__cause__ = None
    error.__context__ = None
    return error


def _decode_source(source_bytes: bytes) -> str:
    if len(source_bytes) > _MAX_SOURCE_BYTES:
        raise _safe_resolution_error("too_large")
    invalid_utf8 = False
    source = ""
    try:
        source = source_bytes.decode("utf-8")
    except UnicodeDecodeError:
        invalid_utf8 = True
    if invalid_utf8:
        raise _safe_resolution_error("invalid_utf8")
    return source


def _read_source_file(path: str, *, capture_signature: bool) -> tuple[bytes, CapturedFileSignature | None]:
    failure_reason: str | None = None
    before: CapturedFileSignature | None = None
    after: CapturedFileSignature | None = None
    source_bytes = b""
    try:
        with open(path, "rb") as source_file:
            before = _signature(os.fstat(source_file.fileno()))
            source_bytes = source_file.read(_MAX_SOURCE_BYTES + 1)
            after = _signature(os.fstat(source_file.fileno()))
    except FileNotFoundError:
        failure_reason = "missing"
    except OSError:
        failure_reason = "unreadable"
    if failure_reason is not None:
        raise _safe_resolution_error(failure_reason)
    assert before is not None and after is not None
    if before != after:
        raise _safe_resolution_error("unreadable")
    _decode_source(source_bytes)
    return source_bytes, after if capture_signature else None


def _find_source_spec(module_name: str):
    if type(module_name) is not str:
        return None
    parts = module_name.split(".")
    if not parts or any(not part or not part.isidentifier() for part in parts):
        return None
    search_path = None
    fullname = ""
    spec = None
    finder_failed = False
    try:
        for index, part in enumerate(parts):
            fullname = part if not fullname else f"{fullname}.{part}"
            spec = importlib.machinery.BuiltinImporter.find_spec(fullname)
            if spec is None:
                spec = importlib.machinery.FrozenImporter.find_spec(fullname)
            if spec is None:
                spec = importlib.machinery.PathFinder.find_spec(fullname, search_path)
            if spec is None:
                return None
            if index < len(parts) - 1:
                locations = spec.submodule_search_locations
                if locations is None:
                    return None
                search_path = tuple(str(item) for item in locations)
    except BaseException:
        finder_failed = True
    if finder_failed:
        error = _StrategySourceFinderError()
        error.__cause__ = None
        error.__context__ = None
        raise error
    return spec


def resolve_strategy_source(
    strategy_path: str,
    strategy_code: str | None,
    *,
    hot_reload: bool = False,
) -> ResolvedStrategySource:
    if type(strategy_path) is not str or type(hot_reload) is not bool:
        raise _safe_resolution_error("unsupported_source")
    if strategy_code is not None:
        if type(strategy_code) is not str:
            raise _safe_resolution_error("unsupported_source")
        invalid_utf8 = False
        source_bytes = b""
        try:
            source_bytes = strategy_code.encode("utf-8")
        except UnicodeEncodeError:
            invalid_utf8 = True
        if invalid_utf8:
            raise _safe_resolution_error("invalid_utf8")
        _decode_source(source_bytes)
        digest = hashlib.sha256(source_bytes).hexdigest()
        return ResolvedStrategySource(
            filename=strategy_path,
            source_bytes=source_bytes,
            source_sha256=digest,
            module_name=f"_hushine_strategy_{digest}",
            package_name="",
            is_package=False,
            package_search_locations=(),
            source_kind="db",
        )

    path = Path(strategy_path).expanduser()
    try:
        is_file = path.is_file()
    except OSError:
        is_file = False
    if is_file:
        absolute_path = os.path.realpath(os.path.abspath(os.fspath(path)))
        source_bytes, captured = _read_source_file(
            absolute_path,
            capture_signature=hot_reload,
        )
        digest = hashlib.sha256(source_bytes).hexdigest()
        return ResolvedStrategySource(
            filename=absolute_path,
            source_bytes=source_bytes,
            source_sha256=digest,
            module_name=f"_hushine_strategy_{digest}",
            package_name="",
            is_package=False,
            package_search_locations=(),
            source_kind="file",
            hot_reload_path=absolute_path if hot_reload else None,
            hot_reload_signature=captured,
        )

    spec = _find_source_spec(strategy_path)
    origin = getattr(spec, "origin", None) if spec is not None else None
    loader = getattr(spec, "loader", None) if spec is not None else None
    if spec is None:
        if os.path.isabs(strategy_path) or os.sep in strategy_path or strategy_path.endswith(".py"):
            raise _safe_resolution_error("missing")
        raise _safe_resolution_error("missing")
    if (
        type(origin) is not str
        or not origin.endswith(".py")
        or type(loader) is not importlib.machinery.SourceFileLoader
    ):
        raise _safe_resolution_error("unsupported_source")
    absolute_origin = os.path.realpath(os.path.abspath(origin))
    source_bytes, captured = _read_source_file(
        absolute_origin,
        capture_signature=hot_reload,
    )
    digest = hashlib.sha256(source_bytes).hexdigest()
    raw_locations = getattr(spec, "submodule_search_locations", None)
    is_package = raw_locations is not None
    locations = (
        tuple(os.path.realpath(os.path.abspath(str(item))) for item in raw_locations)
        if raw_locations is not None
        else ()
    )
    package_name = strategy_path if is_package else strategy_path.rpartition(".")[0]
    return ResolvedStrategySource(
        filename=absolute_origin,
        source_bytes=source_bytes,
        source_sha256=digest,
        module_name=strategy_path,
        package_name=package_name,
        is_package=is_package,
        package_search_locations=locations,
        source_kind="module",
        hot_reload_path=absolute_origin if hot_reload else None,
        hot_reload_signature=captured,
    )


def _profile_expectation(profile: RuntimeProfile) -> ExpectedProfile:
    return ExpectedProfile(
        name=profile.name,
        version=profile.version,
        contract_sha256=profile.contract_sha256,
    )


def _dependency_error(result: ImportProbeResult, profile: RuntimeProfile) -> StrategyDependencyError:
    if result.code == "STRATEGY_DEPENDENCY_UNAVAILABLE":
        message = (
            f"strategy dependency {result.requested_module} is unavailable in "
            f"runtime profile {profile.name}"
        )
    else:
        message = "strategy import initialization failed"
    return StrategyDependencyError(
        code=result.code,
        module=result.requested_module,
        runtime_profile=profile.name,
        runtime_profile_version=profile.version,
        image_build_id=profile.image_build_id,
        message=message,
    )


def _parse_and_collect(resolved: ResolvedStrategySource) -> tuple[ast.AST, tuple[ImportRecord, ...]]:
    try:
        snapshot = _resolved_fingerprint(resolved)
    except (TypeError, ValueError):
        raise TypeError("invalid strategy probe input") from None
    if hashlib.sha256(snapshot.source_bytes).hexdigest() != snapshot.source_sha256:
        raise TypeError("invalid strategy probe input")
    source = bytes.decode(snapshot.source_bytes, "utf-8")
    tree = ast.parse(source, filename=snapshot.filename)
    records = collect_import_records(tree)
    if type(records) is not tuple or any(type(item) is not ImportRecord for item in records):
        raise RuntimeError("invalid import collector result")
    return tree, records


def probe_strategy_imports(
    resolved: ResolvedStrategySource,
    *,
    python_invocation_path: str,
    profile: RuntimeProfile | None = None,
    timeout_seconds: float = 30.0,
) -> StrategyDependencyError | None:
    active_profile = profile or current_runtime_profile()
    _, records = _parse_and_collect(resolved)
    result = probe_import_records(
        records,
        python_invocation_path=python_invocation_path,
        expected_profile=_profile_expectation(active_profile),
        timeout_seconds=timeout_seconds,
    )
    if result.ok:
        return None
    return _dependency_error(result, active_profile)


def _probe_strategy_imports_for_test(
    resolved: ResolvedStrategySource,
    *,
    python_invocation_path: str,
    profile: RuntimeProfile | None = None,
    timeout_seconds: float = 30.0,
    extra_python_path: tuple[str, ...],
) -> StrategyDependencyError | None:
    from hushine_runtime_import_probe.protocol import _probe_import_records_for_test

    active_profile = profile or current_runtime_profile()
    _, records = _parse_and_collect(resolved)
    result = _probe_import_records_for_test(
        records,
        python_invocation_path=python_invocation_path,
        expected_profile=_profile_expectation(active_profile),
        timeout_seconds=timeout_seconds,
        extra_python_path=extra_python_path,
    )
    if result.ok:
        return None
    return _dependency_error(result, active_profile)


def _resolved_fingerprint(resolved: ResolvedStrategySource) -> _ResolvedSnapshot:
    if type(resolved) is not ResolvedStrategySource:
        raise ValueError("invalid resolved strategy source")
    if (
        type(resolved.filename) is not str
        or type(resolved.source_bytes) is not bytes
        or type(resolved.source_sha256) is not str
        or type(resolved.module_name) is not str
        or type(resolved.package_name) is not str
        or type(resolved.is_package) is not bool
        or type(resolved.package_search_locations) is not tuple
        or any(type(item) is not str for item in resolved.package_search_locations)
        or type(resolved.source_kind) is not str
        or resolved.source_kind not in {"db", "file", "module"}
        or (
            resolved.hot_reload_path is not None
            and type(resolved.hot_reload_path) is not str
        )
    ):
        raise ValueError("invalid resolved strategy source")
    signature = resolved.hot_reload_signature
    signature_fields: tuple[int, int, int, int, int] | None = None
    if signature is not None:
        if type(signature) is not CapturedFileSignature:
            raise ValueError("invalid captured file signature")
        signature_fields = (
            signature.device,
            signature.inode,
            signature.mtime_ns,
            signature.ctime_ns,
            signature.size,
        )
        if any(type(item) is not int for item in signature_fields):
            raise ValueError("invalid captured file signature")
    return _ResolvedSnapshot(
        filename=resolved.filename,
        source_bytes=resolved.source_bytes,
        source_sha256=resolved.source_sha256,
        module_name=resolved.module_name,
        package_name=resolved.package_name,
        is_package=resolved.is_package,
        package_search_locations=resolved.package_search_locations,
        source_kind=resolved.source_kind,
        hot_reload_path=resolved.hot_reload_path,
        hot_reload_signature_fields=signature_fields,
    )


def _copy_exact_snapshot(snapshot: _ResolvedSnapshot) -> _ResolvedSnapshot:
    if (
        type(snapshot) is not _ResolvedSnapshot
        or type(snapshot.filename) is not str
        or type(snapshot.source_bytes) is not bytes
        or type(snapshot.source_sha256) is not str
        or type(snapshot.module_name) is not str
        or type(snapshot.package_name) is not str
        or type(snapshot.is_package) is not bool
        or type(snapshot.package_search_locations) is not tuple
        or any(type(item) is not str for item in snapshot.package_search_locations)
        or type(snapshot.source_kind) is not str
        or snapshot.source_kind not in {"db", "file", "module"}
        or (
            snapshot.hot_reload_path is not None
            and type(snapshot.hot_reload_path) is not str
        )
        or (
            snapshot.hot_reload_signature_fields is not None
            and (
                type(snapshot.hot_reload_signature_fields) is not tuple
                or len(snapshot.hot_reload_signature_fields) != 5
                or any(
                    type(item) is not int
                    for item in snapshot.hot_reload_signature_fields
                )
            )
        )
    ):
        raise ValueError("invalid resolved strategy snapshot")
    return _ResolvedSnapshot(
        filename=snapshot.filename,
        source_bytes=snapshot.source_bytes,
        source_sha256=snapshot.source_sha256,
        module_name=snapshot.module_name,
        package_name=snapshot.package_name,
        is_package=snapshot.is_package,
        package_search_locations=tuple(
            item for item in snapshot.package_search_locations
        ),
        source_kind=snapshot.source_kind,
        hot_reload_path=snapshot.hot_reload_path,
        hot_reload_signature_fields=(
            tuple(item for item in snapshot.hot_reload_signature_fields)
            if snapshot.hot_reload_signature_fields is not None
            else None
        ),
    )


def _freeze_declaration_values(
    inputs: object,
    order_targets: object,
    risk_controls: object,
) -> tuple[
    tuple[tuple[str, str, str, str, str, str], ...],
    tuple[tuple[str, str, str], ...],
    float | None,
]:
    if type(inputs) is list:
        input_values = list.__iter__(inputs)
    elif type(inputs) is tuple:
        input_values = tuple.__iter__(inputs)
    else:
        raise ValueError("invalid strategy inputs")
    frozen_inputs: list[tuple[str, str, str, str, str, str]] = []
    for item in input_values:
        if type(item) is not StrategyInput:
            raise ValueError("invalid strategy input")
        fields = (
            item.exchange,
            item.market,
            item.symbol,
            item.interval,
            item.stream_id,
            item.kind,
        )
        if any(type(field) is not str for field in fields):
            raise ValueError("invalid strategy input")
        frozen_inputs.append(fields)

    if type(order_targets) is list:
        target_values = list.__iter__(order_targets)
    elif type(order_targets) is tuple:
        target_values = tuple.__iter__(order_targets)
    else:
        raise ValueError("invalid strategy order targets")
    frozen_targets: list[tuple[str, str, str]] = []
    for item in target_values:
        if type(item) is not StrategyOrderTarget:
            raise ValueError("invalid strategy order target")
        fields = (item.exchange, item.market, item.symbol)
        if any(type(field) is not str for field in fields):
            raise ValueError("invalid strategy order target")
        frozen_targets.append(fields)

    if type(risk_controls) is not StrategyRiskControls:
        raise ValueError("invalid strategy risk controls")
    risk_value = risk_controls.max_loss_close_pct
    if risk_value is not None and type(risk_value) is not float:
        raise ValueError("invalid strategy risk controls")
    return tuple(frozen_inputs), tuple(frozen_targets), risk_value


def _thaw_declaration_values(
    inputs: tuple[tuple[str, str, str, str, str, str], ...],
    order_targets: tuple[tuple[str, str, str], ...],
    risk_value: float | None,
) -> StrategyDeclarations:
    return StrategyDeclarations(
        inputs=[StrategyInput(*item) for item in inputs],
        order_targets=[StrategyOrderTarget(*item) for item in order_targets],
        risk_controls=StrategyRiskControls(max_loss_close_pct=risk_value),
    )


def _interpreter_identity(path: str) -> tuple[int, int] | None:
    try:
        stat_result = os.stat(path)
    except OSError:
        return None
    return int(stat_result.st_dev), int(stat_result.st_ino)


def _is_exact_interpreter_identity(value: object) -> bool:
    return value is None or (
        type(value) is tuple
        and len(value) == 2
        and type(value[0]) is int
        and type(value[1]) is int
    )


class _SealedGatedStrategySource:
    __slots__ = (
        "_seal",
        "_issuance_identity",
        "_resolved",
        "_fingerprint",
        "_runtime_contract_sha256",
        "_python_invocation_path",
        "_interpreter_identity",
        "__weakref__",
    )

    def __new__(cls, *args, **kwargs):
        del cls, args, kwargs
        raise TypeError("gated strategy sources are factory-issued")

    @property
    def resolved(self) -> ResolvedStrategySource:
        return self._resolved

    @property
    def runtime_contract_sha256(self) -> str:
        return self._runtime_contract_sha256

    @property
    def python_invocation_path(self) -> str:
        return self._python_invocation_path

    def __copy__(self):
        raise TypeError("gated strategy source is not copyable")

    def __deepcopy__(self, memo):
        del memo
        raise TypeError("gated strategy source is not copyable")

    def __reduce__(self):
        raise TypeError("gated strategy source is not serializable")


@dataclass(frozen=True, slots=True)
class _GatedIssuance:
    reference: weakref.ReferenceType[_SealedGatedStrategySource]
    identity: object
    resolved: ResolvedStrategySource
    snapshot: _ResolvedSnapshot
    visible_snapshot: _ResolvedSnapshot
    runtime_contract_sha256: str
    python_invocation_path: str
    interpreter_identity: tuple[int, int] | None


def _source_load_error(reason: str) -> StrategySourceLoadError:
    error = StrategySourceLoadError(reason=reason)  # type: ignore[arg-type]
    error.__cause__ = None
    error.__context__ = None
    return error


def _execute_fresh_module(issuance: _GatedIssuance) -> ModuleType:
    snapshot = issuance.snapshot
    module_name = snapshot.module_name
    module = ModuleType(module_name)
    source_loader = importlib.machinery.SourceFileLoader(
        module_name,
        snapshot.filename,
    )
    spec = importlib.util.spec_from_loader(
        module_name,
        loader=source_loader,
        origin=snapshot.filename,
        is_package=snapshot.is_package,
    )
    module.__name__ = module_name
    module.__package__ = snapshot.package_name
    module.__spec__ = spec
    module.__file__ = snapshot.filename
    if snapshot.is_package:
        module.__path__ = tuple(snapshot.package_search_locations)

    with _MODULE_EXECUTION_LOCK:
        module_globals = globals()
        sys_module = sys
        modules = sys_module.modules
        active_markers = _ACTIVE_MODULE_NAMES
        execution_state = _MODULE_EXECUTION_STATE
        active_stack = getattr(execution_state, "names", ())
        if (
            module_globals.get("sys") is not sys_module
            or type(modules) is not dict
            or type(active_markers) is not dict
            or type(active_stack) is not tuple
            or module_name in active_markers
            or module_name in active_stack
        ):
            raise _source_load_error("gated_source_invalid")
        prior_present = module_name in modules
        prior = modules.get(module_name)
        marker = object()
        execution_stack = (*active_stack, module_name)
        active_markers[module_name] = marker
        execution_state.names = execution_stack
        modules[module_name] = module
        failure = False
        try:
            source = bytes.decode(snapshot.source_bytes, "utf-8")
            code = compile(source, snapshot.filename, "exec")
            exec(code, module.__dict__)  # noqa: S102
            if (
                module_globals.get("sys") is not sys_module
                or sys_module.modules is not modules
                or modules.get(module_name) is not module
                or active_markers.get(module_name) is not marker
                or module_globals.get("_ACTIVE_MODULE_NAMES") is not active_markers
                or module_globals.get("_MODULE_EXECUTION_STATE")
                is not execution_state
                or getattr(execution_state, "names", None) is not execution_stack
            ):
                failure = True
        except BaseException:
            failure = True
        finally:
            active_markers.pop(module_name, None)
            module_globals["sys"] = sys_module
            sys_module.modules = modules
            module_globals["_ACTIVE_MODULE_NAMES"] = active_markers
            module_globals["_MODULE_EXECUTION_STATE"] = execution_state
            execution_state.names = active_stack
            if prior_present:
                modules[module_name] = prior  # type: ignore[assignment]
            else:
                modules.pop(module_name, None)
        if failure:
            raise _source_load_error("compile_or_exec_failed")
    return module


class _IndicatorBudget:
    __slots__ = ("nodes",)

    def __init__(self) -> None:
        self.nodes = 0


def _freeze_indicator_value(
    value: object,
    *,
    depth: int,
    ancestors: set[int],
    budget: _IndicatorBudget,
) -> object:
    budget.nodes += 1
    if budget.nodes > _MAX_INDICATOR_NODES or depth > _MAX_INDICATOR_DEPTH:
        raise ValueError("indicator config exceeds bounds")
    value_type = type(value)
    if value is None or value_type in {bool, int, str}:
        return value
    if value_type is float:
        if not math.isfinite(value):
            raise ValueError("indicator float must be finite")
        return value
    if value_type not in {dict, list}:
        raise ValueError("indicator config has unsupported value")
    identity = id(value)
    if identity in ancestors:
        raise ValueError("indicator config cycle")
    next_ancestors = set(ancestors)
    next_ancestors.add(identity)
    if value_type is list:
        return ("list", tuple(
            _freeze_indicator_value(
                item,
                depth=depth + 1,
                ancestors=next_ancestors,
                budget=budget,
            )
            for item in list.__iter__(value)
        ))
    pairs: list[tuple[str, object]] = []
    for key, item in dict.items(value):
        if type(key) is not str:
            raise ValueError("indicator config keys must be strings")
        pairs.append((key, _freeze_indicator_value(
            item,
            depth=depth + 1,
            ancestors=next_ancestors,
            budget=budget,
        )))
    return ("dict", tuple(pairs))


def _thaw_indicator_value(value: object) -> object:
    if type(value) is tuple and len(value) == 2 and value[0] == "list":
        return [_thaw_indicator_value(item) for item in value[1]]
    if type(value) is tuple and len(value) == 2 and value[0] == "dict":
        return {key: _thaw_indicator_value(item) for key, item in value[1]}
    return value


def _freeze_indicator_definition(definition: IndicatorDefinition) -> tuple[object, ...]:
    budget = _IndicatorBudget()
    config = _freeze_indicator_value(
        definition.config,
        depth=1,
        ancestors=set(),
        budget=budget,
    )
    encoded = json.dumps(
        _thaw_indicator_value(config),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > _MAX_INDICATOR_JSON_BYTES:
        raise ValueError("indicator config exceeds byte bound")
    text_fields = (
        definition.key,
        definition.name,
        definition.type,
        definition.pane,
        definition.stream_key,
        definition.color,
        definition.unit,
        definition.description,
    )
    if any(type(field) is not str for field in text_fields):
        raise ValueError("indicator definition text must be exact strings")
    return (*text_fields, config)


def _validate_raw_indicator_configs(raw: object) -> None:
    if raw is None:
        return
    if type(raw) is list:
        if len(raw) == 0:
            return
        raise ValueError("INDICATORS must be a dict")
    if type(raw) is not dict:
        raise ValueError("INDICATORS must be an exact dict")
    for key, definition in dict.items(raw):
        if type(key) is not str or type(definition) is not dict:
            raise ValueError("invalid indicator definition")
        config = dict.get(definition, "config")
        if config is None:
            continue
        _freeze_indicator_value(
            config,
            depth=1,
            ancestors=set(),
            budget=_IndicatorBudget(),
        )


def _thaw_indicator_definition(frozen: tuple[object, ...]) -> IndicatorDefinition:
    return IndicatorDefinition(
        key=frozen[0],
        name=frozen[1],
        type=frozen[2],
        pane=frozen[3],
        stream_key=frozen[4],
        color=frozen[5],
        unit=frozen[6],
        description=frozen[7],
        config=_thaw_indicator_value(frozen[8]),
    )


class _SealedPreparedStrategy:
    __slots__ = (
        "_seal",
        "_issuance_identity",
        "_gated_source",
        "_gated_fingerprint",
        "_instance",
        "_inputs",
        "_order_targets",
        "_risk_controls",
        "_indicators",
        "_lock",
        "__weakref__",
    )

    def __new__(cls, *args, **kwargs):
        del cls, args, kwargs
        raise TypeError("prepared strategies are factory-issued")

    @property
    def gated_source(self) -> GatedStrategySource:
        return _prepared_gated_source(self)

    @property
    def declarations(self) -> StrategyDeclarations:
        return _prepared_declarations(self)

    @property
    def indicator_definitions(self) -> tuple[IndicatorDefinition, ...]:
        return _prepared_indicator_definitions(self)

    def __copy__(self):
        raise TypeError("prepared strategy is not copyable")

    def __deepcopy__(self, memo):
        del memo
        raise TypeError("prepared strategy is not copyable")

    def __reduce__(self):
        raise TypeError("prepared strategy is not serializable")


@dataclass(slots=True)
class _PreparedIssuance:
    reference: weakref.ReferenceType[_SealedPreparedStrategy]
    identity: object
    gated_source: _SealedGatedStrategySource
    gated_snapshot: _ResolvedSnapshot
    visible_gated_snapshot: _ResolvedSnapshot
    instance: object
    inputs: tuple[tuple[str, str, str, str, str, str], ...]
    order_targets: tuple[tuple[str, str, str], ...]
    risk_value: float | None
    visible_inputs: tuple[StrategyInput, ...]
    visible_order_targets: tuple[StrategyOrderTarget, ...]
    visible_risk_controls: StrategyRiskControls
    indicators: tuple[tuple[object, ...], ...]
    lock: threading.Lock
    state: str


def _build_capability_api():
    # Weakref callbacks can run synchronously while registry operations release
    # the last reference held by an issuance.  The authority must therefore be
    # safe for same-thread cleanup re-entry.
    gated_issuance_lock = threading.RLock()
    gated_issuances: dict[int, _GatedIssuance] = {}
    prepared_issuance_lock = threading.RLock()
    prepared_issuances: dict[int, _PreparedIssuance] = {}

    def lookup_gated_issuance(
        gated_source: object,
    ) -> _GatedIssuance | None:
        if type(gated_source) is not _SealedGatedStrategySource:
            return None
        with gated_issuance_lock:
            issuance = gated_issuances.get(id(gated_source))
            if issuance is None or issuance.reference() is not gated_source:
                return None
            return issuance

    def issue_gated_source(
        resolved: ResolvedStrategySource,
        snapshot: _ResolvedSnapshot,
        *,
        runtime_contract_sha256: str,
        python_invocation_path: str,
    ) -> _SealedGatedStrategySource:
        gated = object.__new__(_SealedGatedStrategySource)
        identity = object()
        interpreter_identity = _interpreter_identity(python_invocation_path)
        authoritative_snapshot = _copy_exact_snapshot(snapshot)
        visible_snapshot = _copy_exact_snapshot(snapshot)
        object.__setattr__(gated, "_seal", _SEALED_GATE)
        object.__setattr__(gated, "_issuance_identity", identity)
        object.__setattr__(gated, "_resolved", resolved)
        object.__setattr__(gated, "_fingerprint", visible_snapshot)
        object.__setattr__(
            gated,
            "_runtime_contract_sha256",
            runtime_contract_sha256,
        )
        object.__setattr__(gated, "_python_invocation_path", python_invocation_path)
        object.__setattr__(gated, "_interpreter_identity", interpreter_identity)
        identity_key = id(gated)

        def discard_issuance(
            reference: weakref.ReferenceType[_SealedGatedStrategySource],
        ) -> None:
            with gated_issuance_lock:
                current = gated_issuances.get(identity_key)
                if current is not None and current.reference is reference:
                    gated_issuances.pop(identity_key, None)

        reference = weakref.ref(gated, discard_issuance)
        issuance = _GatedIssuance(
            reference=reference,
            identity=identity,
            resolved=resolved,
            snapshot=authoritative_snapshot,
            visible_snapshot=visible_snapshot,
            runtime_contract_sha256=runtime_contract_sha256,
            python_invocation_path=python_invocation_path,
            interpreter_identity=interpreter_identity,
        )
        with gated_issuance_lock:
            gated_issuances[identity_key] = issuance
        return gated

    def validate_gated(gated_source: GatedStrategySource) -> _GatedIssuance:
        if type(gated_source) is not _SealedGatedStrategySource:
            raise _source_load_error("gated_source_invalid")
        issuance = lookup_gated_issuance(gated_source)
        if issuance is None:
            raise _source_load_error("gated_source_invalid")
        invalid = False
        try:
            current_fingerprint = _resolved_fingerprint(gated_source._resolved)
            visible_fingerprint = _copy_exact_snapshot(
                gated_source._fingerprint
            )
            invalid = (
                gated_source._seal is not _SEALED_GATE
                or gated_source._issuance_identity is not issuance.identity
                or gated_source._resolved is not issuance.resolved
                or gated_source._fingerprint is not issuance.visible_snapshot
                or visible_fingerprint != issuance.snapshot
                or issuance.snapshot != current_fingerprint
                or hashlib.sha256(issuance.snapshot.source_bytes).hexdigest()
                != issuance.snapshot.source_sha256
                or type(gated_source._runtime_contract_sha256) is not str
                or gated_source._runtime_contract_sha256
                != issuance.runtime_contract_sha256
                or current_runtime_profile().contract_sha256
                != issuance.runtime_contract_sha256
                or type(gated_source._python_invocation_path) is not str
                or gated_source._python_invocation_path
                != issuance.python_invocation_path
                or not _is_exact_interpreter_identity(issuance.interpreter_identity)
                or not _is_exact_interpreter_identity(
                    gated_source._interpreter_identity
                )
                or _interpreter_identity(gated_source._python_invocation_path)
                != issuance.interpreter_identity
                or gated_source._interpreter_identity
                != issuance.interpreter_identity
            )
        except BaseException:
            invalid = True
        if invalid:
            raise _source_load_error("gated_source_invalid")
        return issuance

    def gate_strategy_source(
        resolved: ResolvedStrategySource,
        *,
        python_invocation_path: str,
    ) -> StrategySourceGateResult:
        if (
            type(resolved) is not ResolvedStrategySource
            or type(python_invocation_path) is not str
        ):
            raise TypeError("invalid strategy gate input")
        try:
            fingerprint = _resolved_fingerprint(resolved)
        except (TypeError, ValueError):
            raise TypeError("invalid strategy gate input") from None
        if (
            hashlib.sha256(fingerprint.source_bytes).hexdigest()
            != fingerprint.source_sha256
        ):
            raise TypeError("invalid strategy gate input")
        profile = current_runtime_profile()
        invocation = os.path.abspath(os.path.normpath(python_invocation_path))
        source = bytes.decode(fingerprint.source_bytes, "utf-8")
        validation = validate_strategy_code(source)
        issues = tuple(validation.issues)
        if issues:
            dependency_issue = next(
                (
                    item
                    for item in issues
                    if item.code == "UNSUPPORTED_STRATEGY_DEPENDENCY"
                ),
                None,
            )
            dependency_error = None
            if dependency_issue is not None:
                dependency_error = StrategyDependencyError(
                    code="UNSUPPORTED_STRATEGY_DEPENDENCY",
                    module=dependency_issue.module,
                    runtime_profile=profile.name,
                    runtime_profile_version=profile.version,
                    image_build_id=profile.image_build_id,
                    message=(
                        "strategy dependency is not supported by the runtime profile"
                    ),
                )
            return StrategySourceGateResult(
                ok=False,
                issues=issues,
                runtime_profile=profile.name,
                runtime_profile_version=profile.version,
                contract_sha256=profile.contract_sha256,
                image_build_id=profile.image_build_id,
                dependency_error=dependency_error,
            )
        dependency_error = probe_strategy_imports(
            resolved,
            python_invocation_path=invocation,
            profile=profile,
        )
        if dependency_error is not None:
            return StrategySourceGateResult(
                ok=False,
                issues=(),
                runtime_profile=profile.name,
                runtime_profile_version=profile.version,
                contract_sha256=profile.contract_sha256,
                image_build_id=profile.image_build_id,
                dependency_error=dependency_error,
            )
        gated = issue_gated_source(
            resolved,
            fingerprint,
            runtime_contract_sha256=profile.contract_sha256,
            python_invocation_path=invocation,
        )
        return StrategySourceGateResult(
            ok=True,
            issues=(),
            runtime_profile=profile.name,
            runtime_profile_version=profile.version,
            contract_sha256=profile.contract_sha256,
            image_build_id=profile.image_build_id,
            gated_source=gated,
        )

    def lookup_prepared_issuance(
        prepared: object,
    ) -> _PreparedIssuance | None:
        if type(prepared) is not _SealedPreparedStrategy:
            return None
        with prepared_issuance_lock:
            issuance = prepared_issuances.get(id(prepared))
            if issuance is None or issuance.reference() is not prepared:
                return None
            return issuance

    def issue_prepared_strategy(
        gated_source: _SealedGatedStrategySource,
        gated_snapshot: _ResolvedSnapshot,
        instance: object,
        declarations: StrategyDeclarations,
        indicators: tuple[tuple[object, ...], ...],
    ) -> _SealedPreparedStrategy:
        prepared = object.__new__(_SealedPreparedStrategy)
        identity = object()
        inputs, order_targets, risk_value = _freeze_declaration_values(
            declarations.inputs,
            declarations.order_targets,
            declarations.risk_controls,
        )
        authoritative_snapshot = _copy_exact_snapshot(gated_snapshot)
        visible_snapshot = _copy_exact_snapshot(gated_snapshot)
        visible_declarations = _thaw_declaration_values(
            inputs,
            order_targets,
            risk_value,
        )
        visible_inputs = tuple(visible_declarations.inputs)
        visible_order_targets = tuple(visible_declarations.order_targets)
        visible_risk_controls = visible_declarations.risk_controls
        lock = threading.Lock()
        object.__setattr__(prepared, "_seal", _SEALED_PREPARED)
        object.__setattr__(prepared, "_issuance_identity", identity)
        object.__setattr__(prepared, "_gated_source", gated_source)
        object.__setattr__(prepared, "_gated_fingerprint", visible_snapshot)
        object.__setattr__(prepared, "_instance", instance)
        object.__setattr__(prepared, "_inputs", visible_inputs)
        object.__setattr__(prepared, "_order_targets", visible_order_targets)
        object.__setattr__(prepared, "_risk_controls", visible_risk_controls)
        object.__setattr__(prepared, "_indicators", indicators)
        object.__setattr__(prepared, "_lock", lock)
        identity_key = id(prepared)

        def discard_issuance(
            reference: weakref.ReferenceType[_SealedPreparedStrategy],
        ) -> None:
            with prepared_issuance_lock:
                current = prepared_issuances.get(identity_key)
                if current is not None and current.reference is reference:
                    prepared_issuances.pop(identity_key, None)

        reference = weakref.ref(prepared, discard_issuance)
        issuance = _PreparedIssuance(
            reference=reference,
            identity=identity,
            gated_source=gated_source,
            gated_snapshot=authoritative_snapshot,
            visible_gated_snapshot=visible_snapshot,
            instance=instance,
            inputs=inputs,
            order_targets=order_targets,
            risk_value=risk_value,
            visible_inputs=visible_inputs,
            visible_order_targets=visible_order_targets,
            visible_risk_controls=visible_risk_controls,
            indicators=indicators,
            lock=lock,
            state="UNCLAIMED",
        )
        with prepared_issuance_lock:
            prepared_issuances[identity_key] = issuance
        return prepared

    def prepared_gated_source(
        prepared: _SealedPreparedStrategy,
    ) -> GatedStrategySource:
        issuance = lookup_prepared_issuance(prepared)
        if issuance is None:
            raise TypeError("invalid prepared strategy")
        return issuance.gated_source

    def prepared_declarations(
        prepared: _SealedPreparedStrategy,
    ) -> StrategyDeclarations:
        issuance = lookup_prepared_issuance(prepared)
        if issuance is None:
            raise TypeError("invalid prepared strategy")
        return _thaw_declaration_values(
            issuance.inputs,
            issuance.order_targets,
            issuance.risk_value,
        )

    def prepared_indicator_definitions(
        prepared: _SealedPreparedStrategy,
    ) -> tuple[IndicatorDefinition, ...]:
        issuance = lookup_prepared_issuance(prepared)
        if issuance is None:
            raise TypeError("invalid prepared strategy")
        return tuple(
            _thaw_indicator_definition(item) for item in issuance.indicators
        )

    def prepare_strategy(gated_source: GatedStrategySource) -> PreparedStrategy:
        gated_issuance = validate_gated(gated_source)
        module = _execute_fresh_module(gated_issuance)
        class_lookup_failed = False
        strategy_class = None
        try:
            strategy_class = module.__dict__.get("MyStrategy")
        except BaseException:
            class_lookup_failed = True
        if class_lookup_failed or type(strategy_class) is not type:
            raise _source_load_error("strategy_class_missing")
        failed = False
        try:
            instance = strategy_class()
        except BaseException:
            failed = True
            instance = None
        if failed:
            raise _source_load_error("strategy_construction_failed")
        declaration_failed = False
        declarations = None
        frozen_indicators: tuple[tuple[object, ...], ...] = ()
        try:
            declarations = extract_declarations(instance)
            raw_indicators = getattr(instance, "INDICATORS", None)
            _validate_raw_indicator_configs(raw_indicators)
            definitions = parse_indicator_definitions(raw_indicators)
            frozen_indicators = tuple(
                _freeze_indicator_definition(item) for item in definitions
            )
        except BaseException:
            declaration_failed = True
        if declaration_failed or declarations is None:
            raise _source_load_error("declaration_failed")
        return issue_prepared_strategy(
            gated_source,
            gated_issuance.snapshot,
            instance,
            declarations,
            frozen_indicators,
        )

    def claim_prepared_strategy(
        prepared: PreparedStrategy,
        binder: Callable[
            [
                object,
                StrategyDeclarations,
                tuple[IndicatorDefinition, ...],
                GatedStrategySource,
            ],
            _T,
        ],
    ) -> _T:
        if type(prepared) is not _SealedPreparedStrategy:
            raise _source_load_error("gated_source_invalid")
        issuance = lookup_prepared_issuance(prepared)
        if issuance is None:
            raise _source_load_error("gated_source_invalid")
        with issuance.lock:
            if issuance.state != "UNCLAIMED":
                raise _source_load_error("gated_source_invalid")
            issuance.state = "CLAIMING"
        try:
            visible_snapshot = _copy_exact_snapshot(
                prepared._gated_fingerprint
            )
            visible_declarations = _freeze_declaration_values(
                prepared._inputs,
                prepared._order_targets,
                prepared._risk_controls,
            )
            if (
                prepared._seal is not _SEALED_PREPARED
                or prepared._issuance_identity is not issuance.identity
                or prepared._gated_source is not issuance.gated_source
                or prepared._gated_fingerprint
                is not issuance.visible_gated_snapshot
                or visible_snapshot != issuance.gated_snapshot
                or prepared._instance is not issuance.instance
                or prepared._inputs is not issuance.visible_inputs
                or prepared._order_targets is not issuance.visible_order_targets
                or prepared._risk_controls is not issuance.visible_risk_controls
                or visible_declarations
                != (
                    issuance.inputs,
                    issuance.order_targets,
                    issuance.risk_value,
                )
                or prepared._indicators is not issuance.indicators
                or prepared._lock is not issuance.lock
            ):
                raise _source_load_error("gated_source_invalid")
            gated_issuance = validate_gated(issuance.gated_source)
            if gated_issuance.snapshot != issuance.gated_snapshot:
                raise _source_load_error("gated_source_invalid")
            declarations = _thaw_declaration_values(
                issuance.inputs,
                issuance.order_targets,
                issuance.risk_value,
            )
            indicators = tuple(
                _thaw_indicator_definition(item) for item in issuance.indicators
            )
            result = binder(
                issuance.instance,
                declarations,
                indicators,
                issuance.gated_source,
            )
        except BaseException:
            with issuance.lock:
                issuance.state = "INVALID"
            raise
        with issuance.lock:
            issuance.state = "CLAIMED"
        return result

    def is_sealed_prepared_strategy(value: object) -> bool:
        return type(value) is _SealedPreparedStrategy

    return (
        gate_strategy_source,
        prepare_strategy,
        claim_prepared_strategy,
        is_sealed_prepared_strategy,
        prepared_gated_source,
        prepared_declarations,
        prepared_indicator_definitions,
    )


(
    gate_strategy_source,
    prepare_strategy,
    _claim_prepared_strategy,
    _is_sealed_prepared_strategy,
    _prepared_gated_source,
    _prepared_declarations,
    _prepared_indicator_definitions,
) = _build_capability_api()
del _build_capability_api


__all__ = [
    "CapturedFileSignature",
    "GatedStrategySource",
    "PreparedStrategy",
    "ResolvedStrategySource",
    "StrategyDependencyError",
    "StrategySourceGateResult",
    "StrategySourceLoadError",
    "StrategySourceResolutionError",
    "gate_strategy_source",
    "prepare_strategy",
    "probe_strategy_imports",
    "resolve_strategy_source",
]
