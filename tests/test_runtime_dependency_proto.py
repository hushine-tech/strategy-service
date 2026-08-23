from __future__ import annotations

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest
from google.protobuf import descriptor_pb2

from strategy_service.gen import control_panel_service_pb2 as control_pb2
from strategy_service.gen import runtime_worker_pb2 as worker_pb2
from strategy_service.gen import strategy_service_pb2 as strategy_pb2


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
GENERATE_PROTO = REPOSITORY_ROOT / "generate_proto.sh"


def _message(module: object, name: str):
    descriptor = module.DESCRIPTOR.message_types_by_name.get(name)
    assert descriptor is not None, f"{module.DESCRIPTOR.package}.{name} is missing"
    return descriptor


def _assert_field_numbers(descriptor, expected: dict[str, int]) -> None:
    actual = {field.name: field.number for field in descriptor.fields}
    assert actual == expected


def _assert_message_field(
    descriptor,
    *,
    name: str,
    number: int,
    message_type: str,
) -> None:
    field = descriptor.fields_by_name.get(name)
    assert field is not None, f"{descriptor.full_name}.{name} is missing"
    assert field.number == number
    assert field.message_type is not None
    assert field.message_type.full_name == message_type


def _assert_method(
    module: object,
    *,
    service_name: str,
    method_name: str,
    input_type: str,
    output_type: str,
) -> None:
    service = module.DESCRIPTOR.services_by_name.get(service_name)
    assert service is not None, f"{service_name} is missing"
    method = service.methods_by_name.get(method_name)
    assert method is not None, f"{service_name}.{method_name} is missing"
    assert method.input_type.full_name == input_type
    assert method.output_type.full_name == output_type


def _worker_message_proto(name: str) -> descriptor_pb2.DescriptorProto:
    file_proto = descriptor_pb2.FileDescriptorProto.FromString(
        worker_pb2.DESCRIPTOR.serialized_pb
    )
    for message in file_proto.message_type:
        if message.name == name:
            return message
    raise AssertionError(f"runtime.worker.v1.{name} is missing")


def _descriptor_reserves(name: str, number: int) -> bool:
    message = _worker_message_proto(name)
    return any(item.start <= number < item.end for item in message.reserved_range)


def _descriptor_reserves_name(name: str, field_name: str) -> bool:
    return field_name in _worker_message_proto(name).reserved_name


def test_shared_dependency_messages_and_validate_rpc_are_exact():
    _assert_field_numbers(
        _message(strategy_pb2, "RuntimeDependencyProfile"),
        {
            "schema_version": 1,
            "profile_name": 2,
            "profile_version": 3,
            "contract_sha256": 4,
            "hosted_python": 5,
            "public_import_roots": 6,
            "strategy_service_commit": 7,
            "strategy_library_commit": 8,
            "image_build_id": 9,
        },
    )
    _assert_field_numbers(
        _message(strategy_pb2, "RuntimeDependencyError"),
        {
            "code": 1,
            "module": 2,
            "runtime_profile": 3,
            "runtime_profile_version": 4,
            "image_build_id": 5,
            "message": 6,
        },
    )
    _assert_field_numbers(
        _message(strategy_pb2, "StrategyValidationIssueProto"),
        {"code": 1, "message": 2, "module": 3, "line": 4, "symbol": 5},
    )
    _assert_field_numbers(
        _message(strategy_pb2, "ValidateStrategySourceRequest"),
        {
            "source": 1,
            "user_id": 100,
            "runtime_id": 101,
            "include_declarations": 102,
        },
    )
    _assert_field_numbers(
        _message(strategy_pb2, "StrategyInputDeclaration"),
        {
            "stream_id": 1,
            "exchange": 2,
            "market": 3,
            "kind": 4,
            "symbol": 5,
            "interval": 6,
        },
    )
    response = _message(strategy_pb2, "ValidateStrategySourceResponse")
    _assert_field_numbers(
        response,
        {
            "ok": 1,
            "issues": 2,
            "runtime_profile": 3,
            "declared_inputs": 4,
            "declared_order_targets": 5,
        },
    )
    _assert_message_field(
        response,
        name="runtime_profile",
        number=3,
        message_type="strategy.v1.RuntimeDependencyProfile",
    )
    _assert_message_field(
        response,
        name="declared_inputs",
        number=4,
        message_type="strategy.v1.StrategyInputDeclaration",
    )
    _assert_message_field(
        response,
        name="declared_order_targets",
        number=5,
        message_type="strategy.v1.StrategyOrderTargetBinding",
    )
    _assert_method(
        strategy_pb2,
        service_name="StrategyService",
        method_name="ValidateStrategySource",
        input_type="strategy.v1.ValidateStrategySourceRequest",
        output_type="strategy.v1.ValidateStrategySourceResponse",
    )


def test_validate_source_rpc_exists():
    request = strategy_pb2.ValidateStrategySourceRequest(
        source="import numpy",
        user_id=7,
        runtime_id="rt-1",
        include_declarations=True,
    )
    assert request.runtime_id == "rt-1"
    assert request.include_declarations is True


def test_worker_dependency_fields_and_indicator_evolution_are_exact():
    for message_name, number in {
        "SessionProgress": 6,
        "PlatformCallResult": 5,
        "FinalStatus": 5,
        "WorkerError": 5,
    }.items():
        _assert_message_field(
            _message(worker_pb2, message_name),
            name="dependency_error",
            number=number,
            message_type="strategy.v1.RuntimeDependencyError",
        )

    frame = _message(worker_pb2, "WorkerFrame")
    fields = frame.fields_by_name
    hello = _message(worker_pb2, "WorkerHello")
    assert hello.fields_by_name["protocol_version"].number == 5
    assert worker_pb2.WorkerHello(protocol_version=2).protocol_version == 2
    assert "indicator_frame" not in fields
    assert fields["indicator_frame_v2"].number == 21
    assert _descriptor_reserves("WorkerFrame", 15)
    assert _descriptor_reserves_name("WorkerFrame", "indicator_frame")
    assert "IndicatorValue" not in worker_pb2.DESCRIPTOR.message_types_by_name
    assert "IndicatorFrame" not in worker_pb2.DESCRIPTOR.message_types_by_name

    final_fields = _message(worker_pb2, "FinalStatus").fields_by_name
    if "reconciliation_run_id" in final_fields:
        assert final_fields["reconciliation_run_id"].number == 6


def test_control_panel_dependency_admission_contract_is_exact():
    _assert_message_field(
        _message(control_pb2, "RuntimeHello"),
        name="dependency_profile",
        number=15,
        message_type="strategy.v1.RuntimeDependencyProfile",
    )
    _assert_message_field(
        _message(control_pb2, "RuntimeResume"),
        name="dependency_profile",
        number=4,
        message_type="strategy.v1.RuntimeDependencyProfile",
    )
    _assert_message_field(
        _message(control_pb2, "StreamError"),
        name="dependency_error",
        number=3,
        message_type="strategy.v1.RuntimeDependencyError",
    )
    startup_request = _message(control_pb2, "ReportRuntimeStartupFailureRequest")
    _assert_field_numbers(
        startup_request,
        {
            "key_id": 1,
            "runtime_id": 2,
            "source": 3,
            "issued_at_unix_ms": 4,
            "nonce": 5,
            "dependency_error": 6,
            "actual_profile": 7,
            "signature": 8,
        },
    )
    _assert_message_field(
        startup_request,
        name="dependency_error",
        number=6,
        message_type="strategy.v1.RuntimeDependencyError",
    )
    _assert_message_field(
        startup_request,
        name="actual_profile",
        number=7,
        message_type="strategy.v1.RuntimeDependencyProfile",
    )
    _assert_field_numbers(
        _message(control_pb2, "ReportRuntimeStartupFailureResponse"),
        {"recorded": 1},
    )
    _assert_method(
        control_pb2,
        service_name="ControlPanelService",
        method_name="ValidateStrategySource",
        input_type="strategy.v1.ValidateStrategySourceRequest",
        output_type="strategy.v1.ValidateStrategySourceResponse",
    )
    _assert_method(
        control_pb2,
        service_name="ControlPanelService",
        method_name="ReportRuntimeStartupFailure",
        input_type="controlpanel.v1.ReportRuntimeStartupFailureRequest",
        output_type="controlpanel.v1.ReportRuntimeStartupFailureResponse",
    )


def test_generated_runtime_worker_imports_outside_repository_cwd(tmp_path: Path):
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT)
    completed = subprocess.run(
        [sys.executable, "-c", "import strategy_service.gen.runtime_worker_pb2"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _generator_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "workspace"
    strategy = root / "strategy-service"
    core_proto = root / "core-service" / "proto"
    control_proto = root / "control-panel-service" / "proto"
    strategy_proto = strategy / "proto"
    for directory in (core_proto, control_proto, strategy_proto):
        directory.mkdir(parents=True, exist_ok=True)
    shutil.copy2(GENERATE_PROTO, strategy / "generate_proto.sh")

    protos = {
        core_proto / "portfolio_service.proto": "core.portfolio.v1",
        core_proto / "order_service.proto": "core.order.v1",
        control_proto / "control_panel_service.proto": "controlpanel.v1",
        control_proto / "marketdata_service.proto": "controlpanel.marketdata.v1",
        strategy_proto / "strategy_service.proto": "strategy.v1",
        strategy_proto / "runtime_worker.proto": "runtime.worker.v1",
    }
    for path, package in protos.items():
        path.write_text(
            f'syntax = "proto3"; package {package}; message Placeholder {{}}\n',
            encoding="utf-8",
        )

    fake_bin = root / "fake-bin"
    fake_bin.mkdir()
    log = root / "python-selection.log"
    quoted_interpreter = shlex.quote(sys.executable)
    for name, marker in (("python3", "fallback"), ("python-override", "override")):
        _write_executable(
            fake_bin / name,
            "#!/bin/sh\n"
            f"printf '%s\\n' {shlex.quote(marker)} >> \"$HUSHINE_TEST_PYTHON_LOG\"\n"
            f"exec {quoted_interpreter} \"$@\"\n",
        )
    for name in ("protoc", "protoc-gen-go", "protoc-gen-go-grpc"):
        _write_executable(fake_bin / name, "#!/bin/sh\nexit 0\n")
    return strategy, fake_bin, log


@pytest.mark.parametrize(
    ("use_override", "expected_marker"),
    [(False, "fallback"), (True, "override")],
)
def test_generate_proto_selects_portable_python(
    tmp_path: Path,
    use_override: bool,
    expected_marker: str,
):
    strategy, fake_bin, log = _generator_fixture(tmp_path)
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["HOME"] = str(tmp_path / "home")
    environment["HUSHINE_TEST_PYTHON_LOG"] = str(log)
    if use_override:
        environment["PYTHON"] = str(fake_bin / "python-override")
    else:
        environment.pop("PYTHON", None)

    completed = subprocess.run(
        ["bash", str(strategy / "generate_proto.sh")],
        cwd=strategy,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    selected = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    assert selected
    assert set(selected) == {expected_marker}
