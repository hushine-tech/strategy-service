import inspect

import pytest
from google.protobuf.any_pb2 import Any as ProtoAny
from google.protobuf import descriptor_pb2

from strategy_service.gen import runtime_worker_pb2 as pb2
from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service import session_worker_entry


def _strategy_message(name: str):
    message = strategy_pb2.DESCRIPTOR.message_types_by_name.get(name)
    assert message is not None, f"strategy.v1.{name} is missing"
    return message


def test_worker_hello_carries_session_and_token():
    msg = pb2.WorkerHello(
        session_id="sess-1",
        token="token-1",
        worker_version="test",
        pid=123,
        protocol_version=2,
    )

    assert msg.session_id == "sess-1"
    assert msg.token == "token-1"
    assert msg.pid == 123
    assert msg.protocol_version == 2


def test_indicator_v2_frame_carries_sequence_scalar_and_typed_markers():
    frame = pb2.IndicatorFrameV2(
        session_id="sess-1",
        user_id=7,
        strategy_id=11,
        stream_key="binance:perpetual_futures:ZECUSDT:1m",
        stream_sequence=9,
        market_time_ms=99_000,
        interval_ms=60_000,
        samples=[
            pb2.IndicatorSampleV2(indicator_key="bb_mid", scalar_value=10.5),
            pb2.IndicatorSampleV2(
                indicator_key="trade_signal",
                markers=[
                    pb2.IndicatorMarkerV2(
                        text="BUY",
                        price=100.5,
                        color="#16a34a",
                        position="belowBar",
                        shape="arrowUp",
                    )
                ],
            ),
        ],
    )

    assert frame.stream_sequence == 9
    assert frame.samples[0].HasField("scalar_value")
    assert frame.samples[0].scalar_value == 10.5
    assert frame.samples[1].markers[0].text == "BUY"
    assert frame.samples[1].markers[0].HasField("price")


def test_worker_frame_reserves_removed_v1_tag_and_name():
    fields = pb2.WorkerFrame.DESCRIPTOR.fields_by_name

    assert "indicator_frame" not in fields
    assert fields["indicator_frame_v2"].number == 21
    assert not hasattr(pb2, "IndicatorValue")
    assert not hasattr(pb2, "IndicatorFrame")

    file_proto = descriptor_pb2.FileDescriptorProto.FromString(
        pb2.DESCRIPTOR.serialized_pb
    )
    worker_frame = next(
        message for message in file_proto.message_type if message.name == "WorkerFrame"
    )
    assert any(item.start <= 15 < item.end for item in worker_frame.reserved_range)
    assert "indicator_frame" in worker_frame.reserved_name


def test_dependency_error_fields_survive_worker_progress():
    detail = strategy_pb2.RuntimeDependencyError(
        code="STRATEGY_DEPENDENCY_UNAVAILABLE",
        module="google.cloud",
        runtime_profile="platform-python-3.13",
        runtime_profile_version="1.0.0",
        image_build_id="build-1",
        message="module unavailable",
    )
    progress = pb2.SessionProgress(
        session_id="s1", status="failed", dependency_error=detail
    )

    assert progress.dependency_error == detail


def test_strategy_target_binding_and_run_result_contract_is_additive():
    target = _strategy_message("StrategyOrderTargetBinding")
    assert {field.name: field.number for field in target.fields} == {
        "exchange": 1,
        "market": 2,
        "symbol": 3,
        "effective_leverage": 4,
        "leverage_source": 5,
        "current_leverage": 6,
        "change_required": 7,
        "venue_id": 8,
        "leverage_status": 9,
    }
    assert target.fields_by_name["current_leverage"].has_presence

    response = _strategy_message("RunStrategyResponse")
    assert {field.name: field.number for field in response.fields} == {
        "session_id": 1,
        "ok": 2,
        "failures": 3,
        "target_results": 4,
        "code": 5,
        "rollback_failed": 6,
    }
    assert response.fields_by_name["failures"].is_repeated
    assert response.fields_by_name["failures"].message_type.full_name == (
        "strategy.v1.PreflightFailureProto"
    )
    assert response.fields_by_name["target_results"].is_repeated
    assert response.fields_by_name["target_results"].message_type.full_name == (
        "strategy.v1.StrategyLeverageTargetResult"
    )


def test_prepare_start_manifest_and_bootstrap_are_typed_and_secret_free():
    service = strategy_pb2.DESCRIPTOR.services_by_name["StrategyService"]
    method = service.methods_by_name.get("PrepareRunStrategyStart")
    assert method is not None
    assert method.input_type.full_name == "strategy.v1.PrepareRunStrategyStartRequest"
    assert method.output_type.full_name == "strategy.v1.PreparedRunStrategyStart"

    request = _strategy_message("PrepareRunStrategyStartRequest")
    assert {field.name: field.number for field in request.fields} == {
        "run_request": 1,
        "session_id": 2,
        "launch_operation_id": 3,
    }
    assert request.fields_by_name["run_request"].message_type.full_name == (
        "strategy.v1.RunStrategyRequest"
    )

    prepared = _strategy_message("PreparedRunStrategyStart")
    assert {field.name: field.number for field in prepared.fields} == {
        "ok": 1,
        "session": 2,
        "launch_operation_id": 3,
        "strategy_source_sha256": 4,
        "declared_inputs": 5,
        "declared_order_targets": 6,
        "required_routes": 7,
        "required_symbols": 8,
        "preflight": 9,
        "risk_controls": 10,
        "failures": 11,
    }
    assert prepared.fields_by_name["required_routes"].is_repeated
    assert prepared.fields_by_name["required_symbols"].is_repeated
    for name, message_type in {
        "session": "strategy.v1.StrategySessionMetadata",
        "declared_inputs": "strategy.v1.StrategyInputDeclaration",
        "declared_order_targets": "strategy.v1.StrategyOrderTargetBinding",
        "required_routes": "strategy.v1.StrategyRouteBinding",
        "required_symbols": "strategy.v1.StrategyRequiredSymbolBinding",
        "preflight": "strategy.v1.PreviewRunStrategyResponse",
        "risk_controls": "strategy.v1.RiskControls",
        "failures": "strategy.v1.PreflightFailureProto",
    }.items():
        assert prepared.fields_by_name[name].message_type.full_name == message_type

    session = _strategy_message("StrategySessionMetadata")
    assert {field.name: field.number for field in session.fields} == {
        "session_id": 1,
        "portfolio_id": 2,
        "strategy_id": 3,
        "environment": 4,
        "interval": 5,
        "start_time_ms": 6,
        "end_time_ms": 7,
        "user_id": 8,
        "runtime_id": 9,
        "runtime_source": 10,
        "runtime_name": 11,
        "session_type": 12,
        "runtime_version": 13,
        "session_name": 14,
        "initial_status": 15,
    }
    required_symbol = _strategy_message("StrategyRequiredSymbolBinding")
    assert {field.name: field.number for field in required_symbol.fields} == {
        "exchange": 1,
        "market": 2,
        "symbol": 3,
        "order_target": 4,
        "required_order_types": 5,
        "effective_leverage": 6,
        "leverage_source": 7,
    }

    bootstrap = _strategy_message("StrategySessionBootstrap")
    assert {field.name: field.number for field in bootstrap.fields} == {
        "session_id": 1,
        "launch_operation_id": 2,
        "strategy_source_sha256": 3,
        "confirmed_target_facts": 4,
        "environment": 5,
    }
    assert bootstrap.fields_by_name["confirmed_target_facts"].is_repeated
    forbidden = {
        "credentials",
        "credential_fingerprint",
        "venue_secrets",
        "database_url",
        "kafka_url",
        "order_endpoint",
    }
    assert forbidden.isdisjoint(bootstrap.fields_by_name)

    confirmed = _strategy_message("StrategySessionTargetLeverageFact")
    assert {field.name: field.number for field in confirmed.fields} == {
        "venue_id": 1,
        "exchange": 2,
        "environment": 3,
        "market": 4,
        "symbol": 5,
        "effective_leverage": 6,
        "leverage_source": 7,
        "previous_leverage": 8,
        "confirmed_leverage": 9,
        "confirmed_at_unix_ms": 10,
    }
    assert confirmed.fields_by_name["previous_leverage"].has_presence

    result = _strategy_message("StrategyLeverageTargetResult")
    assert {field.name: field.number for field in result.fields} == {
        "venue_id": 1,
        "exchange": 2,
        "market": 3,
        "symbol": 4,
        "effective_leverage": 5,
        "leverage_source": 6,
        "previous_leverage": 7,
        "current_leverage": 8,
        "confirmed_leverage": 9,
        "change_required": 10,
        "status": 11,
        "error_code": 12,
        "error_message": 13,
        "retryable": 14,
    }
    for name in ("previous_leverage", "current_leverage", "confirmed_leverage"):
        assert result.fields_by_name[name].has_presence


def test_start_without_typed_bootstrap_is_rejected():
    start = pb2.StartSession(session_id="1" * 32)

    with pytest.raises(RuntimeError, match="bootstrap is required"):
        session_worker_entry._validated_start_bootstrap(start)


def test_start_bootstrap_validation_has_no_optional_compatibility_switch():
    assert "required" not in inspect.signature(
        session_worker_entry._validated_start_bootstrap
    ).parameters


def test_new_protocol_start_rejects_bootstrap_identity_mismatch():
    bootstrap = strategy_pb2.StrategySessionBootstrap(
        session_id="2" * 32,
        launch_operation_id="operation-1",
        strategy_source_sha256="a" * 64,
    )
    packed = ProtoAny()
    packed.Pack(bootstrap)
    start = pb2.StartSession(session_id="1" * 32, session_bootstrap=packed)

    with pytest.raises(RuntimeError, match="session_id mismatch"):
        session_worker_entry._validated_start_bootstrap(start)


@pytest.mark.parametrize(
    "digest",
    ["A" * 64, "g" * 64, "a" * 63],
)
def test_new_protocol_start_rejects_noncanonical_digest(digest):
    bootstrap = strategy_pb2.StrategySessionBootstrap(
        session_id="1" * 32,
        launch_operation_id="operation-1",
        strategy_source_sha256=digest,
        environment=0,
    )
    packed = ProtoAny()
    packed.Pack(bootstrap)
    start = pb2.StartSession(session_id="1" * 32, session_bootstrap=packed)

    with pytest.raises(RuntimeError, match="strategy_source_sha256 is invalid"):
        session_worker_entry._validated_start_bootstrap(start)
