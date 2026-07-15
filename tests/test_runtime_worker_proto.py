from strategy_service.gen import runtime_worker_pb2 as pb2
from strategy_service.gen import strategy_service_pb2 as strategy_pb2


def test_worker_hello_carries_session_and_token():
    msg = pb2.WorkerHello(
        session_id="sess-1",
        token="token-1",
        worker_version="test",
        pid=123,
    )

    assert msg.session_id == "sess-1"
    assert msg.token == "token-1"
    assert msg.pid == 123


def test_indicator_frame_carries_multiple_values():
    frame = pb2.IndicatorFrame(
        session_id="sess-1",
        stream_key="binance:futures:ZECUSDT:1m",
        market_time_ms=1000,
        interval_ms=60000,
        values=[
            pb2.IndicatorValue(indicator_key="bb_mid", value=10.5, has_value=True),
            pb2.IndicatorValue(indicator_key="trade_signal", marker_json='{"text":"BUY"}'),
        ],
    )

    assert len(frame.values) == 2
    assert frame.values[0].indicator_key == "bb_mid"


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
