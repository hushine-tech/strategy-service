from strategy_service.gen import runtime_worker_pb2 as pb2
from strategy_service.gen import strategy_service_pb2 as strategy_pb2


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


def test_worker_frame_keeps_v1_tag_during_additive_v2_gate():
    fields = pb2.WorkerFrame.DESCRIPTOR.fields_by_name

    assert fields["indicator_frame"].number == 15
    assert fields["indicator_frame_v2"].number == 21


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
