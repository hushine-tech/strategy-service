import json
import threading

from google.protobuf.any_pb2 import Any
from google.protobuf.struct_pb2 import Struct

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.gen import order_service_pb2
from strategy_service.runtime_agent import RuntimeAgent


class FakeChannel:
    def __init__(self) -> None:
        self.backpressure = []
        self.status_patches = []

    def send_data_backpressure(
        self,
        *,
        session_id: str,
        stream_key: str,
        reason: str,
        resume_after_unix_ms: int = 0,
    ) -> None:
        self.backpressure.append((session_id, stream_key, reason, resume_after_unix_ms))

    def send_status_patch(self, **kwargs) -> None:
        self.status_patches.append(kwargs)


class RaisingChannel:
    def send_data_backpressure(self, **_kwargs) -> None:
        raise RuntimeError("channel unavailable")

    def send_status_patch(self, **_kwargs) -> None:
        raise RuntimeError("channel unavailable")


def live_frame(
    session_id: str,
    sequence: int,
    *,
    stream_key: str = "binance:futures:kline:ETHUSDT:1m",
) -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_LIVE_KLINE_BATCH,
        live_kline_batch=cp_pb2.RuntimeLiveKlineBatch(
            session_id=session_id,
            stream_key=stream_key,
            sequence=sequence,
            klines=[kline_any()],
        ),
    )


def dataset_frame(session_id: str, sequence: int) -> cp_pb2.RuntimeFrame:
    payload = json.dumps(
        {
            "klines": [
                {
                    "symbol": "ETHUSDT",
                    "interval": "1m",
                    "market": "futures",
                    "open_time": 1000 + sequence,
                    "close_time": 2000 + sequence,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 10.0,
                    "timestamp": 2000 + sequence,
                }
            ]
        }
    ).encode("utf-8")
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_DATASET_CHUNK,
        dataset_chunk=cp_pb2.RuntimeDatasetChunk(
            session_id=session_id,
            dataset_id="dataset-1",
            sequence=sequence,
            payload=payload,
        ),
    )


def order_update_frame(session_id: str, sequence: int) -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_ORDER_UPDATE_BATCH,
        order_update_batch=cp_pb2.RuntimeOrderUpdateBatch(
            session_id=session_id,
            stream_key="order_lifecycle",
            sequence=sequence,
            events=[order_update_any(sequence)],
        ),
    )


def kline_any() -> Any:
    kline = Struct()
    kline.update(
        {
            "symbol": "ETHUSDT",
            "interval": "1m",
            "market": "futures",
            "open_time": 1000,
            "close_time": 2000,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "timestamp": 2000,
        }
    )
    packed = Any()
    packed.Pack(kline)
    return packed


def order_update_any(event_id: int) -> Any:
    item = order_service_pb2.OrderLifecycleEventEntry(
        event_id=event_id,
        session_id="sess-1",
        account_id=10,
        venue_id=1,
        order_id="order-1",
        exchange_order_id="exchange-order-1",
        exchange_trade_id=f"trade-{event_id}",
        event_type="fill",
        order_status="FILLED",
        environment=2,
        exchange=1,
        market=2,
        position_side=0,
        side="BUY",
        fill_delta=order_service_pb2.FillDeltaEntry(
            exchange_trade_id=f"trade-{event_id}",
            exchange_order_id="exchange-order-1",
            symbol="ETHUSDT",
            qty=0.01,
            fill_price=3000.0,
        ),
        order_state=order_service_pb2.OrderStateEntry(
            exchange_order_id="exchange-order-1",
            symbol="ETHUSDT",
            status="FILLED",
            orig_qty=0.01,
            executed_qty=0.01,
            avg_price=3000.0,
        ),
    )
    packed = Any()
    packed.Pack(item)
    return packed


def test_agent_drops_stale_live_market_data_by_time_not_queue_size() -> None:
    channel = FakeChannel()
    now = [100.0]
    agent = RuntimeAgent(
        channel,
        runtime_id="rt-1",
        live_queue_maxsize=1,
        live_market_data_max_lag_seconds=1.0,
        now_fn=lambda: now[0],
    )

    agent.handle_data_frame(live_frame("sess-1", 1))
    agent.handle_data_frame(live_frame("sess-1", 2))

    assert channel.backpressure == []

    now[0] = 102.0
    rows = list(agent.iter_live_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert rows == []
    assert len(channel.backpressure) == 2
    assert channel.backpressure[0][0] == "sess-1"
    assert channel.backpressure[0][2].startswith("market_data_dropped:")
    assert "kind=live_kline" in channel.backpressure[0][2]
    assert "lag_seconds=2.000" in channel.backpressure[0][2]


def test_agent_fails_session_after_three_live_market_data_drop_events() -> None:
    channel = FakeChannel()
    now = [100.0]
    agent = RuntimeAgent(
        channel,
        runtime_id="rt-1",
        live_market_data_max_lag_seconds=1.0,
        live_market_data_drop_fail_threshold=3,
        now_fn=lambda: now[0],
    )

    agent.handle_data_frame(live_frame("sess-1", 1))
    agent.handle_data_frame(live_frame("sess-1", 2))
    agent.handle_data_frame(live_frame("sess-1", 3))

    now[0] = 102.0
    rows = list(agent.iter_live_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert rows == []
    assert len(channel.backpressure) == 3
    assert channel.status_patches
    assert channel.status_patches[-1]["session_id"] == "sess-1"
    assert channel.status_patches[-1]["status"] == "failed"
    assert "market_data_dropped_threshold_exceeded" in channel.status_patches[-1]["reason"]
    assert "stream_key=binance:futures:kline:ETHUSDT:1m" in channel.status_patches[-1]["reason"]


def test_agent_counts_live_market_data_drop_events_per_stream() -> None:
    channel = FakeChannel()
    now = [100.0]
    agent = RuntimeAgent(
        channel,
        runtime_id="rt-1",
        live_market_data_max_lag_seconds=1.0,
        live_market_data_drop_fail_threshold=3,
        now_fn=lambda: now[0],
    )

    for sequence in range(1, 3):
        agent.handle_data_frame(live_frame(
            "sess-1",
            sequence,
            stream_key="binance:futures:kline:ETHUSDT:1m",
        ))
        agent.handle_data_frame(live_frame(
            "sess-1",
            sequence,
            stream_key="binance:futures:kline:BTCUSDT:1m",
        ))

    now[0] = 102.0
    rows = list(agent.iter_live_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert rows == []
    assert len(channel.backpressure) == 4
    assert channel.status_patches == []

    agent.handle_data_frame(live_frame(
        "sess-1",
        3,
        stream_key="binance:futures:kline:ETHUSDT:1m",
    ))
    now[0] = 104.0
    rows = list(agent.iter_live_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert rows == []
    assert channel.status_patches
    assert "stream_key=binance:futures:kline:ETHUSDT:1m" in channel.status_patches[-1]["reason"]


def test_agent_order_update_queue_does_not_drop_callbacks_even_when_sized() -> None:
    channel = FakeChannel()
    agent = RuntimeAgent(channel, runtime_id="rt-1", order_queue_maxsize=1)

    agent.handle_data_frame(order_update_frame("sess-1", 1))
    agent.handle_data_frame(order_update_frame("sess-1", 2))

    updates = list(agent.iter_order_updates(
        session_id="sess-1",
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert [update.event_id for update in updates] == [1, 2]
    assert not channel.backpressure


def test_agent_default_order_update_queue_does_not_drop_callbacks() -> None:
    channel = FakeChannel()
    agent = RuntimeAgent(channel, runtime_id="rt-1")

    agent.handle_data_frame(order_update_frame("sess-1", 1))
    agent.handle_data_frame(order_update_frame("sess-1", 2))
    agent.handle_data_frame(order_update_frame("sess-1", 3))

    updates = list(agent.iter_order_updates(
        session_id="sess-1",
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert [update.event_id for update in updates] == [1, 2, 3]
    assert not [bp for bp in channel.backpressure if bp[2].startswith("data_dropped:")]


def test_agent_reports_backpressure_when_dataset_queue_is_full() -> None:
    channel = FakeChannel()
    agent = RuntimeAgent(channel, runtime_id="rt-1", dataset_queue_maxsize=1)

    agent.handle_data_frame(dataset_frame("sess-1", 1))
    agent.handle_data_frame(dataset_frame("sess-1", 2))

    assert channel.backpressure
    assert channel.backpressure[0][0] == "sess-1"
    assert channel.backpressure[0][1] == "dataset-1"
    assert channel.backpressure[0][2].startswith("data_dropped:")
    assert "kind=dataset" in channel.backpressure[0][2]
    assert "queue_depth=" in channel.backpressure[0][2]


def test_agent_keeps_receiving_when_backpressure_notification_fails() -> None:
    now = [100.0]
    agent = RuntimeAgent(
        RaisingChannel(),
        runtime_id="rt-1",
        live_market_data_max_lag_seconds=1.0,
        now_fn=lambda: now[0],
    )

    agent.handle_data_frame(live_frame("sess-1", 1))
    agent.handle_data_frame(live_frame("sess-1", 2))
    now[0] = 102.0
    _ = list(agent.iter_live_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))

    assert agent.cached_data("sess-1", "binance:futures:kline:ETHUSDT:1m", 2) is not None
