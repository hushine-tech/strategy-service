from __future__ import annotations

import multiprocessing
import queue
import json
import threading
import time

from google.protobuf.any_pb2 import Any
from google.protobuf.struct_pb2 import Struct

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.gen import order_service_pb2
from strategy_service.runtime_agent import RuntimeAgent, RuntimeWorkerProcess
from strategy_service.runtime_channel import RuntimeChannelClient, RuntimeCredential, RuntimeHelloArgs


def test_runtime_agent_heartbeat_and_status_continue_while_worker_is_paused():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
        heartbeat_seconds=1,
    )
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    with client._outbound_lock:
        client._outbound = outbound
    client._connected.set()

    stop_worker = multiprocessing.Event()
    worker = RuntimeWorkerProcess(name="test-debug-worker")
    worker.start(_paused_worker, args=(stop_worker,))
    agent = RuntimeAgent(client, worker, runtime_id="runtime-1")

    heartbeat_stop = threading.Event()
    heartbeat = threading.Thread(target=client._heartbeat_loop, args=(outbound, heartbeat_stop), daemon=True)
    heartbeat.start()
    try:
        heartbeat_frame = outbound.get(timeout=1.5)
        assert heartbeat_frame.frame_type == cp_pb2.FRAME_TYPE_HEARTBEAT

        agent.report_worker_health(session_id="sess-1")
        status_patch = outbound.get(timeout=1)
        assert status_patch.frame_type == cp_pb2.FRAME_TYPE_STATUS_PATCH
        assert status_patch.status_patch.runtime_id == "runtime-1"
        assert status_patch.status_patch.session_id == "sess-1"
        assert status_patch.status_patch.status == "worker_active"
    finally:
        heartbeat_stop.set()
        stop_worker.set()
        worker.stop(timeout_seconds=1)


def test_runtime_agent_caches_data_frames_and_runtime_channel_acks():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")
    client.set_data_handler(agent.handle_data_frame)
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()

    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_DATASET_CHUNK,
            dataset_chunk=cp_pb2.RuntimeDatasetChunk(
                dataset_id="dataset-1",
                session_id="sess-1",
                sequence=7,
                payload=b"chunk",
            ),
        ),
        outbound,
    )

    ack = outbound.get_nowait()
    assert ack.frame_type == cp_pb2.FRAME_TYPE_DATA_ACK
    assert ack.data_ack.session_id == "sess-1"
    assert ack.data_ack.stream_key == "dataset-1"
    assert ack.data_ack.sequence == 7
    assert agent.cached_data("sess-1", "dataset-1", 7) == b"chunk"


def test_runtime_agent_buffers_dataset_chunks_for_backtest_delivery():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")
    payload = json.dumps({
        "klines": [
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
        ]
    }).encode("utf-8")

    agent.handle_data_frame(cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_DATASET_CHUNK,
        dataset_chunk=cp_pb2.RuntimeDatasetChunk(
            dataset_id="dataset-1",
            session_id="sess-1",
            sequence=1,
            payload=payload,
            end=True,
        ),
    ))

    rows = list(agent.iter_dataset_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))
    assert len(rows) == 1
    assert rows[0].symbol == "ETHUSDT"
    assert rows[0].market == "futures"


def test_runtime_agent_dataset_end_stops_backtest_iterator():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")
    stop_event = threading.Event()
    rows = []

    def consume():
        rows.extend(agent.iter_dataset_klines(
            session_id="sess-empty",
            required_streams=[],
            stop_event=stop_event,
            idle_timeout_seconds=0.01,
        ))

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    agent.handle_data_frame(cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_DATASET_CHUNK,
        dataset_chunk=cp_pb2.RuntimeDatasetChunk(
            dataset_id="dataset-empty",
            session_id="sess-empty",
            sequence=1,
            payload=b'{"klines":[]}',
            end=True,
        ),
    ))
    thread.join(timeout=0.5)
    still_running = thread.is_alive()
    if still_running:
        stop_event.set()
        thread.join(timeout=1)

    assert not still_running
    assert rows == []


def test_runtime_agent_buffers_live_kline_batches_for_session_delivery():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")
    client.set_data_handler(agent.handle_data_frame)
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    kline = Struct()
    kline.update({
        "symbol": "BTCUSDT",
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
    })
    packed = Any()
    packed.Pack(kline)

    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_LIVE_KLINE_BATCH,
            live_kline_batch=cp_pb2.RuntimeLiveKlineBatch(
                session_id="sess-1",
                stream_key="binance/futures/kline/BTCUSDT/1m",
                sequence=3,
                klines=[packed],
            ),
        ),
        outbound,
    )

    ack = outbound.get_nowait()
    assert ack.frame_type == cp_pb2.FRAME_TYPE_DATA_ACK
    assert ack.data_ack.session_id == "sess-1"
    assert ack.data_ack.stream_key == "binance/futures/kline/BTCUSDT/1m"
    assert ack.data_ack.sequence == 3

    stop_event = threading.Event()
    rows = list(agent.iter_live_klines(
        session_id="sess-1",
        required_streams=[],
        stop_event=stop_event,
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))
    assert len(rows) == 1
    assert rows[0].symbol == "BTCUSDT"
    assert rows[0].market == "futures"
    assert rows[0].close == 1.5


def test_runtime_agent_queues_order_update_batch_and_acks():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")
    client.set_data_handler(agent.handle_data_frame)
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()

    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_ORDER_UPDATE_BATCH,
            order_update_batch=cp_pb2.RuntimeOrderUpdateBatch(
                session_id="sess-1",
                stream_key="order_lifecycle",
                sequence=11,
                events=[_packed_order_lifecycle_event(event_id=501)],
            ),
        ),
        outbound,
    )

    ack = outbound.get_nowait()
    assert ack.frame_type == cp_pb2.FRAME_TYPE_DATA_ACK
    assert ack.data_ack.session_id == "sess-1"
    assert ack.data_ack.stream_key == "order_lifecycle"
    assert ack.data_ack.sequence == 11

    updates = list(agent.iter_order_updates(
        session_id="sess-1",
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))
    assert len(updates) == 1
    assert updates[0].event_id == 501
    assert updates[0].event_type == "fill"
    assert updates[0].fill is not None
    assert updates[0].fill.symbol == "ETHUSDT"


def test_runtime_agent_iter_session_events_prioritizes_order_update_before_next_kline():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")

    kline = Struct()
    kline.update({
        "symbol": "BTCUSDT",
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
    })
    packed_kline = Any()
    packed_kline.Pack(kline)

    agent.handle_data_frame(cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_LIVE_KLINE_BATCH,
        live_kline_batch=cp_pb2.RuntimeLiveKlineBatch(
            session_id="sess-1",
            stream_key="binance/futures/kline/BTCUSDT/1m",
            sequence=3,
            klines=[packed_kline],
        ),
    ))
    agent.handle_data_frame(cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_ORDER_UPDATE_BATCH,
        order_update_batch=cp_pb2.RuntimeOrderUpdateBatch(
            session_id="sess-1",
            stream_key="order_lifecycle:fills",
            sequence=4,
            events=[_packed_order_lifecycle_event(event_id=777)],
        ),
    ))

    events = list(agent.iter_session_events(
        session_id="sess-1",
        required_streams=[],
        stop_event=threading.Event(),
        idle_timeout_seconds=0.01,
        stop_when_idle=True,
    ))
    assert [event.kind for event in events] == ["order_update", "kline"]
    assert events[0].stream_key == "order_lifecycle:fills"
    assert events[0].payload.event_id == 777
    assert events[1].payload.symbol == "BTCUSDT"


def test_runtime_agent_prepare_debug_workspace_command(tmp_path):
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")
    client.set_command_handler(agent.handle_runtime_command)
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()

    payload = Any(
        type_url="type.googleapis.com/controlpanel.v1.RuntimeCommandPayloadJSON",
        value=json.dumps({"container_path": str(tmp_path), "host_path": "/host/ws"}).encode("utf-8"),
    )
    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            correlation_id="cmd-1",
            frame_type=cp_pb2.FRAME_TYPE_COMMAND,
            command=cp_pb2.RuntimeCommandFrame(
                command_id="cmd-1",
                command_type="prepare_debug_workspace",
                payload=payload,
            ),
        ),
        outbound,
    )

    ack = outbound.get_nowait()
    result = outbound.get_nowait()
    assert ack.frame_type == cp_pb2.FRAME_TYPE_COMMAND_ACK
    assert result.frame_type == cp_pb2.FRAME_TYPE_COMMAND_RESULT
    assert result.command_result.status == "succeeded"
    body = json.loads(result.command_result.result.value.decode("utf-8"))
    assert body["template_path"].endswith("self_hosted_strategy.py")
    assert (tmp_path / "self_hosted_strategy.py").exists()
    assert (tmp_path / ".vscode" / "launch.json").exists()
    assert (tmp_path / "PYCHARM_DEBUG.md").exists()


def test_runtime_agent_load_debug_dataset_command():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50055",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem, runtime_id="runtime-1"),
    )
    agent = RuntimeAgent(client, runtime_id="runtime-1")

    result = agent.handle_runtime_command("load_debug_dataset", json.dumps({
        "dataset_id": "dbg-1",
        "user_id": 7,
        "account_id": 10,
        "runtime_id": "runtime-1",
        "market": "spot",
        "symbol": "ETHUSDT",
        "interval": "1m",
        "start_time_ms": 1000,
        "end_time_ms": 61000,
        "loaded_at_ms": 70000,
        "klines": [
            {
                "symbol": "ETHUSDT",
                "interval": "1m",
                "market": "spot",
                "open_time_ms": 1000,
                "close_time_ms": 60999,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
            }
        ],
    }).encode("utf-8"))

    body = json.loads(result.decode("utf-8"))
    assert body["dataset_id"] == "dbg-1"
    assert body["bar_count"] == 1
    dataset = agent.active_debug_dataset()
    assert dataset is not None
    assert dataset.account_id == 10
    assert dataset.klines[0].symbol == "ETHUSDT"
    assert dataset.klines[0].market == "spot"


def _packed_order_lifecycle_event(*, event_id: int) -> Any:
    item = order_service_pb2.OrderLifecycleEventEntry(
        event_id=event_id,
        session_id="sess-1",
        account_id=10,
        venue_id=1,
        intent_id="intent-1",
        attempt_id="attempt-1",
        order_id="order-1",
        exchange_order_id="exchange-order-1",
        exchange_trade_id="trade-1",
        event_type="fill",
        order_status="FILLED",
        environment=2,
        exchange=1,
        market=2,
        position_side=0,
        side="BUY",
        event_source="binance_user_data",
        fill_delta=order_service_pb2.FillDeltaEntry(
            exchange_trade_id="trade-1",
            exchange_order_id="exchange-order-1",
            symbol="ETHUSDT",
            qty=0.02,
            fill_price=3000.0,
            fee=0.03,
            fee_asset="USDT",
        ),
        order_state=order_service_pb2.OrderStateEntry(
            exchange_order_id="exchange-order-1",
            symbol="ETHUSDT",
            status="FILLED",
            orig_qty=0.02,
            executed_qty=0.02,
            remaining_qty=0.0,
            avg_price=3000.0,
        ),
    )
    packed = Any()
    packed.Pack(item)
    return packed


def _paused_worker(stop_event):
    stop_event.wait(5)
