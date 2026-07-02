"""策略执行 session 管理：内存存储 + 线程安全。"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from strategy_service.data_loop import LiveDataLoop

_TERMINAL_STATUSES = frozenset({"completed", "finished", "stopped", "failed", "stop_failed", "recoverable"})
_ACTIVE_STATUSES = frozenset({"running", "stopping"})


@dataclass(frozen=True)
class StreamBinding:
    stream_id: int
    exchange: str
    market: str
    kind: str
    symbol: str
    interval: str
    canonical_market: str = ""


@dataclass
class SessionState:
    status: str = "running"          # running / stopping / recoverable / finished / stopped / failed / stop_failed
    bars_processed: int = 0
    error: str = ""
    environment: int = 0
    user_id: int = 0
    account_id: int = 0
    strategy_id: int = 0
    runtime_id: str = ""
    runtime_source: str = ""
    runtime_name: str = ""
    thread: threading.Thread | None = None
    live_loop: "LiveDataLoop | None" = None  # demo/live session stop hook
    required_streams: list[StreamBinding] = field(default_factory=list)
    live_consumer_group: str = ""
    lease_thread: threading.Thread | None = None
    lease_stop_event: threading.Event | None = None
    lease_heartbeat_at_ms: int = 0
    unroutable_events: int = 0
    last_unroutable_at_ms: int = 0
    last_unroutable_reason: str = ""
    wallet: object | None = None
    order_client: object | None = None
    order_target_keys: set[tuple[str, str, str]] = field(default_factory=set)
    max_loss_close_pct: float = 0.30
    max_loss_close_source: str = "platform_default"
    leverage: float = 1.0
    leverage_source: str = "platform_default"
    initial_margin_balance: float = 0.0
    max_loss_close_triggered: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def transition(self, new_status: str, bars: int | None = None, error: str | None = None) -> bool:
        """Atomically transition status. Returns True if transition succeeded.
        Terminal statuses cannot be overwritten."""
        with self._lock:
            if self.status in _TERMINAL_STATUSES:
                return False  # already terminal, reject
            self.status = new_status
            if bars is not None:
                self.bars_processed = bars
            if error is not None:
                self.error = error
            return True

    def force_failed(self, error: str) -> None:
        """Mark the session failed even after a terminal runtime transition."""
        with self._lock:
            if self.status not in {"failed", "stop_failed", "recoverable"}:
                self.status = "failed"
                self.error = error
            elif not self.error:
                self.error = error

    def is_active(self) -> bool:
        with self._lock:
            return self.status in _ACTIVE_STATUSES

    def is_terminal(self) -> bool:
        with self._lock:
            return self.status in _TERMINAL_STATUSES

    def configure_live_runtime(
        self,
        *,
        account_id: int,
        strategy_id: int,
        required_streams: list[StreamBinding],
        consumer_group: str,
    ) -> None:
        with self._lock:
            self.account_id = account_id
            self.strategy_id = strategy_id
            self.required_streams = list(required_streams)
            self.live_consumer_group = consumer_group

    def bind_runtime(
        self,
        *,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
    ) -> None:
        with self._lock:
            self.runtime_id = str(runtime_id or "")
            self.runtime_source = str(runtime_source or "")
            self.runtime_name = str(runtime_name or "")

    def set_lease_runtime(
        self,
        *,
        stop_event: threading.Event | None,
        lease_thread: threading.Thread | None = None,
    ) -> None:
        with self._lock:
            self.lease_stop_event = stop_event
            self.lease_thread = lease_thread

    def configure_stop_runtime(
        self,
        *,
        wallet: object | None = None,
        order_client: object | None = None,
    ) -> None:
        with self._lock:
            self.wallet = wallet
            self.order_client = order_client

    def configure_risk_runtime(
        self,
        *,
        order_target_keys: set[tuple[str, str, str]],
        max_loss_close_pct: float,
        max_loss_close_source: str,
        initial_margin_balance: float = 0.0,
        leverage: float = 1.0,
        leverage_source: str = "platform_default",
    ) -> None:
        with self._lock:
            self.order_target_keys = set(order_target_keys)
            self.max_loss_close_pct = float(max_loss_close_pct)
            self.max_loss_close_source = str(max_loss_close_source or "platform_default")
            self.leverage = float(leverage)
            self.leverage_source = str(leverage_source or "platform_default")
            self.initial_margin_balance = float(initial_margin_balance)
            self.max_loss_close_triggered = False

    def mark_max_loss_close_triggered(self) -> bool:
        with self._lock:
            if self.max_loss_close_triggered:
                return False
            self.max_loss_close_triggered = True
            return True

    def note_lease_heartbeat(self, now_ms: int | None = None) -> None:
        with self._lock:
            self.lease_heartbeat_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)

    def record_unroutable(self, reason: str, now_ms: int | None = None) -> None:
        with self._lock:
            self.unroutable_events += 1
            self.last_unroutable_reason = reason
            self.last_unroutable_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)

    def record_runtime_error(self, error: str) -> bool:
        with self._lock:
            message = str(error or "").strip()
            if not message or self.error == message:
                return False
            self.error = message
            return True

    def clear_runtime_error(self, prefix: str = "") -> bool:
        with self._lock:
            if not self.error:
                return False
            if prefix and not self.error.startswith(prefix):
                return False
            self.error = ""
            return True


class SessionManager:
    """线程安全的 session 注册表。"""

    _CLEANUP_AFTER_SECS = 3600  # 终态 session 保留 1 小时后清理

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionState] = {}
        self._completed_at: dict[str, float] = {}  # session_id → time.time() when terminal

    def create(
        self,
        environment: int = 0,
        user_id: int = 0,
        account_id: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
    ) -> tuple[str, SessionState]:
        session_id = uuid.uuid4().hex
        state = SessionState(
            environment=environment,
            user_id=user_id,
            account_id=account_id,
            runtime_id=str(runtime_id or ""),
            runtime_source=str(runtime_source or ""),
            runtime_name=str(runtime_name or ""),
        )
        with self._lock:
            self._sessions[session_id] = state
        return session_id, state

    def restore(self, session_id: str, state: SessionState) -> None:
        with self._lock:
            self._sessions[session_id] = state
            self._completed_at.pop(session_id, None)

    def get(self, session_id: str) -> SessionState | None:
        with self._lock:
            return self._sessions.get(session_id)

    def discard(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)
            self._completed_at.pop(session_id, None)

    def find_active_session_for_account(self, account_id: int) -> tuple[str, SessionState] | None:
        with self._lock:
            for session_id, state in self._sessions.items():
                if int(state.account_id) != int(account_id):
                    continue
                if state.status in _ACTIVE_STATUSES:
                    return session_id, state
        return None

    def set_thread(self, session_id: str, thread: threading.Thread) -> None:
        with self._lock:
            s = self._sessions.get(session_id)
            if s is not None:
                s.thread = thread

    def set_live_loop(self, session_id: str, loop: "LiveDataLoop") -> None:
        with self._lock:
            s = self._sessions.get(session_id)
            if s is not None:
                s.live_loop = loop

    def list_active_live_sessions(self) -> list[tuple[str, SessionState]]:
        with self._lock:
            return [
                (session_id, state)
                for session_id, state in self._sessions.items()
                if state.environment == 1 and state.status == "running"
            ]

    def mark_terminal(self, session_id: str) -> None:
        """Mark a session for eventual cleanup."""
        with self._lock:
            self._completed_at[session_id] = time.time()
        self._cleanup()

    def _cleanup(self) -> None:
        """Remove sessions that have been terminal for longer than the threshold."""
        now = time.time()
        with self._lock:
            expired = [sid for sid, t in self._completed_at.items()
                       if now - t > self._CLEANUP_AFTER_SECS]
            for sid in expired:
                self._sessions.pop(sid, None)
                self._completed_at.pop(sid, None)
