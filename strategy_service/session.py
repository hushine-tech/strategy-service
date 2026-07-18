"""策略执行 session 管理：内存存储 + 线程安全。"""

from __future__ import annotations

import threading
import time
import uuid
import weakref
from dataclasses import FrozenInstanceError, dataclass, field
import re
from typing import Callable, Literal

_TERMINAL_STATUSES = frozenset({"completed", "finished", "stopped", "failed", "stop_failed", "recoverable"})
_ACTIVE_STATUSES = frozenset({"running", "stopping"})
_SESSION_ID_RE = re.compile(r"[0-9a-f]{32}")
_PUBLICATION_BLOCKED = "BLOCKED"
_PUBLICATION_READY = "READY"
_PUBLICATION_PUBLISHING = "PUBLISHING"
_PUBLICATION_RELEASED = "RELEASED"
_PUBLICATION_TERMINAL = "TERMINAL"


@dataclass(frozen=True, slots=True)
class SessionRegistrationError(Exception):
    reason: Literal[
        "invalid_session_id",
        "state_mismatch",
        "session_id_in_use",
    ]

    def __str__(self) -> str:
        return "session registration failed"


def _exception_setattr(self: BaseException, name: str, value: object) -> None:
    if name in {"__traceback__", "__cause__", "__context__", "__suppress_context__"}:
        BaseException.__setattr__(self, name, value)
        return
    raise FrozenInstanceError(f"cannot assign to field {name!r}")


SessionRegistrationError.__setattr__ = _exception_setattr  # type: ignore[method-assign]


class _SessionOwnershipToken:
    __slots__ = ("__weakref__",)


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
    session_id: str = ""
    _ownership_token: object | None = field(default=None, repr=False, compare=False)
    status: str = "running"          # running / stopping / recoverable / finished / stopped / failed / stop_failed
    bars_processed: int = 0
    error: str = ""
    environment: int = 0
    user_id: int = 0
    portfolio_id: int = 0
    strategy_id: int = 0
    runtime_id: str = ""
    runtime_source: str = ""
    runtime_name: str = ""
    thread: threading.Thread | None = None
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
    order_update_handler: Callable[[object], object] | None = None
    order_target_keys: set[tuple[str, str, str]] = field(default_factory=set)
    reconciliation_run_id: str = ""
    stop_operation_id: str = ""
    max_loss_close_pct: float = 0.30
    max_loss_close_source: str = "platform_default"
    leverage: float = 1.0
    leverage_source: str = "platform_default"
    initial_margin_balance: float = 0.0
    max_loss_close_triggered: bool = False
    user_code_fatal_stage: str = ""
    user_code_fatal_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _running_publication_state: str = field(
        default=_PUBLICATION_BLOCKED,
        repr=False,
        compare=False,
    )
    _running_publication_fatal_pending: bool = field(
        default=False,
        repr=False,
        compare=False,
    )
    _startup_result: object | None = field(default=None, repr=False, compare=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _decision_condition: threading.Condition = field(init=False, repr=False, compare=False)
    _strategy_decision_admission_open: bool = field(default=True, init=False, repr=False, compare=False)
    _strategy_decisions_inflight: int = field(default=0, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._decision_condition = threading.Condition(self._lock)

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
            self.status = "failed"
            self.error = error
            self._running_publication_state = _PUBLICATION_TERMINAL

    def is_active(self) -> bool:
        with self._lock:
            return self.status in _ACTIVE_STATUSES

    def is_terminal(self) -> bool:
        with self._lock:
            return self.status in _TERMINAL_STATUSES

    def configure_live_runtime(
        self,
        *,
        portfolio_id: int,
        strategy_id: int,
        required_streams: list[StreamBinding],
        consumer_group: str,
    ) -> None:
        with self._lock:
            self.portfolio_id = portfolio_id
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
        order_update_handler: Callable[[object], object] | None = None,
    ) -> None:
        with self._lock:
            self.wallet = wallet
            self.order_client = order_client
            self.order_update_handler = order_update_handler

    def remember_stop_operation_id(self, operation_id: str) -> str:
        """Keep one durable close identity for every retry of this Session stop."""
        normalized = str(operation_id or "").strip()
        with self._lock:
            if self.stop_operation_id:
                return self.stop_operation_id
            self.stop_operation_id = normalized
            return self.stop_operation_id

    def current_stop_operation_id(self) -> str:
        with self._lock:
            return self.stop_operation_id

    def begin_stopping(
        self,
        *,
        error: str | None = None,
        operation_id: str = "",
    ) -> tuple[bool, str]:
        """Claim the single stop execution slot for this Session generation."""
        normalized_operation_id = str(operation_id or "").strip()
        with self._decision_condition:
            if self.status in _TERMINAL_STATUSES or self.status == "stopping":
                return False, self.stop_operation_id
            self._strategy_decision_admission_open = False
            self.status = "stopping"
            if error is not None:
                self.error = error
            if normalized_operation_id and not self.stop_operation_id:
                self.stop_operation_id = normalized_operation_id
            return True, self.stop_operation_id

    def try_enter_strategy_decision(self) -> bool:
        """Enter one strategy callback only while the Session still admits decisions."""
        with self._decision_condition:
            if self.status != "running" or not self._strategy_decision_admission_open:
                return False
            self._strategy_decisions_inflight += 1
            return True

    def leave_strategy_decision(self) -> None:
        with self._decision_condition:
            if self._strategy_decisions_inflight <= 0:
                raise RuntimeError("strategy decision admission underflow")
            self._strategy_decisions_inflight -= 1
            if self._strategy_decisions_inflight == 0:
                self._decision_condition.notify_all()

    def wait_for_strategy_decisions(self, *, timeout_seconds: float) -> bool:
        """Wait until callbacks admitted before stop have returned."""
        timeout = max(0.0, float(timeout_seconds))
        deadline = time.monotonic() + timeout
        with self._decision_condition:
            while self._strategy_decisions_inflight > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._decision_condition.wait(timeout=remaining)
            return True

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

    def latch_user_code_fatal(self, stage: str) -> bool:
        with self._lock:
            if self.user_code_fatal_stage:
                return False
            self.user_code_fatal_stage = str(stage)
            if self._running_publication_state == _PUBLICATION_PUBLISHING:
                self._running_publication_fatal_pending = True
            elif self._running_publication_state in {
                _PUBLICATION_BLOCKED,
                _PUBLICATION_READY,
                _PUBLICATION_RELEASED,
            }:
                self._running_publication_state = _PUBLICATION_TERMINAL
            stop_event = getattr(self, "_stop_event", None)
            lease_stop_event = self.lease_stop_event
            self.user_code_fatal_event.set()
        if stop_event is not None:
            stop_event.set()
        if lease_stop_event is not None:
            lease_stop_event.set()
        return True

    def publication_state(self) -> str:
        with self._lock:
            return self._running_publication_state

    def has_user_code_fatal(self) -> bool:
        with self._lock:
            return bool(self.user_code_fatal_stage)

    def bind_startup_result(self, startup_result: object) -> None:
        with self._lock:
            if self._startup_result is not None and self._startup_result is not startup_result:
                raise RuntimeError("session startup result is already bound")
            self._startup_result = startup_result

    def startup_result(self) -> object | None:
        with self._lock:
            return self._startup_result

    def mark_running_publication_ready(self) -> bool:
        """Atomically expose local running only when no fatal owner has won."""
        with self._lock:
            if (
                self.status != "pending"
                or self.user_code_fatal_stage
                or self._running_publication_state != _PUBLICATION_BLOCKED
            ):
                return False
            self.status = "running"
            self._running_publication_state = _PUBLICATION_READY
            return True

    def claim_running_publication(self) -> bool:
        with self._lock:
            if (
                self.status != "running"
                or self.user_code_fatal_stage
                or self._running_publication_state != _PUBLICATION_READY
            ):
                return False
            self._running_publication_state = _PUBLICATION_PUBLISHING
            return True

    def complete_running_publication_submission(
        self,
        release_event: threading.Event | None = None,
    ) -> bool:
        """Finish the ordered running enqueue; True means user work may release."""
        with self._lock:
            if self._running_publication_state != _PUBLICATION_PUBLISHING:
                return False
            if self._running_publication_fatal_pending or self.user_code_fatal_stage:
                self._running_publication_state = _PUBLICATION_TERMINAL
                return False
            self._running_publication_state = _PUBLICATION_RELEASED
            if release_event is not None:
                release_event.set()
            return True

    def fail_running_publication(self, error: str) -> bool:
        with self._lock:
            if self._running_publication_state == _PUBLICATION_RELEASED:
                return False
            self._running_publication_state = _PUBLICATION_TERMINAL
            self.status = "failed"
            self.error = str(error or "strategy session startup failed")
            return True


@dataclass(frozen=True, slots=True)
class _SessionIssuance:
    session_id: str
    ownership_token: _SessionOwnershipToken
    state_ref: weakref.ReferenceType[SessionState]


class SessionManager:
    """线程安全的 session 注册表。"""

    _CLEANUP_AFTER_SECS = 3600  # 终态 session 保留 1 小时后清理

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionState] = {}
        self._completed_at: dict[str, tuple[SessionState, float]] = {}
        self._issuances: dict[int, _SessionIssuance] = {}

    @staticmethod
    def _validate_session_id(session_id: object) -> str:
        if type(session_id) is not str or _SESSION_ID_RE.fullmatch(session_id) is None:
            raise SessionRegistrationError(reason="invalid_session_id")
        return session_id

    def prepare(
        self,
        *,
        session_id: str | None = None,
        initial_status: Literal["pending", "running"] = "running",
        environment: int = 0,
        user_id: int = 0,
        portfolio_id: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
    ) -> tuple[str, SessionState]:
        if type(initial_status) is not str or initial_status not in {"pending", "running"}:
            raise ValueError("initial_status must be pending or running")
        final_id = uuid.uuid4().hex if session_id is None else self._validate_session_id(session_id)
        token = _SessionOwnershipToken()
        state = SessionState(
            session_id=final_id,
            _ownership_token=token,
            status=initial_status,
            environment=environment,
            user_id=user_id,
            portfolio_id=portfolio_id,
            runtime_id=str(runtime_id or ""),
            runtime_source=str(runtime_source or ""),
            runtime_name=str(runtime_name or ""),
        )
        state_key = id(state)
        manager_ref = weakref.ref(self)

        def discard_abandoned_issuance(state_ref: weakref.ReferenceType[SessionState]) -> None:
            manager = manager_ref()
            if manager is None:
                return
            with manager._lock:
                issuance = manager._issuances.get(state_key)
                if issuance is not None and issuance.state_ref is state_ref:
                    del manager._issuances[state_key]

        state_ref = weakref.ref(state, discard_abandoned_issuance)
        issuance = _SessionIssuance(
            session_id=final_id,
            ownership_token=token,
            state_ref=state_ref,
        )
        with self._lock:
            self._issuances[state_key] = issuance
        return final_id, state

    def register(self, session_id: str, state: SessionState) -> None:
        with self._lock:
            if type(state) is not SessionState:
                raise SessionRegistrationError(reason="state_mismatch")
            issuance = self._issuances.get(id(state))
            if issuance is None or issuance.state_ref() is not state:
                raise SessionRegistrationError(reason="state_mismatch")
            del self._issuances[id(state)]
            final_id = self._validate_session_id(session_id)
            if (
                state._ownership_token is not issuance.ownership_token
                or type(state.session_id) is not str
                or state.session_id != issuance.session_id
                or final_id != issuance.session_id
            ):
                raise SessionRegistrationError(reason="state_mismatch")
            if final_id in self._sessions:
                raise SessionRegistrationError(reason="session_id_in_use")
            self._sessions[final_id] = state
            self._completed_at.pop(final_id, None)

    def create(
        self,
        environment: int = 0,
        user_id: int = 0,
        portfolio_id: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
    ) -> tuple[str, SessionState]:
        session_id, state = self.prepare(
            environment=environment,
            user_id=user_id,
            portfolio_id=portfolio_id,
            runtime_id=str(runtime_id or ""),
            runtime_source=str(runtime_source or ""),
            runtime_name=str(runtime_name or ""),
        )
        self.register(session_id, state)
        return session_id, state

    def get(self, session_id: str) -> SessionState | None:
        with self._lock:
            return self._sessions.get(session_id)

    def list_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._sessions))

    def discard(self, session_id: str, expected_state: SessionState) -> bool:
        with self._lock:
            if self._sessions.get(session_id) is not expected_state:
                return False
            del self._sessions[session_id]
            self._completed_at.pop(session_id, None)
            return True

    def find_active_session_for_portfolio(self, portfolio_id: int) -> tuple[str, SessionState] | None:
        with self._lock:
            for session_id, state in self._sessions.items():
                if int(state.portfolio_id) != int(portfolio_id):
                    continue
                if state.status in _ACTIVE_STATUSES:
                    return session_id, state
        return None

    def set_thread(
        self,
        session_id: str,
        expected_state: SessionState,
        thread: threading.Thread,
    ) -> bool:
        with self._lock:
            s = self._sessions.get(session_id)
            if s is not expected_state:
                return False
            s.thread = thread
            return True

    def claim_running_publication(
        self,
        session_id: str,
        expected_state: SessionState,
    ) -> bool:
        with self._lock:
            state = self._sessions.get(session_id)
            if state is not expected_state:
                return False
            return state.claim_running_publication()


    def mark_terminal(self, session_id: str, expected_state: SessionState) -> bool:
        """Mark a session for eventual cleanup."""
        with self._lock:
            if self._sessions.get(session_id) is not expected_state:
                return False
            self._completed_at[session_id] = (expected_state, time.time())
        self._cleanup()
        return True

    def _cleanup(self) -> None:
        """Remove sessions that have been terminal for longer than the threshold."""
        now = time.time()
        with self._lock:
            expired = [
                (sid, expected_state)
                for sid, (expected_state, completed_at) in self._completed_at.items()
                if now - completed_at > self._CLEANUP_AFTER_SECS
            ]
            for sid, expected_state in expired:
                if self._sessions.get(sid) is expected_state:
                    del self._sessions[sid]
                retained = self._completed_at.get(sid)
                if retained is not None and retained[0] is expected_state:
                    del self._completed_at[sid]
