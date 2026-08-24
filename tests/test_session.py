from __future__ import annotations

import re
import threading

import pytest

from strategy_service import session as session_module
from strategy_service.session import SessionManager, SessionState

SessionRegistrationError = getattr(
    session_module,
    "SessionRegistrationError",
    type("MissingSessionRegistrationError", (Exception,), {}),
)


_SESSION_ID = "1" * 32
_MUTATED_SESSION_ID = "2" * 32


def _assign_field(state: SessionState, name: str, value: object) -> None:
    setattr(state, name, value)


def _force_assign_field(state: SessionState, name: str, value: object) -> None:
    object.__setattr__(state, name, value)


def test_prepare_none_generates_lowercase_hex_and_records_ownership():
    manager = SessionManager()
    session_id, state = manager.prepare()

    assert re.fullmatch(r"[0-9a-f]{32}", session_id)
    assert state.session_id == session_id
    assert manager.get(session_id) is None
    manager.register(session_id, state)
    assert manager.get(session_id) is state


@pytest.mark.parametrize(
    "session_id",
    ["", "A" * 32, "1" * 31, "1" * 33, "12345678-1234-1234-1234-123456789abc", b"1" * 32],
)
def test_prepare_rejects_noncanonical_session_id(session_id):
    with pytest.raises(SessionRegistrationError) as caught:
        SessionManager().prepare(session_id=session_id)
    assert caught.value.reason == "invalid_session_id"
    assert str(caught.value) == "session registration failed"


def test_register_requires_exact_id_and_state():
    manager = SessionManager()
    session_id, state = manager.prepare(session_id=_SESSION_ID)
    with pytest.raises(SessionRegistrationError) as caught:
        manager.register("2" * 32, state)
    assert caught.value.reason == "state_mismatch"
    with pytest.raises(SessionRegistrationError) as retry:
        manager.register(session_id, state)
    assert retry.value.reason == "state_mismatch"


@pytest.mark.parametrize(
    "mutator",
    [_assign_field, _force_assign_field],
    ids=["assignment", "object-setattr"],
)
def test_register_consumes_issuance_when_prepared_session_id_is_rebound(mutator):
    manager = SessionManager()
    session_id, state = manager.prepare(session_id=_SESSION_ID)
    mutator(state, "session_id", _MUTATED_SESSION_ID)

    with pytest.raises(SessionRegistrationError) as caught:
        manager.register(_MUTATED_SESSION_ID, state)
    assert caught.value.reason == "state_mismatch"
    assert manager.get(_MUTATED_SESSION_ID) is None

    mutator(state, "session_id", session_id)
    with pytest.raises(SessionRegistrationError) as retry:
        manager.register(session_id, state)
    assert retry.value.reason == "state_mismatch"
    assert manager.get(session_id) is None


@pytest.mark.parametrize(
    "mutator",
    [_assign_field, _force_assign_field],
    ids=["assignment", "object-setattr"],
)
def test_register_consumes_issuance_when_ownership_token_is_rebound(mutator):
    manager = SessionManager()
    session_id, state = manager.prepare(session_id=_SESSION_ID)
    issued_token = state._ownership_token
    mutator(state, "_ownership_token", object())

    with pytest.raises(SessionRegistrationError) as caught:
        manager.register(session_id, state)
    assert caught.value.reason == "state_mismatch"

    mutator(state, "_ownership_token", issued_token)
    with pytest.raises(SessionRegistrationError) as retry:
        manager.register(session_id, state)
    assert retry.value.reason == "state_mismatch"
    assert manager.get(session_id) is None


@pytest.mark.parametrize(
    ("field_name", "mutated_value", "register_id"),
    [
        ("session_id", _MUTATED_SESSION_ID, _MUTATED_SESSION_ID),
        ("_ownership_token", object(), _SESSION_ID),
    ],
    ids=["session-id", "ownership-token"],
)
@pytest.mark.parametrize(
    "mutator",
    [_assign_field, _force_assign_field],
    ids=["assignment", "object-setattr"],
)
def test_raced_register_of_rebound_state_is_permanently_state_mismatch(
    field_name,
    mutated_value,
    register_id,
    mutator,
):
    manager = SessionManager()
    session_id, state = manager.prepare(session_id=_SESSION_ID)
    issued_value = getattr(state, field_name)
    mutator(state, field_name, mutated_value)
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def register() -> None:
        barrier.wait()
        try:
            manager.register(register_id, state)
        except SessionRegistrationError as exc:
            outcomes.append(exc.reason)
        else:
            outcomes.append("registered")

    threads = [threading.Thread(target=register) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(outcomes) == ["state_mismatch", "state_mismatch"]
    assert manager.get(register_id) is None

    mutator(state, field_name, issued_value)
    with pytest.raises(SessionRegistrationError) as retry:
        manager.register(session_id, state)
    assert retry.value.reason == "state_mismatch"
    assert manager.get(session_id) is None


def test_register_first_attempt_consumes_capability_after_collision():
    manager = SessionManager()
    occupant_id, occupant = manager.prepare(session_id=_SESSION_ID)
    manager.register(occupant_id, occupant)
    _, candidate = manager.prepare(session_id=_SESSION_ID)

    with pytest.raises(SessionRegistrationError) as collision:
        manager.register(_SESSION_ID, candidate)
    assert collision.value.reason == "session_id_in_use"
    assert manager.discard(_SESSION_ID, occupant) is True
    with pytest.raises(SessionRegistrationError) as retry:
        manager.register(_SESSION_ID, candidate)
    assert retry.value.reason == "state_mismatch"


def test_discarded_state_cannot_reregister_after_id_reuse():
    manager = SessionManager()
    _, old = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, old)
    assert manager.discard(_SESSION_ID, old) is True
    _, fresh = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, fresh)
    assert manager.discard(_SESSION_ID, fresh) is True

    with pytest.raises(SessionRegistrationError) as caught:
        manager.register(_SESSION_ID, old)
    assert caught.value.reason == "state_mismatch"
    assert manager.get(_SESSION_ID) is None


def test_raced_stale_reregister_cannot_beat_fresh_state_after_id_reuse():
    manager = SessionManager()
    _, old = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, old)
    assert manager.discard(_SESSION_ID, old)
    _, fresh = manager.prepare(session_id=_SESSION_ID)
    barrier = threading.Barrier(2)
    outcomes: dict[str, str] = {}

    def register(label: str, state: SessionState) -> None:
        barrier.wait()
        try:
            manager.register(_SESSION_ID, state)
        except SessionRegistrationError as exc:
            outcomes[label] = exc.reason
        else:
            outcomes[label] = "registered"

    old_thread = threading.Thread(target=register, args=("old", old))
    fresh_thread = threading.Thread(target=register, args=("fresh", fresh))
    old_thread.start()
    fresh_thread.start()
    old_thread.join(timeout=2)
    fresh_thread.join(timeout=2)

    assert outcomes == {"old": "state_mismatch", "fresh": "registered"}
    assert manager.get(_SESSION_ID) is fresh


def test_list_ids_is_a_sorted_snapshot():
    manager = SessionManager()
    for session_id in ("f" * 32, "0" * 32, "8" * 32):
        _, state = manager.prepare(session_id=session_id)
        manager.register(session_id, state)
    assert manager.list_ids() == ("0" * 32, "8" * 32, "f" * 32)


def test_discard_is_expected_state_cas():
    manager = SessionManager()
    _, state = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, state)
    assert manager.discard(_SESSION_ID, SessionState()) is False
    assert manager.get(_SESSION_ID) is state
    assert manager.discard(_SESSION_ID, state) is True


def test_mark_terminal_and_expiry_are_expected_state_cas():
    manager = SessionManager()
    _, old = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, old)
    assert manager.mark_terminal(_SESSION_ID, SessionState()) is False
    assert manager.mark_terminal(_SESSION_ID, old) is True
    assert manager.discard(_SESSION_ID, old) is True
    _, fresh = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, fresh)
    manager._CLEANUP_AFTER_SECS = -1
    manager._cleanup()
    assert manager.get(_SESSION_ID) is fresh


def test_set_thread_is_expected_state_cas():
    manager = SessionManager()
    _, state = manager.prepare(session_id=_SESSION_ID)
    manager.register(_SESSION_ID, state)
    thread = threading.Thread()
    assert manager.set_thread(_SESSION_ID, SessionState(), thread) is False
    assert state.thread is None
    assert manager.set_thread(_SESSION_ID, state, thread) is True
    assert state.thread is thread


def test_direct_session_state_constructor_stays_compatible_but_cannot_register():
    manager = SessionManager()
    state = SessionState(environment=2)
    assert state.session_id == ""
    with pytest.raises(SessionRegistrationError) as caught:
        manager.register(_SESSION_ID, state)
    assert caught.value.reason == "state_mismatch"


@pytest.mark.parametrize(
    "initial_status",
    [
        "running",
        "stopping",
        "completed",
        "finished",
        "stopped",
        "failed",
        "stop_failed",
        "recoverable",
        "unexpected",
    ],
)
def test_force_failed_overrides_every_prior_status_and_error(initial_status):
    state = SessionState(status=initial_status, error="old terminal detail")

    state.force_failed("fixed fatal detail")

    assert state.status == "failed"
    assert state.error == "fixed fatal detail"


def test_removed_completed_status_is_not_terminal():
    state = SessionState(status="completed")
    assert state.is_terminal() is False


def test_begin_stopping_atomically_closes_strategy_decision_admission():
    state = SessionState(status="running")

    assert state.try_enter_strategy_decision() is True
    started, _operation_id = state.begin_stopping(operation_id="stop-1")

    assert started is True
    assert state.status == "stopping"
    assert state.try_enter_strategy_decision() is False
    assert state.wait_for_strategy_decisions(timeout_seconds=0) is False

    state.leave_strategy_decision()

    assert state.wait_for_strategy_decisions(timeout_seconds=0) is True


def test_manager_has_no_id_only_restore_escape_hatch():
    assert not hasattr(SessionManager, "restore")


def test_prepare_pending_is_registered_but_not_active():
    manager = SessionManager()
    session_id, state = manager.prepare(
        session_id=_SESSION_ID,
        initial_status="pending",
        portfolio_id=17,
    )

    manager.register(session_id, state)

    assert state.status == "pending"
    assert state.is_active() is False
    assert manager.find_active_session_for_portfolio(17) is None
    assert state.publication_state() == "BLOCKED"


@pytest.mark.parametrize("initial_status", ["", "stopping", "failed", "PENDING"])
def test_prepare_rejects_unknown_initial_status(initial_status):
    with pytest.raises(ValueError, match="initial_status"):
        SessionManager().prepare(
            session_id=_SESSION_ID,
            initial_status=initial_status,
        )


def test_running_publication_requires_exact_registered_state():
    manager = SessionManager()
    session_id, state = manager.prepare(
        session_id=_SESSION_ID,
        initial_status="pending",
    )
    manager.register(session_id, state)

    assert state.mark_running_publication_ready() is True
    assert state.status == "running"
    assert state.publication_state() == "READY"
    assert manager.claim_running_publication(session_id, SessionState()) is False
    assert manager.claim_running_publication(session_id, state) is True
    assert state.publication_state() == "PUBLISHING"
    assert state.complete_running_publication_submission() is True
    assert state.publication_state() == "RELEASED"


def test_fatal_before_publication_claim_forbids_running():
    manager = SessionManager()
    session_id, state = manager.prepare(
        session_id=_SESSION_ID,
        initial_status="pending",
    )
    manager.register(session_id, state)
    assert state.mark_running_publication_ready() is True

    assert state.latch_user_code_fatal("on_market_data") is True

    assert state.publication_state() == "TERMINAL"
    assert manager.claim_running_publication(session_id, state) is False
    assert state.complete_running_publication_submission() is False


def test_fatal_during_publication_defers_until_running_submission_finishes():
    manager = SessionManager()
    session_id, state = manager.prepare(
        session_id=_SESSION_ID,
        initial_status="pending",
    )
    manager.register(session_id, state)
    assert state.mark_running_publication_ready() is True
    assert manager.claim_running_publication(session_id, state) is True

    assert state.latch_user_code_fatal("callback") is True

    assert state.publication_state() == "PUBLISHING"
    assert state.complete_running_publication_submission() is False
    assert state.publication_state() == "TERMINAL"
