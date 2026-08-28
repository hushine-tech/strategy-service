"""Classify a failed place-order call without replaying the request."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from strategy_service.worker_agent_client import WorkerPlatformCallError


_REQUEST_REJECTION_CODES = frozenset({
    "invalidargument",
    "failedprecondition",
    "permissiondenied",
    "notfound",
})


def _transport_code(error: BaseException) -> str:
    raw_code: Any = getattr(error, "code", "")
    if callable(raw_code):
        try:
            raw_code = raw_code()
        except Exception:  # noqa: BLE001
            raw_code = ""
    name = getattr(raw_code, "name", "")
    return str(name or raw_code or "").strip()


def _normalized_code(code: str) -> str:
    return "".join(character for character in code.lower() if character.isalnum()).removeprefix("statuscode")


def _detail(*, intent_id: str, error: BaseException, resolution_error: BaseException | None = None) -> str:
    payload = {
        "intent_id": str(intent_id or ""),
        "transport_cause": type(error).__name__,
        "transport_code": _transport_code(error),
    }
    if resolution_error is not None:
        payload["resolution_cause"] = type(resolution_error).__name__
        payload["resolution_code"] = _transport_code(resolution_error)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _fatal(
    *,
    code: str,
    message: str,
    intent_id: str,
    error: BaseException,
    resolution_error: BaseException | None = None,
) -> WorkerPlatformCallError:
    return WorkerPlatformCallError(
        message,
        code=code,
        detail_json=_detail(
            intent_id=intent_id,
            error=error,
            resolution_error=resolution_error,
        ),
    )


def resolve_order_placement_error(
    error: BaseException,
    *,
    intent_id: str,
    resolve_attempt: Callable[[], object],
) -> object:
    """Return a persisted attempt or raise a typed fatal order outcome.

    Deterministic request rejection is known not to have reached persistence.
    Every other failed transport is treated as ambiguous and resolved once;
    callers must not retry ``PlaceOrder`` from this boundary.
    """
    if _normalized_code(_transport_code(error)) in _REQUEST_REJECTION_CODES:
        raise _fatal(
            code="ORDER_REQUEST_REJECTED",
            message="order request was rejected before persistence",
            intent_id=intent_id,
            error=error,
        ) from error

    try:
        response = resolve_attempt()
    except Exception as resolution_error:  # noqa: BLE001
        raise _fatal(
            code="ORDER_EXECUTION_UNKNOWN",
            message="order execution outcome could not be established",
            intent_id=intent_id,
            error=error,
            resolution_error=resolution_error,
        ) from resolution_error

    if str(getattr(response, "attempt_id", "") or "").strip():
        return response
    raise _fatal(
        code="ORDER_EXECUTION_UNKNOWN",
        message="order execution outcome could not be established",
        intent_id=intent_id,
        error=error,
    ) from error
