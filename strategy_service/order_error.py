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


def order_error_context(
    *,
    portfolio_id: int,
    symbol: str,
    exchange: int,
    market: int,
    venue_id: int = 0,
) -> dict[str, object]:
    venue: dict[str, int] = {
        "portfolio_id": int(portfolio_id),
        "exchange": int(exchange),
        "market": int(market),
    }
    if int(venue_id) > 0:
        venue["venue_id"] = int(venue_id)
    return {"symbol": str(symbol or "").strip().upper(), "venue": venue}


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


def _detail(
    *,
    intent_id: str,
    context: dict[str, object],
    stage: str,
    cause: str,
    error: BaseException | None = None,
    resolution_error: BaseException | None = None,
) -> str:
    transport_code = _transport_code(error) if error is not None else ""
    payload = {
        "intent_id": str(intent_id or ""),
        "symbol": str(context.get("symbol", "") or ""),
        "venue": dict(context.get("venue", {})),
        "stage": str(stage),
        "cause": str(cause),
        "transport_cause": (
            f"{type(error).__name__}:{transport_code}"
            if error is not None
            else "successful_response"
        ),
        "transport_code": transport_code,
    }
    if resolution_error is not None:
        resolution_code = _transport_code(resolution_error)
        payload["resolution_cause"] = f"{type(resolution_error).__name__}:{resolution_code}"
        payload["resolution_code"] = resolution_code
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _fatal(
    *,
    code: str,
    message: str,
    intent_id: str,
    context: dict[str, object],
    stage: str,
    cause: str,
    error: BaseException | None = None,
    resolution_error: BaseException | None = None,
) -> WorkerPlatformCallError:
    return WorkerPlatformCallError(
        message,
        code=code,
        detail_json=_detail(
            intent_id=intent_id,
            context=context,
            stage=stage,
            cause=cause,
            error=error,
            resolution_error=resolution_error,
        ),
    )


def require_persisted_order_attempt(
    response: object,
    *,
    intent_id: str,
    context: dict[str, object],
    stage: str,
    error: BaseException | None = None,
) -> object:
    """Return only responses that prove a persisted order-attempt identity."""
    if str(getattr(response, "attempt_id", "") or "").strip():
        return response
    cause = (
        "no persisted order attempt was found"
        if stage == "resolve_order_attempt"
        else "response missing persisted order attempt identity"
    )
    raise _fatal(
        code="ORDER_EXECUTION_UNKNOWN",
        message="order execution outcome could not be established",
        intent_id=intent_id,
        context=context,
        stage=stage,
        cause=cause,
        error=error,
    ) from error


def resolve_order_placement_error(
    error: BaseException,
    *,
    intent_id: str,
    context: dict[str, object],
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
            context=context,
            stage="place_order",
            cause="place_order transport failure",
            error=error,
        ) from error

    try:
        response = resolve_attempt()
    except Exception as resolution_error:  # noqa: BLE001
        raise _fatal(
            code="ORDER_EXECUTION_UNKNOWN",
            message="order execution outcome could not be established",
            intent_id=intent_id,
            context=context,
            stage="resolve_order_attempt",
            cause="resolve_order_attempt transport failure",
            error=error,
            resolution_error=resolution_error,
        ) from resolution_error

    return require_persisted_order_attempt(
        response,
        intent_id=intent_id,
        context=context,
        stage="resolve_order_attempt",
        error=error,
    )
