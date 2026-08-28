"""Shared futures position-side contract from core-service protobuf bindings."""

from __future__ import annotations

from strategy_service.gen import portfolio_service_pb2


# These aliases deliberately refer to the generated protobuf values.  This
# module owns no duplicate enum declaration or independently-maintained codes.
BOTH = portfolio_service_pb2.FUTURES_POSITION_SIDE_BOTH
LONG = portfolio_service_pb2.FUTURES_POSITION_SIDE_LONG
SHORT = portfolio_service_pb2.FUTURES_POSITION_SIDE_SHORT

_ENUM = portfolio_service_pb2.FuturesPositionSide.DESCRIPTOR
_PREFIX = "FUTURES_POSITION_SIDE_"


def position_side_label(value: int) -> str:
    """Return the canonical short label for one generated enum value."""
    if type(value) is not int:
        raise ValueError(f"invalid FuturesPositionSide: {value!r}")
    descriptor = _ENUM.values_by_number.get(value)
    if descriptor is None or not descriptor.name.startswith(_PREFIX):
        raise ValueError(f"invalid FuturesPositionSide: {value!r}")
    return descriptor.name.removeprefix(_PREFIX)


def position_side_from_label(value: str | None) -> int:
    """Translate an exact public short label to its generated enum value."""
    if value is None:
        return BOTH
    if not isinstance(value, str):
        raise ValueError(f"invalid FuturesPositionSide label: {value!r}")
    descriptor = _ENUM.values_by_name.get(f"{_PREFIX}{value}")
    if descriptor is None:
        raise ValueError(f"invalid FuturesPositionSide label: {value!r}")
    return descriptor.number


def position_direction_key(*, position_mode: str, position_side: int) -> int:
    """Derive the private wallet key solely from the shared enum value."""
    position_side_label(position_side)
    mode = str(position_mode or "").strip().lower()
    if mode == "one_way":
        if position_side != BOTH:
            raise ValueError("one-way FuturesPosition must use BOTH")
        return 0
    if mode != "hedge":
        raise ValueError("canonical Futures position_mode is missing or invalid")
    if position_side == LONG:
        return 1
    if position_side == SHORT:
        return -1
    if position_side == BOTH:
        raise ValueError("hedge FuturesPosition requires LONG or SHORT")
    raise ValueError(f"invalid FuturesPositionSide: {position_side!r}")
