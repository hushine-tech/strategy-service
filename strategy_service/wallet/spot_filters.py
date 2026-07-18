"""Pure-Python Binance Spot filter evaluator used by Hosted Backtest/Demo.

The evaluator consumes immutable facts supplied by core-service. It performs
no REST, WebSocket, DNS, clock, or filesystem I/O and returns the same stable
reason codes as core's authoritative filter gate.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Mapping, Sequence


ZERO = Decimal("0")
_MAX_NUMERIC_PRECISION = 38
_MAX_NUMERIC_SCALE = 18


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _decimal(raw: Any) -> Decimal:
    text = _text(raw)
    if not text or text.startswith("-") or text.startswith("+") or "e" in text.lower():
        raise ValueError("invalid unsigned decimal")
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("invalid unsigned decimal") from exc
    if not value.is_finite() or value < ZERO:
        raise ValueError("invalid unsigned decimal")
    return value


def _request_decimal(raw: Any) -> Decimal:
    text = _text(raw)
    value = _decimal(text)
    integer, dot, fraction = text.partition(".")
    integer_digits = len(integer.lstrip("0"))
    if integer_digits == 0:
        integer_digits = 1
    scale = len(fraction) if dot else 0
    if scale > _MAX_NUMERIC_SCALE or integer_digits + scale > _MAX_NUMERIC_PRECISION:
        raise ValueError("decimal outside NUMERIC(38,18)")
    return value


def _effective_scale(raw: Any) -> int:
    text = _text(raw)
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1].rstrip("0"))


def _enabled(raw: Any) -> bool:
    text = _text(raw)
    if not text:
        return False
    try:
        return _decimal(text) != ZERO
    except ValueError:
        return True


def _aligned(raw: Any, step_raw: Any) -> bool:
    value = _decimal(raw)
    step = _decimal(step_raw)
    if step == ZERO:
        return True
    return value % step == ZERO


def _find_filter(filters: Sequence[Mapping[str, Any]], filter_type: str) -> Mapping[str, Any] | None:
    wanted = filter_type.strip().upper()
    for item in filters:
        if _text(item.get("filter_type")).upper() == wanted:
            return item
    return None


def _bounds(value: Decimal, item: Mapping[str, Any], low: str, high: str, code: str) -> str:
    try:
        if _enabled(item.get(low)) and value < _decimal(item.get(low)):
            return code
        if _enabled(item.get(high)) and value > _decimal(item.get(high)):
            return code
    except ValueError:
        return "SPOT_RISK_FACTS_UNAVAILABLE"
    return ""


def _quantity_filter(raw: str, qty: Decimal, item: Mapping[str, Any], code: str) -> str:
    violation = _bounds(qty, item, "min_qty", "max_qty", code)
    if violation:
        return violation
    if _enabled(item.get("step_size")):
        try:
            if not _aligned(raw, item.get("step_size")):
                return code
        except ValueError:
            return "SPOT_RISK_FACTS_UNAVAILABLE"
    return ""


def _price_filter(raw: str, price: Decimal, item: Mapping[str, Any]) -> str:
    violation = _bounds(price, item, "min_price", "max_price", "SPOT_PRICE_FILTER")
    if violation:
        return violation
    if _enabled(item.get("tick_size")):
        try:
            if not _aligned(raw, item.get("tick_size")):
                return "SPOT_PRICE_FILTER"
        except ValueError:
            return "SPOT_RISK_FACTS_UNAVAILABLE"
    return ""


def _notional_bounds(notional: Decimal, low: Any, high: Any, code: str) -> str:
    try:
        if _enabled(low) and notional < _decimal(low):
            return code
        if _enabled(high) and notional > _decimal(high):
            return code
    except ValueError:
        return "SPOT_RISK_FACTS_UNAVAILABLE"
    return ""


def _percent_price(price: Decimal, reference_raw: Any, down_raw: Any, up_raw: Any, code: str) -> str:
    try:
        reference = _decimal(reference_raw)
        if reference <= ZERO:
            return "SPOT_RISK_FACTS_UNAVAILABLE"
        if _enabled(down_raw) and price < reference * _decimal(down_raw):
            return code
        if _enabled(up_raw) and price > reference * _decimal(up_raw):
            return code
    except ValueError:
        return "SPOT_RISK_FACTS_UNAVAILABLE"
    return ""


def _balance(balances: Sequence[Mapping[str, Any]], asset: str, field: str) -> tuple[Decimal, bool]:
    wanted = asset.strip().upper()
    for item in balances:
        if _text(item.get("asset")).upper() != wanted:
            continue
        try:
            return _decimal(item.get(field, "0")), True
        except ValueError:
            return ZERO, False
    return ZERO, False


def _order_count(
    open_orders: Sequence[Mapping[str, Any]],
    item: Mapping[str, Any],
    *,
    symbol: str,
    symbol_scoped: bool,
) -> str:
    limit = int(item.get("max_num_orders") or item.get("limit") or 0)
    if limit <= 0:
        return ""
    count = sum(
        1
        for order in open_orders
        if not symbol_scoped or _text(order.get("symbol")).upper() == symbol
    )
    return "SPOT_MAX_NUM_ORDERS" if count + 1 > limit else ""


def evaluate_spot_filter_vector(vector: Mapping[str, Any]) -> str:
    """Return the stable rejection code, or ``""`` when the order is allowed."""
    with localcontext() as context:
        # NUMERIC(38,18) operations plus multiplication/remainder require more
        # than Decimal's process-default precision of 28.
        context.prec = 100
        return _evaluate_spot_filter_vector(vector)


def _evaluate_spot_filter_vector(vector: Mapping[str, Any]) -> str:
    request = vector.get("request") or {}
    facts = vector.get("facts") or {}
    metadata = facts.get("metadata") or {}
    balances = vector.get("balances") or []
    symbol = _text(request.get("symbol")).upper()
    side = _text(request.get("side")).upper()
    order_type = _text(request.get("order_type")).upper()
    qty_raw = _text(request.get("qty_decimal"))
    price_raw = _text(request.get("price_decimal"))
    base_asset = _text(metadata.get("base_asset")).upper()
    quote_asset = _text(metadata.get("quote_asset")).upper()

    if _text(metadata.get("symbol")).upper() != symbol or not base_asset or not quote_asset:
        return "SPOT_RISK_FACTS_UNAVAILABLE"
    try:
        qty = _request_decimal(qty_raw)
    except ValueError:
        return "ORDER_DECIMAL_OUT_OF_RANGE"
    if qty <= ZERO:
        return "ORDER_DECIMAL_OUT_OF_RANGE"
    if _effective_scale(qty_raw) > int(metadata.get("base_asset_precision") or 0):
        return "SPOT_ASSET_PRECISION"
    if price_raw and _effective_scale(price_raw) > int(metadata.get("quote_asset_precision") or 0):
        return "SPOT_ASSET_PRECISION"

    if order_type == "LIMIT":
        if not price_raw:
            return "PRICE_REQUIRED_FOR_RISK"
        try:
            price = _request_decimal(price_raw)
        except ValueError:
            return "ORDER_DECIMAL_OUT_OF_RANGE"
        if price <= ZERO:
            return "ORDER_DECIMAL_OUT_OF_RANGE"
    else:
        try:
            price = _decimal(facts.get("reference_price_decimal"))
        except ValueError:
            return "SPOT_REFERENCE_PRICE_UNAVAILABLE"
        if price <= ZERO:
            return "SPOT_REFERENCE_PRICE_UNAVAILABLE"

    filters = list(metadata.get("filters") or [])
    if order_type == "LIMIT":
        price_filter = _find_filter(filters, "PRICE_FILTER")
        if price_filter is not None:
            code = _price_filter(price_raw, price, price_filter)
            if code:
                return code

    quantity_type = "LOT_SIZE"
    quantity_filter = _find_filter(filters, quantity_type)
    market_filter = _find_filter(filters, "MARKET_LOT_SIZE") if order_type == "MARKET" else None
    if market_filter is not None and any(
        _enabled(market_filter.get(name)) for name in ("min_qty", "max_qty", "step_size")
    ):
        quantity_type = "MARKET_LOT_SIZE"
        quantity_filter = market_filter
    if quantity_filter is None:
        return "SPOT_RISK_FACTS_UNAVAILABLE"
    code = _quantity_filter(
        qty_raw,
        qty,
        quantity_filter,
        "SPOT_MARKET_LOT_SIZE" if quantity_type == "MARKET_LOT_SIZE" else "SPOT_LOT_SIZE",
    )
    if code:
        return code

    notional = qty * price
    open_orders = list(facts.get("open_orders") or [])
    ignored = {
        "",
        "PRICE_FILTER",
        "LOT_SIZE",
        "MARKET_LOT_SIZE",
        "ICEBERG_PARTS",
        "TRAILING_DELTA",
        "MAX_NUM_ALGO_ORDERS",
        "MAX_NUM_ICEBERG_ORDERS",
        "MAX_NUM_ORDER_AMENDS",
        "MAX_NUM_ORDER_LISTS",
    }
    for item in filters:
        filter_type = _text(item.get("filter_type")).upper()
        if filter_type in ignored:
            continue
        if filter_type == "MIN_NOTIONAL":
            if order_type != "MARKET" or bool(item.get("apply_to_market")):
                code = _notional_bounds(notional, item.get("min_notional"), "", "SPOT_MIN_NOTIONAL")
        elif filter_type == "NOTIONAL":
            low = item.get("min_notional")
            high = item.get("max_notional")
            if order_type == "MARKET":
                low = low if bool(item.get("apply_min_to_market")) else ""
                high = high if bool(item.get("apply_max_to_market")) else ""
            code = _notional_bounds(notional, low, high, "SPOT_NOTIONAL")
        elif filter_type == "PERCENT_PRICE":
            code = "" if order_type != "LIMIT" else _percent_price(
                price,
                facts.get("reference_price_decimal"),
                item.get("multiplier_down"),
                item.get("multiplier_up"),
                "SPOT_PERCENT_PRICE",
            )
        elif filter_type == "PERCENT_PRICE_BY_SIDE":
            if order_type != "LIMIT":
                code = ""
            else:
                prefix = "bid" if side == "BUY" else "ask"
                code = _percent_price(
                    price,
                    facts.get("reference_price_decimal"),
                    item.get(f"{prefix}_multiplier_down"),
                    item.get(f"{prefix}_multiplier_up"),
                    "SPOT_PERCENT_PRICE_BY_SIDE",
                )
        elif filter_type == "MAX_POSITION":
            code = ""
            if side == "BUY" and _enabled(item.get("max_position")):
                available, ok_free = _balance(balances, base_asset, "available_decimal")
                locked, ok_locked = _balance(balances, base_asset, "locked_decimal")
                if not ok_free and not ok_locked:
                    available = locked = ZERO
                position = available + locked + qty
                try:
                    for order in open_orders:
                        if (
                            _text(order.get("symbol")).upper() == symbol
                            and _text(order.get("side")).upper() == "BUY"
                        ):
                            position += _decimal(order.get("orig_qty_decimal")) - _decimal(
                                order.get("executed_qty_decimal")
                            )
                    if position > _decimal(item.get("max_position")):
                        code = "SPOT_MAX_POSITION"
                except ValueError:
                    code = "SPOT_RISK_FACTS_UNAVAILABLE"
        elif filter_type == "MAX_NUM_ORDERS":
            code = _order_count(open_orders, item, symbol=symbol, symbol_scoped=True)
        else:
            return f"SPOT_FILTER_UNSUPPORTED:{filter_type}"
        if code:
            return code

    for item in facts.get("exchange_filters") or []:
        filter_type = _text(item.get("filter_type")).upper()
        if filter_type in {"MAX_NUM_ORDERS", "EXCHANGE_MAX_NUM_ORDERS"}:
            code = _order_count(
                open_orders,
                item,
                symbol=symbol,
                symbol_scoped=filter_type == "MAX_NUM_ORDERS",
            )
        elif filter_type in {
            "MAX_NUM_ALGO_ORDERS",
            "EXCHANGE_MAX_NUM_ALGO_ORDERS",
            "MAX_NUM_ICEBERG_ORDERS",
            "MAX_NUM_ORDER_AMENDS",
            "MAX_NUM_ORDER_LISTS",
        }:
            code = ""
        else:
            return f"SPOT_FILTER_UNSUPPORTED:{filter_type}"
        if code:
            return code
    for item in facts.get("symbol_filters") or []:
        filter_type = _text(item.get("filter_type")).upper()
        if filter_type in {"MAX_NUM_ORDERS", "EXCHANGE_MAX_NUM_ORDERS"}:
            code = _order_count(open_orders, item, symbol=symbol, symbol_scoped=True)
        elif filter_type in {
            "MAX_NUM_ALGO_ORDERS",
            "EXCHANGE_MAX_NUM_ALGO_ORDERS",
            "MAX_NUM_ICEBERG_ORDERS",
            "MAX_NUM_ORDER_AMENDS",
            "MAX_NUM_ORDER_LISTS",
        }:
            code = ""
        else:
            return f"SPOT_FILTER_UNSUPPORTED:{filter_type}"
        if code:
            return code

    for item in facts.get("asset_filters") or []:
        filter_type = _text(item.get("filter_type")).upper()
        if filter_type != "MAX_ASSET":
            return f"SPOT_FILTER_UNSUPPORTED:{filter_type}"
        asset = _text(item.get("asset")).upper()
        value = qty if asset == base_asset else notional if asset == quote_asset else None
        if value is not None:
            try:
                limit = _decimal(item.get("limit"))
            except ValueError:
                return "SPOT_RISK_FACTS_UNAVAILABLE"
            if limit > ZERO and value > limit:
                return "SPOT_MAX_ASSET"

    if bool(request.get("reduce_only")) and side == "BUY":
        return "SPOT_REDUCE_ONLY_BUY"
    if side == "SELL":
        available, ok = _balance(balances, base_asset, "available_decimal")
        if not ok or available < qty:
            return "INSUFFICIENT_UNLOCKED_QTY"
    elif side == "BUY":
        available, ok = _balance(balances, quote_asset, "available_decimal")
        if not ok or available < notional:
            return "INSUFFICIENT_QUOTE_BALANCE"
    return ""
