from __future__ import annotations

import importlib
import logging
import uuid
from typing import Any

from strategy_service.wallet.runtime import WalletRuntime

from strategy_service.inputs import (
    InputView,
    StrategyDeclarationError,
    StrategyInput,
    parse_declared_inputs,
)
from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.types import MarketData, OrderDecision
from strategy_service.wallet.order_types import ExecutionFeedback, OrderResponse

logger = logging.getLogger(__name__)


def _norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def _norm_market(market: str) -> str:
    return str(market).strip().lower()


def _norm_interval(interval: str) -> str:
    return str(interval).strip()


def _load_strategy_instance(strategy_path: str, strategy_code: str | None = None) -> Any:
    if strategy_code:
        # Dynamic exec for DB-backed strategies.
        ns: dict = {}
        try:
            code = compile(strategy_code, strategy_path, "exec")
            exec(code, ns)  # noqa: S102
        except Exception as e:
            raise ImportError(
                f"failed to exec strategy code: {e}"
            ) from e
        if "MyStrategy" not in ns:
            raise AttributeError(
                "strategy code has no 'MyStrategy' class"
            )
        return ns["MyStrategy"]()

    try:
        module = importlib.import_module(strategy_path)
    except ImportError as e:
        raise ImportError(
            f"failed to import strategy module {strategy_path!r}: {e}"
        ) from e
    if not hasattr(module, "MyStrategy"):
        raise AttributeError(
            f"module {strategy_path!r} has no 'MyStrategy' class"
        )
    cls = getattr(module, "MyStrategy")
    return cls()


def _read_declared_inputs(strategy_instance: Any) -> list[StrategyInput]:
    """Extract + normalize ``MyStrategy.INPUTS`` from an already-loaded strategy.

    Accepted forms (see ``inputs.parse_declared_inputs``):
      - class attribute ``INPUTS``
      - callable ``inputs()`` / ``declared_inputs()`` returning the same shape
    """
    raw = getattr(strategy_instance, "INPUTS", None)
    if raw is None:
        # Also accept a method variant for future compatibility.
        for attr in ("inputs", "declared_inputs"):
            fn = getattr(strategy_instance, attr, None)
            if callable(fn):
                try:
                    raw = fn()
                except Exception as e:
                    raise StrategyDeclarationError(
                        f"strategy.{attr}() raised: {e}"
                    ) from e
                break
    return parse_declared_inputs(raw)


def extract_strategy_inputs(
    strategy_path: str,
    strategy_code: str | None = None,
) -> list[StrategyInput]:
    """Introspection helper — loads the strategy and returns its declared inputs.

    Callers that want a best-effort symbol preview (e.g. gateway / preflight)
    can use this; it will raise ``StrategyDeclarationError`` if the strategy
    has no valid declaration, surfacing the same contract violation the
    runtime would see.
    """
    strategy = _load_strategy_instance(strategy_path, strategy_code)
    return _read_declared_inputs(strategy)


def flatten_declared_inputs_to_symbols(
    declared: list[StrategyInput],
) -> list[tuple[str, str]]:
    """Collapse a list of declared inputs to ``[(symbol, market), ...]`` for
    the preflight / live-subscription code that still operates at that
    granularity. Deduplicates by ``(symbol, market)``.

    Callers MUST call ``extract_strategy_inputs`` (which raises on declaration
    errors) first and surface those errors themselves; this helper deliberately
    does not swallow anything.
    """
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for inp in declared:
        key = (inp.symbol, inp.market)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


class BaseStrategy:
    """Dispatcher from declared-input ticks to wallet + user signal + order.

    Contract (pre_C3):
      - Strategy declares its ``(market, symbol, interval)`` universe via
        ``INPUTS`` on the class.
      - The router only binds to that declaration; wallet positions / assets
        do NOT expand the strategy universe.
      - ``on_market_data(view, wallet)`` receives a declaration-bound
        ``InputView`` — never a raw ``MarketData``.
    """

    def __init__(
        self,
        strategy_path: str,
        wallet: WalletRuntime,
        order_client: OrderClient | None = None,
        account_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        strategy_code: str | None = None,
        notifier: StrategyNotifier | None = None,
    ) -> None:
        self.strategy_path = strategy_path
        self.wallet = wallet
        self.on_order_callback: Any | None = None
        self._order_client: OrderClient = order_client or OrderClient()
        self._account_id: int = account_id
        self._strategy_id: int = strategy_id
        self._session_id: str = session_id
        self._strategy_code: str | None = strategy_code
        self._notifier = (notifier or StrategyNotifier()).bind_context(
            account_id=account_id,
            strategy_id=strategy_id,
            session_id=session_id,
        )

        # Load + validate the user strategy at construction time so any
        # declaration error fails fast (before any tick is routed).
        self._strategy_instance: Any = _load_strategy_instance(
            strategy_path, strategy_code=strategy_code,
        )
        setattr(self._strategy_instance, "notify", self._notifier)
        self._inputs: list[StrategyInput] = _read_declared_inputs(self._strategy_instance)
        self._input_keys: set[tuple[str, str, str]] = {i.key for i in self._inputs}
        # Order-side universe check: OrderDecision doesn't carry ``interval`` so
        # we guard at (market, symbol) granularity. A strategy that declared
        # (futures, ETHUSDT, 1m) cannot place a BTCUSDT or spot-market order.
        self._order_universe: set[tuple[str, str]] = {(i.market, i.symbol) for i in self._inputs}
        self._view: InputView = InputView(self._inputs)
        self._blocked_order_keys: set[tuple[str, str]] = set()

    @property
    def declared_inputs(self) -> list[StrategyInput]:
        """Read-only snapshot of the strategy's declared universe."""
        return list(self._inputs)

    def _get_strategy(self) -> Any:
        return self._strategy_instance

    def _notify_order_response(self, order_resp: Any) -> None:
        fn = getattr(self._strategy_instance, "on_order_response", None)
        if callable(fn):
            fn(order_resp)

    @staticmethod
    def _coerce_execution_feedback(payload: Any) -> ExecutionFeedback:
        if isinstance(payload, ExecutionFeedback):
            return payload
        if isinstance(payload, OrderResponse):
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=payload,
                fill_count=1 if str(payload.status).upper() == "FILLED" else 0,
                delta_qty=float(getattr(payload, "qty", 0.0) or 0.0),
            )
        status = str(getattr(payload, "attempt_status", "") or "").strip().upper()
        if status:
            return payload
        if hasattr(payload, "status"):
            return ExecutionFeedback(
                attempt_status="ACCEPTED",
                order=payload,
                fill_count=1 if str(getattr(payload, "status", "")).upper() == "FILLED" else 0,
                delta_qty=float(getattr(payload, "qty", 0.0) or 0.0),
            )
        raise TypeError(f"unsupported execution feedback payload: {type(payload)!r}")

    def running_strategy(self, market_data: MarketData) -> None:
        market = _norm_market(market_data.market)
        sym = _norm_symbol(market_data.symbol)
        interval = _norm_interval(getattr(market_data, "interval", ""))
        key = (market, sym, interval)

        # Declaration gate: only declared (market, symbol, interval) keys reach
        # the strategy. Wallet state is irrelevant here per pre_C3.
        if key not in self._input_keys:
            return

        # Refresh wallet mark price (wallet does not care about interval).
        self.wallet.on_market_data(sym, market, float(market_data.price))

        # Refresh the bound view with this tick.
        if not self._view.update(market_data):
            # Defensive: update() also enforces the declaration. Stay silent
            # rather than raise — the router gate above already screened.
            return

        # Call user strategy with the view, not the raw tick.
        signal: OrderDecision | None = self._strategy_instance.on_market_data(
            self._view, self.wallet
        )
        if signal is None:
            return
        if float(signal.qty) == 0:
            raise ValueError("OrderDecision.qty must be != 0 when returning a signal")

        # Determine order market: signal override > tick market.
        sig_market = _norm_market(signal.market or market)
        sig_sym = _norm_symbol(signal.symbol)

        # Order-side universe guard (pre_C3): reject orders that fall outside
        # the strategy's declared (market, symbol) set. Without this, a
        # strategy that only declared ETHUSDT could still place BTCUSDT orders
        # by returning a different symbol on OrderDecision — bypassing
        # preflight and stream-readiness entirely.
        if (sig_market, sig_sym) not in self._order_universe:
            raise ValueError(
                f"strategy attempted to place order outside declared universe: "
                f"signal=({sig_market}, {sig_sym}), declared="
                f"{sorted(self._order_universe)}"
            )
        if (sig_market, sig_sym) in self._blocked_order_keys:
            logger.warning(
                "skip order on blocked symbol while execution state unresolved: symbol=%s market=%s",
                sig_sym, sig_market,
            )
            return

        fill_price = float(signal.price) if signal.price is not None else float(market_data.price)

        # Look up a position/asset to get leverage for margin math.
        fw = getattr(self.wallet, "futures", None)
        pos = None
        if fw is not None and sig_market == "futures":
            matched = fw._get_positions_for_symbol(sig_sym)
            if matched:
                pos = matched[0][1]
        if pos is None and sig_market == "spot":
            sw = getattr(self.wallet, "spot", None)
            pos = sw.assets.get(sig_sym) if sw is not None else None
        leverage = float(getattr(pos, "leverage", 1.0)) if pos else 1.0
        qty = abs(float(signal.qty))
        side_upper = str(signal.side).upper()

        # Balance guard: distinguish open/close, spot buy/sell.
        if sig_market == "spot":
            sw = getattr(self.wallet, "spot", None)
            if side_upper in ("SELL", "SHORT"):
                asset = sw.assets.get(sig_sym) if sw is not None else None
                available_qty = (
                    float(getattr(asset, "qty", 0.0)) - float(getattr(asset, "locked", 0.0))
                    if asset else 0.0
                )
                if available_qty < qty:
                    logger.debug(
                        "[Skip] insufficient spot qty to SELL: need %.6f, have %.6f %s",
                        qty, available_qty, sig_sym,
                    )
                    return
            else:
                need = qty * fill_price
                free = float(sw.free) if sw is not None else 0.0
                if free < need:
                    logger.debug(
                        "[Skip] insufficient spot free USDT to BUY: need %.2f, have %.2f",
                        need, free,
                    )
                    return
        else:
            current_qty = float(getattr(pos, "net_qty", 0.0)) if pos else 0.0
            is_closing = False
            if pos is not None and current_qty != 0.0:
                if (current_qty > 0 and side_upper in ("SELL", "SHORT")) or \
                   (current_qty < 0 and side_upper in ("BUY", "LONG")):
                    if qty <= abs(current_qty):
                        is_closing = True
            if not is_closing:
                margin_needed = qty * fill_price / leverage
                if hasattr(self.wallet, "get_available_balance"):
                    available_balance = float(self.wallet.get_available_balance())
                else:
                    available_balance = 0.0
                if available_balance < margin_needed:
                    logger.debug(
                        "[Skip] insufficient available balance to open: need %.2f, have %.2f",
                        margin_needed, available_balance,
                    )
                    return

        intent_id = uuid.uuid4().hex
        feedback = self._coerce_execution_feedback(self._order_client.place_order(
            self._account_id, signal, fill_price,
            account_symbol=sig_sym,
            strategy_id=self._strategy_id,
            market=sig_market,
            session_id=self._session_id,
            intent_id=intent_id,
        ))
        has_settleable_fill = bool(feedback.fill_events) or (
            int(getattr(feedback, "fill_count", 0) or 0) > 0
            and feedback.order is not None
            and abs(float(getattr(feedback.order, "qty", 0.0) or 0.0)) > 0.0
        )
        pending_fill_confirmation = (
            feedback.attempt_status in {"ACCEPTED", "RECOVERED"}
            and feedback.order is not None
            and float(getattr(feedback.order, "executed_qty", 0.0) or 0.0) > 0.0
            and not has_settleable_fill
        )
        if feedback.attempt_status in {"UNKNOWN", "RECOVERING", "RECOVERY_FAILED"} or pending_fill_confirmation:
            self._blocked_order_keys.add((sig_market, sig_sym))
        elif feedback.attempt_status in {"FAILED", "ACCEPTED", "RECOVERED"}:
            self._blocked_order_keys.discard((sig_market, sig_sym))
        if feedback.attempt_status not in {"ACCEPTED", "RECOVERED"} or pending_fill_confirmation:
            self._notify_order_response(feedback)
            logger.warning(
                "order attempt unresolved or failed: symbol=%s market=%s attempt=%s status=%s error=%s",
                sig_sym, sig_market, feedback.attempt_id, feedback.attempt_status, feedback.error_message,
            )
            return

        if feedback.order is None:
            self._notify_order_response(feedback)
            logger.warning(
                "attempt accepted without order payload: symbol=%s market=%s attempt=%s",
                sig_sym, sig_market, feedback.attempt_id,
            )
            return

        if feedback.fill_events:
            for fill_event in feedback.fill_events:
                self.wallet.on_order(sig_sym, sig_market, fill_event)
        elif has_settleable_fill:
            self.wallet.on_order(sig_sym, sig_market, feedback.order)
        else:
            self._notify_order_response(feedback)
            logger.warning(
                "attempt accepted without confirmed fill details: symbol=%s market=%s attempt=%s",
                sig_sym, sig_market, feedback.attempt_id,
            )
            return
        self._notify_order_response(feedback)
        if self.on_order_callback is not None:
            try:
                self.on_order_callback()
            except Exception:
                logger.warning("on_order_callback failed", exc_info=True)
