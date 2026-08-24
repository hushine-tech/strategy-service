"""Proxy-only platform clients for self-hosted runtimes.

Self-hosted runtimes cannot dial internal core-service, order API,
market-data control-plane, Kafka, or platform databases. These clients keep
the strategy runtime API shape the same while sending approved platform RPCs
back over RuntimeChannel to control-panel-service.
"""

from __future__ import annotations

import logging
import threading
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from strategy_service.portfolio_client import (
    _compute_total_value,
    _exchange_enum,
    _get_available_balance,
    _get_wallet_balance,
    _market_enum,
    _required_symbol_protos,
    _serialize_future_wallet,
    _serialize_spot_wallet,
)
from strategy_service.order_client import (
    OrderClient,
    _market_time_to_proto,
    canonical_decimal_text,
)
from strategy_service.types import ExecutionFeedback, OrderDecision

logger = logging.getLogger(__name__)


PORTFOLIO_GET_PORTFOLIO = "portfolio.GetPortfolioSnapshot"
PORTFOLIO_UPDATE_WALLET_STATE = "portfolio.UpdatePortfolioWalletState"
PORTFOLIO_PREFLIGHT_STRATEGY_SESSION = "portfolio.PreflightStrategySession"
PORTFOLIO_COMMIT_STRATEGY_SESSION_START = "portfolio.CommitStrategySessionStart"
PORTFOLIO_GET_ACTIVE_STRATEGY = "portfolio.GetActiveStrategy"
PORTFOLIO_UPDATE_SESSION = "portfolio.UpdateSession"
ORDER_PLACE = "order.PlaceOrder"
ORDER_RESOLVE_ATTEMPT = "order.ResolveOrderAttempt"
ORDER_CLOSE_SPOT_TARGETS = "order.CloseSpotTargets"
ORDER_LIST_LIFECYCLE_EVENTS = "order.ListOrderLifecycleEvents"
MARKETDATA_GET_STATUS = "marketdata.GetMarketDataStreamStatus"
MARKETDATA_FETCH_KLINES = "marketdata.FetchKlines"
MARKETDATA_FETCH_BACKTEST_PAGE = "marketdata.FetchBacktestPage"
MARKETDATA_DELIVER_DATASET = "marketdata.DeliverDataset"
MARKETDATA_RENEW_LEASE = "marketdata.CreateOrRenewMarketDataLease"
MARKETDATA_RELEASE_LEASE = "marketdata.ReleaseMarketDataLease"
MARKETDATA_CREATE_SESSION_SUBSCRIPTIONS = "marketdata.CreateSessionMarketDataSubscriptions"
MARKETDATA_RELEASE_SESSION_SUBSCRIPTIONS = "marketdata.ReleaseSessionMarketDataSubscriptions"
LOGS_EMIT = "logs.Emit"
NOTIFICATION_PUBLISH = "notification.Publish"
BACKTEST_PAGE_SIZE = 8192


class RuntimeChannelPlatformProxy:
    def __init__(self, runtime_channel_client) -> None:
        if runtime_channel_client is None:
            raise ValueError("runtime_channel_client is required")
        self._client = runtime_channel_client

    def invoke(self, method: str, request, response_type, *, timeout_seconds: float = 30.0):
        return self._client.invoke_platform_unary(
            method,
            request,
            response_type,
            timeout_seconds=timeout_seconds,
        )

    def portfolio_client(self) -> "ProxyPortfolioClient":
        return ProxyPortfolioClient(self)

    def order_client(self) -> "ProxyOrderClient":
        return ProxyOrderClient(self)

    def marketdata_client(self) -> "ProxyMarketDataClient":
        return ProxyMarketDataClient(self)

    def log_client(self) -> "ProxyLogClient":
        return ProxyLogClient(self)

    def notification_client(self) -> "ProxyNotificationClient":
        return ProxyNotificationClient(self)


class ProxyPortfolioClient:
    def __init__(self, proxy: RuntimeChannelPlatformProxy) -> None:
        self._proxy = proxy

    def get_portfolio_snapshot(
        self,
        portfolio_id: int,
        user_id: int = 0,
        required_symbols: list[tuple[str, str, str]] | set[tuple[str, str, str]] | None = None,
    ):
        try:
            from strategy_service.gen import portfolio_service_pb2

            resp = self._proxy.invoke(
                PORTFOLIO_GET_PORTFOLIO,
                portfolio_service_pb2.GetPortfolioSnapshotRequest(
                    portfolio_id=int(portfolio_id),
                    user_id=int(user_id),
                    required_symbols=_required_symbol_protos(portfolio_service_pb2, required_symbols),
                ),
                portfolio_service_pb2.GetPortfolioSnapshotResponse,
            )
            return resp.snapshot
        except Exception:
            logger.warning(
                "Proxy GetPortfolioSnapshot failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            return None

    def update_portfolio_wallet_state(
        self,
        portfolio_id: int,
        user_id: int = 0,
        future_wallet: Optional[Any] = None,
        spot_wallet: Optional[Any] = None,
        snapshot_reason: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        snapshot_time: object | None = None,
    ):
        """Push strategy-computed wallet state for snapshot/audit sync."""
        try:
            from strategy_service.gen import portfolio_service_pb2

            kwargs = {
                "portfolio_id": int(portfolio_id),
                "user_id": int(user_id),
                "futures": _serialize_future_wallet(future_wallet) if future_wallet else None,
                "spot": _serialize_spot_wallet(spot_wallet) if spot_wallet else None,
                "total_value": _compute_total_value(future_wallet, spot_wallet),
                "wallet_balance": _get_wallet_balance(future_wallet),
                "available_balance": _get_available_balance(future_wallet),
                "snapshot_reason": int(snapshot_reason),
                "strategy_id": int(strategy_id),
                "session_id": str(session_id or ""),
            }
            snapshot_time_pb = _market_time_to_proto(snapshot_time)
            if snapshot_time_pb is not None:
                kwargs["snapshot_time"] = snapshot_time_pb
            resp = self._proxy.invoke(
                PORTFOLIO_UPDATE_WALLET_STATE,
                portfolio_service_pb2.UpdatePortfolioWalletStateRequest(**kwargs),
                portfolio_service_pb2.UpdatePortfolioWalletStateResponse,
            )
            return resp.wallet
        except Exception as exc:
            logger.warning(
                "Proxy UpdatePortfolioWalletState failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            raise RuntimeError(
                f"Proxy UpdatePortfolioWalletState failed for portfolio_id={portfolio_id} user_id={user_id}: {exc}"
            ) from exc

    def preflight_strategy_session(
        self,
        portfolio_id: int,
        user_id: int = 0,
        required_routes: list[tuple[str, str]] | set[tuple[str, str]] | None = None,
        required_symbols: list[tuple[str, str, str]] | set[tuple[str, str, str]] | None = None,
        order_target_symbols: list[tuple[str, str, str]] | set[tuple[str, str, str]] | None = None,
        order_targets: list[Any] | tuple[Any, ...] | None = None,
        session_id: str = "",
        strategy_id: int = 0,
    ):
        try:
            from strategy_service.gen import portfolio_service_pb2

            req = portfolio_service_pb2.PreflightStrategySessionRequest(
                portfolio_id=int(portfolio_id),
                user_id=int(user_id),
                session_id=str(session_id or ""),
                strategy_id=int(strategy_id),
                required_routes=[
                    portfolio_service_pb2.RequiredRoute(
                        exchange=_exchange_enum(exchange),
                        market=_market_enum(market),
                    )
                    for exchange, market in sorted(required_routes or [])
                ],
                required_symbols=_required_symbol_protos(
                    portfolio_service_pb2,
                    required_symbols,
                    order_target_symbols=order_target_symbols,
                    order_targets=order_targets,
                ),
            )
            return self._proxy.invoke(
                PORTFOLIO_PREFLIGHT_STRATEGY_SESSION,
                req,
                portfolio_service_pb2.PreflightStrategySessionResponse,
            )
        except Exception:
            logger.warning(
                "Proxy PreflightStrategySession failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            return None

    def commit_strategy_session_start(self, request: Any, *, timeout_seconds: float = 60.0):
        """Relay one typed strategy launch commit over RuntimeChannel."""
        from strategy_service.gen import portfolio_service_pb2

        return self._proxy.invoke(
            PORTFOLIO_COMMIT_STRATEGY_SESSION_START,
            request,
            portfolio_service_pb2.CommitStrategySessionStartResponse,
            timeout_seconds=float(timeout_seconds),
        )

    def get_active_strategy(self, portfolio_id: int):
        try:
            from strategy_service.gen import portfolio_service_pb2

            return self._proxy.invoke(
                PORTFOLIO_GET_ACTIVE_STRATEGY,
                portfolio_service_pb2.GetActiveStrategyRequest(portfolio_id=int(portfolio_id)),
                portfolio_service_pb2.GetActiveStrategyResponse,
            )
        except Exception:
            logger.warning("Proxy GetActiveStrategy failed for portfolio_id=%s", portfolio_id, exc_info=True)
            return None

    def update_session(
        self,
        session_id: str,
        status: str,
        bars_processed: int = 0,
        error: str = "",
        runtime_id: str = "",
        expected_status: str = "",
        strict: bool = False,
    ) -> bool:
        try:
            from strategy_service.gen import portfolio_service_pb2

            req = portfolio_service_pb2.UpdateSessionRequest(
                session_id=session_id,
                status=status,
                bars_processed=int(bars_processed),
                error=error,
                runtime_id=str(runtime_id or ""),
                expected_status=str(expected_status or ""),
            )
            self._proxy.invoke(
                PORTFOLIO_UPDATE_SESSION,
                req,
                portfolio_service_pb2.UpdateSessionResponse,
                timeout_seconds=60.0,
            )
            return True
        except Exception:
            logger.warning("Proxy UpdateSession failed for session=%s", session_id, exc_info=True)
            if strict:
                raise
            return False

    def list_running_sessions(self, runtime_id: str = ""):
        del runtime_id
        return []

    def require_running_sessions(self, runtime_id: str = ""):
        del runtime_id
        raise RuntimeError("self-hosted runtime recovery must be coordinated by control-panel")

@dataclass(frozen=True)
class BacktestPage:
    stream_key: str
    klines: list[Any]
    next_cursor_time_ms: int
    has_more: bool


class ProxyMarketDataClient:
    def __init__(self, proxy: RuntimeChannelPlatformProxy) -> None:
        self._proxy = proxy

    def get_market_data_stream_status(
        self,
        *,
        stream_id: int = 0,
        exchange: str = "",
        market: str = "",
        kind: str = "kline",
        symbol: str = "",
        interval: str = "",
    ):
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.GetMarketDataStreamStatusRequest(stream_id=int(stream_id))
            if stream_id <= 0:
                req.key.CopyFrom(
                    marketdata_service_pb2.StreamKey(
                        exchange=exchange,
                        market=market,
                        kind=kind,
                        symbol=symbol,
                        interval=interval,
                    )
                )
            resp = self._proxy.invoke(
                MARKETDATA_GET_STATUS,
                req,
                marketdata_service_pb2.GetMarketDataStreamStatusResponse,
            )
            return resp.stream
        except Exception:
            logger.warning(
                "Proxy GetMarketDataStreamStatus failed for %s/%s/%s/%s/%s",
                exchange,
                market,
                kind,
                symbol,
                interval,
                exc_info=True,
            )
            return None

    def create_or_renew_market_data_lease(
        self,
        *,
        session_id: str,
        strategy_id: int = 0,
        portfolio_id: int = 0,
        stream_id: int,
        ttl_seconds: int,
    ) -> bool:
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.CreateOrRenewMarketDataLeaseRequest(
                session_id=session_id,
                strategy_id=int(strategy_id),
                portfolio_id=int(portfolio_id),
                stream_id=int(stream_id),
                ttl_seconds=int(ttl_seconds),
            )
            self._proxy.invoke(
                MARKETDATA_RENEW_LEASE,
                req,
                marketdata_service_pb2.CreateOrRenewMarketDataLeaseResponse,
            )
            return True
        except Exception:
            logger.warning(
                "Proxy CreateOrRenewMarketDataLease failed for session=%s stream_id=%s",
                session_id,
                stream_id,
                exc_info=True,
            )
            return False

    def create_session_market_data_subscriptions(
        self,
        *,
        user_id: int,
        session_id: str,
        runtime_id: str,
        environment: int,
        streams,
    ) -> bool:
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.CreateSessionMarketDataSubscriptionsRequest(
                user_id=int(user_id),
                session_id=session_id,
                runtime_id=runtime_id,
                environment=int(environment),
            )
            for stream in streams:
                req.keys.append(marketdata_service_pb2.StreamKey(
                    exchange=getattr(stream, "exchange", "") or "binance",
                    market=getattr(stream, "market", ""),
                    kind=getattr(stream, "kind", "") or "kline",
                    symbol=getattr(stream, "symbol", ""),
                    interval=getattr(stream, "interval", ""),
                ))
            resp = self._proxy.invoke(
                MARKETDATA_CREATE_SESSION_SUBSCRIPTIONS,
                req,
                marketdata_service_pb2.CreateSessionMarketDataSubscriptionsResponse,
            )
            return len(resp.subscriptions) == len(req.keys)
        except Exception:
            logger.warning(
                "Proxy CreateSessionMarketDataSubscriptions failed for session=%s runtime_id=%s",
                session_id,
                runtime_id,
                exc_info=True,
            )
            return False

    def release_session_market_data_subscriptions(self, *, session_id: str, runtime_id: str = "") -> bool:
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.ReleaseSessionMarketDataSubscriptionsRequest(
                session_id=session_id,
                runtime_id=runtime_id,
            )
            self._proxy.invoke(
                MARKETDATA_RELEASE_SESSION_SUBSCRIPTIONS,
                req,
                marketdata_service_pb2.ReleaseSessionMarketDataSubscriptionsResponse,
            )
            return True
        except Exception:
            logger.warning(
                "Proxy ReleaseSessionMarketDataSubscriptions failed for session=%s runtime_id=%s",
                session_id,
                runtime_id,
                exc_info=True,
            )
            return False

    def fetch_klines(
        self,
        *,
        exchange: str = "binance",
        market: str,
        symbol: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> list[Any]:
        try:
            req = Struct()
            req.update({
                "exchange": exchange,
                "market": market,
                "symbol": symbol,
                "interval": interval,
                "start_time_ms": float(int(start_time_ms)),
                "end_time_ms": float(int(end_time_ms)),
                "limit": float(int(limit)),
            })
            resp = self._proxy.invoke(MARKETDATA_FETCH_KLINES, req, Struct)
            return _klines_from_struct(resp, market=market, symbol=symbol, interval=interval)
        except Exception:
            logger.warning(
                "Proxy FetchKlines failed for %s/%s/%s/%s %s-%s",
                exchange,
                market,
                symbol,
                interval,
                start_time_ms,
                end_time_ms,
                exc_info=True,
            )
            return []

    def fetch_backtest_page(
        self,
        *,
        exchange: str = "binance",
        market: str,
        kind: str = "kline",
        symbol: str,
        interval: str,
        start_after_time_ms: int,
        end_time_ms: int,
    ) -> BacktestPage:
        req = Struct()
        req.update({
            "exchange": exchange,
            "market": market,
            "kind": kind or "kline",
            "symbol": symbol,
            "interval": interval,
            "start_after_time_ms": float(int(start_after_time_ms)),
            "end_time_ms": float(int(end_time_ms)),
            "limit": float(BACKTEST_PAGE_SIZE),
        })
        resp = self._proxy.invoke(
            MARKETDATA_FETCH_BACKTEST_PAGE,
            req,
            Struct,
            timeout_seconds=30.0,
        )
        data = MessageToDict(resp)
        return BacktestPage(
            stream_key=str(data.get("stream_key") or ""),
            klines=_klines_from_struct(resp, market=market, symbol=symbol, interval=interval),
            next_cursor_time_ms=int(data.get("next_cursor_time_ms") or 0),
            has_more=bool(data.get("has_more") or False),
        )

    def deliver_dataset_klines(
        self,
        *,
        session_id: str,
        runtime_id: str = "",
        start_time_ms: int,
        end_time_ms: int,
        streams,
        chunk_size: int = 1000,
    ) -> bool:
        try:
            req = Struct()
            req.update({
                "session_id": session_id,
                "runtime_id": runtime_id,
                "start_time_ms": float(int(start_time_ms)),
                "end_time_ms": float(int(end_time_ms)),
                "chunk_size": float(int(chunk_size)),
                "streams": [
                    {
                        "exchange": getattr(stream, "exchange", "") or "binance",
                        "market": getattr(stream, "market", ""),
                        "kind": getattr(stream, "kind", "") or "kline",
                        "symbol": getattr(stream, "symbol", ""),
                        "interval": getattr(stream, "interval", ""),
                    }
                    for stream in streams
                ],
            })
            self._proxy.invoke(
                MARKETDATA_DELIVER_DATASET,
                req,
                Struct,
                timeout_seconds=60.0,
            )
            return True
        except Exception:
            logger.warning(
                "Proxy DeliverDataset failed for session=%s runtime_id=%s",
                session_id,
                runtime_id,
                exc_info=True,
            )
            return False


def _klines_from_struct(resp: Struct, *, market: str, symbol: str, interval: str) -> list[Any]:
    from market_data.models import MarketKline

    data = MessageToDict(resp)
    out: list[Any] = []
    for item in data.get("klines", []):
        if not isinstance(item, dict):
            continue
        out.append(MarketKline(
            symbol=str(item.get("symbol") or symbol),
            interval=str(item.get("interval") or interval),
            open_time=int(item.get("open_time") or 0),
            close_time=int(item.get("close_time") or 0),
            open=float(item.get("open") or 0.0),
            high=float(item.get("high") or 0.0),
            low=float(item.get("low") or 0.0),
            close=float(item.get("close") or 0.0),
            volume=float(item.get("volume") or 0.0),
            timestamp=int(item.get("timestamp") or item.get("close_time") or 0),
            market=str(item.get("market") or market),
        ))
    return out

    def release_market_data_lease(self, *, session_id: str, stream_id: int) -> bool:
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.ReleaseMarketDataLeaseRequest(
                session_id=session_id,
                stream_id=int(stream_id),
            )
            self._proxy.invoke(
                MARKETDATA_RELEASE_LEASE,
                req,
                marketdata_service_pb2.ReleaseMarketDataLeaseResponse,
            )
            return True
        except Exception:
            logger.warning(
                "Proxy ReleaseMarketDataLease failed for session=%s stream_id=%s",
                session_id,
                stream_id,
                exc_info=True,
            )
            return False


class ProxyOrderClient(OrderClient):
    def __init__(self, proxy: RuntimeChannelPlatformProxy) -> None:
        self._proxy = proxy
        self._stub = None
        self._address = "runtime-channel"

    def place_order(
        self,
        portfolio_id: int,
        decision: OrderDecision,
        mark_price: float,
        *,
        portfolio_symbol: str | None = None,
        strategy_id: int = 0,
        market: str | None = None,
        session_id: str = "",
        intent_id: str = "",
        market_time: object | None = None,
        spot_risk_snapshot_id: str = "",
    ) -> ExecutionFeedback:
        symbol = portfolio_symbol or decision.symbol
        intent = intent_id.strip() or uuid.uuid4().hex
        effective_market = str(getattr(decision, "market", None) or "").strip()
        exchange_code = self._exchange_code(getattr(decision, "exchange", None))
        market_code = self._market_code(effective_market)
        if market is not None and str(market or "").strip():
            market_arg_code = self._market_code(market)
            if market_arg_code != market_code:
                raise ValueError(
                    f"market argument {market!r} does not match decision.market {effective_market!r}"
                )
        position_side_code = self._position_side_code(getattr(decision, "position_side", None))
        try:
            from strategy_service.gen import order_service_pb2

            kwargs: dict[str, Any] = dict(
                portfolio_id=int(portfolio_id),
                symbol=symbol,
                side=decision.side,
                strategy_id=int(strategy_id),
                market=market_code,
                session_id=session_id,
                intent_id=intent,
                exchange=exchange_code,
                position_side=position_side_code,
                qty_decimal=canonical_decimal_text(decision.qty),
                mark_price_decimal=canonical_decimal_text(mark_price),
            )
            if decision.price is not None:
                kwargs["price_decimal"] = canonical_decimal_text(decision.price)
            order_type = str(getattr(decision, "order_type", None) or "").strip().upper()
            if not order_type:
                order_type = "LIMIT" if decision.price is not None else "MARKET"
            kwargs["order_type"] = order_type
            time_in_force = str(getattr(decision, "time_in_force", None) or "").strip().upper()
            if order_type == "LIMIT":
                kwargs["time_in_force"] = time_in_force or "GTC"
            kwargs["post_only"] = bool(getattr(decision, "post_only", False))
            kwargs["reduce_only"] = bool(getattr(decision, "reduce_only", False))
            good_till_date_pb = _market_time_to_proto(getattr(decision, "good_till_date", None))
            if good_till_date_pb is not None:
                kwargs["good_till_date"] = good_till_date_pb
            market_time_pb = _market_time_to_proto(market_time)
            if market_time_pb is not None:
                kwargs["market_time"] = market_time_pb
            effective_spot_snapshot_id = str(
                spot_risk_snapshot_id
                or getattr(decision, "spot_risk_snapshot_id", "")
                or ""
            ).strip()
            if effective_spot_snapshot_id:
                kwargs["spot_risk_snapshot_id"] = effective_spot_snapshot_id
            resp = self._proxy.invoke(
                ORDER_PLACE,
                order_service_pb2.PlaceOrderRequest(**kwargs),
                order_service_pb2.PlaceOrderResponse,
            )
            return self._feedback_from_response(resp, decision=decision, market=effective_market, symbol=symbol)
        except Exception as exc:
            logger.warning("Proxy OrderClient.place_order failed for %d/%s", portfolio_id, symbol, exc_info=True)
            return self._resolve_unknown_attempt(
                portfolio_id=portfolio_id,
                intent_id=intent,
                error_message=str(exc),
                decision=decision,
                market=effective_market,
                symbol=symbol,
            )

    def close_spot_targets(
        self,
        *,
        user_id: int,
        portfolio_id: int,
        strategy_id: int,
        session_id: str,
        operation_id: str,
        targets,
    ):
        from strategy_service.gen import order_service_pb2
        from strategy_service.order_client import _spot_close_target_proto

        request_targets = [_spot_close_target_proto(self, order_service_pb2, target) for target in targets]
        return self._proxy.invoke(
            ORDER_CLOSE_SPOT_TARGETS,
            order_service_pb2.CloseSpotTargetsRequest(
                user_id=int(user_id),
                portfolio_id=int(portfolio_id),
                strategy_id=int(strategy_id),
                session_id=str(session_id or "").strip(),
                operation_id=str(operation_id or "").strip(),
                targets=request_targets,
            ),
            order_service_pb2.CloseSpotTargetsResponse,
        )

    def list_order_lifecycle_events(
        self,
        *,
        session_id: str,
        after_event_id: int = 0,
        limit: int = 100,
        timeout_seconds: float | None = None,
    ):
        from strategy_service.gen import order_service_pb2

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id is required")
        request = order_service_pb2.ListOrderLifecycleEventsRequest(
            session_id=normalized_session_id,
            after_event_id=int(after_event_id),
            limit=int(limit),
        )
        if timeout_seconds is None:
            response = self._proxy.invoke(
                ORDER_LIST_LIFECYCLE_EVENTS,
                request,
                order_service_pb2.ListOrderLifecycleEventsResponse,
            )
        else:
            timeout = float(timeout_seconds)
            if timeout <= 0:
                raise TimeoutError("order lifecycle deadline has expired")
            response = self._proxy.invoke(
                ORDER_LIST_LIFECYCLE_EVENTS,
                request,
                order_service_pb2.ListOrderLifecycleEventsResponse,
                timeout_seconds=timeout,
            )
        return [self._order_update_event_from_proto(item) for item in response.events]

    def _resolve_unknown_attempt(
        self,
        *,
        portfolio_id: int,
        intent_id: str,
        error_message: str,
        decision: OrderDecision,
        market: str,
        symbol: str,
    ) -> ExecutionFeedback:
        try:
            from strategy_service.gen import order_service_pb2

            resp = self._proxy.invoke(
                ORDER_RESOLVE_ATTEMPT,
                order_service_pb2.ResolveOrderAttemptRequest(
                    portfolio_id=int(portfolio_id),
                    intent_id=intent_id,
                ),
                order_service_pb2.ResolveOrderAttemptResponse,
            )
            feedback = self._feedback_from_response(resp, decision=decision, market=market, symbol=symbol)
            if not feedback.error_message:
                feedback.error_message = error_message
            return feedback
        except Exception:
            logger.warning("Proxy OrderClient.resolve_unknown_attempt failed for %d/%s", portfolio_id, symbol, exc_info=True)
            return ExecutionFeedback(
                intent_id=intent_id,
                attempt_status="UNKNOWN",
                error_message=error_message,
                order=None,
                fill_count=0,
                delta_qty=0.0,
            )


class ProxyLogClient:
    def __init__(self, proxy: RuntimeChannelPlatformProxy) -> None:
        self._proxy = proxy

    def emit(
        self,
        *,
        level: str,
        logger_name: str,
        message: str,
        log_type: str = "root",
        created_at_unix_ms: int = 0,
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        extra: Optional[dict[str, Any]] = None,
    ) -> bool:
        req = Struct()
        payload: dict[str, Any] = {
            "level": level,
            "logger": logger_name,
            "message": message,
            "log_type": log_type,
            "created_at_unix_ms": float(int(created_at_unix_ms or 0)),
            "portfolio_id": float(int(portfolio_id or 0)),
            "strategy_id": float(int(strategy_id or 0)),
            "session_id": session_id,
        }
        if extra:
            for key, value in extra.items():
                if value is None:
                    continue
                if isinstance(value, (str, bool, int, float)):
                    payload[key] = value
                else:
                    payload[key] = str(value)
        req.update(payload)
        self._proxy.invoke(LOGS_EMIT, req, Struct, timeout_seconds=2.0)
        return True


class ProxyNotificationClient:
    def __init__(self, proxy: RuntimeChannelPlatformProxy) -> None:
        self._proxy = proxy

    def publish(
        self,
        *,
        message: str,
        severity: str = "info",
        title: str = "",
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        dedupe_key: str = "",
        category: str = "custom",
    ) -> bool:
        message = str(message or "").strip()
        if not message:
            return False
        req = Struct()
        req.update({
            "category": str(category or "custom"),
            "severity": _normalize_notification_severity(severity),
            "title": str(title or ""),
            "message": message,
            "portfolio_id": float(int(portfolio_id or 0)),
            "strategy_id": float(int(strategy_id or 0)),
            "session_id": str(session_id or ""),
            "dedupe_key": str(dedupe_key or ""),
        })
        try:
            resp = self._proxy.invoke(NOTIFICATION_PUBLISH, req, Struct, timeout_seconds=2.0)
            accepted = resp.fields.get("accepted") if hasattr(resp, "fields") else None
            if accepted is None:
                return True
            return bool(accepted.bool_value)
        except Exception:
            logger.warning("Proxy notification publish failed", exc_info=True)
            return False


def _normalize_notification_severity(severity: str) -> str:
    value = str(severity or "info").strip().lower()
    if value in ("warn", "warning"):
        return "warn"
    if value == "error":
        return "error"
    return "info"


class RuntimeChannelLogHandler(logging.Handler):
    """Forward self-hosted runtime logs through RuntimeChannel.

    The handler intentionally skips RuntimeChannel/proxy internals so a proxy
    failure does not recursively generate more proxy log attempts.
    """

    _skip_prefixes = (
        "strategy_service.worker_agent_client",
        "strategy_service.platform_proxy",
        "grpc",
    )

    def __init__(self, proxy: RuntimeChannelPlatformProxy, *, level: int = logging.INFO) -> None:
        super().__init__(level=level)
        self._client = proxy.log_client()
        self._busy = threading.local()

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(self._busy, "active", False):
            return
        if any(record.name.startswith(prefix) for prefix in self._skip_prefixes):
            return
        self._busy.active = True
        try:
            message = record.getMessage()
            if record.exc_info:
                message = "\n".join([
                    message,
                    "".join(traceback.format_exception(*record.exc_info)).rstrip(),
                ])
            extra = {
                "pathname": record.pathname,
                "lineno": record.lineno,
                "module": record.module,
                "func_name": record.funcName,
                "process": record.process,
                "thread": record.thread,
            }
            self._client.emit(
                level=record.levelname,
                logger_name=record.name,
                message=message,
                log_type=getattr(record, "log_type", "root") or "root",
                created_at_unix_ms=int(record.created * 1000),
                portfolio_id=int(getattr(record, "portfolio_id", 0) or 0),
                strategy_id=int(getattr(record, "strategy_id", 0) or 0),
                session_id=str(getattr(record, "session_id", "") or ""),
                extra=extra,
            )
        except Exception:
            # Logging must never break strategy execution.
            return
        finally:
            self._busy.active = False


def install_runtime_channel_log_handler(proxy: RuntimeChannelPlatformProxy) -> RuntimeChannelLogHandler:
    root = logging.getLogger()
    for existing in root.handlers:
        if isinstance(existing, RuntimeChannelLogHandler):
            return existing
    handler = RuntimeChannelLogHandler(proxy)
    root.addHandler(handler)
    return handler
