"""Proxy-only platform clients for self-hosted runtimes.

Self-hosted runtimes cannot dial internal account-service, order API,
market-data control-plane, Kafka, or platform databases. These clients keep
the strategy runtime API shape the same while sending approved platform RPCs
back over RuntimeChannel to control-panel-service.
"""

from __future__ import annotations

import logging
import threading
import traceback
import uuid
from typing import Any, Optional

from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from strategy_service.account_client import (
    _compute_total_value,
    _get_available_balance,
    _get_wallet_balance,
    _serialize_future_wallet,
    _serialize_spot_wallet,
)
from strategy_service.order_client import OrderClient
from strategy_service.types import ExecutionFeedback, OrderDecision

logger = logging.getLogger(__name__)


ACCOUNT_GET_ONLINE = "account.GetOnlineAccountInfo"
ACCOUNT_GET_ACTIVE_STRATEGY = "account.GetActiveStrategy"
ACCOUNT_SAVE_SESSION = "account.SaveSession"
ACCOUNT_UPDATE_SESSION = "account.UpdateSession"
ACCOUNT_UPDATE_WALLET = "account.UpdateAccountWalletState"
ORDER_PLACE = "order.PlaceOrder"
ORDER_RESOLVE_ATTEMPT = "order.ResolveOrderAttempt"
MARKETDATA_GET_STATUS = "marketdata.GetMarketDataStreamStatus"
MARKETDATA_FETCH_KLINES = "marketdata.FetchKlines"
MARKETDATA_DELIVER_DATASET = "marketdata.DeliverDataset"
MARKETDATA_RENEW_LEASE = "marketdata.CreateOrRenewMarketDataLease"
MARKETDATA_RELEASE_LEASE = "marketdata.ReleaseMarketDataLease"
MARKETDATA_CREATE_SESSION_SUBSCRIPTIONS = "marketdata.CreateSessionMarketDataSubscriptions"
MARKETDATA_RELEASE_SESSION_SUBSCRIPTIONS = "marketdata.ReleaseSessionMarketDataSubscriptions"
LOGS_EMIT = "logs.Emit"
NOTIFICATION_PUBLISH = "notification.Publish"


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

    def account_client(self) -> "ProxyAccountClient":
        return ProxyAccountClient(self)

    def order_client(self) -> "ProxyOrderClient":
        return ProxyOrderClient(self)

    def marketdata_client(self) -> "ProxyMarketDataClient":
        return ProxyMarketDataClient(self)

    def log_client(self) -> "ProxyLogClient":
        return ProxyLogClient(self)

    def notification_client(self) -> "ProxyNotificationClient":
        return ProxyNotificationClient(self)


class ProxyAccountClient:
    def __init__(self, proxy: RuntimeChannelPlatformProxy) -> None:
        self._proxy = proxy

    def get_online_account_info(self, account_id: int, user_id: int):
        try:
            from strategy_service.gen import account_service_pb2

            resp = self._proxy.invoke(
                ACCOUNT_GET_ONLINE,
                account_service_pb2.GetOnlineAccountInfoRequest(
                    account_id=int(account_id),
                    user_id=int(user_id),
                ),
                account_service_pb2.GetOnlineAccountInfoResponse,
            )
            return resp.wallet
        except Exception:
            logger.warning(
                "Proxy GetOnlineAccountInfo failed for account_id=%s user_id=%s",
                account_id,
                user_id,
                exc_info=True,
            )
            return None

    def get_active_strategy(self, account_id: int):
        try:
            from strategy_service.gen import account_service_pb2

            return self._proxy.invoke(
                ACCOUNT_GET_ACTIVE_STRATEGY,
                account_service_pb2.GetActiveStrategyRequest(account_id=int(account_id)),
                account_service_pb2.GetActiveStrategyResponse,
            )
        except Exception:
            logger.warning("Proxy GetActiveStrategy failed for account_id=%s", account_id, exc_info=True)
            return None

    def save_session(
        self,
        session_id: str,
        account_id: int,
        strategy_id: int,
        mode: int,
        interval: str = "1m",
        start_time_ms: int = 0,
        end_time_ms: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
        session_type: str = "",
        runtime_version: str = "",
        session_name: str = "",
    ) -> bool:
        try:
            self.require_save_session(
                session_id=session_id,
                account_id=account_id,
                strategy_id=strategy_id,
                mode=mode,
                interval=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                runtime_id=runtime_id,
                runtime_source=runtime_source,
                runtime_name=runtime_name,
                session_type=session_type,
                runtime_version=runtime_version,
                session_name=session_name,
            )
            return True
        except Exception:
            logger.warning("Proxy SaveSession failed for session=%s", session_id, exc_info=True)
            return False

    def require_save_session(
        self,
        session_id: str,
        account_id: int,
        strategy_id: int,
        mode: int,
        interval: str = "1m",
        start_time_ms: int = 0,
        end_time_ms: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
        session_type: str = "",
        runtime_version: str = "",
        session_name: str = "",
    ) -> None:
        from strategy_service.gen import account_service_pb2

        req = account_service_pb2.SaveSessionRequest(
            session_id=session_id,
            account_id=int(account_id),
            strategy_id=int(strategy_id),
            mode=int(mode),
            interval=interval,
            start_time_ms=int(start_time_ms),
            end_time_ms=int(end_time_ms),
            runtime_id=str(runtime_id or ""),
            runtime_source=str(runtime_source or ""),
            runtime_name=str(runtime_name or ""),
            session_type=str(session_type or ""),
            runtime_version=str(runtime_version or ""),
            session_name=str(session_name or ""),
        )
        self._proxy.invoke(ACCOUNT_SAVE_SESSION, req, account_service_pb2.SaveSessionResponse)

    def update_session(
        self,
        session_id: str,
        status: str,
        bars_processed: int = 0,
        error: str = "",
        runtime_id: str = "",
    ) -> bool:
        try:
            from strategy_service.gen import account_service_pb2

            req = account_service_pb2.UpdateSessionRequest(
                session_id=session_id,
                status=status,
                bars_processed=int(bars_processed),
                error=error,
                runtime_id=str(runtime_id or ""),
            )
            self._proxy.invoke(ACCOUNT_UPDATE_SESSION, req, account_service_pb2.UpdateSessionResponse)
            return True
        except Exception:
            logger.warning("Proxy UpdateSession failed for session=%s", session_id, exc_info=True)
            return False

    def list_running_sessions(self, runtime_id: str = ""):
        del runtime_id
        return []

    def require_running_sessions(self, runtime_id: str = ""):
        del runtime_id
        raise RuntimeError("self-hosted runtime recovery must be coordinated by control-panel")

    def update_account_wallet_state(
        self,
        account_id: int,
        future_wallet: Optional[Any] = None,
        spot_wallet: Optional[Any] = None,
        snapshot_reason: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
    ):
        try:
            from strategy_service.gen import account_service_pb2

            req = account_service_pb2.UpdateAccountWalletStateRequest(
                account_id=int(account_id),
                futures=_serialize_future_wallet(future_wallet) if future_wallet else None,
                spot=_serialize_spot_wallet(spot_wallet) if spot_wallet else None,
                total_value=_compute_total_value(future_wallet, spot_wallet),
                wallet_balance=_get_wallet_balance(future_wallet),
                available_balance=_get_available_balance(future_wallet),
                snapshot_reason=int(snapshot_reason),
                strategy_id=int(strategy_id),
                session_id=session_id,
            )
            resp = self._proxy.invoke(
                ACCOUNT_UPDATE_WALLET,
                req,
                account_service_pb2.UpdateAccountWalletStateResponse,
            )
            return resp.wallet
        except Exception:
            logger.warning("Proxy UpdateAccountWalletState failed for account_id=%s", account_id, exc_info=True)
            return None


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
        account_id: int = 0,
        stream_id: int,
        ttl_seconds: int,
    ) -> bool:
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.CreateOrRenewMarketDataLeaseRequest(
                session_id=session_id,
                strategy_id=int(strategy_id),
                account_id=int(account_id),
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
        mode: int,
        streams,
    ) -> bool:
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.CreateSessionMarketDataSubscriptionsRequest(
                user_id=int(user_id),
                session_id=session_id,
                runtime_id=runtime_id,
                mode=int(mode),
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
            from market_data.models import MarketKline

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
        account_id: int,
        decision: OrderDecision,
        mark_price: float,
        *,
        account_symbol: str | None = None,
        strategy_id: int = 0,
        market: str = "futures",
        session_id: str = "",
        intent_id: str = "",
    ) -> ExecutionFeedback:
        symbol = account_symbol or decision.symbol
        intent = intent_id.strip() or uuid.uuid4().hex
        try:
            from strategy_service.gen import order_service_pb2

            kwargs: dict[str, Any] = dict(
                account_id=int(account_id),
                symbol=symbol,
                side=decision.side,
                qty=float(decision.qty),
                mark_price=float(mark_price),
                strategy_id=int(strategy_id),
                market=market,
                session_id=session_id,
                intent_id=intent,
            )
            if decision.price is not None:
                kwargs["price"] = float(decision.price)
            resp = self._proxy.invoke(
                ORDER_PLACE,
                order_service_pb2.PlaceOrderRequest(**kwargs),
                order_service_pb2.PlaceOrderResponse,
            )
            return self._feedback_from_response(resp, decision=decision, market=market, symbol=symbol)
        except Exception as exc:
            logger.warning("Proxy OrderClient.place_order failed for %d/%s", account_id, symbol, exc_info=True)
            return self._resolve_unknown_attempt(
                account_id=account_id,
                intent_id=intent,
                error_message=str(exc),
                decision=decision,
                market=market,
                symbol=symbol,
            )

    def _resolve_unknown_attempt(
        self,
        *,
        account_id: int,
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
                    account_id=int(account_id),
                    intent_id=intent_id,
                ),
                order_service_pb2.ResolveOrderAttemptResponse,
            )
            feedback = self._feedback_from_response(resp, decision=decision, market=market, symbol=symbol)
            if not feedback.error_message:
                feedback.error_message = error_message
            return feedback
        except Exception:
            logger.warning("Proxy OrderClient.resolve_unknown_attempt failed for %d/%s", account_id, symbol, exc_info=True)
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
        account_id: int = 0,
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
            "account_id": float(int(account_id or 0)),
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
        account_id: int = 0,
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
            "account_id": float(int(account_id or 0)),
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
        "strategy_service.runtime_channel",
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
                account_id=int(getattr(record, "account_id", 0) or 0),
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
