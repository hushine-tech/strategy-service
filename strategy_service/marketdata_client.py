"""gRPC client for control-panel-service market-data control plane (Phase D2).

Before D2, these RPCs lived on core-service and were proxied through
:class:`AccountClient`. The control plane was migrated to control-panel-service
along with the underlying tables (`market_data_*` in the `control_panel`
database). This client owns the strategy-service side of that subset:

  * GetMarketDataStreamStatus  — preflight: confirm a stream is healthy
                                 before the live data loop attaches to it.
  * CreateOrRenewMarketDataLease — heartbeat: announce live consumption
                                   so scraper keeps the collector alive.
  * ReleaseMarketDataLease       — clean stop on session shutdown.

If *grpc_address* is empty, all methods return ``None`` / ``False`` (noop
mode) so a partial dev environment still boots. gRPC failures are caught
and logged as warnings — they never raise to callers.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class MarketDataClient:
    """Thin wrapper around control-panel-service marketdata gRPC stubs.

    Mirrors the soft-dependency contract of :class:`AccountClient`:
    empty address ⇒ noop mode; transient gRPC failures ⇒ warning + None/False.
    """

    def __init__(self, grpc_address: str = "") -> None:
        self._address = grpc_address.strip()
        self._stub = None
        if self._address:
            self._connect()

    def _connect(self) -> None:
        try:
            import grpc
            from strategy_service.gen import marketdata_service_pb2_grpc

            channel = grpc.insecure_channel(self._address)

            # Wire grpc_ext access logging the same way AccountClient does.
            try:
                from utils.log import ClientExtInterceptor  # type: ignore

                channel = grpc.intercept_channel(
                    channel, ClientExtInterceptor(target_service=self._address)
                )
            except Exception:  # noqa: BLE001
                logger.debug(
                    "ClientExtInterceptor unavailable; no grpc_ext log for MarketDataClient"
                )

            self._stub = marketdata_service_pb2_grpc.MarketDataControlPlaneServiceStub(channel)

            logger.info("MarketDataClient connected to %s", self._address)
        except Exception:
            logger.warning(
                "MarketDataClient: failed to connect to %s",
                self._address,
                exc_info=True,
            )
            self._stub = None

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
        """Lookup a control-plane stream by id or key. Returns proto stream or None."""
        if not self._stub:
            return None
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
            resp = self._stub.GetMarketDataStreamStatus(req)
            return resp.stream
        except Exception:
            logger.warning(
                "GetMarketDataStreamStatus failed for %s/%s/%s/%s/%s",
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
        if not self._stub:
            return False
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.CreateOrRenewMarketDataLeaseRequest(
                session_id=session_id,
                strategy_id=int(strategy_id),
                account_id=int(account_id),
                stream_id=int(stream_id),
                ttl_seconds=int(ttl_seconds),
            )
            self._stub.CreateOrRenewMarketDataLease(req)
            return True
        except Exception:
            logger.warning(
                "CreateOrRenewMarketDataLease failed for session=%s stream_id=%s",
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
        if not self._stub:
            return False
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
            resp = self._stub.CreateSessionMarketDataSubscriptions(req)
            return len(resp.subscriptions) == len(req.keys)
        except Exception:
            logger.warning(
                "CreateSessionMarketDataSubscriptions failed for session=%s runtime_id=%s",
                session_id,
                runtime_id,
                exc_info=True,
            )
            return False

    def release_session_market_data_subscriptions(self, *, session_id: str, runtime_id: str = "") -> bool:
        if not self._stub:
            return False
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.ReleaseSessionMarketDataSubscriptionsRequest(
                session_id=session_id,
                runtime_id=runtime_id,
            )
            self._stub.ReleaseSessionMarketDataSubscriptions(req)
            return True
        except Exception:
            logger.warning(
                "ReleaseSessionMarketDataSubscriptions failed for session=%s runtime_id=%s",
                session_id,
                runtime_id,
                exc_info=True,
            )
            return False

    def release_market_data_lease(self, *, session_id: str, stream_id: int) -> bool:
        if not self._stub:
            return False
        try:
            from strategy_service.gen import marketdata_service_pb2

            req = marketdata_service_pb2.ReleaseMarketDataLeaseRequest(
                session_id=session_id,
                stream_id=int(stream_id),
            )
            self._stub.ReleaseMarketDataLease(req)
            return True
        except Exception:
            logger.warning(
                "ReleaseMarketDataLease failed for session=%s stream_id=%s",
                session_id,
                stream_id,
                exc_info=True,
            )
            return False
