"""gRPC client for core-service. Soft dependency: noop when grpc_address is empty."""

from __future__ import annotations

import logging
from typing import Any, Optional

from strategy_service.order_client import _market_time_to_proto

logger = logging.getLogger(__name__)

_EXCHANGE_ENUMS = {
    "binance": 1,
    "okx": 2,
}

_MARKET_ENUMS = {
    "spot": 1,
    "perpetual_futures": 2,
    "delivery_futures": 3,
}


class PortfolioClient:
    """Thin wrapper around core-service gRPC stubs.

    If *grpc_address* is empty, all methods return ``None`` immediately (noop mode).
    gRPC failures are caught and logged as warnings — never raised to callers.
    """

    def __init__(self, grpc_address: str = "") -> None:
        self._address = grpc_address.strip()
        self._stub = None
        if self._address:
            self._connect()

    def _connect(self) -> None:
        try:
            import grpc
            from strategy_service.gen import portfolio_service_pb2_grpc

            channel = grpc.insecure_channel(self._address)

            # Attach channel-level interceptor so every outbound call to
            # core-service is written to `grpc_ext.log`. Unlike the older
            # stub-wrap `GRPCClientMiddleware`, the interceptor looks up the
            # default logger at call time rather than capturing it at init
            # time, so it is immune to init-order ambiguity between logging
            # bootstrap and gRPC client construction.
            try:
                from utils.log import ClientExtInterceptor  # type: ignore

                channel = grpc.intercept_channel(
                    channel, ClientExtInterceptor(target_service=self._address)
                )
            except Exception:  # noqa: BLE001
                logger.debug("ClientExtInterceptor unavailable; no grpc_ext log for PortfolioClient")

            self._stub = portfolio_service_pb2_grpc.PortfolioServiceStub(channel)

            logger.info("PortfolioClient connected to %s", self._address)
        except Exception:
            logger.warning("PortfolioClient: failed to connect to %s", self._address, exc_info=True)
            self._stub = None

    def get_portfolio_snapshot(
        self,
        portfolio_id: int,
        user_id: int = 0,
        required_symbols: list[tuple[str, str, str]] | set[tuple[str, str, str]] | None = None,
    ):
        """Fetch the portfolio portfolio snapshot from core-service."""
        if not self._stub:
            return None
        try:
            from strategy_service.gen import portfolio_service_pb2

            req = portfolio_service_pb2.GetPortfolioSnapshotRequest(
                portfolio_id=int(portfolio_id),
                user_id=int(user_id),
                required_symbols=_required_symbol_protos(portfolio_service_pb2, required_symbols),
            )
            resp = self._stub.GetPortfolioSnapshot(req)
            return resp.snapshot
        except Exception:
            logger.warning(
                "GetPortfolioSnapshot failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            return None

    def update_portfolio_snapshot(
        self,
        portfolio_id: int,
        user_id: int = 0,
        snapshot_reason: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        snapshot_time: object | None = None,
    ):
        """Refresh and persist a portfolio snapshot through core-service."""
        if not self._stub:
            return None
        try:
            from strategy_service.gen import portfolio_service_pb2

            kwargs = {
                "portfolio_id": int(portfolio_id),
                "user_id": int(user_id),
                "snapshot_reason": int(snapshot_reason),
                "strategy_id": int(strategy_id),
                "session_id": str(session_id or ""),
            }
            snapshot_time_pb = _market_time_to_proto(snapshot_time)
            if snapshot_time_pb is not None:
                kwargs["snapshot_time"] = snapshot_time_pb
            req = portfolio_service_pb2.UpdatePortfolioSnapshotRequest(**kwargs)
            resp = self._stub.UpdatePortfolioSnapshot(req)
            return resp.snapshot
        except Exception:
            logger.warning(
                "UpdatePortfolioSnapshot failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            return None

    def preflight_strategy_session(
        self,
        portfolio_id: int,
        user_id: int = 0,
        required_routes: list[tuple[str, str]] | set[tuple[str, str]] | None = None,
        required_symbols: list[tuple[str, str, str]] | set[tuple[str, str, str]] | None = None,
        order_target_symbols: list[tuple[str, str, str]] | set[tuple[str, str, str]] | None = None,
        session_id: str = "",
        strategy_id: int = 0,
        leverage: float = 0.0,
    ):
        """Validate venue route/symbol availability before strategy runtime creation."""
        if not self._stub:
            return None
        try:
            from strategy_service.gen import portfolio_service_pb2

            req = portfolio_service_pb2.PreflightStrategySessionRequest(
                portfolio_id=int(portfolio_id),
                user_id=int(user_id),
                session_id=str(session_id or ""),
                strategy_id=int(strategy_id),
                leverage=float(leverage or 0.0),
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
                ),
            )
            return self._stub.PreflightStrategySession(req)
        except Exception:
            logger.warning(
                "PreflightStrategySession failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            return None

    def get_active_strategy(self, portfolio_id: int):
        """Fetch the active strategy for an portfolio. Returns proto GetActiveStrategyResponse or None."""
        if not self._stub:
            return None
        try:
            from strategy_service.gen import portfolio_service_pb2

            req = portfolio_service_pb2.GetActiveStrategyRequest(portfolio_id=int(portfolio_id))
            return self._stub.GetActiveStrategy(req)
        except Exception:
            logger.warning("GetActiveStrategy failed for portfolio_id=%s", portfolio_id, exc_info=True)
            return None

    def save_session(
        self,
        session_id: str,
        portfolio_id: int,
        strategy_id: int,
        environment: int,
        interval: str = "1m",
        start_time_ms: int = 0,
        end_time_ms: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
        session_type: str = "",
        runtime_version: str = "",
        session_name: str = "",
        leverage: float = 1.0,
        initial_status: str = "",
    ) -> bool:
        """Create a session record in core-service. Returns True on success."""
        try:
            self.require_save_session(
                session_id=session_id,
                portfolio_id=portfolio_id,
                strategy_id=strategy_id,
                environment=environment,
                interval=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                runtime_id=runtime_id,
                runtime_source=runtime_source,
                runtime_name=runtime_name,
                session_type=session_type,
                runtime_version=runtime_version,
                session_name=session_name,
                leverage=leverage,
                initial_status=initial_status,
            )
            return True
        except Exception:
            logger.warning("SaveSession failed for session=%s", session_id, exc_info=True)
            return False

    def require_save_session(
        self,
        session_id: str,
        portfolio_id: int,
        strategy_id: int,
        environment: int,
        interval: str = "1m",
        start_time_ms: int = 0,
        end_time_ms: int = 0,
        runtime_id: str = "",
        runtime_source: str = "",
        runtime_name: str = "",
        session_type: str = "",
        runtime_version: str = "",
        session_name: str = "",
        leverage: float = 1.0,
        initial_status: str = "",
    ) -> None:
        """Create a session record and raise on core-service errors."""
        if not self._stub:
            raise RuntimeError("PortfolioClient is not connected")
        from strategy_service.gen import portfolio_service_pb2

        req = portfolio_service_pb2.SaveSessionRequest(
            session_id=session_id, portfolio_id=int(portfolio_id),
            strategy_id=int(strategy_id), environment=int(environment),
            interval=interval, start_time_ms=int(start_time_ms), end_time_ms=int(end_time_ms),
            runtime_id=str(runtime_id or ""),
            runtime_source=str(runtime_source or ""),
            runtime_name=str(runtime_name or ""),
            session_type=str(session_type or ""),
            runtime_version=str(runtime_version or ""),
            session_name=str(session_name or ""),
            leverage=float(leverage or 1.0),
            initial_status=str(initial_status or ""),
        )
        self._stub.SaveSession(req)

    def update_session(
        self,
        session_id: str,
        status: str,
        bars_processed: int = 0,
        error: str = "",
        runtime_id: str = "",
    ) -> bool:
        """Update session status in core-service. Returns True on success."""
        if not self._stub:
            return False
        try:
            from strategy_service.gen import portfolio_service_pb2
            req = portfolio_service_pb2.UpdateSessionRequest(
                session_id=session_id, status=status,
                bars_processed=int(bars_processed), error=error,
                runtime_id=str(runtime_id or ""),
            )
            self._stub.UpdateSession(req)
            return True
        except Exception:
            logger.warning("UpdateSession failed for session=%s", session_id, exc_info=True)
            return False

    def list_running_sessions(self, runtime_id: str = ""):
        """List all running sessions. Returns list of proto StrategySessionEntry or empty list."""
        try:
            return self.require_running_sessions(runtime_id=runtime_id)
        except Exception:
            logger.warning("ListRunningSessions failed", exc_info=True)
            return []

    def require_running_sessions(self, runtime_id: str = ""):
        """List running sessions, raising on core-service failures.

        Startup recovery uses this strict variant so "core-service is
        unreachable" is not silently treated as "there are no running sessions".
        """
        if not self._stub:
            if not self._address:
                return []
            raise RuntimeError(f"PortfolioClient is not connected to {self._address}")
        try:
            from strategy_service.gen import portfolio_service_pb2
            resp = self._stub.ListRunningSessions(
                portfolio_service_pb2.ListRunningSessionsRequest(
                    runtime_id=str(runtime_id or ""),
                )
            )
            return list(resp.sessions)
        except Exception as exc:
            raise RuntimeError("ListRunningSessions failed") from exc

    # Phase D2: get_market_data_stream_status / create_or_renew_market_data_lease /
    # release_market_data_lease moved to MarketDataClient (control-panel-service).
    # See strategy_service/marketdata_client.py.

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
        """Push strategy-computed wallet state for snapshot/audit sync.

        Backtest portfolios persist this local wallet as authoritative state.
        Exchange-backed portfolios use the payload as the local side of
        reconciliation while core-service refreshes the exchange snapshot.
        """
        if not self._stub:
            return None
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
            req = portfolio_service_pb2.UpdatePortfolioWalletStateRequest(**kwargs)
            resp = self._stub.UpdatePortfolioWalletState(req)
            return resp.wallet
        except Exception as exc:
            logger.warning(
                "UpdatePortfolioWalletState failed for portfolio_id=%s user_id=%s",
                portfolio_id,
                user_id,
                exc_info=True,
            )
            raise RuntimeError(
                f"UpdatePortfolioWalletState failed for portfolio_id={portfolio_id} user_id={user_id}: {exc}"
            ) from exc


def _exchange_enum(exchange: str) -> int:
    key = str(exchange or "").strip().lower()
    if key not in _EXCHANGE_ENUMS:
        raise ValueError(f"unsupported exchange for preflight: {exchange!r}")
    return _EXCHANGE_ENUMS[key]


def _market_enum(market: str) -> int:
    key = str(market or "").strip().lower()
    if key not in _MARKET_ENUMS:
        raise ValueError(f"unsupported market for preflight: {market!r}")
    return _MARKET_ENUMS[key]


def _required_symbol_protos(
    portfolio_service_pb2,
    required_symbols,
    *,
    order_target_symbols=None,
):
    target_keys = {
        (str(exchange).strip().lower(), str(market).strip().lower(), str(symbol).strip().upper())
        for exchange, market, symbol in (order_target_symbols or [])
    }
    return [
        portfolio_service_pb2.RequiredSymbol(
            exchange=_exchange_enum(exchange),
            market=_market_enum(market),
            symbol=str(symbol or "").strip().upper(),
            order_target=(
                str(exchange).strip().lower(),
                str(market).strip().lower(),
                str(symbol or "").strip().upper(),
            ) in target_keys,
            required_order_types=(
                ["MARKET", "LIMIT"]
                if (
                    str(exchange).strip().lower(),
                    str(market).strip().lower(),
                    str(symbol or "").strip().upper(),
                ) in target_keys
                else []
            ),
        )
        for exchange, market, symbol in sorted(required_symbols or [])
    ]


def _get_wallet_balance(fw: Any) -> float:
    if fw is None:
        return 0.0
    getter = getattr(fw, "get_wallet_balance", None)
    if callable(getter):
        return float(getter())
    return float(getattr(fw, "wallet_balance", 0.0) or 0.0)


def _get_available_balance(fw: Any) -> float:
    if fw is None:
        return 0.0
    getter = getattr(fw, "get_available_balance", None)
    if callable(getter):
        return float(getter())
    return float(getattr(fw, "available_balance", 0.0) or 0.0)


def _get_unrealized_pnl(fw: Any) -> float:
    if fw is None:
        return 0.0
    getter = getattr(fw, "get_unrealized_pnl", None)
    if callable(getter):
        return float(getter())
    return float(getattr(fw, "unrealized_pnl", 0.0) or 0.0)


def _get_margin_balance(fw: Any) -> float:
    if fw is None:
        return 0.0
    getter = getattr(fw, "get_margin_balance", None)
    if callable(getter):
        return float(getter())
    getter = getattr(fw, "get_total_position_equity", None)
    if callable(getter):
        return float(getter())
    return float(
        getattr(fw, "margin_balance", 0.0)
        or getattr(fw, "total_margin_balance", 0.0)
        or 0.0
    )


def _serialize_future_wallet(fw: Any):
    """Map strategy-service futures runtime to proto FuturesWallet."""
    from strategy_service.gen import portfolio_service_pb2

    positions = []
    position_mode = str(getattr(fw, "position_mode", "") or "").strip().lower()
    for (symbol, direction), pos in fw.positions.items():
        net_qty = float(getattr(pos, "net_qty", 0.0) or 0.0)
        net_direction = int(getattr(pos, "net_direction", 0) or 0)
        if position_mode != "hedge":
            position_side = "BOTH"
        elif net_direction > 0:
            position_side = "LONG"
        elif net_direction < 0:
            position_side = "SHORT"
        else:
            position_side = str(getattr(pos, "position_side", "") or "")
        mark_price = getattr(pos, "mark_price", None)
        position_qty = float(getattr(pos, "position_qty", net_qty) or net_qty)
        unrealized_pnl = 0.0
        upnl_getter = getattr(pos, "get_unrealized_pnl", None)
        if callable(upnl_getter) and mark_price is not None:
            unrealized_pnl = float(upnl_getter())
        else:
            unrealized_pnl = float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
        pf = portfolio_service_pb2.FuturesPosition(
            symbol=symbol,
            direction=direction,
            initial_balance=float(getattr(pos, "initial_balance", 0.0) or 0.0),
            leverage=float(getattr(pos, "leverage", 1.0) or 1.0),
            fee_rate=float(getattr(pos, "fee_rate", 0.0004) or 0.0004),
            mark_price=float(mark_price) if mark_price is not None else 0.0,
            qty=net_qty,
            position_qty=position_qty,
            entry_price=float(getattr(pos, "avg_entry_price", getattr(pos, "entry_price", 0.0)) or 0.0),
            unrealized_pnl=unrealized_pnl,
            position_side=position_side,
            margin_mode=str(getattr(pos, "margin_mode", "") or ""),
            margin_type=str(getattr(pos, "margin_type", getattr(pos, "margin_mode", "")) or ""),
            notional=float(getattr(pos, "notional", 0.0) or 0.0),
            initial_margin=float(getattr(pos, "initial_margin", 0.0) or 0.0),
            position_initial_margin=float(getattr(pos, "position_initial_margin", 0.0) or 0.0),
            open_order_initial_margin=float(getattr(pos, "open_order_initial_margin", 0.0) or 0.0),
            maint_margin=float(getattr(pos, "maint_margin", 0.0) or 0.0),
            isolated_wallet=float(getattr(pos, "isolated_wallet", 0.0) or 0.0),
            liquidation_price=float(getattr(pos, "liquidation_price", 0.0) or 0.0),
            break_even_price=float(getattr(pos, "break_even_price", 0.0) or 0.0),
        )
        positions.append(pf)

    raw_risk_metadata = getattr(fw, "risk_metadata", []) or []
    if isinstance(raw_risk_metadata, dict):
        metadata_items = list(raw_risk_metadata.values())
    else:
        metadata_items = list(raw_risk_metadata)

    risk_metadata = []
    for item in metadata_items:
        brackets = []
        for bracket in getattr(item, "brackets", []) or []:
            brackets.append(portfolio_service_pb2.FuturesRiskBracket(
                bracket=int(getattr(bracket, "bracket", 0) or 0),
                notional_floor=float(getattr(bracket, "notional_floor", 0.0) or 0.0),
                notional_cap=float(getattr(bracket, "notional_cap", 0.0) or 0.0),
                initial_leverage=float(getattr(bracket, "initial_leverage", 0.0) or 0.0),
                maint_margin_ratio=float(getattr(bracket, "maint_margin_ratio", 0.0) or 0.0),
                cumulative=float(getattr(bracket, "cumulative", 0.0) or 0.0),
            ))
        risk_metadata.append(portfolio_service_pb2.FuturesRiskMetadata(
            symbol=str(getattr(item, "symbol", "") or ""),
            configured_leverage=float(getattr(item, "configured_leverage", 0.0) or 0.0),
            configured_margin_mode=str(getattr(item, "configured_margin_mode", "") or ""),
            price_precision=int(getattr(item, "price_precision", 0) or 0),
            quantity_precision=int(getattr(item, "quantity_precision", 0) or 0),
            tick_size=float(getattr(item, "tick_size", 0.0) or 0.0),
            step_size=float(getattr(item, "step_size", 0.0) or 0.0),
            brackets=brackets,
        ))

    return portfolio_service_pb2.FuturesWallet(
        margin_mode=str(getattr(fw, "margin_mode", "") or ""),
        position_mode=str(getattr(fw, "position_mode", "") or ""),
        initial_balance=float(getattr(fw, "initial_balance", 0.0) or 0.0),
        deposit_sum=float(getattr(fw, "deposit_sum", 0.0) or 0.0),
        withdrawal_sum=float(getattr(fw, "withdrawal_sum", 0.0) or 0.0),
        positions=positions,
        wallet_balance=_get_wallet_balance(fw),
        available_balance=_get_available_balance(fw),
        total_unrealized_pnl=_get_unrealized_pnl(fw),
        unrealized_pnl=_get_unrealized_pnl(fw),
        total_margin_balance=float(getattr(fw, "total_margin_balance", _get_margin_balance(fw)) or _get_margin_balance(fw)),
        total_position_initial_margin=float(getattr(fw, "total_position_initial_margin", 0.0) or 0.0),
        total_open_order_initial_margin=float(getattr(fw, "total_open_order_initial_margin", 0.0) or 0.0),
        total_maint_margin=float(getattr(fw, "total_maint_margin", 0.0) or 0.0),
        total_cross_wallet_balance=float(getattr(fw, "total_cross_wallet_balance", 0.0) or 0.0),
        total_cross_un_pnl=float(getattr(fw, "total_cross_un_pnl", 0.0) or 0.0),
        risk_metadata=risk_metadata,
        margin_balance=_get_margin_balance(fw),
        multi_assets_mode=bool(getattr(fw, "multi_assets_mode", False)),
        portfolio_margin=bool(getattr(fw, "portfolio_margin", False)),
    )


def _serialize_spot_wallet(sw: Any):
    """Map strategy-service SpotWallet to proto SpotWallet."""
    from strategy_service.gen import portfolio_service_pb2

    assets = []
    for asset_code, asset in sw.assets.items():
        kwargs: dict = dict(
            symbol=asset_code,
            qty=float(asset.qty),
            locked=float(asset.locked),
            avg_entry_price=float(asset.avg_entry_price),
            asset=asset_code,
            free=float(asset.free),
            free_decimal=str(asset.free),
            locked_decimal=str(asset.locked),
        )
        if asset.price is not None:
            kwargs["price"] = float(asset.price)
        assets.append(portfolio_service_pb2.SpotAsset(**kwargs))

    return portfolio_service_pb2.SpotWallet(
        free=float(sw.free),
        locked=float(sw.locked),
        assets=assets,
    )


def _compute_total_value(
    fw: Optional[Any],
    sw: Optional[Any],
) -> float:
    total = 0.0
    if fw:
        total += _get_margin_balance(fw)
    if sw:
        try:
            total += float(sw.get_estimated_value())
        except ValueError:
            total += float(sw.free + sw.locked)
    return total
