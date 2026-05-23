"""Convert account-service wallet proto payloads into canonical wallet state.

Per ``canonical-wallet-display-boundary`` the ingress contract from
account-service to strategy-service is **canonical** wallet data only.
Provider-specific display totals (``display_wallet_balance_usd`` and
siblings) travel on the same proto but are NEVER read during this
conversion — they would be dead bytes here if the gRPC layer didn't also
forward them unchanged for display consumers upstream.

The fields copied off the proto into ``CanonicalAccountState`` below that
LOOK display-ish (``total_value``, ``spot_estimated_value``,
``futures_position_equity``, ``metrics_authoritative``) are there for
snapshot round-trip fidelity only; runtime risk / precheck / reconciliation
code must NOT read them (see ``CanonicalAccountState`` docstring).
"""

from __future__ import annotations

from typing import Any

from strategy_service.wallet.canonical import (
    CanonicalAccountState,
    CanonicalFuturesPositionState,
    CanonicalFuturesRiskBracket,
    CanonicalFuturesRiskMetadata,
    CanonicalFuturesState,
    CanonicalSpotAssetState,
    CanonicalSpotState,
    derive_position_key,
)


def _strict_position_qty(pos: Any) -> float:
    position_qty = float(getattr(pos, "position_qty", 0.0) or 0.0)
    raw_qty = float(getattr(pos, "qty", 0.0) or 0.0)
    if raw_qty != 0.0 and position_qty == 0.0:
        raise ValueError(
            f"canonical contract error: missing FuturesPosition.position_qty for {getattr(pos, 'symbol', '<unknown>')}"
        )
    return position_qty


def _strict_margin_mode(pos: Any) -> str:
    margin_mode = str(getattr(pos, "margin_mode", "") or "").strip().lower()
    raw_margin_type = str(getattr(pos, "margin_type", "") or "").strip().lower()
    if raw_margin_type and not margin_mode:
        raise ValueError(
            f"canonical contract error: missing FuturesPosition.margin_mode for {getattr(pos, 'symbol', '<unknown>')}"
        )
    if not margin_mode:
        raise ValueError(
            f"canonical contract error: empty FuturesPosition.margin_mode for {getattr(pos, 'symbol', '<unknown>')}"
        )
    return margin_mode


def _strict_margin_balance(fw: Any) -> float:
    margin_balance = float(getattr(fw, "margin_balance", 0.0) or 0.0)
    raw_total_margin = float(getattr(fw, "total_margin_balance", 0.0) or 0.0)
    if raw_total_margin != 0.0 and margin_balance == 0.0:
        raise ValueError("canonical contract error: missing FuturesWallet.margin_balance")
    return margin_balance


def _strict_unrealized_pnl(fw: Any) -> float:
    unrealized_pnl = float(getattr(fw, "unrealized_pnl", 0.0) or 0.0)
    raw_total_upnl = float(getattr(fw, "total_unrealized_pnl", 0.0) or 0.0)
    if raw_total_upnl != 0.0 and unrealized_pnl == 0.0:
        raise ValueError("canonical contract error: missing FuturesWallet.unrealized_pnl")
    return unrealized_pnl


def proto_to_account_spec(wallet_proto: Any) -> CanonicalAccountState:
    """Convert ``GetOnlineAccountInfoResponse.wallet`` into canonical wallet state."""
    fw = wallet_proto.futures if wallet_proto.futures else None
    sw = wallet_proto.spot if wallet_proto.spot else None

    futures_state = CanonicalFuturesState()
    if fw is not None:
        futures_positions: list[CanonicalFuturesPositionState] = []
        position_mode = fw.position_mode or "one_way"
        for pos in fw.positions:
            position_qty = _strict_position_qty(pos)
            margin_mode = _strict_margin_mode(pos)
            direction_key = derive_position_key(
                position_mode=position_mode,
                position_side=pos.position_side,
                direction=int(getattr(pos, "direction", 0) or 0),
                position_qty=position_qty,
            )
            futures_positions.append(CanonicalFuturesPositionState(
                symbol=pos.symbol,
                direction_key=direction_key,
                initial_balance=float(pos.initial_balance),
                leverage=float(pos.leverage or 0.0),
                fee_rate=float(pos.fee_rate or 0.0004),
                mark_price=float(pos.mark_price) if pos.mark_price != 0.0 else None,
                position_qty=position_qty,
                entry_price=float(pos.entry_price or 0.0),
                unrealized_pnl=float(pos.unrealized_pnl or 0.0),
                position_side=str(pos.position_side or ""),
                margin_mode=str(margin_mode or ""),
                notional=float(getattr(pos, "notional", 0.0) or 0.0),
                initial_margin=float(getattr(pos, "initial_margin", 0.0) or 0.0),
                position_initial_margin=float(getattr(pos, "position_initial_margin", 0.0) or 0.0),
                open_order_initial_margin=float(getattr(pos, "open_order_initial_margin", 0.0) or 0.0),
                maint_margin=float(getattr(pos, "maint_margin", 0.0) or 0.0),
                isolated_wallet=float(getattr(pos, "isolated_wallet", 0.0) or 0.0),
                liquidation_price=float(getattr(pos, "liquidation_price", 0.0) or 0.0),
                break_even_price=float(getattr(pos, "break_even_price", 0.0) or 0.0),
            ))

        risk_metadata = [
            CanonicalFuturesRiskMetadata(
                symbol=item.symbol,
                configured_leverage=float(getattr(item, "configured_leverage", 0.0) or 0.0),
                configured_margin_mode=str(getattr(item, "configured_margin_mode", "") or "").strip().lower(),
                price_precision=int(getattr(item, "price_precision", 0) or 0),
                quantity_precision=int(getattr(item, "quantity_precision", 0) or 0),
                tick_size=float(getattr(item, "tick_size", 0.0) or 0.0),
                step_size=float(getattr(item, "step_size", 0.0) or 0.0),
                brackets=[
                    CanonicalFuturesRiskBracket(
                        bracket=int(getattr(bracket, "bracket", 0) or 0),
                        notional_floor=float(getattr(bracket, "notional_floor", 0.0) or 0.0),
                        notional_cap=float(getattr(bracket, "notional_cap", 0.0) or 0.0),
                        initial_leverage=float(getattr(bracket, "initial_leverage", 0.0) or 0.0),
                        maint_margin_ratio=float(getattr(bracket, "maint_margin_ratio", 0.0) or 0.0),
                        cumulative=float(getattr(bracket, "cumulative", 0.0) or 0.0),
                    )
                    for bracket in getattr(item, "brackets", [])
                ],
            )
            for item in getattr(fw, "risk_metadata", [])
        ]

        futures_state = CanonicalFuturesState(
            margin_mode=fw.margin_mode or "cross",
            position_mode=position_mode,
            multi_assets_mode=bool(getattr(fw, "multi_assets_mode", False)),
            portfolio_margin=bool(getattr(fw, "portfolio_margin", False)),
            initial_balance=float(fw.initial_balance or 0.0),
            deposit_sum=float(fw.deposit_sum or 0.0),
            withdrawal_sum=float(fw.withdrawal_sum or 0.0),
            positions=futures_positions,
            wallet_balance=float(fw.wallet_balance or 0.0),
            available_balance=float(fw.available_balance or 0.0),
            margin_balance=_strict_margin_balance(fw),
            unrealized_pnl=_strict_unrealized_pnl(fw),
            total_position_initial_margin=float(getattr(fw, "total_position_initial_margin", 0.0) or 0.0),
            total_open_order_initial_margin=float(getattr(fw, "total_open_order_initial_margin", 0.0) or 0.0),
            total_maint_margin=float(getattr(fw, "total_maint_margin", 0.0) or 0.0),
            total_cross_wallet_balance=float(getattr(fw, "total_cross_wallet_balance", 0.0) or 0.0),
            total_cross_un_pnl=float(getattr(fw, "total_cross_un_pnl", 0.0) or 0.0),
            risk_metadata=risk_metadata,
        )

    spot_state = CanonicalSpotState()
    if sw is not None:
        spot_state = CanonicalSpotState(
            free=float(sw.free),
            locked=float(sw.locked),
            assets=[
                CanonicalSpotAssetState(
                    symbol=asset.symbol,
                    qty=float(asset.qty),
                    locked=float(asset.locked),
                    avg_entry_price=float(asset.avg_entry_price),
                    price=float(asset.price) if asset.HasField("price") else None,
                )
                for asset in sw.assets
            ],
        )

    return CanonicalAccountState(
        mode=int(wallet_proto.mode),
        futures=futures_state,
        spot=spot_state,
        total_value=float(wallet_proto.total_value or 0.0),
        updated_at=wallet_proto.updated_at,
        spot_estimated_value=float(getattr(wallet_proto, "spot_estimated_value", 0.0) or 0.0),
        futures_position_equity=float(getattr(wallet_proto, "futures_position_equity", 0.0) or 0.0),
        metrics_authoritative=bool(getattr(wallet_proto, "metrics_authoritative", False)),
    )
