"""Build wallet runtimes from either HTTP backtest specs or canonical snapshots."""

from __future__ import annotations

from typing import Any

from strategy_service.wallet.canonical import (
    CanonicalAccountState,
    CanonicalFuturesPositionState,
    CanonicalFuturesState,
    CanonicalSpotAssetState,
    CanonicalSpotState,
    norm_symbol,
)


def _bootstrap_futures_equity(state: CanonicalFuturesState) -> float:
    deposit_sum = float(state.deposit_sum or 0.0)
    withdrawal_sum = float(state.withdrawal_sum or 0.0)
    margin_mode = str(state.margin_mode or "cross").strip().lower()
    if margin_mode == "cross":
        return float(state.initial_balance or 0.0) + deposit_sum - withdrawal_sum
    isolated_seed = sum(float(pos.initial_balance or 0.0) for pos in state.positions)
    return isolated_seed + deposit_sum - withdrawal_sum


def _estimate_spot_value(state: CanonicalSpotState) -> float:
    total = float(state.free or 0.0) + float(state.locked or 0.0)
    for asset in state.assets:
        if asset.price is None:
            continue
        total += float(asset.qty or 0.0) * float(asset.price)
    return total


def account_dict_to_canonical_state(account: dict[str, Any]) -> CanonicalAccountState:
    """Convert the HTTP/backtest request body to strict canonical state.

    The HTTP API is a backtest-only entrypoint, so dict payloads are treated as
    ``environment=backtest`` and routed through the same Binance runtime used
    by the gRPC backtest path.
    """
    fa = account.get("futures") or {}
    margin_mode = str(fa.get("margin_mode", "isolated")).strip().lower()
    position_mode = str(fa.get("position_mode", "one_way")).strip().lower()

    futures_positions: list[CanonicalFuturesPositionState] = []
    for raw in fa.get("positions") or []:
        direction_key = int(raw.get("direction", 0) or 0)
        position_side = "BOTH"
        if position_mode == "hedge":
            if direction_key > 0:
                position_side = "LONG"
            elif direction_key < 0:
                position_side = "SHORT"
        futures_positions.append(
            CanonicalFuturesPositionState(
                symbol=norm_symbol(raw.get("symbol", "")),
                direction_key=direction_key if position_mode == "hedge" else 0,
                initial_balance=float(raw.get("initial_balance", 0.0) or 0.0),
                leverage=float(raw.get("leverage", 1.0) or 1.0),
                fee_rate=float(raw.get("fee_rate", 0.0004) or 0.0004),
                mark_price=float(raw["mark_price"]) if raw.get("mark_price") is not None else None,
                position_qty=float(raw.get("position_qty", raw.get("qty", 0.0)) or 0.0),
                entry_price=float(raw.get("entry_price", 0.0) or 0.0),
                position_side=position_side,
                margin_mode=str(raw.get("margin_mode", margin_mode) or margin_mode).strip().lower(),
            )
        )

    futures_state = CanonicalFuturesState(
        margin_mode=margin_mode,
        position_mode=position_mode,
        initial_balance=float(fa.get("initial_balance", 0.0) or 0.0),
        deposit_sum=float(fa.get("deposit_sum", 0.0) or 0.0),
        withdrawal_sum=float(fa.get("withdrawal_sum", 0.0) or 0.0),
        positions=futures_positions,
    )

    sa = account.get("spot") or {}
    spot_state = CanonicalSpotState(
        free=float(sa.get("free", 0.0) or 0.0),
        locked=float(sa.get("locked", 0.0) or 0.0),
        assets=[
            CanonicalSpotAssetState(
                symbol=norm_symbol(sym),
                qty=float((ad or {}).get("qty", 0.0) or 0.0),
                locked=float((ad or {}).get("locked", 0.0) or 0.0),
                avg_entry_price=float((ad or {}).get("avg_entry_price", 0.0) or 0.0),
                price=float((ad or {})["price"]) if (ad or {}).get("price") is not None else None,
            )
            for sym, ad in (sa.get("assets") or {}).items()
        ],
    )

    futures_equity = _bootstrap_futures_equity(futures_state)
    spot_value = _estimate_spot_value(spot_state)
    return CanonicalAccountState(
        environment=0,
        futures=futures_state,
        spot=spot_state,
        total_value=futures_equity + spot_value,
        spot_estimated_value=spot_value,
        futures_position_equity=futures_equity,
    )


# Provider + environment registry for canonical account state.
#
# Keys are ``(provider, environment)`` tuples; values are runtime classes that
# expose a ``from_canonical`` classmethod. Mode=1 ``("binance", "live")`` is
# intentionally NOT registered in Phase B/C1: live runtime remains disabled
# and the registry miss is how we fail closed. Additional exchanges (OKX etc.)
# plug in here without touching ``build_wallet_from_account``.
#
RUNTIME_REGISTRY: dict[tuple[str, str], type] = {}
_QTY_EPS = 1e-12


def _populate_runtime_registry() -> None:
    """Populate ``RUNTIME_REGISTRY`` on first access.

    Imports are deferred here so ``wallet_factory`` stays decoupled from
    concrete runtime modules until first use.
    """
    if RUNTIME_REGISTRY:
        return
    from strategy_service.wallet.binance import BinanceWalletRuntime

    RUNTIME_REGISTRY[("local", "backtest")] = BinanceWalletRuntime
    RUNTIME_REGISTRY[("binance", "demo")] = BinanceWalletRuntime


def resolve_target(environment: int) -> tuple[str, str]:
    """Map a numeric account environment to a ``(provider, environment)`` registry key.

    - ``0`` -> ``("local", "backtest")`` — backtest runtime
    - ``1`` -> ``("binance", "demo")`` — exchange demo runtime
    - ``2`` -> ``("binance", "live")`` — resolves but is NOT registered;
      this produces a fail-closed registry miss in Phase B/C1
    """
    if environment == 0:
        return ("local", "backtest")
    if environment == 1:
        return ("binance", "demo")
    if environment == 2:
        return ("binance", "live")
    raise ValueError(f"unsupported account environment: {environment}")


def _validate_exchange_leverage_contract(account: CanonicalAccountState) -> None:
    """Exchange-backed runtime must receive explicit leverage facts."""
    if int(account.environment) != 1:
        return

    configured_by_symbol = {
        item.normalized_symbol(): float(item.configured_leverage or 0.0)
        for item in account.futures.risk_metadata
    }
    missing: list[str] = []
    for pos in account.futures.positions:
        if abs(float(pos.position_qty or 0.0)) <= _QTY_EPS:
            continue
        if float(pos.leverage or 0.0) > 0.0:
            continue
        if configured_by_symbol.get(pos.normalized_symbol(), 0.0) > 0.0:
            continue
        missing.append(pos.normalized_symbol())

    if missing:
        symbols = ", ".join(sorted(set(missing)))
        raise ValueError(
            "canonical contract error: missing FuturesPosition.leverage or "
            f"FuturesRiskMetadata.configured_leverage for {symbols}"
        )


def build_wallet_from_account(account: dict[str, Any] | CanonicalAccountState):
    """Build the appropriate wallet runtime.

    - ``dict`` input is normalized to canonical backtest state first
    - ``CanonicalAccountState`` input resolves ``(provider, environment)`` from
      ``account.environment`` and dispatches through ``RUNTIME_REGISTRY``
    """
    if not isinstance(account, CanonicalAccountState):
        account = account_dict_to_canonical_state(account)

    _populate_runtime_registry()
    provider, environment_name = resolve_target(int(account.environment))
    runtime_cls = RUNTIME_REGISTRY.get((provider, environment_name))
    if runtime_cls is None:
        raise ValueError(
            f"no wallet runtime registered for ({provider!r}, {environment_name!r}); "
            f"environment={account.environment} is not enabled"
        )

    # Canonical runtime is intentionally constrained to single-asset USDT@-M
    # futures + USDT-mediated spot (see ``canonical-wallet-display-boundary``
    # spec). Any Binance wallet configuration that deviates from that canonical
    # shape — multi-asset collateral, portfolio-margin cross-risk — is a
    # *provider* capability, NOT a canonical runtime capability. Supporting
    # those would require a new runtime model, not a silent expansion of the
    # canonical contract. Fail-closed for every environment that routes through
    # BinanceWalletRuntime (both backtest and demo today).
    from strategy_service.wallet.binance import BinanceWalletRuntime

    if runtime_cls is BinanceWalletRuntime or issubclass(runtime_cls, BinanceWalletRuntime):
        if bool(getattr(account.futures, "multi_assets_mode", False)):
            raise ValueError(
                "unsupported wallet state: multi-assets mode is outside the "
                "canonical single-asset USDT@-M runtime contract"
            )
        if bool(getattr(account.futures, "portfolio_margin", False)):
            raise ValueError(
                "unsupported wallet state: portfolio margin is outside the "
                "canonical single-asset USDT@-M runtime contract"
            )
        _validate_exchange_leverage_contract(account)

    return runtime_cls.from_canonical(account)
