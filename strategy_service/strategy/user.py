from __future__ import annotations

from strategy_service.order_client import OrderClient
from strategy_service.notification import StrategyNotifier
from strategy_service.wallet.runtime import WalletRuntime

from strategy_service.strategy.base import BaseStrategy


class UserStrategy(BaseStrategy):
    """Thin BaseStrategy subclass retained for API compatibility.

    Historically this class also performed wallet-positions / wallet-assets
    validation. Pre-C3 (see ``docs/pre_C3.md``) removed that gate: strategies
    declare their own ``(market, symbol, interval)`` universe via ``INPUTS``
    and the runtime routes accordingly — an empty wallet is a valid starting
    point. The declaration validation happens in ``BaseStrategy.__init__`` via
    ``parse_declared_inputs``; no additional wallet-derived check is performed
    here.
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
        super().__init__(
            strategy_path, wallet,
            order_client=order_client,
            account_id=account_id,
            strategy_id=strategy_id,
            session_id=session_id,
            strategy_code=strategy_code,
            notifier=notifier,
        )
