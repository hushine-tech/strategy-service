"""每根 K 线都开多单 0.001 BTC，用于验证 4 步钱包更新流程。"""

from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    """
    简化策略：每根 K 线都下一个固定数量的 LONG。

    不做任何技术指标计算，纯粹验证：
    Step 1: wallet.on_market_data  更新 mark_price
    Step 2: 策略.on_market_data     返回 OrderDecision
    Step 3: place_order             Mock 成交
    Step 4: wallet.on_order         更新持仓
    """

    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def __init__(self) -> None:
        self._counter: int = 0

    def on_market_data(self, data: MarketData, wallet) -> OrderDecision | None:
        self._counter += 1
        # 每 100 根打印一次进度
        if self._counter % 100 == 1:
            print(f"  [bar {self._counter}] price={data.klines['close']}", flush=True)
        return OrderDecision(symbol=data.symbol, side="LONG", qty=0.001)

    def on_order_response(self, order_resp) -> None:
        print(
            f"  [Filled] {order_resp.side} {order_resp.qty} @ {order_resp.fill_price}",
            flush=True,
        )
