# Wallet 设计文档

> **Phase C2b 状态更新(2026-04-18)**:legacy `FutureWallet` / `Position` /
> `Account` / `Wallet` 这些类已经从 `strategy_service.wallet` 包中**删除**。
> 本文档下方的代码示例保留为**历史设计档案**,当前实际运行时请通过
> `build_wallet_from_account(proto_or_canonical) → BinanceWalletRuntime`
> 构造钱包。`wallet.futures.positions` / `wallet.spot.assets` /
> `wallet.get_wallet_balance()` 等对外接口保持不变,字段命名与本文档描述
> 的 `mark_price` / `position_qty` / `wallet_balance` / `margin_balance` /
> `initial_margin` 等全部一致。指标公式小节(第 3 节之后)描述的数学不变。

## 设计目标

支持多币种、多交易类型(期货/现货)的统一钱包管理:单合约 USDT-M 数学 → 期货多品种账本 → 与现货账本组合为账户总价值。

---

## 架构

```
Account(margin_mode, initial_balance, deposit_sum, withdrawal_sum)
├── futures: FutureWallet(margin_mode, initial_balance, deposit_sum, withdrawal_sum)
│   ├── margin_mode: "isolated" | "cross"
│   ├── initial_balance / deposit_sum / withdrawal_sum  ← 全仓共享资金池
│   └── positions = {
│           "BTCUSDT": Position(margin_mode=...),
│           "ETHUSDT": Position(margin_mode=...),
│           ...
│       }
└── spot: SpotWallet
    ├── free, locked          # 计价货币（如 USDT）
    ├── assets: dict[symbol, SpotAsset]
    └── _prices: dict[symbol, mark_price]

Position（期货仓位 — 单合约 USDT-M 账本）
├── __init__(initial_balance, leverage, fee_rate, deposit_sum=0, withdrawal_sum=0, margin_mode="isolated")
├── 内部 `_CoreState`：qty / direction / avg_entry_price / mark_price / 手续费与已实现累计等
├── 只读属性：mark_price / net_qty / net_direction / avg_entry_price / open_fee_sum / has_open_net_position
└── Methods
    ├── update_mark_price / open_position / close_position
    ├── get_unrealized_pnl / get_realized_pnl / get_IM       ← 任何模式均可调用
    ├── get_margin_required / get_maintenance_margin / notional_at_mark
    ├── get_wallet_balance / get_available_balance / get_position_equity  ← 仅逐仓可调用，全仓抛 RuntimeError
    └── get_liquidation_price                                 ← 仅逐仓可调用，全仓抛 RuntimeError

SpotAsset（现货标的）
├── qty, locked, avg_entry_price, price
├── get_unrealized_pnl(current_price)
└── get_estimated_value(current_price)

SpotWallet
├── on_market_data(symbol, price)
├── on_order(symbol, order_resp)   # 统一订单回调入口
├── on_fill(symbol, side, qty, fill_price, fee=0)  # 直接 fill 辅助入口
├── get_unrealized_pnl()
└── get_estimated_value()
```

### 逐仓 vs 全仓

| | 逐仓 (Isolated) | 全仓 (Cross) |
|---|---|---|
| 资金池 | 每个 Position 独立 `initial_balance` | FutureWallet 统一 `initial_balance` |
| wallet_balance / available_balance / equity | Position 层各自计算，FutureWallet 求和 | FutureWallet 层全局计算 |
| 爆仓价 | Position 独立计算 | FutureWallet.get_liquidation_price(symbol)，考虑所有仓位 |
| Position 上的 get_wallet_balance 等 | 正常返回 | 抛出 RuntimeError |

---

## 统一调用入口

策略层持有 **`Account`**，通过两个入口更新行情与成交：

### account.on_market_data(symbol, symbol_type, price)

```text
futures → FutureWallet.on_market_data
          1. positions[symbol].update_mark_price(price)
          2. _compute_isolated_indicators(symbol) 或 _compute_cross_indicators()：按当前 mark 与持仓计算爆仓边界（``LiquidationRiskEvent.breached``）；若设置了 ``on_liquidation_risk`` 则回调（便于对接通知服务）。全仓下需各开仓腿均已有 mark 才评估。
spot    → SpotWallet.on_market_data → 更新 assets[symbol].price
```

不引入额外缓存字段——Position 内部状态已维护 `mark_price`、`qty`、`realized_pnl` 等，FutureWallet 的 getter 每次调用时直接遍历 Position 获取。

多品种全仓场景：某时刻只有单币种行情时，未收到更新的合约仍使用其**上次** `update_mark_price` 的值参与计算。

### account.on_order(symbol, symbol_type, order_resp)

```text
非 FILLED → 直接返回

futures → FutureWallet.on_order
          1. Position.open_position（BUY=+1，SELL=-1；hedge 方向由 position_side 表示）
          2. 同上 _compute_* 钩子（默认空实现）
spot    → SpotWallet.on_order（内部按 order_resp.fee 结算；BUY 扣减 free 增加仓位；SELL 减仓增加 free）
```

---

## 创建仓位与启动顺序（示例）

新流程：先装配 `Wallet`（包含 futures/spot 与仓位），再创建策略。策略层不再自动建仓或注资。

**逐仓模式：**
```python
# 当前(Phase C2b 后):测试统一走 helper
from tests.helpers.wallet_fixtures import make_backtest_wallet

wallet = make_backtest_wallet(
    margin_mode="isolated",
    futures_positions=[{
        "symbol": "BTCUSDT",
        "position_qty": 0.0,
        "entry_price": 0.0,
        "mark_price": 0.0,
        "leverage": 20.0,
        "initial_balance": 10000.0,
        "fee_rate": 0.0004,
        "margin_mode": "isolated",
    }],
    spot_free=10000.0,
    spot_assets=[{"symbol": "BTCUSDT"}],
)
# 等价于: wallet.futures.positions[("BTCUSDT", 0)] 已就位,
# wallet.spot.assets["BTCUSDT"] / wallet.spot.free 已初始化
```

**全仓模式(示意,不再可执行):**
```python
# Historical, pre-Phase-C2b:
# account = Account(
#     futures=FutureWallet(margin_mode="cross", initial_balance=10000.0),
#     spot=SpotWallet(),
# )
# account.futures.positions[("BTCUSDT", 0)] = Position(
#     initial_balance=0.0, leverage=20.0, fee_rate=0.0004, margin_mode="cross",
# )
# account.futures.positions[("ETHUSDT", 0)] = Position(
#     initial_balance=0.0, leverage=10.0, fee_rate=0.0004, margin_mode="cross",
# )

# 当前(Phase C2b 后):
from tests.helpers.wallet_fixtures import make_backtest_wallet
wallet = make_backtest_wallet(
    margin_mode="cross",
    wallet_balance=10000.0,
    futures_positions=[
        {"symbol": "BTCUSDT", "leverage": 20.0, "fee_rate": 0.0004, "margin_mode": "cross"},
        {"symbol": "ETHUSDT", "leverage": 10.0, "fee_rate": 0.0004, "margin_mode": "cross"},
    ],
)

account.futures.get_wallet_balance()             # 全局钱包余额
account.futures.get_liquidation_price("BTCUSDT")  # 全仓爆仓价
```

---

## OrderResponse 结构

```python
@dataclass
class OrderResponse:
    symbol: str
    side: str       # BUY/SELL
    qty: float
    fill_price: float
    status: str     # FILLED / REJECTED / CANCELLED
    fee: float = 0.0   # 现货结算手续费来源
```

定义见 `wallet/order_types.py`，`strategy_service.types` 从 `wallet` 再导出。

---

## 指标重算规则

| 操作 | 重算范围 |
|------|----------|
| on_market_data（期货，逐仓） | 目标 Position 的 UPNL/WB/available/equity/liq |
| on_market_data（期货，全仓） | FutureWallet 级 WB/available/equity/UPNL + 所有仓位 liq |
| on_market_data（现货） | 更新价格缓存，影响 SpotWallet 聚合估值与 UPNL |
| on_order（期货，逐仓） | 同 on_market_data 逐仓 |
| on_order（期货，全仓） | 同 on_market_data 全仓 |
| on_order（现货） | SpotAsset 与 free/locked |

---

## 文件位置

```text
wallet/
├── future.py                 # lookup_mmr_tier, Position, FutureWallet
├── spot.py                   # SpotAsset, SpotWallet
├── account.py                # Account
├── order_types.py            # OrderResponse
└── __init__.py               # 导出
```
