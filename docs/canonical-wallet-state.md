# Canonical Wallet State

日期：`2026-04-18`

## 目的

`Phase B` 起，`core-service -> strategy-service` 之间的钱包 contract 以 canonical 命名为准。

原则：

- 标准层优先表达交易所语义
- `legacy` 兼容放在 adapter 内部处理
- 不再新增 `WB`、`balance`、`marginRequired` 这类模糊字段名

## Canonical 字段

### 顶层账户

| Canonical | 当前来源 | 说明 |
| --- | --- | --- |
| `total_value` | `AccountWalletState.total_value` | 账户总资产价值 |
| `spot_estimated_value` | `AccountWalletState.spot_estimated_value` | 现货展示估值 |
| `futures_position_equity` | `AccountWalletState.futures_position_equity` | 期货腿展示值；canonical 目标口径为 `futures.margin_balance`，对 Binance 快照等同 `futures.total_margin_balance` |
| `environment` | `AccountWalletState.environment` | 账户环境：`0=backtest`、`1=demo`、`2=live` |
| `updated_at` | `AccountWalletState.updated_at` | 快照时间 |
| `metrics_authoritative` | `AccountWalletState.metrics_authoritative` | 展示指标是否由服务端权威给出 |

说明：顶层 canonical contract 只保留账户总览字段；`wallet_balance`、`available_balance`、`margin_balance`、`unrealized_pnl` 不再属于顶层 canonical，统一从 `futures.*` 读取。顶层若仍有同名 wire 字段，视为兼容镜像，不作为标准消费入口。

### Futures 账户级

| Canonical | 当前来源 | 说明 |
| --- | --- | --- |
| `margin_mode` | `futures.margin_mode` | `cross` / `isolated` |
| `position_mode` | `futures.position_mode` | `one_way` / `hedge` |
| `multi_assets_mode` | `futures.multi_assets_mode` | Binance 多资产模式；demo/live 交易所环境下若为 `true` 则 fail-closed |
| `portfolio_margin` | `futures.portfolio_margin` | Binance 组合保证金模式；demo/live 交易所环境下若为 `true` 则 fail-closed |
| `wallet_balance` | `futures.wallet_balance` | 期货钱包余额 |
| `available_balance` | `futures.available_balance` | 期货可用余额 |
| `margin_balance` | `futures.margin_balance` | 期货保证金余额 |
| `unrealized_pnl` | `futures.unrealized_pnl` | 期货账户级未实现盈亏 |
| `position_initial_margin` | `futures.total_position_initial_margin` | 持仓初始保证金 |
| `open_order_initial_margin` | `futures.total_open_order_initial_margin` | 挂单初始保证金 |
| `maint_margin` | `futures.total_maint_margin` | 维持保证金 |
| `cross_wallet_balance` | `futures.total_cross_wallet_balance` | 全仓钱包余额 |
| `cross_unrealized_pnl` | `futures.total_cross_un_pnl` | 全仓未实现盈亏 |
| `risk_metadata[]` | `futures.risk_metadata[]` | Binance 风险元数据，供 metadata-backed 公式使用 |

说明：期货层 canonical contract 不再保留 `total_equity`；期货权益统一使用 `margin_balance`。`strategy-service` ingress 对 `margin_balance`、`unrealized_pnl` 不再接受 `total_*` fallback；缺失时直接报 contract error。

补充：回测 / 本地 bootstrap 约定如下：

- `cross`：`wallet_balance_0 = futures.initial_balance + deposit_sum - withdrawal_sum`
- `isolated`：`wallet_balance_0 = Σ position.initial_balance + deposit_sum - withdrawal_sum`
- 启动后 `wallet_balance` 只受账本事件影响，不会因 `mark_price` 波动而重新推导
- futures 开仓前置检查应读取 `available_balance`，不是 `wallet_balance`

### Futures 仓位级

| Canonical | 当前来源 | 说明 |
| --- | --- | --- |
| `symbol` | `position.symbol` | 合约 |
| `position_qty` | `position.position_qty` | 带方向数量；long 为正，short 为负 |
| `entry_price` | `position.entry_price` | 开仓均价 |
| `mark_price` | `position.mark_price` | 标记价格 |
| `unrealized_pnl` | `position.unrealized_pnl` | 仓位未实现盈亏 |
| `position_side` | `position.position_side` | `LONG` / `SHORT` / `BOTH` |
| `margin_mode` | `position.margin_mode` | `cross` / `isolated` |
| `leverage` | `position.leverage` | 杠杆 |
| `initial_margin` | `position.initial_margin` | 总初始保证金（持仓 IM + 挂单 IM） |
| `position_initial_margin` | `position.position_initial_margin` | 持仓初始保证金 |
| `open_order_initial_margin` | `position.open_order_initial_margin` | 挂单初始保证金 |
| `maint_margin` | `position.maint_margin` | 维持保证金 |
| `isolated_wallet` | `position.isolated_wallet` | 逐仓钱包余额 |
| `liquidation_price` | `position.liquidation_price` | 爆仓价 |
| `break_even_price` | `position.break_even_price` | 保本价 |
| `notional` | `position.notional` | 名义价值 |

### Spot

| Canonical | 当前来源 | 说明 |
| --- | --- | --- |
| `free` | `spot.free` | 可用现货余额 |
| `locked` | `spot.locked` | quote-side 冻结余额 |
| `qty` | `spot.assets[].qty` | 资产数量 |
| `price` | `spot.assets[].price` | 标价 |
| `avg_entry_price` | `spot.assets[].avg_entry_price` | 均价 |

补充：

- spot 卖出前置检查应读取 `qty - locked`
- futures / spot 的 `NEW / PARTIALLY_FILLED / CANCELED / EXPIRED` 这类 lifecycle 事件必须带显式 `order_id`
- 只有直接 `FILLED` 的即时成交路径可以省略 `order_id`

## Legacy 对照

| Legacy | Canonical | 处理方式 |
| --- | --- | --- |
| `WB` | `wallet_balance` | 仅 legacy 内部继续使用，不再作为标准层命名 |
| `balance` | `wallet_balance` / `available_balance` / `margin_balance` | 必须拆分语义 |
| `marginRequired` | `initial_margin` / `position_initial_margin` / `open_order_initial_margin` | 必须拆分语义 |
| `qty` | `position_qty` | 标准层不再保留 alias 读取；仅 legacy / wire mirror 可出现 |
| `margin_type` | `margin_mode` | 标准层不再保留 alias 读取；仅 legacy / wire mirror 可出现 |

## 兼容边界

- `environment=0`：`BinanceWalletRuntime` 直接消费 canonical state；backtest 共用同一 runtime
- `environment=1`：`BinanceWalletRuntime` 直接消费 canonical state（旧别名 `BinanceParityWallet` 已在 `C2b` 清理中删除）
- demo/live 交易所环境若 `multi_assets_mode=true` 或 `portfolio_margin=true`：直接 fail-closed
- 缺 metadata 的风险字段仍可保留 exchange/oracle 值，但不再回退 legacy 近似公式
- `LegacyWalletAdapter` 已在 `Phase C / C2b` 删除；主代码不再依赖 canonical -> legacy 转换层
