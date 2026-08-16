# BTC/ETH/ZEC 多币种 Futures 动量测试策略设计

## 目标

提供一个单一 `MyStrategy`，同时消费 Binance USDT-M Futures 的
`BTCUSDT`、`ETHUSDT`、`ZECUSDT` 三条 1m Kline 流，用于验证一个策略内的
多输入路由、多交易目标、独立策略状态、下单和 Indicator V2 展示。

该策略只用于功能测试，不以收益或风险控制为目标。

## 运行前提

- Futures 初始资金为 1000 USDT。
- Venue 配置的保证金模式为 Cross，且三个 symbol 的交易所 risk metadata 均为 Cross。
- Venue 配置的持仓模式为 One-way，并由 core-service 原样传入 Runtime 钱包快照。
- BTCUSDT、ETHUSDT、ZECUSDT 的配置杠杆均为 10 倍。
- 策略不修改交易所账户设置；账户设置不满足上述条件时必须明确告警并跳过下单。

## 输入与交易目标

`INPUTS` 声明三个带稳定 `stream_id` 的 Binance
`perpetual_futures`/`kline`/`1m` 输入，每个 symbol 一条。

`ORDER_TARGETS` 声明同一 Venue 上的三个 Futures symbol。策略只允许向触发当前
回调的 symbol 下单，不能用 BTC 行情触发 ETH 或 ZEC 订单。

## 信号和独立状态

策略通过 `data.trigger` 识别本次回调对应的 stream 和 symbol，并为三个 symbol
分别维护参考价。第一个有效价格只初始化该 symbol 的参考价，不下单。

- 当前价相对该 symbol 参考价上涨至少 0.1%：BUY。
- 当前价相对该 symbol 参考价下跌至少 0.1%：SELL。
- 未达到阈值：不下单、不移动参考价。
- 触发后：只发一笔订单，并把当前价设为该 symbol 的新参考价。
- 单次 Kline 更新即使跨越多个 0.1% 档位，也只发一笔订单，避免订单风暴。

BUY/SELL 使用 `PositionSide.BOTH`。在 One-way 模式中，反向订单遵循 Binance
净持仓语义：先减少已有反向仓位，数量超过已有仓位后才形成新的净方向。

## 仓位计算

每次触发使用 Futures `wallet_balance` 的 1% 作为保证金预算：

```text
margin_budget = wallet_balance * 0.01
order_notional = margin_budget * configured_leverage
raw_qty = order_notional / current_price
```

在初始 1000 USDT、10 倍杠杆下，每笔保证金预算约 10 USDT，订单名义价值约
100 USDT。单纯的价格变化不会改变 `wallet_balance`；已实现盈亏、手续费、资金费和
其他账本事件可以改变后续订单大小。

策略从当前 Futures risk metadata 读取 symbol 的 `configured_margin_mode`、
`step_size` 和配置杠杆，按 `step_size` 向下取整数量。metadata 缺失、symbol 不是
Cross、杠杆不是 10 或数量为零时，策略告警并跳过该笔订单。按当前 Binance Demo
约束，约 100 USDT 的名义价值高于三个合约的最小下单金额；服务端订单风控仍使用
请求时的交易所规则执行最终校验。

## 自定义指标

策略声明以下 Indicator V2 定义：

- `reference_price`：当前 symbol 的参考价，价格面板折线。
- `change_bps`：当前价相对参考价的变化，单位为基点，策略面板折线。
- `trade_signal`：BUY/SELL 标记，显示在触发价格上。

每次回调只写当前触发 stream 的 frame。Runtime 使用当前行情的 `stream_key` 发送并
持久化 frame，因此三条流可复用同一组 indicator key，同时仍在 BTC、ETH、ZEC
各自图表中独立展示。

## 异常处理

- 忽略空数据、非声明 symbol、非正数价格和无法解析的价格。
- 钱包路由、risk metadata 或账户模式不满足前提时，输出不含敏感信息的告警并跳过。
- 参考价只在首次有效价格和成功构造订单时更新；配置错误不能吞掉一个有效价格档位。
- 策略不捕获或隐藏订单提交后的交易所拒单；平台现有订单生命周期负责展示失败原因。

## 备选方案

1. **统一 trigger 分发（采用）**：一个处理函数和三个独立状态项，重复最少，直接验证
   V2 多流路由。
2. 三个显式 symbol 分支：行为直观，但计算、校验和指标逻辑重复，容易出现币种间漂移。
3. 从完整 `data.exchange` 树轮询三条流：一个回调可能重复处理并非本次触发的数据，
   不适合验证精确的 stream 路由。

## 验证范围

新增策略模板及针对性回放测试，至少覆盖：

1. 三个输入和三个订单目标通过策略声明校验。
2. 三个 symbol 的首条数据分别初始化，互不覆盖参考价。
3. 交错输入下，BTC、ETH、ZEC 分别在正负 0.1% 触发正确 symbol 和 side。
4. 未达到阈值不下单；一次跨越多档只下一单并重置对应参考价。
5. 1000 USDT、Cross、One-way、10 倍杠杆时，订单数量符合 1% 保证金预算和 step size。
6. 账户模式、杠杆或 metadata 不满足前提时不产生订单并提供告警。
7. 三个 stream 分别输出参考价、变化基点和 BUY/SELL 标记。

不修改 Runtime、钱包、订单网关或 Indicator V2 的外部协议。最终复审发现
core-service 的 Binance 钱包快照把保证金/持仓模式写死，因而增加一项内部修正：
Venue fact 经内部 `PortfolioSnapshotRequest` 传给 Binance reader，reader 原样写入
钱包快照；策略仍以 symbol 的 Binance metadata 判断实际保证金模式。该修正不新增
跨服务字段，也不在策略内绕过平台契约。
