# Pre-C3 补充设计稿

日期：`2026-04-19`

状态：`draft`

关联文档：

- `phaseC.md`
- `canonical-wallet-state.md`

## 1. 背景

`Phase C / C3` 原计划进入 `mode=2 testnet smoke / 阈值校准 / reconciliation` 阶段，但实际推进过程中，启动链路连续暴露出一类共性问题：

- 策略输入 universe 没有显式声明，系统只能从 wallet / position / asset 反推
- `wallet.spot.assets` 里既可能是余额资产（如 `USDC` / `BTC`），也被错误当成行情 symbol 使用
- live/testnet 启动前的 preflight、live subscription、strategy router 三处语义并不一致
- `mode=0 / 1 / 2` 被混入了业务兼容性判断，但它本质上更接近 runtime source profile，而不是策略匹配条件

这说明当前阻塞 `C3` 的，不只是实现 bug，而是若干关键 contract 还没有冻结。

本文件的目的，是在真正继续推进 `C3` 之前，先补一份版本级设计补充，把相关语义正式收敛。

## 2. 本文结论

### 2.1 策略必须显式声明自己依赖的 market-data universe

禁止继续使用“收到一条 tick，再从 `data.symbol` 临时判断自己在处理什么”的弱约定。

原因：

- 策略作者无法清楚知道自己消费的是哪一组 symbol / market / interval
- 系统无法在启动前验证策略需要的数据是否已经准备好
- 一旦数据流推错，策略很容易出现张冠李戴的问题

因此：

- 策略必须声明自己的输入集合
- preflight 只检查这组声明输入是否 ready
- live subscription 也只订阅这组声明输入
- strategy router 也只允许这组声明输入进入策略

### 2.2 账户当前仓位不应作为策略是否可启动的前置条件

空仓账户是正常起点。

因此，不允许继续用“账户当前有没有该 symbol 的 position / asset”来判断策略是否可以启动。

原因：

- 线上账户可能本来就没有仓位，策略启动后才建仓
- 现有仓位是账户状态，不是策略输入声明
- 用账户状态推导策略 universe，会把“当前持仓”误当成“策略依赖”

启动前真正应该检查的，是：

- 策略声明的输入流是否存在
- 输入流是否 ready
- 当前 runtime source profile 是否能提供该策略所需的 wallet / order / market-data 能力

不应该检查：

- 当前账户是否已经持有该 symbol 仓位
- 当前 spot 钱包是否已经有该资产余额

### 2.3 `mode` 不是策略兼容性条件，而是内部 runtime source profile

`mode=0 / 1 / 2` 保留，但语义收敛为“内部运行来源 / source profile”。

它决定的是：

- wallet 从哪里来
- order 走哪条 adapter
- market data 从哪里读
- 是否需要交易所 authoritative hydration

它不应该再决定：

- 某个策略能不能启动
- 某个策略和某个账户是否匹配

换句话说：

- 业务层面，账户应当是平等的
- 系统内部，`mode` 仍然可以影响 runtime wiring

### 2.4 内部只认 canonical wallet；exchange display wallet 只用于 UI 展示

后续所有交易所接入，都必须先转换到 canonical wallet。

明确边界如下：

- 内部策略运行、对账、session、风控、wallet 演化，只认 canonical wallet
- canonical wallet 的 futures 语义，统一按单资产 `USDT@-M` 推进
- spot 也统一按 `USDT` 作为媒介交易
- exchange display wallet 纯粹用于页面展示，目的是消除用户误解
- display wallet 不入核心运行态，不入策略语义，不参与风控，不作为持久化 source of truth

## 3. 新的策略输入 contract

## 3.1 显式声明输入

策略必须声明自己依赖的 `(market, symbol, interval)` 集合。

推荐形式：

```python
class MyStrategy:
    INPUTS = [
        {"market": "futures", "symbol": "ETHUSDT", "interval": "1m"},
        {"market": "spot", "symbol": "BTCUSDT", "interval": "5m"},
    ]
```

也允许后续压缩为字符串形式，例如：

```python
INPUTS = [
    "futures:ETHUSDT:1m",
    "spot:BTCUSDT:5m",
]
```

但无论哪种写法，语义都必须一致：策略先声明 universe，系统再绑定数据。

## 3.2 `on_market_data` 的目标输入形态

不再把 `on_market_data` 设计成“单条事件 + 策略自己猜 symbol”。

目标形态应是显式、可索引、可校验的数据视图，例如：

```python
def on_market_data(self, data, wallet):
    eth_1m = data.market["futures"].symbol["ETHUSDT"].interval["1m"]
    btc_5m = data.market["spot"].symbol["BTCUSDT"].interval["5m"]
```

这里的核心要求不是具体语法糖，而是：

- symbol 必须显式写出
- market 必须显式写出
- interval 必须显式写出
- 未声明的流，不允许隐式进入策略

### 3.3 策略 authoring 阶段必须能暴露输入错误

既然策略要对自己的输入负责，那么“symbol / market / interval 不匹配”的问题，应尽量在策略测试阶段暴露，而不是在真实启动时靠账户状态兜底。

因此后续应补：

- 策略声明字段的静态校验
- 本地回测 / 调试时的 universe 校验
- live 启动前的 declaration-based preflight

## 4. 新的启动前检查 contract

## 4.1 preflight 的权威输入来源

启动前 preflight 的输入，只能来自策略声明的 `INPUTS`。

禁止再使用以下来源作为权威 universe：

- `wallet.futures.positions`
- `wallet.spot.assets`
- 账户当前余额资产
- 账户当前是否持仓

这些都只能视为“当前账户状态”，不能视为“策略声明的输入集合”。

## 4.2 preflight 应检查什么

preflight 只检查两类事情：

1. 策略声明的输入是否能被当前 runtime source profile 提供
2. 当前 runtime source profile 是否处于可运行状态

这里要按 profile 区分：

- `mode=0 / backtest profile`
  - 不检查 stream readiness
  - 检查声明的 `(market, symbol, interval)` 在目标时间范围内是否有可用历史数据
  - 本质是 `data availability check`
- `mode=1 / live profile`
  - 检查声明的 `(market, symbol, interval)` 是否存在对应 stream
  - 检查 stream 是否 ready
- `mode=2 / testnet profile`
  - 检查声明的 `(market, symbol, interval)` 是否存在对应 stream
  - 检查 stream 是否 ready

其中 live / testnet 的 `ready` 当前至少应覆盖：

- stream exists
- collector running
- live delivery ready
- freshness within threshold

关键约束：

- backtest 检查的是数据库历史数据是否可取
- live/testnet 检查的是共享流是否 ready
- 这些差异属于 runtime source profile 语义，不属于策略层语义

## 4.3 preflight 不应检查什么

preflight 不应检查：

- 当前账户是否已经持有对应 futures 仓位
- 当前 spot 是否已经持有对应资产
- 当前余额资产能否直接映射成交易 symbol

## 5. runtime source profile contract

为避免把 `mode` 混成业务语义，后续需要把运行期配置抽象成 profile。

示意如下：

```text
runtime source profile
├─ wallet source
├─ order sink
├─ market-data source
└─ exchange hydration policy
```

当前可以继续保留 `mode=0 / 1 / 2` 作为实现细节，但在业务语义上应理解为：

- `mode=0`：本地构造 / 回测 profile
- `mode=1`：live profile
- `mode=2`：testnet profile

重点是：

- 这些 profile 决定“从哪里取数据、往哪里下单”
- 这些 profile 也决定启动前检查是“历史数据可用性”还是“stream readiness”
- 不决定“策略是否和账户匹配”

因此，策略层只声明输入 universe，不感知：

- 当前是查 Timescale 历史数据
- 还是查 live/testnet stream readiness
- 也不感知底层 wallet 是本地构造还是交易所 hydration

## 6. canonical wallet 边界

## 6.1 内部统一口径

后续统一约束如下：

- futures canonical wallet：单资产 `USDT@-M`
- spot canonical wallet：`USDT` 作为交易媒介
- 所有 provider 接入，先转换到 canonical wallet，再进入策略系统

## 6.2 display wallet 定位

exchange display wallet 只用于 UI：

- 对齐用户在交易所页面看到的展示口径
- 降低“为什么 Binance 页面和平台页面显示不同”的误解

但它不参与：

- 策略执行
- session 运行
- reconciliation source of truth
- canonical wallet 演化
- 核心持久化

## 7. 多 symbol / 多 market 支持范围

后续应把“多种 SPOT 和 POSITION 操作”从“底层部分可行”升级为正式支持能力。

正式支持的语义应当是：

- 一个策略可以声明多个 futures symbol
- 一个策略可以声明多个 spot symbol
- 一个策略可以同时声明 spot + futures
- 一个策略可以声明多个 interval

系统需要保证三件事一致：

1. declaration
2. subscription
3. router

即：

- 策略声明什么
- 系统就订阅什么
- 策略也只会收到这些流

## 8. 对当前实现的影响

本设计落地后，当前若干实现都需要调整：

1. `strategy-service`
   - strategy input declaration
   - declaration-based preflight
   - declaration-based live subscription
   - declaration-based strategy router

2. `gateway`
   - 页面上展示策略声明的输入 universe
   - 启动前 readiness 提示改为基于策略声明，而不是基于账户已有请求的模糊猜测

3. `account-service`
   - 继续提供 stream status / wallet / session 支撑能力
   - 不再承担“从账户余额推导策略输入 symbol”的职责

## 9. 建议拆分为三个 spec

为避免一次性改动过大，也避免把 contract、preflight、wallet 展示边界混成一个大需求，建议拆成三个 spec，分三次实现。

### 9.1 spec-1: strategy-input-universe

目标：

- 让策略显式声明 `(market, symbol, interval)` universe
- 收敛 `on_market_data` 的输入 contract
- 正式支持多 `spot / futures / symbol / interval`

范围：

- `INPUTS` 或等价声明格式
- declaration-based strategy router
- declaration-based live subscription 输入来源
- strategy authoring / 本地测试时的 universe 校验
- 从旧 `data.symbol` 驱动方式迁移到显式声明方式

这是第一优先级，也是后续两个 spec 的前置条件。

### 9.2 spec-2: runtime-source-profile-preflight

目标：

- 把启动前检查从“账户当前状态驱动”改为“策略声明驱动”
- 把 `mode` 收敛为 runtime source profile 语义

范围：

- `mode=0` 检查历史数据可用性
- `mode=1 / 2` 检查 stream readiness
- preflight 只消费策略声明的 universe
- gateway 的启动前 readiness 提示与后端语义对齐

这个 spec 应在 `strategy-input-universe` 之后实现。

### 9.3 spec-3: canonical-wallet-display-boundary

目标：

- 冻结 internal canonical wallet 与 exchange display wallet 的边界
- 明确系统内部只认 canonical wallet

范围：

- `USDT@-M` 单资产 canonical futures 边界
- spot 以 `USDT` 为媒介的统一口径
- display wallet 仅用于 UI，不进入核心运行态
- gateway / account-service 的展示字段边界收敛

这个 spec 重要，但不必阻塞前两个 spec 的主线推进。

### 9.4 推荐实施顺序

建议按以下顺序分三次实现：

1. `strategy-input-universe`
2. `runtime-source-profile-preflight`
3. `canonical-wallet-display-boundary`

原因：

- 第一项先冻结“策略到底依赖什么”
- 第二项再冻结“系统如何检查这些依赖是否可运行”
- 第三项最后收敛“内部运行口径”和“外部展示口径”的边界

其中前两个是 `C3` 真正继续推进前的 blocker；第三个建议立项同步推进，但可以略晚落地。

## 10. 不在本补充设计范围内的内容

本文件不解决以下问题：

- exchange display wallet 的最终 UI 排版
- market-data 页面最终三态 UX（`OFF / READY / ERROR`）
- 多交易所 provider 的具体接入实现
- `mode=1` 的正式开放策略

这些问题可以在本 contract 冻结后继续推进，但不应阻塞本补充设计的接受。

## 11. C3 开启前的 gate

在继续推进 `C3` 之前，应先确认以下结论被接受：

1. 策略输入 universe 改为显式声明
2. 启动前检查从“账户当前状态驱动”改为“策略声明驱动”
3. `mode` 退回内部 runtime source profile 语义
4. 内部只认 canonical wallet；display wallet 只做 UI 展示

若以上四点未冻结，则继续推进 `C3` 很可能仍会反复卡在 contract 解释问题，而不是实现问题。
