# Phase C：Shadow Compare + mode=0 迁移 + Legacy 下线

日期：`2026-04-18`

状态：`C1` 已落地，`C2a/C2b` 已在本地代码完成并通过全量测试；剩余 `C3` testnet smoke / 阈值校准 / archive

## 1. 定位

截至当前代码状态：

- `Phase A` 已完成：`core-service` 已切到 Binance v3 快照与帐号级凭证
- `Phase B1` 已完成：canonical contract、mode-selected runtime、`mode=2` hydration 已落地
- `Phase B2` 已完成：strict canonical ingress、backtest bootstrap、metadata-backed 风险字段已落地
- `Phase B3` 已完成：futures open-order margin lifecycle、ledger events、isolated wallet / break-even、本地 spot locked lifecycle 已落地
- `Phase B` 收尾 review 也已完成：lifecycle 事件缺 `order_id` 时 fail-closed；futures 开仓前置检查改读 `available_balance`；spot 卖出前置检查改读 `qty - locked`

因此，`Phase C` 不再是“补公式”的阶段，而是：

1. 建立 `mode=2` 的 **shadow compare / reconciliation**
2. 用 reconciliation 数据为 `mode=0 -> Binance runtime` 迁移提供信心
3. 在 checkpoint 通过后，下线 legacy wallet

最终目标不变：

- `Binance runtime` 替换 legacy
- `mode=0` 与 `mode=2` 收敛到同一套 wallet runtime
- 为未来 OKX 等 provider 保留 runtime 插入点

## 2. 核心边界

| 议题 | 本阶段结论 |
| --- | --- |
| `mode=2` | `Phase C` 主战场。先做 shadow compare，再用它给后续 mode=0 迁移提供验证信号。 |
| `mode=0` | 已在 `C2a` 切到 `BinanceWalletRuntime`，并在 `C2b` 删除 `LegacyWalletAdapter`；当前和 `mode=2` 共用 Binance runtime。 |
| `mode=1` | 整个 `Phase C` 继续 fail-closed；只做结构预留，不进入 gate。 |
| reconciliation | 只做旁路、异步、只读 compare；不阻塞主流程，不自动停策略，不自动回写修正。 |
| legacy 删除 | 不和 `C1` 混做；只在 `C2a` checkpoint 通过后进入 `C2b`。 |
| UI / 运营展示 | 不在 `Phase C` 主实现范围内；后续可以直接消费对账表或 metrics。 |
| OKX | 只预留 runtime 抽象插口，不在本阶段落 provider 实现。 |

## 3. 核心决策

| # | 议题 | 决策 |
| --- | --- | --- |
| 1 | Phase C 切分 | 分 `C1 / C2 / C3` 三段：先非破坏 reconciliation，再破坏性迁移，再全量验证。 |
| 2 | Shadow compare 适用范围 | `C1` 只做 `mode=2`。`mode=1` 不参与，不打开。 |
| 3 | 接口形态 | 当时复用旧钱包快照 RPC；不改 proto。Phase 3 正常 runtime 已迁到 portfolio snapshot。 |
| 4 | 对账执行模型 | `core-service` 内独立协程 fire-and-forget；主流程立即返回。 |
| 5 | exchange snapshot 来源 | 主流程只拉一次 exchange authoritative snapshot；协程直接复用主流程拿到的 authoritative payload，不再二次调用 Binance。 |
| 6 | panic / timeout 处理 | 协程内 `defer recover()`；使用独立 timeout；任何错误只记 log / metrics。 |
| 7 | DB 写入策略 | 每次 compare run 都持久化 `local snapshot + exchange snapshot + diff summary`；双快照统一使用 canonical JSON，不再只留 fail 样本。 |
| 8 | 统计口径 | DB 明细与 metrics counters 同时保留；分母既可由 DB 聚合，也可由 metrics 提供。 |
| 9 | compare 维度 | `checkpoint / event / sampled` 是 run 类型；`Hard / Soft / Advisory` 是字段等级。所有 compare run 都检测 `Hard + Soft`，并记录 `Advisory`。 |
| 10 | runtime 抽象 | 引入 `ExchangeWalletRuntime` Protocol；`BinanceParityWallet` 在 `C1` 改名为 `BinanceWalletRuntime`，短期 alias 已在 `C2b` 清理一并删除。 |
| 11 | runtime 选择 | `wallet_factory` 已引入 `RUNTIME_REGISTRY`；当前 `("local", "backtest")` 与 `("binance", "testnet")` 都解析到 `BinanceWalletRuntime`。 |
| 12 | mode=0 切换 | `C2a` 已完成：`mode=0` 已切到 `BinanceWalletRuntime`，且 HTTP / gRPC backtest 两条入口都走同一 runtime。 |
| 13 | legacy 去留 | `C2b` 已完成：`LegacyWalletAdapter` 已删除；主代码不再依赖 legacy runtime。 |
| 14 | backtest 无 oracle 场景 | backtest 可以没有 exchange-backed 初值；已有本地状态机的字段继续按本地逻辑演化，不强制归零。 |
| 15 | worker queue / 背压 | 不进入 `C1`；如果后续 testnet compare 压力上来，再单独作为优化项引入。 |
| 16 | 对账存储后端 | `C1` 继续使用现有 `core-service` 数据库（Timescale/Postgres）；不提前为 TiDB/分库分表做抽象。 |

## 4. 三阶段执行

```text
C1  非破坏：接口抽象 + mode=2 shadow compare         ~3.5-4.5 天
    ↓ Gate: 现有 mode=0/2 行为不变，testnet smoke 正常，主流程 latency 不变

C2  破坏：mode=0 切 Binance runtime + legacy 清理     ~2.5-3.5 天
    ├─ C2a 切 mode=0（可回滚 checkpoint）
    └─ C2b 删除 legacy（burn the bridge）
    ↓ Gate: backtest 差异可解释、grep 无 legacy、pytest 全绿

C3  验证：全量回归 + 阈值校准 + 收尾归档              ~2-3 天
    ↓ Gate: hard fail=0，soft fail 在阈值内，差异可解释
```

### 4.1 为什么这样分

- `C1` 只加代码，不改当前行为，是最低风险阶段
- `C2a` 是唯一需要“承诺新 runtime 行为”的地方，所以必须留 rollback checkpoint
- `C2b` 只有在 `C2a` 跑稳后才值得做
- `C3` 不再改核心结构，只做验证、调阈值、归档

### 4.2 回滚矩阵

| 出问题的位置 | 回滚动作 |
| --- | --- |
| `C1` 接口抽象有问题 | 直接 revert；不涉及行为切换 |
| `C1` 对账协程有问题 | `reconciliation.enabled = false` 关掉协程；主流程不动 |
| `C2a/C2b` 切换后发现问题 | git 恢复 legacy 代码 + 必要时回滚注册表；当前已不保留一行热回滚 |
| `C3` 阈值问题 | 调 config / 修 parity 逻辑；不回滚整体结构 |

## 5. Shadow Compare 架构

### 5.1 总体原则

1. 主流程写 authoritative snapshot
2. 对账在独立协程里进行
3. 对账失败不返错给 strategy-service
4. 每次 compare run 都保留 `local snapshot + exchange snapshot + diff summary`
5. pass / soft fail / hard fail / error 的总量同时写 DB 与 metrics
6. 对账协程不再重新拉 Binance；只消费主流程已经拿到的 authoritative snapshot

### 5.2 主流程与协程分层

```text
strategy-service                           core-service
────────────────                           ───────────────

wallet sync（历史旧路径）
  │
  └─ 旧钱包快照 RPC(local wallet, snapshot_reason)
                                           │
                                           │ 主流程
                                           ├─ 1. 读取 portfolio.environment
                                           ├─ 2. demo/live 交易所环境拉一次 exchange authoritative snapshot
                                           ├─ 3. 持久化 authoritative wallet / portfolio snapshot
                                           ├─ 4. 组织 compare payload:
                                           │      - local snapshot（来自 request）
                                           │      - exchange snapshot（来自本次 authoritative fetch）
                                           │      - snapshot_reason / session_id / strategy_id
                                           ├─ 5. LaunchAsync(compare task)
                                           └─ 6. 立即返回 response
                                                                      │
                                                                      ▼
                                           独立 goroutine（compare）
                                           ├─ defer recover()
                                           ├─ 独立 timeout
                                           ├─ local canonical vs exchange canonical diff
                                           ├─ 计算 hard/soft/advisory 结果
                                           ├─ 持久化 reconciliation run（all runs）
                                           ├─ metrics++（all runs）
                                           └─ log INFO / WARN / ERROR
```

### 5.3 Trigger 与 `snapshot_reason`

现有 proto 已有 `snapshot_reason`，不需要再改接口。

| 业务事件 | `snapshot_reason` | compare run 类型 | 主流程写 snapshot | 启动对账协程 |
| --- | --- | --- | --- | --- |
| 成交 fill | `OrderFill(1)` | `event run` | ✅ | ✅ `mode=2` |
| 策略启动 | `StrategyStart(2)` | `checkpoint run` | ✅ | ✅ `mode=2` |
| 策略结束 | `StrategyEnd(3)` | `checkpoint run` | ✅ | ✅ `mode=2` |
| K 线 periodic | `PeriodicSample(6)` | `sampled run` | ✅ | ✅ `mode=2` |
| 服务重启恢复 | `RestartRecovery(7)` | `checkpoint run` | ✅ | ✅ `mode=2` |
| 账户创建 seed | `InitialSeed(0)` | 无 | ✅ | ❌ |

说明：

- `mode=0` 不做 periodic compare，因为没有外部 oracle
- `mode=1` 本阶段不启用 compare，因为它仍 fail-closed
- `run_type` 由 `snapshot_reason` 在服务端推导，不需要 strategy-service 额外传新字段
- 不论是 `checkpoint run`、`event run` 还是 `sampled run`，都统一检测 `Hard + Soft` 字段，并记录 `Advisory` diff
- `OrderFill(1)` 的 `event run` 默认全量执行，不采样；因为订单事件频度低、信号强，采样只会丢信息

### 5.4 字段分层与阈值

这些阈值是 **reconciliation compare tolerance**，不是字段公式。

意思是：

- 本地 runtime 算出一份 local snapshot
- exchange authoritative 也有一份 compare baseline
- 对账时允许存在一个小的误差窗口
- 超过窗口才记为 fail；未超过则视为 pass

这里要明确区分两个概念：

- `checkpoint / event / sampled`：决定什么时候做 compare
- `Hard / Soft / Advisory`：决定 compare 后如何判定严重性

两者是正交维度，不能混用。

#### `Hard / Soft / Advisory` 总表

| 层级 | 字段 | 字段含义 | 阈值类型 | 默认值 | 处理策略 |
| --- | --- | --- | --- | --- | --- |
| `Hard` | `symbol` / `position_side` / `margin_mode` / `position_mode` | 结构与方向语义 | exact match | 必须完全一致 | 任一不一致即 `hard fail` |
| `Hard` | `position_qty` | 仓位数量 | `stepSize` 倍数 | `<= 0.5 × stepSize` | 超阈值记 `hard fail` |
| `Hard` | `entry_price` | 开仓均价 | `max(tick, ratio)` | `<= max(1 tick, 0.02%)` | 超阈值记 `hard fail` |
| `Soft` | `wallet_balance` | 账本余额 | `max(abs, ratio)` | `<= max(0.01 USDT, 0.02%)` | 超阈值记 `soft fail` |
| `Soft` | `available_balance` | 可用保证金 | `max(abs, ratio)` | `<= max(0.05 USDT, 0.20%)` | 超阈值记 `soft fail` |
| `Soft` | `margin_balance` / `total_margin_balance` | 保证金余额 / 期货权益 | `max(abs, ratio)` | `<= max(0.05 USDT, 0.20%)` | 超阈值记 `soft fail` |
| `Soft` | `unrealized_pnl` | 未实现盈亏 | `max(abs, ratio)` | `<= max(0.05 USDT, 0.20%)` | 超阈值记 `soft fail` |
| `Soft` | `position_initial_margin` / `open_order_initial_margin` / `initial_margin` | 持仓 IM / 挂单 IM / 总 IM | `max(abs, ratio)` | `<= max(0.05 USDT, 0.20%)` | 超阈值记 `soft fail` |
| `Soft` | `total_position_initial_margin` / `total_open_order_initial_margin` / `total_maint_margin` | 账户级 IM / MM 汇总 | `max(abs, ratio)` | `<= max(0.05 USDT, 0.20%)` | 超阈值记 `soft fail` |
| `Soft` | `maint_margin` | 维持保证金 | `max(abs, ratio)` | `<= max(0.05 USDT, 0.20%)` | 超阈值记 `soft fail` |
| `Soft` | `liquidation_price` | 爆仓价 | `max(abs, ratio)` | `<= max(0.05 USDT, 0.50%)` | 超阈值记 `soft fail` |
| `Advisory` | `mark_price` | 标记价格 | drift only | 不参与 gate；单独记录 drift | 只记 diff，不影响 pass/fail |
| `Advisory` | `break_even_price` | 保本价 | drift only | 不参与 gate | 只记 diff，不影响 pass/fail |
| `Advisory` | `isolated_wallet` | 逐仓钱包余额 | drift only | 不参与 gate | 只记 diff，不影响 pass/fail |
| `Advisory` | `notional` | 名义价值 | drift only | 不参与 gate | 只记 diff，不影响 pass/fail |

说明：

- `wallet_balance` 是 ledger-driven 字段；它不随市场价格波动。
- `available_balance` 不是纯 ledger 字段。当前实现里它受 `wallet_balance`、`unrealized_pnl`、`position_initial_margin`、`open_order_initial_margin` 共同影响，所以归到 `Soft` 风险派生层，而不是 `Hard` 或单独的 ledger 层。
- `open_order_initial_margin` 已有本地模型，进入 `Soft compare`。
- `break_even_price` / `isolated_wallet` 虽然已经有本地状态，但 `C1` 先只做 `Advisory`，不直接卡 gate。
- `break_even_price` 当前按 `mode=2` testnet 样本推断的 Binance 口径维护：`break_even_price = entry_price + sign(qty) * carry_cost / abs(qty)`；同向 partial close 会把 `-realized_pnl + fee` 继续摊入剩余仓位 `carry_cost`。Binance 未公开完整公式，后续若样本推翻该推断，应只回滚 carry-cost 分配规则。
- cross funding fee 在 Binance user-data 语义下通常只更新 balance；没有明确 position attribution 时，不把 funding 直接写入 `break_even_price`。
- `mark_price` 更像行情时点一致性问题，不作为钱包账本 hard/soft gate 字段。

#### `tickSize` / `stepSize` 说明

| 名称 | 含义 | 示例 | 用途 |
| --- | --- | --- | --- |
| `tickSize` | 最小价格跳动单位 | 若 `tickSize = 0.1`，允许价格为 `100.0 / 100.1 / 100.2`，不允许 `100.05` | 用于 `entry_price`、`mark_price`、`liquidation_price` 这类价格字段的 compare tolerance |
| `stepSize` | 最小数量步长 | 若 `stepSize = 0.001`，允许数量为 `0.001 / 0.002 / 0.003`，不允许 `0.0015` | 用于 `position_qty`、order qty、remaining qty 这类数量字段的 compare tolerance |

也就是说：

- `tickSize` 是价格精度边界
- `stepSize` 是数量精度边界
- 在 reconciliation 里使用它们，是为了避免把正常的精度舍入差异误判成 fail

### 5.5 配置

```yaml
exchange:
  mock_binance: false
  symbol_cache_ttl: "6h"
  reconciliation:
    enabled: true
    goroutine_timeout_seconds: 5
    order_fill_run_mode: "all"   # all = 每次 OrderFill 都 compare
    periodic_sample_every_bars: 20
    thresholds:
      position_qty_step_tolerance: 0.5
      entry_price_tick_tolerance: 1.0
      entry_price_ratio_tolerance: 0.0002
      wallet_balance_abs_tolerance_usdt: 0.01
      wallet_balance_ratio_tolerance: 0.0002
      derived_risk_abs_tolerance_usdt: 0.05
      derived_risk_ratio_tolerance: 0.002
      mark_price_drift_tick_warn: 3.0
      liquidation_price_abs_tolerance_usdt: 0.05
      liquidation_price_ratio_tolerance: 0.005
```

### 5.6 持久化与 metrics

#### `reconciliation_runs`

每次 compare run 都写一条明细，作为审计与回放基线：

```sql
CREATE TABLE reconciliation_runs (
    time              TIMESTAMPTZ      NOT NULL,
    portfolio_id        BIGINT           NOT NULL,
    user_id           BIGINT           NOT NULL,
    session_id        TEXT             NULL,
    strategy_id       BIGINT           NULL,
    mode              INTEGER          NOT NULL,
    snapshot_reason   SMALLINT         NOT NULL,
    run_type          TEXT             NOT NULL, -- checkpoint / event / sampled
    exchange_snapshot JSONB            NOT NULL, -- canonical JSON
    local_snapshot    JSONB            NOT NULL, -- canonical JSON
    field_diffs       JSONB            NOT NULL,
    advisory_diffs    JSONB            NOT NULL,
    hard_pass         BOOLEAN          NOT NULL,
    soft_pass         BOOLEAN          NOT NULL,
    PRIMARY KEY (time, portfolio_id)
);
```

说明：

- `hard_pass` / `soft_pass` 来自同一次 compare run 对 `Hard + Soft` 字段的统一判定
- `Advisory` 字段始终记录 diff，但不参与 `hard_pass` / `soft_pass`
- 因为每次 compare 都落库，所以分母可以直接从该表统计
- `exchange_snapshot` / `local_snapshot` 统一存 canonical 形状，不存 provider raw schema；后续接入其他交易所时可直接复用同一套对账表

#### metrics counters

即使 DB 已有全量分母，metrics 仍然需要，方便在线监控。最少需要：

- `reconciliation_runs_total`
- `reconciliation_hard_fail_total`
- `reconciliation_soft_fail_total`
- `reconciliation_error_total`

`C3` 的 `soft fail ratio` 可以从 DB 聚合，也可以从 metrics counters 推导；两者应保持一致。

## 6. C1：接口抽象 + mode=2 Shadow Compare

### 6.1 C1.1 接口抽象（不改行为）

目标：

- 抽 `ExchangeWalletRuntime` Protocol
- `BinanceParityWallet` 改名为 `BinanceWalletRuntime`
- `wallet_factory` 引入 `RUNTIME_REGISTRY`

关键约束：

- `C1` 阶段曾保留 `mode=0 -> LegacyWalletAdapter` 作为 checkpoint；当前代码已进入 `C2b`，`mode=0` 走 `BinanceWalletRuntime`
- `mode=2` 仍走现有 parity runtime
- `mode=1` 继续 fail-closed，不因为 registry 引入而被顺手打开

建议初始注册表：

```python
RUNTIME_REGISTRY = {
    ("local", "backtest"): BinanceWalletRuntime,
    ("binance", "testnet"): BinanceWalletRuntime,
}
```

`mode=1` 在 `resolve_target()` 或 build 阶段继续直接报错。

### 6.2 C1.2 Shadow Compare（只做 mode=2）

目标：

- 在 `core-service` 内新增 reconciliation 模块
- 在旧钱包快照 RPC 的 mode=2 分支末尾 `LaunchAsync`
- 在 strategy session 增加 K 线 bar 计数与 `PeriodicSample`

说明：

- `PeriodicSample` 位置应在 session / grpc_server 级，不放到 `BaseStrategy`
- 对账输入使用 strategy-service 传来的 local wallet 快照
- 对账基准使用主流程本次 authoritative fetch 的 exchange snapshot
- compare 协程只做 diff / persist / metrics，不再额外打一次 Binance 用户账户接口
- `OrderFill` 事件默认每次都触发 compare；只有 `PeriodicSample` 才是抽样运行
- worker queue / 背压控制不在 `C1` 首轮实现范围内，先保持简单异步协程模型

### 6.3 C1 Gate

必须全部通过后才进 `C2`：

- `pytest tests/` 全绿
- 手动跑一个 `mode=0` backtest，数字与 `C1` 前完全一致
- 手动跑一个 `mode=2` testnet smoke，主流程返回结果与 `C1` 前一致
- 开/关 reconciliation 两种情况下，旧钱包快照 RPC P99 无明显差异
- 当前本地代码验证：`go test ./...` 全绿，`PYTHONPATH=.:../strategy-library pytest tests/ -q` 全绿
- panic / timeout / DB 写失败注入后，主流程仍正常返回
- testnet 5-10 笔 compare，log / DB / metrics 行为符合预期

## 7. C2：mode=0 切换 + Legacy 下线

### 7.1 C2a：mode=0 切到 Binance runtime（可回滚 checkpoint）

目标：

- 把 `("local", "backtest")` 从 checkpoint 阶段的 legacy runtime 切到 `BinanceWalletRuntime`
- 跑一组代表性 backtest，确认不崩、差异可解释

这里不再重复假设“oracle 字段全部为 0”。更准确的要求是：

1. backtest 没有 exchange snapshot 时，runtime 不应因缺少 exchange-backed 初值而报错
2. `wallet_balance` bootstrap 继续沿用 `Phase B2/B3` 已落地规则
3. 本地已经建模的字段继续按本地状态机演化：
   - `open_order_initial_margin`
   - `isolated_wallet`
   - `break_even_price`
4. 没有外部行情前，允许 `mark_price` 走 backtest 初始化约定；随后由 `on_market_data` 更新

核心改动仍然只有一行：

```python
RUNTIME_REGISTRY[("local", "backtest")] = BinanceWalletRuntime
```

当前代码已经进入 `C2b`，不再保留一行热回滚；若需要回退，做法是恢复 legacy 代码并重新绑定注册表。

### 7.2 C2a Gate

- 代表性 backtest 路径跑通
- 再跑 3-5 个不同 case：`cross / isolated / hedge / 不同频率`
- 所有数字差异都能归类为：
  - parity 修正了 legacy 的旧口径
  - 已知的 isolated/backtest 语义差异
  - 已验证的 rounding / timing 差异
- 出现“无法解释的差异”时，停止，不进 `C2b`

### 7.3 C2b：删除 legacy（burn the bridge）

删除范围：

- `strategy_service/wallet/legacy_adapter.py`
- `strategy_service/wallet/future.py`
- `wallet/__init__.py` 中 legacy export
- `wallet_factory.py` 中 legacy 注册项
- 纯 legacy 行为测试
- `BinanceParityWallet` 短期 alias

历史 `strategy-service/docs/wallet-calculation.md` 已移除；当前钱包口径以
`strategy-service/docs/canonical-wallet-state.md`、`docs/audit/wallet_interface.md`
和 `progress/binance-wallet-reconciliation-v1.md` 为准。

### 7.4 C2 Gate

```bash
grep -r "LegacyWalletAdapter\|legacy_adapter" strategy-service/ --exclude-dir=docs --exclude-dir=openspec
grep -r "from strategy_service.wallet.future" strategy-service/
grep -r "BinanceParityWallet" strategy-service/ --exclude-dir=docs --exclude-dir=openspec
```

以上应无匹配；并且：

- `pytest tests/` 全绿
- 代表性 backtest 再跑一遍无回归

**状态 (2026-04-18):** 全绿。Phase C2b 的代码清理已在主 phase-c change 中
完成 (`LegacyWalletAdapter` 删除、`BinanceParityWallet` 短期别名删除);
legacy harness (`future.py` / `portfolio.py` / legacy 入口脚本 / 5 个测试 fixture)
由独立的 change `strategy-wallet-legacy-cleanup` (本轮) 完成。具体 grep 结果:

- `LegacyWalletAdapter` / `legacy_adapter` —— 零命中(含测试)
- `from strategy_service.wallet.future` —— 零命中(文件本身已删)
- `BinanceParityWallet` —— 零命中(别名已删)
- `FutureWallet` / `Position` / `Portfolio` / `Wallet` / `FuturesPosition`
  作为导出符号 —— 零命中(`wallet/__init__.py` 已精简)

## 8. C3：全量回归 + 阈值校准

### 8.1 backtest 回归

跑所有现有 backtest E2E，逐项看：

- 崩溃：`0` 容忍
- 数字变化：允许存在，但必须可解释
- 方向合理性：新 runtime 是否比 legacy 更接近 Binance 口径

### 8.2 testnet diff 分布观察

跑 30-50 笔不同规模的单，重点观察：

- `total_margin_balance`
- `available_balance`
- `unrealized_pnl`
- `open_order_initial_margin`
- `liquidation_price`
- `mark_price`

### 8.2.1 当前仓库里的 C3 smoke 入口

- 提交/挂载/激活 `ETHUSDT` 对账策略:

```bash
cd strategy-service
export HUSHINE_USERNAME=<PORTAL_USERNAME>
export HUSHINE_PASSWORD=<PORTAL_PASSWORD>
python scripts/submit_eth_pyramid_strategy.py --portfolio-id <MODE2_PORTFOLIO_ID> --activate
```

- 启动 live `mode=2` session:

```bash
curl -s -X POST http://127.0.0.1:8090/api/portfolios/<MODE2_PORTFOLIO_ID>/run-strategy \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"strategy_path":"","interval":"1m"}'
```

- 观察 `checkpoint / event / sampled` 对账输出:
  - `gateway` 账户详情 / session reconciliation 视图
  - `core-service` 库里的 `reconciliation_runs`
  - `core-service` 结构化日志里的 reconciliation metrics

### 8.3 阈值校准原则

- 偏差方向随机：阈值保持或略放宽
- 偏差单向：不调阈值，先修公式
- 偏差幅度一致但系统性偏大：先确认是否为时序/精度问题，再决定是否放宽

### 8.4 C3 Gate（Phase C 结束）

- `reconciliation_hard_fail_total = 0`
- `soft fail ratio` 在目标阈值内
- backtest 数字变化全部有解释
- log 中无未解释 ERROR
- `CLAUDE.md` 更新阶段状态
- OpenSpec change archive

## 9. 不在 Phase C 范围内的事

- 打开 `mode=1` live runtime
- 对账失败时自动 stop strategy 或自动 resync
- UI 展示 diff
- `gateway` / Market Data 的状态语义重构（如 `OFF / READY / ERROR` 三态、独立 delivery health probe、按 `interval` 的 freshness 阈值）
- OKX runtime 实现
- Kafka live market-data pipeline 打通
- 自动归因（commission / funding / rounding / timing 的细分归因）
- reconciliation worker queue / 背压优化
- reconciliation 存储迁移到 TiDB / 分库分表 / 独立分析库

## 10. 建议的 OpenSpec 切分

如果要提 change，我建议直接按下面切：

- `C1`：runtime abstraction + mode=2 shadow compare
- `C2`：mode=0 migration + legacy removal
- `C3`：regression + threshold calibration + archive

如果只建一个 change，也建议 tasks 里显式分成这三段，并保留 `C2a` checkpoint。  
这是 `Phase C` 里最关键的风险隔离点。
