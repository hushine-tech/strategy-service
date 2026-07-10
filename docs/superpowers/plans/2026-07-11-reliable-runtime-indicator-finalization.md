# Reliable Runtime Indicator Finalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make runtime-agent indicator delivery periodic, monotonic, deduplicated, and fully acknowledged before a completed session becomes finished.

**Architecture:** Python session-worker emits current-bar frames and waits for acknowledgement of its existing `FinalStatus`. A focused Go `IndicatorSyncManager` owns buffering, 1024-point chunk transitions, periodic/immediate/final flushes, and retry coordination. Core-service enforces immutable finalized chunks and monotonic UPSERT semantics.

**Tech Stack:** Go, Python 3.13, gRPC/protobuf using existing fields, TimescaleDB/PostgreSQL, pytest, Go testing.

## Global Constraints

- Work only in `/Users/xdy/Workplace/hushine-worktrees/medium-cleanup`.
- Use `cleanup/medium-baseline-20260710` in each affected repository.
- Preserve runtime heartbeat independence from Python workers.
- Route sessions only by `runtime_id` and persist only through RuntimeChannel platform proxy.
- Do not add a migration, protobuf field, frontend API, user strategy API, or local strategy directory change.
- A nominally completed session becomes `recoverable` if final indicator persistence is not confirmed.
- Use red-green-refactor for every production behavior.
- Stage only owned files and commit strategy-service/core-service independently.
- Do not push until repository and end-to-end verification passes.

---

### Task 1: Enforce monotonic indicator chunk UPSERTs

**Files:**
- Modify: `core-service/internal/repository/strategy_indicator_test.go`
- Modify: `core-service/internal/repository/timescale.go:1490-1513`

**Interfaces:**
- Consumes and preserves: `SaveStrategyIndicators(ctx, sessionID, defs, chunks) (int, int, error)`
- Produces: `chunksSaved` equals actual INSERT/UPDATE rows.

- [ ] **Step 1: Write a failing real-database test**

Add `TestStrategyIndicatorRepositoryChunkUpsertIsMonotonic`. Reuse the existing repository/session setup and execute:

```go
open := domain.StrategyIndicatorChunk{
    SessionID: sessionID, StreamKey: streamKey, IndicatorKey: "alpha_score",
    ChunkIndex: 0, StartTimeMS: 1000, EndTimeMS: 2000, IntervalMS: 1000,
    Count: 2, ValuesJSON: `{"values":[1,2],"times":null}`, Finalized: false,
}
assertSaved(open, 1)

originalUpdatedAt := queryChunkUpdatedAt(t, repo, ctx, open)
time.Sleep(10 * time.Millisecond)
assertSaved(open, 0)
if got := queryChunkUpdatedAt(t, repo, ctx, open); !got.Equal(originalUpdatedAt) {
    t.Fatalf("identical upsert changed updated_at: %v -> %v", originalUpdatedAt, got)
}

stale := open
stale.Count = 1
stale.EndTimeMS = 1000
stale.ValuesJSON = `{"values":[9],"times":null}`
assertSaved(stale, 0)

finalized := open
finalized.Finalized = true
assertSaved(finalized, 1)

regressed := open
regressed.Count = 3
regressed.EndTimeMS = 3000
regressed.ValuesJSON = `{"values":[1,2,3],"times":null}`
assertSaved(regressed, 0)

got := queryChunk(t, repo, ctx, sessionID, streamKey, "alpha_score", 0)
if !got.Finalized || got.Count != 2 || got.ValuesJSON != open.ValuesJSON {
    t.Fatalf("persisted chunk regressed: %+v", got)
}
```

Implement `assertSaved`, `queryChunkUpdatedAt`, and `queryChunk` as test helpers using `repo.SaveStrategyIndicators` and direct repository DB queries.

- [ ] **Step 2: Verify RED**

```bash
cd /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/core-service
go test ./internal/repository -run TestStrategyIndicatorRepositoryChunkUpsertIsMonotonic -count=1 -v
```

Expected: FAIL because duplicate, stale, and finalized-regression writes currently update the row.

- [ ] **Step 3: Implement guarded UPSERT**

Use:

```sql
ON CONFLICT (session_id, stream_key, indicator_key, chunk_index) DO UPDATE SET
  start_time_ms=EXCLUDED.start_time_ms,
  end_time_ms=EXCLUDED.end_time_ms,
  interval_ms=EXCLUDED.interval_ms,
  count=EXCLUDED.count,
  values_json=EXCLUDED.values_json,
  finalized=EXCLUDED.finalized,
  updated_at=NOW()
WHERE strategy_indicator_chunks.finalized=FALSE
  AND EXCLUDED.count >= strategy_indicator_chunks.count
  AND (
    EXCLUDED.count > strategy_indicator_chunks.count
    OR (
      EXCLUDED.finalized=TRUE
      AND EXCLUDED.count=strategy_indicator_chunks.count
      AND EXCLUDED.end_time_ms=strategy_indicator_chunks.end_time_ms
      AND EXCLUDED.values_json=strategy_indicator_chunks.values_json
    )
  )
```

Capture `sql.Result.RowsAffected()` and add it to `chunksSaved` instead of incrementing unconditionally.

- [ ] **Step 4: Verify GREEN**

```bash
go test ./internal/repository -run 'TestStrategyIndicatorRepository' -count=1 -v
go test ./internal/service -run 'TestSave.*StrategyIndicators' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/repository/strategy_indicator_test.go internal/repository/timescale.go
git diff --cached --check
git commit -m "fix: enforce monotonic indicator chunks"
```

---

### Task 2: Make `IndicatorBuffer` dirty-aware and deduplicating

**Files:**
- Modify: `strategy-service/internal/runtimeagent/indicator_buffer.go`
- Modify: `strategy-service/internal/runtimeagent/indicator_buffer_test.go`

**Interfaces:**

```go
type IndicatorPointDisposition int
const (
    IndicatorPointAccepted IndicatorPointDisposition = iota
    IndicatorPointDuplicate
    IndicatorPointOutOfOrder
)
type IndicatorAddResult struct {
    Disposition IndicatorPointDisposition
    Sealed bool
}
type IndicatorFlushSnapshot struct {
    Finals []IndicatorChunk
    Open IndicatorChunk
    OpenGeneration uint64
}
func (b *IndicatorBuffer) AddPoint(IndicatorPoint) IndicatorAddResult
func (b *IndicatorBuffer) SnapshotDirtyForFlush() IndicatorFlushSnapshot
func (b *IndicatorBuffer) SealOpen() bool
func (b *IndicatorBuffer) MarkFlushAcked(IndicatorFlushSnapshot)
```

- [ ] **Step 1: Write failing tests**

```go
func TestIndicatorBufferRejectsDuplicateAndOutOfOrderMarketTimes(t *testing.T) {
    b := NewIndicatorBuffer(1024)
    if got := b.AddPoint(IndicatorPoint{MarketTimeMS: 2000, IntervalMS: 1000, ValueJSON: "2"}); got.Disposition != IndicatorPointAccepted {
        t.Fatalf("first = %+v", got)
    }
    if got := b.AddPoint(IndicatorPoint{MarketTimeMS: 2000, IntervalMS: 1000, ValueJSON: "9"}); got.Disposition != IndicatorPointDuplicate {
        t.Fatalf("duplicate = %+v", got)
    }
    if got := b.AddPoint(IndicatorPoint{MarketTimeMS: 1000, IntervalMS: 1000, ValueJSON: "1"}); got.Disposition != IndicatorPointOutOfOrder {
        t.Fatalf("out of order = %+v", got)
    }
    if got := b.SnapshotDirtyForFlush().Open; got.Count != 1 || got.ValuesJSON != `{"values":[2],"times":null}` {
        t.Fatalf("open = %+v", got)
    }
}

func TestIndicatorBufferAckDoesNotClearNewerGeneration(t *testing.T) {
    b := NewIndicatorBuffer(1024)
    b.AddPoint(IndicatorPoint{MarketTimeMS: 1000, IntervalMS: 1000, ValueJSON: "1"})
    old := b.SnapshotDirtyForFlush()
    b.AddPoint(IndicatorPoint{MarketTimeMS: 2000, IntervalMS: 1000, ValueJSON: "2"})
    b.MarkFlushAcked(old)
    if got := b.SnapshotDirtyForFlush().Open.Count; got != 2 {
        t.Fatalf("open count = %d, want 2", got)
    }
}

func TestIndicatorBufferSealsPartialOpenOnTerminal(t *testing.T) {
    b := NewIndicatorBuffer(1024)
    b.AddPoint(IndicatorPoint{MarketTimeMS: 1000, IntervalMS: 1000, ValueJSON: "1"})
    if !b.SealOpen() {
        t.Fatal("SealOpen returned false")
    }
    got := b.SnapshotDirtyForFlush()
    if len(got.Finals) != 1 || got.Finals[0].Count != 1 || !got.Finals[0].Finalized || got.Open.Count != 0 {
        t.Fatalf("snapshot = %+v", got)
    }
}
```

Adapt existing boundary tests to assert `IndicatorAddResult.Sealed` at 1024 and the next chunk at 1025.

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/runtimeagent -run TestIndicatorBuffer -count=1 -v
```

Expected: compile failure because the new API does not exist.

- [ ] **Step 3: Implement minimal state**

Add:

```go
dirty bool
generation uint64
lastMarketTimeMS int64
hasMarketTime bool
```

Classify timestamps before mutation. Accepted points set dirty and increment generation. `SealOpen` moves a non-empty active chunk to pending finalized state. `MarkFlushAcked` removes only snapshot finalized indexes and clears open dirty state only when generations match.

- [ ] **Step 4: Verify GREEN and commit**

```bash
go test ./internal/runtimeagent -run TestIndicatorBuffer -count=1 -v
git add internal/runtimeagent/indicator_buffer.go internal/runtimeagent/indicator_buffer_test.go
git diff --cached --check
git commit -m "fix: make indicator buffer monotonic"
```

---

### Task 3: Add `IndicatorSyncManager`

**Files:**
- Create: `strategy-service/internal/runtimeagent/indicator_sync.go`
- Create: `strategy-service/internal/runtimeagent/indicator_sync_test.go`

**Interfaces:**

```go
type IndicatorSyncConfig struct {
    PlatformInvoker PlatformInvoker
    IndicatorLimit int
    FlushInterval time.Duration
    RequestTimeout time.Duration
    FinalizeTimeout time.Duration
    RetryInitial time.Duration
    RetryMax time.Duration
}
func NewIndicatorSyncManager(IndicatorSyncConfig) *IndicatorSyncManager
func (m *IndicatorSyncManager) Run(context.Context)
func (m *IndicatorSyncManager) ReceiveFrame(*rwv1.IndicatorFrame) error
func (m *IndicatorSyncManager) FlushSession(context.Context, string, bool) error
func (m *IndicatorSyncManager) FinalizeSession(context.Context, string) error
func (m *IndicatorSyncManager) ForgetSession(context.Context, string)
```

- [ ] **Step 1: Write failing manager tests**

Create a thread-safe fake platform invoker and these tests:

```go
func TestIndicatorSyncManagerReceiveDoesNotCallPlatform(t *testing.T)
func TestIndicatorSyncManagerFlushesOnlyDirtyOpenChunk(t *testing.T)
func TestIndicatorSyncManagerRetainsFinalizedChunkUntilAck(t *testing.T)
func TestIndicatorSyncManagerImmediateFlushesFullChunk(t *testing.T)
func TestIndicatorSyncManagerFinalizesPartialOpenChunk(t *testing.T)
func TestIndicatorSyncManagerSerializesPeriodicAndFinalFlush(t *testing.T)
func TestIndicatorSyncManagerRetriesFinalFlushWithinDeadline(t *testing.T)
```

`ReceiveDoesNotCallPlatform` asserts zero calls until explicit flush. `FlushesOnlyDirty` flushes twice without a new point and expects one call. The concurrency test blocks a fake call and asserts maximum concurrent calls is one.

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/runtimeagent -run TestIndicatorSyncManager -count=1 -v
```

Expected: compile failure because the manager does not exist.

- [ ] **Step 3: Implement focused state**

Use:

```go
type indicatorSeriesState struct {
    userID int64
    strategyID int64
    streamKey string
    indicatorKey string
    definition *rwv1.IndicatorDefinition
    definitionDirty bool
    buffer *IndicatorBuffer
}
type indicatorSessionState struct {
    mu sync.Mutex
    flushMu sync.Mutex
    series map[string]*indicatorSeriesState
    outOfOrder uint64
}
```

`ReceiveFrame` normalizes keys, merges definitions, appends accepted points, increments out-of-order count, and sends the session ID to a coalescing immediate channel when a chunk seals. It performs no platform I/O.

- [ ] **Step 4: Implement flush and retry**

`FlushSession` holds only `flushMu` across platform I/O. Snapshot under state lock, release it, call one batched `portfolio.SaveStrategyIndicators`, then acknowledge the exact snapshots.

`Run` selects on a two-second ticker, immediate session channel, and context cancellation.

`FinalizeSession` seals open chunks and retries with `100ms, 200ms, 400ms, 800ms, 1600ms`, then two seconds for every later retry, never sleeping beyond the context deadline.

- [ ] **Step 5: Verify GREEN/races and commit**

```bash
go test -race ./internal/runtimeagent -run 'TestIndicatorSyncManager|TestIndicatorBuffer' -count=1 -v
git add internal/runtimeagent/indicator_sync.go internal/runtimeagent/indicator_sync_test.go
git diff --cached --check
git commit -m "feat: add agent indicator sync loop"
```

---

### Task 4: Route indicator and final status through the manager

**Files:**
- Modify: `strategy-service/internal/runtimeagent/agent.go`
- Modify: `strategy-service/internal/runtimeagent/agent_test.go`

**Interfaces:**
- Add injectable `IndicatorFlushInterval`, `IndicatorFinalizeTimeout`, `IndicatorRetryInitial`, `IndicatorRetryMax` to `AgentConfig`.
- Add `Agent.RunSyncLoop(ctx)` and:

```go
func (a *Agent) handleWorkerFinalStatus(
    ctx context.Context,
    frameID string,
    status *rwv1.FinalStatus,
    send func(*rwv1.AgentFrame) error,
) error
```

- [ ] **Step 1: Write failing routing/finalization tests**

```go
func TestAgentIndicatorFrameBuffersWithoutImmediatePlatformWrite(t *testing.T)
func TestAgentFinalStatusFlushesThenPersistsFinishedThenAcknowledges(t *testing.T)
func TestAgentFinalStatusFlushFailurePersistsRecoverableAndReturnsErrorAck(t *testing.T)
func TestAgentFinalStatusPreservesFailedStatus(t *testing.T)
func TestAgentCleanupForgetsOnlyRequestedIndicatorSession(t *testing.T)
```

The success test asserts platform order:

```go
[]string{"portfolio.SaveStrategyIndicators", "portfolio.UpdateSession"}
```

and a payloadless acknowledgement with `ReplyTo == "final-1"`.

The failure test asserts:

```go
update.Status == "recoverable"
strings.HasPrefix(update.Error, "indicator finalization failed:")
ack.GetError().GetCode() == "INDICATOR_FINALIZATION_FAILED"
```

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/runtimeagent -run 'TestAgent.*Indicator|TestAgentFinalStatus|TestAgentCleanup' -count=1 -v
```

Expected: FAIL because indicator write is immediate and FinalStatus is ignored.

- [ ] **Step 3: Route frames**

Construct the manager in `NewAgent`. Change `HandleWorkerFrame` to:

```go
case *rwv1.WorkerFrame_IndicatorFrame:
    return a.indicatorSync.ReceiveFrame(frame.GetIndicatorFrame())
case *rwv1.WorkerFrame_FinalStatus:
    return a.handleWorkerFinalStatus(ctx, frame.GetFrameId(), frame.GetFinalStatus(), send)
```

Delete indicator buffer/type ownership and immediate SaveStrategyIndicators helpers from `Agent`. `cleanupSessionState` delegates indicator cleanup to `ForgetSession`.

- [ ] **Step 4: Implement terminal ordering**

For finished/completed:

```go
flushCtx, cancel := context.WithTimeout(ctx, a.cfg.IndicatorFinalizeTimeout)
defer cancel()
if err := a.indicatorSync.FinalizeSession(flushCtx, sessionID); err != nil {
    message := "indicator finalization failed: " + err.Error()
    if persistErr := a.updateSession(ctx, sessionID, "recoverable", bars, message); persistErr != nil {
        return persistErr
    }
    return send(&rwv1.AgentFrame{
        ReplyTo: frameID,
        Payload: &rwv1.AgentFrame_Error{Error: &rwv1.AgentError{
            Code: "INDICATOR_FINALIZATION_FAILED", Message: message,
        }},
    })
}
if err := a.updateSession(ctx, sessionID, "finished", bars, ""); err != nil {
    return err
}
if err := send(&rwv1.AgentFrame{ReplyTo: frameID}); err != nil {
    return err
}
a.indicatorSync.ForgetSession(ctx, sessionID)
return nil
```

For failed/stopped, attempt final flush but preserve the original status/error. Reject terminal frames missing session ID, frame ID, sender, or platform invoker.

- [ ] **Step 5: Verify GREEN and commit**

```bash
go test -race ./internal/runtimeagent -count=1
git add internal/runtimeagent/agent.go internal/runtimeagent/agent_test.go
git diff --cached --check
git commit -m "fix: finalize sessions after indicator flush"
```

---

### Task 5: Require Python final acknowledgement

**Files:**
- Modify: `strategy-service/strategy_service/worker_agent_client.py`
- Modify: `strategy-service/strategy_service/session_worker_entry.py`
- Modify: `strategy-service/tests/test_worker_agent_client.py`
- Create: `strategy-service/tests/test_session_worker_entry.py`

**Interfaces:**

```python
class FinalStatusRejected(RuntimeError):
    pass

def send_final_status(
    self, *, session_id: str, status: str, bars_processed: int = 0,
    error: str = "", timeout_seconds: float = 35.0,
) -> None:
    frame_id = self._call_id_factory()
    reply: queue.Queue[worker_pb2.AgentFrame] = queue.Queue(maxsize=1)
    with self._pending_reply_lock:
        self._pending_replies[frame_id] = reply
    try:
        self._outbound.put(worker_pb2.WorkerFrame(
            frame_id=frame_id,
            final_status=worker_pb2.FinalStatus(
                session_id=session_id,
                status=status,
                bars_processed=int(bars_processed),
                error=error,
            ),
        ))
        try:
            ack = reply.get(timeout=max(0.01, float(timeout_seconds)))
        except queue.Empty as exc:
            raise TimeoutError(f"timed out waiting for final status ack: {session_id}") from exc
        if ack.WhichOneof("payload") == "error":
            raise FinalStatusRejected(ack.error.message or ack.error.code)
    finally:
        with self._pending_reply_lock:
            self._pending_replies.pop(frame_id, None)
```

- [ ] **Step 1: Read the testing anti-pattern guide**

```bash
cat /Users/xdy/.codex/plugins/cache/openai-curated-remote/superpowers/6.1.1/skills/test-driven-development/testing-anti-patterns.md
```

Use fakes only at the gRPC transport boundary.

- [ ] **Step 2: Write failing client tests**

```python
class _FinalAckStub:
    def __init__(self, *, error: str = ""):
        self.sent = []
        self.final_seen = threading.Event()
        self.allow_ack = threading.Event()
        self.error = error

    def Connect(self, frames):
        for frame in frames:
            self.sent.append(frame)
            if frame.WhichOneof("payload") != "final_status":
                continue
            self.final_seen.set()
            assert self.allow_ack.wait(timeout=1.0)
            if self.error:
                yield worker_pb2.AgentFrame(
                    reply_to=frame.frame_id,
                    error=worker_pb2.AgentError(
                        code="INDICATOR_FINALIZATION_FAILED",
                        message=self.error,
                    ),
                )
            else:
                yield worker_pb2.AgentFrame(reply_to=frame.frame_id)
            return


def test_send_final_status_waits_until_matching_reply_to():
    stub = _FinalAckStub()
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
        call_id_factory=lambda: "final-1",
    )
    client.start()
    for index in range(1440):
        client._outbound.put(worker_pb2.WorkerFrame(
            indicator_frame=worker_pb2.IndicatorFrame(
                session_id="sess-1",
                stream_key="binance:perpetual_futures:TESTUSDT:1m",
                market_time_ms=index * 60_000,
            ),
        ))
    done = threading.Event()
    failure = []

    def send():
        try:
            client.send_final_status(
                session_id="sess-1",
                status="finished",
                bars_processed=1440,
                timeout_seconds=1.0,
            )
        except BaseException as exc:
            failure.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=send)
    thread.start()
    assert stub.final_seen.wait(timeout=1.0)
    assert not done.is_set()
    assert sum(frame.WhichOneof("payload") == "indicator_frame" for frame in stub.sent) == 1440
    stub.allow_ack.set()
    assert done.wait(timeout=1.0)
    thread.join(timeout=1.0)
    client.close()
    assert failure == []


def test_send_final_status_raises_when_agent_returns_error():
    stub = _FinalAckStub(error="indicator finalization failed: database unavailable")
    stub.allow_ack.set()
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
        call_id_factory=lambda: "final-1",
    )
    client.start()
    with pytest.raises(FinalStatusRejected, match="database unavailable"):
        client.send_final_status(
            session_id="sess-1", status="finished", timeout_seconds=1.0,
        )
    client.close()


def test_send_final_status_times_out_without_ack():
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=_FakeWorkerStub([]),
        call_id_factory=lambda: "final-1",
    )
    client.start()
    with pytest.raises(TimeoutError, match="final status ack"):
        client.send_final_status(
            session_id="sess-1", status="finished", timeout_seconds=0.01,
        )
    client.close()
```

The drain test enqueues 1440 indicator frames, starts `send_final_status` in a thread, verifies the fake received all indicators plus FinalStatus, then yields:

```python
worker_pb2.AgentFrame(reply_to=final_frame.frame_id)
```

Only then may the sender finish.

- [ ] **Step 3: Verify RED**

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest tests/test_worker_agent_client.py -q
```

Expected: FAIL because final reply waiters do not exist and close preemptively sets `_closed`.

- [ ] **Step 4: Implement final waiters and graceful close**

Register a reply queue by generated frame ID, enqueue a `WorkerFrame` containing that frame ID and the supplied `FinalStatus`, wait 35 seconds, reject `AgentError`, and remove the waiter in `finally`.

Route non-empty `reply_to` first in `_handle_agent_frame`.

`close()` puts the outbound sentinel before setting `_closed`, waits two seconds for normal iterator shutdown, then sets `_closed`. Remove the early `_closed` break that abandons queued frames.

- [ ] **Step 5: Write failing session worker tests**

```python
class _TerminalServicer:
    def __init__(self, status: str, bars: int, error: str = ""):
        self.status = status
        self.bars = bars
        self.error = error

    def GetStrategyStatus(self, request, context):
        return strategy_pb2.GetStrategyStatusResponse(
            status=self.status,
            bars_processed=self.bars,
            error=self.error,
        )


class _FinalClient:
    def __init__(self, reject: bool = False):
        self.reject = reject
        self.progress = []
        self.final = []

    def send_progress(self, **kwargs):
        self.progress.append(kwargs)

    def send_final_status(self, **kwargs):
        self.final.append(kwargs)
        if self.reject:
            raise FinalStatusRejected("indicator finalization failed: unavailable")


def test_poll_until_terminal_sends_final_status_and_waits_for_ack():
    client = _FinalClient()
    result = _poll_until_terminal(
        _TerminalServicer("finished", 1440),
        client,
        "sess-1",
        6,
        "rt-1",
    )
    assert result == 0
    assert client.progress == []
    assert client.final == [{
        "session_id": "sess-1",
        "status": "finished",
        "bars_processed": 1440,
        "error": "",
        "timeout_seconds": 35.0,
    }]


def test_poll_until_terminal_returns_failure_when_final_status_rejected():
    client = _FinalClient(reject=True)
    assert _poll_until_terminal(
        _TerminalServicer("finished", 1440), client, "sess-1", 6, "rt-1",
    ) == 1
    assert len(client.final) == 1
    assert client.progress == []


def test_poll_until_terminal_preserves_failed_terminal_status():
    client = _FinalClient()
    assert _poll_until_terminal(
        _TerminalServicer("failed", 17, "strategy error"),
        client,
        "sess-1",
        6,
        "rt-1",
    ) == 1
    assert client.final[0]["status"] == "failed"
    assert client.final[0]["error"] == "strategy error"
```

- [ ] **Step 6: Verify RED, implement, verify GREEN**

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest tests/test_session_worker_entry.py -q
```

Expected RED: terminal states still use ordinary progress.

Change `_poll_until_terminal` to send progress only for non-terminal states and call `send_final_status` once for terminal states. A rejected/timeout acknowledgement returns one without sending a second conflicting progress frame.

Then run:

```bash
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev \
  pytest tests/test_worker_agent_client.py tests/test_session_worker_entry.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add strategy_service/worker_agent_client.py strategy_service/session_worker_entry.py \
  tests/test_worker_agent_client.py tests/test_session_worker_entry.py
git diff --cached --check
git commit -m "fix: await final session persistence"
```

---

### Task 6: Start sync loop and coordinate restart cleanup

**Files:**
- Modify: `strategy-service/cmd/runtime-agent/main.go`
- Modify: `strategy-service/cmd/runtime-agent/main_test.go`
- Modify: `strategy-service/internal/runtimeagent/agent.go`
- Modify: `strategy-service/internal/runtimeagent/agent_test.go`

- [ ] **Step 1: Write failing lifecycle tests**

```go
func TestRunAgentStartsIndicatorSyncLoopWithProcessContext(t *testing.T)
func TestAgentRestartWaitsForFlushThenForgetsOldSession(t *testing.T)
func TestAgentRestartDoesNotForgetNewSession(t *testing.T)
```

Block an old-session flush, begin restart, verify cleanup has not raced ahead, release flush, and verify only old state is removed.

- [ ] **Step 2: Verify RED**

```bash
go test ./cmd/runtime-agent ./internal/runtimeagent \
  -run 'TestRunAgentStartsIndicatorSyncLoop|TestAgentRestart' -count=1 -v
```

Expected: FAIL because runAgent does not start the loop and cleanup has no sync-manager coordination.

- [ ] **Step 3: Integrate lifecycle**

Before `runtimeClient.Run(ctx)`:

```go
go agent.RunSyncLoop(ctx)
```

Use the same process context as RuntimeChannel. Coordinate restart cleanup through `IndicatorSyncManager.ForgetSession`.

- [ ] **Step 4: Verify GREEN and commit**

```bash
go test -race ./cmd/runtime-agent ./internal/runtimeagent -count=1
git add cmd/runtime-agent/main.go cmd/runtime-agent/main_test.go \
  internal/runtimeagent/agent.go internal/runtimeagent/agent_test.go
git diff --cached --check
git commit -m "fix: run indicator sync with runtime agent"
```

---

### Task 7: Repository verification and baseline-schema validation

- [ ] **Step 1: Verify core-service**

```bash
cd /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/core-service
go test ./...
go vet ./...
```

Expected: zero exit codes.

- [ ] **Step 2: Verify strategy-service**

```bash
cd /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/strategy-service
PYTHONPATH=.:../strategy-library uv run --frozen --extra dev pytest tests/ -q
go test ./...
go vet ./...
bash scripts/runtime-agent-platform.test.sh
bash scripts/start-bare-runtime-debugpy.test.sh
```

Expected: all pass.

- [ ] **Step 3: Verify builds, baseline migration contract, and OpenSpec**

```bash
cd /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/core-service
go test ./internal/storage/migrations -count=1 -v
go build ./...

cd /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/strategy-service
go build ./...

cd /Users/xdy/Workplace/hushine
openspec validate --all --strict --no-interactive
```

Expected: the tracked baseline migration contract, both cleanup-worktree builds, and strict OpenSpec validation pass. Task 8 performs the separate empty-database startup proof.

- [ ] **Step 4: Verify clean owned histories**

```bash
git -C /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/core-service status --short
git -C /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/strategy-service status --short
git -C /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/core-service log --oneline origin/cleanup/medium-baseline-20260710..HEAD
git -C /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/strategy-service log --oneline origin/cleanup/medium-baseline-20260710..HEAD
```

Expected: clean worktrees and only planned commits.

---

### Task 8: End-to-end acceptance and push

- [ ] **Step 1: Start clean stack and seed 2049 TESTUSDT bars**

Use existing one-shot bootstrap, a positive backtest margin balance, and the cleanup-worktree runtime-agent.

Expected: all services healthy and bare runtime active/routeable.

- [ ] **Step 2: Verify live tail at 1025**

Pause after bar 1025.

Expected:

```text
chunk 0 count=1024 finalized=true
chunk 1 count=1 finalized=false
sum=1025
```

Chart shows the open tail.

- [ ] **Step 3: Verify final 2049 state and duplicate rejection**

Finish 2049 bars and replay the exact last indicator frame once.

Expected:

```text
chunk 0 count=1024 finalized=true
chunk 1 count=1024 finalized=true
chunk 2 count=1 finalized=true
sum=2049
session=finished
bars_processed=2049
```

- [ ] **Step 4: Verify final flush failure**

Fail indicator saves past the acceptance deadline.

Expected: session recoverable, error starts `indicator finalization failed:`, worker exits non-zero, runtime stays active.

- [ ] **Step 5: Repeat ten-minute blocked heartbeat test**

Record every 30 seconds: active runtime, advancing heartbeat, unchanged agent PID, busy worker, zero bars during block.

Expected: no runtime heartbeat loss.

- [ ] **Step 6: Repeat session-only restart**

```bash
scripts/restart-bare-worker-session.sh <old-session-id> \
  --state-file /tmp/hushine-acceptance-bare/runtime.env
```

Expected: old session recoverable, new session ID, unchanged runtime ID and agent PID, old worker gone, edited code loaded, new session finished.

- [ ] **Step 7: Clean temporary state and push**

After fresh verification:

```bash
git -C /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/core-service \
  push origin cleanup/medium-baseline-20260710
git -C /Users/xdy/Workplace/hushine-worktrees/medium-cleanup/strategy-service \
  push origin cleanup/medium-baseline-20260710
```

Confirm each local HEAD equals upstream HEAD.
