# Runtime Indicator Reliable Finalization Design

## Context

The Go runtime-agent and Python session-worker split successfully isolates runtime heartbeat from user strategy execution, but the current indicator path violates the D4 design in three important ways:

1. `WorkerAgentClient` queues one `IndicatorFrame` per bar and closes its daemon gRPC thread after a fixed two-second join. A fast backtest can leave hundreds of frames plus terminal progress in the queue when the worker process exits.
2. `Agent.handleWorkerIndicatorFrame` performs a synchronous platform/database call for every received frame. This makes worker-stream consumption slower than indicator production and creates the queue tail that is dropped at exit.
3. The database UPSERT permits a finalized chunk to be overwritten by stale open data and permits count rollback.

The observed acceptance run processed 1440 bars in the worker, but only 270 and 278 indicator points reached the database in two sessions. Both sessions remained `running` with zero persisted bars because terminal progress was behind the dropped indicator frames.

## Goals

- Preserve runtime heartbeat while user code or a debugger blocks a Python worker.
- Persist open indicator chunks periodically so the chart shows data before 1024 points.
- Seal and persist full 1024-point chunks without modifying them after platform acknowledgement.
- Drain all indicator state before a successfully completed session is persisted as `finished`.
- Mark a nominally completed session `recoverable` when its final indicator flush cannot be confirmed within the finalization deadline.
- Ignore duplicate and out-of-order indicator points.
- Make database chunk updates monotonic and idempotent.
- Preserve session-only restart behavior without restarting the Go agent or runtime.

## Non-Goals

- No frontend API or chart behavior changes.
- No user strategy API changes.
- No database schema migration or table replacement.
- No change to the local strategy source directory.
- No durable local WAL or outbox.
- No log redaction or observability work in this change.
- No removal of protocol, migrations, or history.

## Chosen Approach

Implement the Agent Sync Loop described by the existing D4 design.

The worker calculates indicators and emits current-bar frames. The Go agent owns deduplication, chunking, periodic flush, final flush, retry state, and session finalization. A session worker cannot exit successfully until the agent acknowledges its final status.

A minimal worker-only drain was rejected because it preserves per-frame synchronous database writes and can make worker shutdown proportional to the number of bars. A durable disk outbox was rejected because it adds lifecycle and cleanup complexity that is not required for the current test-stage platform.

## Component Boundaries

### Python session-worker

The Python worker:

- calculates indicator values after each bar callback;
- sends exactly one `IndicatorFrame` for the current bar;
- does not know chunk indexes, chunk counts, the 1024 boundary, database layout, or retry state;
- sends ordinary `SessionProgress` for non-terminal progress;
- sends the existing `FinalStatus` payload with a unique existing `WorkerFrame.frame_id` for a terminal state;
- waits for an `AgentFrame` whose existing `reply_to` matches the final frame ID;
- exits only after the matching acknowledgement or an explicit acknowledgement timeout.

No protobuf field is added. A successful acknowledgement is an `AgentFrame` with `reply_to` populated and no payload. A failed finalization acknowledgement uses the existing `AgentError` payload with the same `reply_to`, code `INDICATOR_FINALIZATION_FAILED`, and the persisted recoverable-session error message.

`WorkerAgentClient.close()` no longer sets the closed event before terminal acknowledgement. Normal successful shutdown occurs only after the final acknowledgement. Exceptional shutdown remains bounded and returns a failure exit code when delivery cannot be confirmed.

### Go runtime-agent

A new focused `IndicatorSyncManager` owns indicator synchronization. `agent.go` remains responsible for routing worker frames, session lifecycle, restart, and platform method selection.

The sync manager owns:

- indicator definitions deduplicated by `(session_id, stream_key, indicator_key)`;
- one `IndicatorBuffer` per `(session_id, stream_key, indicator_key)`;
- dirty open chunk state;
- pending finalized chunks;
- the last accepted `market_time_ms` per indicator;
- a per-session flush mutex;
- immediate flush signals for newly sealed chunks.

The worker gRPC receive path only validates and appends frames to the manager. It does not perform platform I/O.

The agent starts one sync loop with its process context. The loop flushes dirty open chunks every two seconds. When a buffer reaches 1024 points, the manager atomically moves the full chunk to pending finalized state, opens the next chunk, and signals an immediate flush.

The buffer lock is never held while calling the platform.

### Core-service repository

Core-service remains the durable owner of indicator definitions and chunks. The existing UPSERT is tightened; no schema migration is required.

## Normal Data Flow

1. User strategy finishes one bar callback.
2. Worker creates one `IndicatorFrame`.
3. Worker queues the frame on its local gRPC stream.
4. Agent receives the frame in stream order.
5. Agent rejects duplicate or out-of-order indicator points.
6. Agent appends accepted points to the relevant in-memory buffers.
7. Every two seconds, the sync loop snapshots dirty open chunks and pending finalized chunks.
8. Agent calls `portfolio.SaveStrategyIndicators` outside all buffer locks.
9. After platform acknowledgement:
   - acknowledged finalized chunks are removed from pending state;
   - an open chunk is marked clean only if it has not changed since the snapshot.
10. New points can continue entering the next open chunk while a finalized chunk is being saved.

Definitions are buffered and saved with the next chunk flush. Repeated identical definitions do not cause per-bar database writes.

## Chunk State Semantics

During a running session:

- points 1 through 1023 are one open chunk;
- point 1024 atomically seals chunk 0 as finalized and opens chunk 1;
- point 1025 becomes the first point of open chunk 1;
- a finalized chunk remains pending until platform acknowledgement;
- an acknowledged finalized chunk is never mutated again.

At terminal finalization, a non-empty partial open chunk is sealed as finalized because no later bar can modify it.

For a 2049-bar session:

- while running after bar 2049: chunks 0 and 1 are finalized at 1024 points each, and chunk 2 is open with one point;
- after successful finalization: chunk 2 is also finalized with one point.

## Deduplication and Ordering

For each `(session_id, stream_key, indicator_key)`, the manager stores the last accepted `market_time_ms`.

- A greater timestamp is accepted.
- An equal timestamp is a duplicate and is ignored without dirtying the buffer.
- A lower timestamp is out of order, is ignored, and increments an in-memory out-of-order counter for that session.
- Definitions remain idempotently mergeable even when all values in a frame are rejected.

This matches the one-callback-per-stream-per-bar execution model. Same-timestamp corrections are intentionally not supported because accepting them would require random replacement inside immutable chunk arrays.

## Periodic Flush and Concurrency

Only one flush may run for a session at a time. Periodic flush, immediate full-chunk flush, terminal flush, and restart cleanup coordinate through the same per-session flush mutex.

A periodic flush failure:

- does not block strategy execution or order routing;
- keeps dirty open and pending finalized state in memory;
- retries on the next flush interval;
- never removes pending finalized chunks.

Open chunk snapshots carry a generation. Platform acknowledgement marks the open chunk clean only when the generation still matches, preventing an older acknowledgement from clearing newer dirty data.

## Terminal Finalization

The gRPC stream preserves worker frame order. Therefore receipt of `FinalStatus` means all earlier indicator frames have already entered the agent receive handler.

For `finished` or legacy `completed`:

1. Agent acquires the session flush mutex.
2. Agent seals any partial open chunks.
3. Agent flushes all definitions and chunks with retry starting at 100 milliseconds and doubling to a maximum of two seconds until the 30-second finalization deadline.
4. On success, agent persists the session as `finished` with the final `bars_processed`.
5. Agent replies with a matching success acknowledgement.
6. Worker closes its stream and exits zero.
7. Agent removes the session's in-memory sync state.

If final flush cannot be confirmed before the deadline:

1. Agent persists the session as `recoverable` with an error beginning `indicator finalization failed:`.
2. Agent replies with a matching `AgentError`.
3. Worker exits non-zero.
4. Agent retains unsaved sync state until restart cleanup or runtime shutdown.

For a worker-reported `failed` or user-requested `stopped` state, the agent attempts a final indicator flush but preserves the stronger original terminal status and error. Indicator failure does not replace the actual strategy failure or explicit stop reason.

If the agent cannot persist either `finished` or `recoverable`, it does not send a success acknowledgement. The worker times out and exits non-zero, leaving the platform's active-session state available for existing runtime failure recovery.

## Database Monotonic UPSERT

For an existing chunk row, an UPDATE is permitted only when all applicable conditions hold:

- the existing row is not finalized;
- the incoming count is not lower than the existing count;
- and the incoming request either:
  - advances count/end time; or
  - upgrades the same count from open to finalized.

Consequences:

- a finalized row cannot return to open;
- stale smaller chunks cannot overwrite newer chunks;
- an identical open or finalized retry is a no-op and does not refresh `updated_at`;
- an open chunk can become finalized at the same count;
- a newer open snapshot can extend the current open row.

Repository save counts reflect actual inserts or updates, not ignored stale/idempotent rows.

## Session-only Restart

The existing restart sequence remains:

1. stop only the old Python worker;
2. clean old worker and sync state;
3. mark the old session recoverable;
4. start a new worker with a new session ID;
5. load the existing local strategy source.

The runtime ID and Go agent process remain unchanged. Restart cleanup calls `IndicatorSyncManager.ForgetSession` only after any in-flight session flush has completed or been cancelled.

## Error and Timeout Defaults

- Periodic indicator flush interval: 2 seconds.
- Platform request timeout: existing agent request timeout.
- Terminal finalization deadline: 30 seconds.
- Worker final acknowledgement wait: 35 seconds, consisting of the 30-second terminal deadline plus five seconds of transport grace.
- Retry: start at 100 milliseconds, double after each failure, and cap each delay at two seconds without exceeding the finalization deadline.
- No infinite worker shutdown wait.

These durations are injectable in tests through `AgentConfig`; no new user-facing configuration is required in this change.

## Testing Strategy

All production changes use red-green-refactor TDD.

### Python tests

- A slow fake stream receives 1440 indicator frames followed by final status.
- The worker client does not close before a matching acknowledgement.
- A matching `reply_to` releases the final waiter.
- An acknowledgement timeout produces a failure.
- Exceptional close remains bounded.

### Go runtime-agent tests

- 1023, 1024, and 1025 point transitions.
- Duplicate and out-of-order timestamps do not increment count.
- Periodic flush saves only dirty state.
- An acknowledged finalized chunk is removed; a failed write retains it.
- Open generation prevents an older acknowledgement from clearing newer data.
- Periodic and terminal flush do not run concurrently.
- Successful final status flush persists `finished` before acknowledgement.
- Final flush exhaustion persists `recoverable` and returns an error acknowledgement.
- Restart clears only the old session's sync state and starts a new worker.

### Core-service tests

With a real test database:

- finalized cannot regress to open;
- count cannot regress;
- identical UPSERT does not change `updated_at`;
- equal-count open-to-finalized succeeds;
- a newer open chunk extends count and end time;
- the current baseline migration creates all indicator tables from an empty database.

### End-to-end verification

- Run at least 2049 bars and observe two full finalized chunks plus the live open tail.
- After final status, verify all three chunks are finalized and total count is 2049.
- Verify the session persists `finished` with 2049 bars.
- Verify the chart reads the open chunk before 1024 points.
- Run a ten-minute busy-loop strategy and verify runtime heartbeat and agent PID remain stable.
- Execute session-only restart and verify the runtime ID and agent PID do not change.
- Run strategy-service Python tests, Go tests, shell tests, core-service Go tests/vet, build checks, and root OpenSpec strict validation.

## Compatibility and Deployment

The change modifies only strategy-service and core-service.

- No frontend deployment coordination is required.
- No API or protobuf regeneration is required.
- No migration is added.
- Existing baseline schema deployment remains the one-shot database bootstrap path.
- Existing workers that do not send `FinalStatus` continue to use current progress behavior, but successful completion reliability is guaranteed only by workers using the final acknowledgement flow.
- The two repositories are committed independently on `cleanup/medium-baseline-20260710`.
- Full verification is required before either branch is pushed.

## Acceptance Criteria

- A 2049-bar session persists exactly 2049 indicator points with immutable finalized chunks.
- A completed worker cannot exit zero before the agent acknowledges final persistence.
- A final flush failure produces a recoverable session, never a false finished session.
- Duplicate and out-of-order frames do not change persisted counts.
- Runtime heartbeat remains active through a ten-minute blocked worker.
- Session-only restart preserves runtime ID and agent PID.
- Existing user-visible functionality remains unchanged.
- All affected repositories remain buildable from the current one-shot database baseline.
