# BatchAccumulator and the Checkpoint Protocol

> **Formal spec:** [`batch-accumulator.allium`](./batch-accumulator.allium) — the authoritative behavioural specification for the BatchAccumulator's checkpoint-aware batching. This document is a narrative companion; where they diverge, the Allium spec is canonical.

## What BatchAccumulator Does

`BatchAccumulator` is a buffering component used by sinks (Postgres, ClickHouse) to collect incoming Arrow `RecordBatch`es and flush them in size- or time-based windows. It sits between the transform layer and the sink's write path:

```mermaid
graph LR
    Source --> Transform --> BA[BatchAccumulator] --> Sink["Sink write (INSERT/UPSERT)"]
```

Without checkpointing, this is straightforward batching. With checkpointing, the accumulator becomes a critical participant in the at-least-once delivery guarantee because **checkpoint markers travel as metadata on the very batches being accumulated**.

---

## How Checkpoint Markers Travel

Checkpoint markers don't use a separate control channel between source and sink. Instead, they piggyback on `RecordBatch` schema metadata:

```
RecordBatch {
    schema.metadata: {
        "checkpoint_messages": "[{\"Marker\":{\"epoch\":{\"0\":42},\"created_at_ms\":1709000000}}]"
    },
    columns: [actual data...]
}
```

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant Src as Source
    participant T as Transform
    participant BA as BatchAccumulator
    participant Sink as Sink

    C->>Src: Marker(epoch_N) via broadcast channel
    Src->>Src: Snapshot read position
    Src->>T: RecordBatch + Marker in schema metadata
    T->>T: Extract marker, run SQL/WASM, re-attach
    T->>BA: RecordBatch + Marker in schema metadata
    BA->>BA: Accumulate (preserve marker)
    BA->>Sink: FlushResult (batches with marker)
    Sink->>Sink: Write data durably
    Sink->>C: Ack(epoch_N)
```

---

## The Problem BatchAccumulator Must Solve

Because markers ride on batch metadata, the accumulator must handle three scenarios correctly:

### 1. Empty batches carrying markers

A SQL `WHERE` clause might filter all rows to zero, but the empty batch still carries a checkpoint marker. If the accumulator discards empty batches (as a naive implementation would), the marker is lost, the sink never acks, and checkpointing stalls forever.

**Solution:** `push()` checks for checkpoint metadata on empty batches. Empty batches with markers are preserved in the accumulation queue; empty batches without markers are discarded.

```mermaid
flowchart TD
    A[push batch] --> B{rows == 0?}
    B -- no --> C[add to queue, increment row count]
    B -- yes --> D{has checkpoint marker?}
    D -- yes --> E[add to queue, row count unchanged]
    D -- no --> F[discard]
```

### 2. Batch splitting must not duplicate markers

When accumulated rows exceed `batch_size`, the accumulator splits a batch: the first N rows go to the current flush, the remainder stays in the queue. Arrow's `slice()` operation shares the underlying schema (including metadata), so both halves would carry the same checkpoint marker. If both halves eventually reach the sink's ack logic, the same epoch gets acked twice.

**Solution:** When splitting, the first slice keeps the checkpoint metadata. The remainder is explicitly stripped via `strip_checkpoint_messages()`. When prior batches exactly filled the window (`rows_needed = 0`), the batch is not sliced at all — it is re-queued whole with its marker intact.

```mermaid
flowchart TD
    A[flush: iterate queue] --> B{batch fits in window?}
    B -- yes --> C[include batch with metadata]
    B -- no --> D{rows_needed > 0?}
    D -- yes --> E["slice first_slice = batch[0..rows_needed] — keeps marker"]
    E --> F["remainder = batch[rows_needed..end] — marker STRIPPED"]
    F --> G[re-queue remainder]
    D -- "no (exact fill)" --> H[batch NOT sliced, re-queue whole with marker intact]
```

### 3. Markers must not be acked before prior data is written

A checkpoint marker on batch N means "all data up to this point has been delivered." If the accumulator returns a marker-bearing batch immediately (before prior accumulated data is flushed), the sink might ack epoch N before writing data from batches 1..N-1.

**Solution:** Empty checkpoint batches are **queued** (added to `accumulated_batches`), not returned immediately. They flush together with any preceding data batches. The sink only sees the marker after all prior data has been included in a flush.

```mermaid
sequenceDiagram
    participant P as Pipeline
    participant BA as BatchAccumulator
    participant Sink as Sink

    P->>BA: push(batch_1: 50 rows)
    Note over BA: queued: [50 rows]
    P->>BA: push(batch_2: 30 rows)
    Note over BA: queued: [50, 30 rows]
    P->>BA: push(empty + Marker(epoch=1))
    Note over BA: queued: [50, 30, empty+Marker]
    Note over BA: Time flush triggers
    BA->>Sink: FlushResult [50, 30, empty+Marker]
    Sink->>Sink: Write 80 rows durably
    Sink->>Sink: Ack epoch 1
```

---

## The At-Least-Once Guarantee Chain

The full guarantee depends on every component in the chain behaving correctly:

```mermaid
flowchart LR
    subgraph "Epoch Lifecycle"
        A["1. Coordinator creates epoch_N\nbroadcasts Marker"] --> B["2. Source snapshots position\n(does NOT commit)"]
        B --> C["3. Source attaches Marker\nto batch metadata"]
        C --> D["4. Transforms propagate\nMarker to output"]
        D --> E["5. BatchAccumulator preserves\nMarker through batching"]
        E --> F["6. Sink extracts Marker\nfrom flushed batches"]
        F --> G["7. Sink writes ALL data\nto durable storage"]
        G --> H["8. Sink sends Ack(epoch_N)"]
        H --> I["9. Coordinator waits\nfor ALL sink acks"]
        I --> J["10. Coordinator broadcasts\nFinalizer(epoch_N)"]
        J --> K["11. Source commits position\nto state backend"]
    end
```

**If any step 5-11 fails:** the epoch times out, no Finalizer is sent, sources never commit, and all data from the failed epoch is reprocessed on restart.

**BatchAccumulator's role (step 5)** is to ensure markers survive the batching layer intact:
- Not lost (empty batch preservation)
- Not duplicated (strip on split)
- Not premature (queue, don't return immediately)

---

## Flush Triggers

The accumulator flushes on two conditions:

| Trigger | Condition | Purpose |
|---------|-----------|---------|
| **Size** | `current_row_count >= batch_size` | Efficient bulk writes |
| **Time** | `elapsed >= batch_flush_interval` | Bounded latency for low-throughput streams |

`AsyncBatchAccumulator` wraps the synchronous accumulator with a `tokio::select!` loop that races incoming batches against a timer tick. On stream completion, it calls `flush_all()` to drain any remaining batches (including trailing checkpoint markers).

---

## Edge Cases

### Large batch exceeding batch_size

A single 500-row batch with `batch_size=100` produces 5 flush cycles. The checkpoint marker appears on exactly the first flush; the remaining 4 are stripped.

### Exact fill

If prior batches exactly fill `batch_size` (e.g., 40 + 60 = 100), the next batch with a checkpoint marker starts a fresh accumulation window. The marker is preserved on that batch and flushed in the next cycle.

### Multiple markers from different epochs

Each marker is attached to its own batch. The accumulator preserves ordering. When flushed together, the sink processes each marker independently, sending separate acks for each epoch.

### Idle stream

When no data arrives but the stream is still open, the timer tick fires `flush()` which returns empty. The `AsyncBatchAccumulator` calls the output function with an empty batch list, allowing sinks to perform heartbeat-like operations (e.g., ack checkpoint markers that arrived on earlier empty batches).
