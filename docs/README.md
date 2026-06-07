# Docs

This directory contains design docs, formal specs, and related notes.

For behavior we want to preserve precisely, we track it in two forms:

- [**Allium specs**](https://juxt.github.io/allium/) — canonical, machine-checkable behavior
- **Markdown docs** — short human-readable explanations of the same behavior

## What is here

### Checkpointing protocol

Formal spec and narrative deep dive for the checkpointing protocol.

Covers epoch lifecycle, multi-source and multi-sink coordination, back pressure, timeout, failure modes, recovery, and the at-least-once guarantee.

- `checkpointing.allium` — formal spec
- `checkpointing-deep-dive.md` — narrative deep dive

### Batch accumulator

Formal spec and narrative doc for `BatchAccumulator`'s checkpoint-aware batching behavior.

Covers accumulation, flush triggers (size and time), marker preservation through splits, deduplication on split, exact-fill behavior, marker ordering, stream-completion drain, and the sink flush contract.

- `batch-accumulator.allium` — formal spec
- `batch-accumulator-checkpointing.md` — narrative doc with diagrams

### Hybrid source state

Formal spec for the hybrid source's phase state machine and restore protocol.

Covers the bounded-then-unbounded phase progression, the three independent kinds of persisted state (hybrid phase, bounded resume cursor, unbounded partition offsets) and their independence on recovery, the restore protocol that disambiguates missing hybrid state between FM1, FM6, and a true first run (recovering to unbounded if offsets are complete, else the highest-indexed bounded phase with persisted state, else phase 0), the state-key uniqueness contract that makes per-phase recovery meaningful, and the WARN / ERROR logging obligations on the read and write paths. Includes seven failure modes with production forensics and the invariants that hold across them.

- `hybrid-source-state.allium` — formal spec

## How to keep this up to date

When behavior changes:

1. Update the relevant Allium spec.
2. Re-run validation with `allium check`.
3. Update the matching Markdown doc so the narrative stays aligned.
4. If implementation and spec drift apart, use the **Allium distill** skill to capture the drift and refresh the spec from the code.
5. If there is disagreement, treat the **Allium spec as canonical**.

Keep docs short. They should explain the model, invariants, and edge cases without repeating the implementation line by line.

## How these specs were produced

These specs and docs were generated with LLM using the **Allium distill** skill after reading the checkpoint coordinator, broadcast channels, source and sink connectors, and batch accumulator code, then validated with `allium check`.

For Allium guidance and examples, see:

- <https://juxt.github.io/allium/>
