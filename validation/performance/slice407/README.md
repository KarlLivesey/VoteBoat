# P9 Core proposal batching

Adds bounded direct Core embedding Raft::propose_batch:1..64 original commands in
one ordered suffix/persistence transition. Every operation ID/payload survives;
normal replication/commit/application produces original per-command outcomes.
The existing Event::Propose singleton path retains its original admission limits,
including custom small storage-batch limits and spare payload capacity. No new
Event wildcard, wire format, durability shortcut, authentication or quorum change.
The batch-only limit explicitly charges owned input + staged entry metadata;
selected codecs retain exact independent frame validation. Host application
admission/effect/result reservation duties remain explicit.

**Node/ClientRouter/native-service ready batching and early disk/replication overlap
are not implemented by this slice.** This enables real Core batching without
silently routing many invocations through a singular admission/result record.

## Validation

Linux focused all-feature417/0: library282, native log12, provider29, Raft79,
shared barriers8, framework7. Core-only259/0: library223, Raft29, framework7.
Formatting and all four strict all-target Clippy configurations: zero. Mac frozen
1319-input qualification is terminal442/0, with formatting, all four strict
Clippy configurations and before/after source guards clean. [Mac records](mac/README.md).

## Three interleaved paired trials per cell

Same binary,4096 useful empty commands,1024 singleton warmups,3 actual cores,
exact modeled log tickets, function-call message delivery, no-op applied state.
Four-entry requests finish only after all four original results; missing or
duplicate members cannot release the client. Complete all-replica prefix5121 is
checked. API request batching and logical client concurrency vary independently.
Copied executable/library and source hashes match before/after.

| Clients | Median batch1 ops/s | Median batch4 ops/s | Median paired rate gain | batch1 / batch4 p99 request latency |
| --- | ---: | ---: | ---: | --- |
| 1 | 3,908 | 14,641 | 3.78x | 0.516 / 0.580ms |
| 64 | 14,808 | 45,549 | 3.07x | 7.308 / 8.377ms |
| 256 | 13,690 | 43,917 | 3.21x | 29.359 / 30.887ms |
| 1024 | 14,572 | 41,199 | 2.83x | 103.986 / 99.031ms |

At one client persistence transitions/op fall from approximately6 to1.5.
Under64+ clients existing replication/commit coalescing also amortizes singleton
work; batching further lowers observed transitions/op. Throughput counts original
commands, not four times a single receipt. Request latency ends at every original
result in that request; batch4 latency is not one-member early visibility.

**Unmatched, short, volatile diagnostic; no OpenRaft floor or durable-application
win is claimed.** Single cooperative scheduler and no checkpoint/purge differ
from the pinned upstream benchmark. Linux AMD9800X3D,16 logical CPUs, releaseopt3,
assertions/overflow checks enabled. No builds/tests overlapped trials; desktop/cache
conditions uncontrolled. These larger fixed histories expose remainingO(history)
copying/scanning: rates are lower than406's512-command/64-warmup cells, which are
not comparable speedup baselines. Raw counts/timings/latencies and every trial remain.

Next: native ready collection and bounded disk/replication overlap with exact
ownership; cached derived state and validated log deltas remove the remaining
history-sized work. No arbitrary feature/security removal or Cargo-only tuning.
