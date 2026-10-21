# PR14 cumulative fixture repair — focused evidence (21 October 2026)

Base published/source-reviewed head: `219ad7bb8489c19012c8298d1021ddd959861782`.
These are dirty-input focused checks, each with its exact `.patch`, `.log`,
`.rc`, guarded Cargo command record and resource audit. They are not a reset
full acceptance matrix or permission to merge main.

The original full default remains FAILED: 2662 passed / 4 failed / 1 ignored,
129/129 binaries audited; relay 9/0, routed 110/0, docs 0/0; no abort.
`attempt-219ad7bb` retains logs, inventory, exits and priority caveat.

## Passing focused checks

- `focus-backpressure`: 5/5, rc0, 475.28s. Follower 20,480 writes,
  leader refused0, term1, pressure1304 → release2304ms. Singleton24,576
  writes/100,663,296B, typed pressure, pressure1120 → release2120ms.
  Absent follower49,152 writes, refused0, max retained10300/16384,
  five checkpoints. Noop/reserve/larger-WAL recovery passed.
- `focus-held-write-final-setup`: 1/1, rc0. Election noop durably applied
  at baseline1, zero native requests before the real write. Completion held
  for 6000 host polls: Applied index2, term1, held protocol elapsed0ms;
  clock resumed+1ms after terminal collection.
- `focus-native-benchmark-all`: 68/68, rc0, 46.35s. TCP/QUIC lanes,
  exact uneven lane/global counts, partial replica cleanup, fork/WAL and
  reader joins, direct adoption, unused-half ownership, final reclaim and
  rejected-adoption cleanup all passed.
- `focus-retention-latched`: 1/1, rc0, 299.18s. All32,768 exact Applied
  writes/134,217,728B, last32769; first excess-over-interval witness
  (rewrite1,75ms), three rewrites in327ms, quarter waits0/added_ms0.
  max WAL44,070,808/66,060,288 (<75%), retained7582/16384.
  Original strict occupancy assertion and following zero-wait assertion passed.
- The restart/checkpoint/reclaim and explicit opt-out histories passed in
  `focus-retention`; that command overall failed its separate soak assertion
  as detailed below, and is not reported as a passing command.

## Causal controls

- `mutation-old-clock-final-setup`: rc101, expected exact accepted-write
  `Unknown(LeadershipChanged)` assertion. Baseline noop already committed/
  applied1; node1 last2 with physical request pending; node3 leader term2.
  Only clock gating was disabled; repaired positive used identical setup.
- `mutation-rebind-selected-sockets`: rc101, two expected failures at
  required direct-adoption success, TCP and QUIC both AddrInUse/code98 with
  selected socket clone witnesses. This demonstrates kernel bind ownership,
  not an identified original competing PID. No host-noise attribution.
- `mutation-interval-only-latched`: rc101, expected ORIGINAL strict occupancy
  assertion, after ALL32,768 Applied writes/134,217,728B, last32769.
  Three observed rewrite receipts in4000ms; last receipt3000ms;
  first_occupancy=None, quarter waits4/added_ms3676. max WAL35,818,477
  (<75%), retained7582. All earlier assertions passed; the following zero-wait
  assertion was not reached. Interval branch unchanged; only early filter
  disabled. 369.56s. Mutation restored immediately after audit.

## Retained failed/preliminary attempts

- `focus-held-write`: control reached lease cleanup but used handoff of both
  halves; existing strict reclaim correctly rejected it. Fixed control keeps
  unused UDP owned. Subsequent positive passed.
- `focus-adoption`: private re-export visibility compile error; fixed without
  library/public seam. Subsequent all-feature benchmark68/68 passed.
- `mutation-old-clock`: preliminary control had not explicitly required the
  initial noop applied; superseded by tightened identical-setup pair above.
- `mutation-interval-only`: INVALID causal evidence, rc101 at write11,200
  NotProposed(StoragePressure), before all32,768. Original interval branch
  was unchanged; this does not count as the required occupancy mutation.
- `focus-retention`: overall rc101, 2pass/1fail. All32,768 writes completed,
  original strict occupancy premise passed (three rewrites/375ms), but added
  zero-pause assertion failed (three quarter waits/50ms). Refined latch preserves
  normal pressure; its successful run above records zero waits/added ms.

## Ownership and remaining work

Clock reads accepted native WAL/reclaim, snapshot and physical dialing
lifetimes. Existing terminal/error/fence release paths own credit cleanup;
TCP cancelled dial attempts stay counted until actual worker receipts.
No duplicate fixture bookkeeping, client/ACK/missing-node gate or refused
rewrite hold. Singleton/follower exact1000ms refused-rewrite behavior passed.

Private benchmark assembly moves selected sockets through existing library
connector constructors. One strict one-slot lease per replica preserves the
shared helper's exactly-one-adopted-half reclaim contract during partial
startup; unopened leases retain both halves. Explicit endpoint owners retain
unused halves/registry locks through close, joins and final reclaim. No
`src/` production change is intended; temporary mutation must be reverted.

Post-cleanup checks: shared native reconnect1/1 and accepted-write hold1/1
(baseline1 → Applied2, term1, held0ms/resumed+1ms); native benchmark all68/68
and default58/58 after the private alias cleanup. Persistence worker9/9,
snapshot worker14/14, native physical dial cancellation1/1, shared lease9/9
all passed. The duplicate fixture is now loaded once at startup root and
imported by the existing connection-generation caller. A private result alias
names the unchanged endpoint-owning tuple. No lint level/threshold changed.

First seven-strict attempt: FAILED; default/all Clippy rc101 (duplicate_mod,
private tuple type_complexity), other five steps rc0. Helper shell rc0 was
not treated as aggregate success: audited outer runner exited1. Raw input,
seven logs and aggregate audit are preserved in `strict-attempt-1`.

Remaining at report creation: seven strict checks on the CLEAN source commit
carrying this report, enabled hook and checked push to existing PR14;
parent fresh source confirmation; complete final published-head matrix.
Main remains parent-owned and integration lease remains separate.

The original failed matrix and all controls remain recorded; these focused
checks do not qualify the final full matrix. Exact-head strict/hook results
are recorded after this source commit in the PR CURRENT section and the main
`target/claude/research-lv/pr14-final-suites` command/resource audits. The full
matrix must wait for fresh parent-owned independent Sol source confirmation.
