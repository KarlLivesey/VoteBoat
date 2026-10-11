# Short writes and uncertain manifest publication

Extend the existing actual-core/native WAL-model schedule with bounded append
cuts and failures before/after manifest replacement. The ordinary path retains
persist_effect; delayed admission uses the same public append/admitted contract.
Only a declared actual Uncertain("short write") is handled as an append fault.
Unexpected errors remain loud. Failed barriers share the existing ownership and
fencing path; neither publication outcome manufactures a DurableLog.

Four new checks cover:

- Seven measured vote-frame cuts: zero bytes, one byte, partial/full header,
  partial payload, missing final byte and a complete frame followed by error.
  Both ordinary/delayed admission and process/power-loss modes are tested,
  totaling28 cases. No vote effects escape. Partial tails are discarded;
  complete retained frames can survive process crash and be synchronized by
  recovery, while unsynced power-loss frames disappear.
- Manifest failure before or after actual replacement. Device bytes confirm
  data was synchronized; manifest bytes distinguish the two cuts. Both fence
  store/core, withhold vote requests and recover the durable vote in either
  crash mode, totaling four cases.
- Ambiguous commit publication across both cuts/crash modes. The old core emits
  no committed-command effect; Counter remains0 until recovery resolves the
  synchronized commit. Original
  operation9911/delta9 retries and all three replicas' cold reopen retain value9
  with one application effect, including duplicate consensus entries.
- A [29-action schedule](composed.vbsim) combines short write, both publication
  failures, restarts, Counter proposal and real stale completion delivery, with
  identical replayed states and exact failure counts.

| Scope | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| Replay/codec/reducer checks |17/0|17/0|
| Focused composed-schedule capture |1/0|1/0|
| Complete Raft target, all features |58/0|58/0|
| Complete Raft target, no default features |29/0|29/0|
| Complete Raft target, native only |58/0|58/0|
| Complete Raft target, default |58/0|58/0|

Formatting and all four strict Clippy profiles have zero diagnostics on both
hosts. All660 Rust inputs verify before/after the full-target runs. Profiles
execute sequentially; live lifecycle sources stay separate and immutable.
Original263–265 checks remain. Shared Counter/drain helpers are reused rather
than adding another application or recovery controller. No production,
dependency, public-contract or protocol changes. Linux stable1.98.1 and Mac
stable1.98.0 retain their existing conditions.

The parallel Linux passing log interleaves one trace with test status output.
Its result remains17/0; focused single-test captures on both hosts supply the
exact saved schedule. No product logging change or unrelated rerun is inferred.

This is finite ordinary-majority in-memory device-model evidence, using the
actual native store/core and public Counter StateMachine. Local Counter reads
are applied-state checks. It is not a physical disk-failure, full-platform,
snapshot, timer, reference-Raft or general worker-composition certificate.
Raw.sha256 hashes captured bytes; published.sha256 hashes path-normalized text
without trailing whitespace.
