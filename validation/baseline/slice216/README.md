# Slice216 — same-read QUIC FIN progress and exact checkpoint setup

Source base: f1c18d4. The production change observes an already-known stream FIN
with a zero-length ordered read before finalization. It releases receiving state
and the existing single-stream credit without consuming more plaintext, adding
socket I/O, increasing buffers/streams or changing acknowledged flush semantics.
The protocol engine remains pinned to quinn-proto0.11.19. Raft/default QUIC
timers, ACK configuration, credentials and durability evidence remain unchanged.

The finite3-chunk profile has6ms caller polls and the original50ms default
heartbeat interval. Before the change all48 bytes arrive but the final chunk
remains unflushed at48ms. The identical profile afterward is flushed at48ms.
The12-chunk/1ms/150ms profile passes both versions (46ms before,35ms afterward).
These selected schedules are not wall-clock/network latency promises, measured
macOS inter-poll intervals or a P7 performance result.

The initial test incorrectly assumed flush guaranteed new stream credit; retain
`chunk-before.log`. The corrected caller handles WouldBlock under the unchanged
deadline (`chunk-window-before.log`). `chunk-cadence-before.log` retains the
actual failing coarse schedule. Requesting immediate ACKs does not fix that
failure (`chunk-candidate.log`); the removed candidate is preserved in
`rejected-ack-candidate.patch`. No ACK tuning is installed. `chunk-fin.log`
records both passing final schedules. `quic-all.log` has all12 session tests
passing, including partial reads, packet loss, close, original batch credits
and actual three-replica Raft messages. `connector-all.log` has all15 shared
connector/discovery/lifetime tests passing.

`counter-all.log` retains the first full run:161 pass/1 replacement-drain failure.
The imported checkpoint contains a leadership record but not its required
Completed phase. The setup accepted any positive checkpoint base, which need
not cover the quorum-observed completion or source1's committed prefix.
The fixture now captures the exact original Completed index, waits for local
commitment and requires checkpoint_base at least that index before shutdown and
offline import. Its10s checkpoint deadline and imported Completed assertion stay.
A prior source1 image is explicitly present. `checkpoint-floor-before.log`
passes2 after introducing that older image; this selected run does not reproduce
the full-run failure. `checkpoint-floor-final.log` passes2 with the exact floor.
Do not describe either as an old-code deterministic failure reproduction.
`counter-final.log` retains a different final161/1 failure: drain_retirement's
setup receives explicit UNKNOWN initial drain admission/no matching durable
local record, with the instruction to rerun the same sequence and operation.
The exact-floor replacement test and affected shared-group histories pass in
that run. There is no complete counter-service pass claim or unchanged rerun.

`directory-all.log`/`transfer-all.log` were executed concurrently with separate
fixtures and identical all-feature binaries, unlike the CI sequential target
order. They finish18/1 and20/1: QUIC directory peer rotation has a manifest lookup
deadline; QUIC transfer peer rotation has Unknown(LeadershipChanged) on an initial
write. These are real pressure observations. They are not attributed to host
load or the FIN correction without a matching failure trace. Matching sequential
checks run once after both original handles finish; success there would not
erase these failures or certify resource isolation under concurrent workloads.
`directory-sequential.log` passes19; `transfer-sequential.log` passes21, including
actual phase cuts, lost receipts, merge retirement and peer-rotation recovery.
`chunk-final.log` passes the final2 accurately named assertions; the first name
was corrected to describe acknowledged flush, without changing the schedule.
These results do not turn the failed final counter target into a successful run.
`checkpoint-floor-default.log` passes1 default TCP history, executed after all
all-feature process-spawning targets are terminal.

Commands use `cargo +stable test --locked --offline` with `--all-features`
and `--test quic`, `quic_connect`, `counter_service`, `directory_service` or
`transfer_service`. Focused selections use `chunk_progress` or `drain_replacement`;
the final TCP check omits `--all-features`. Native socket checks run with socket
permission. All before/candidate/final results are separate files. Metadata
checks are `node validation/check-inventory.mjs`,
`node validation/check-provider-conformance.mjs` and
`node validation/check-provider-conformance.test.mjs`; they check references,
not provider correctness. Source/artifact hashes preserve this bounded scope.
`checks.log` records the enabled `.githooks/pre-push`: formatting and strict
default/all-feature/core-only/native-only Clippy all pass with zero diagnostics.
`inventory.log` checks108 contract paths; provider ledger metadata checks pass18
and still report7 reviewed contracts/45 operations,101 contracts unreviewed.

Both preceding-source operator runs are terminal. At a25e5f7, Ubuntu passes
counter162/directory19/transfer21, while macOS counter152/10 fails. At f1c18d4,
Ubuntu passes162/19/21 while macOS counter150/12 fails. Later macOS targets were
not run. Their complete logs and status JSON are retained. These results do not
validate the current FIN change or close supported-platform acceptance.

The schema, dependencies, revised hypothesis and remaining work are in
docs/IMPLEMENTATION.md. Broader security review stays with the user's Daybreak
run. Full P0–P7 and the original performance/platform gates remain active.
