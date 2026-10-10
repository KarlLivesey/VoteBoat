# Slice185: joint source membership with split recovery

Source base: 49e2d0d plus this slice. Linux, native local files and loopback
TCP/TLS or QUIC. This is selected composition evidence, not a release certificate.

| Command | Result / file |
| --- | --- |
| `cargo +stable test --locked --offline --all-features --test routed split::membership -- --nocapture` | Four pass; `joint-split.log` retains phase traces |
| `cargo +stable test --locked --offline --all-features --test routed tcp_split_resumes_every_phase_from_wal_status_without_dual_owners` | Existing static split history passes; `static-split-regression.log` |
| `cargo +stable test --locked --offline --all-features --test native_member_startup authorized_native_promotion_lost_receipt_resumption_and_file_recovery_quic` | Isolated Linux reproduction attempt passes; `linux-promotion.log` |
| `cargo +stable test --locked --offline --all-features --test native_member_startup quic_promoted_leader_loss_requires_retained_restored_witness` | Isolated Linux reproduction attempt passes; `linux-shutdown.log` |
| `cargo +stable test --locked --offline --all-features --test native_member_startup` | All50 pass on Linux; `linux-member-suite.log`; suite concurrency did not reproduce the macOS failures |
| `cargo +stable fmt --all -- --check` | Exit0; `fmt.log` |
| All-target strict Clippy, default / all features / no default features | All exit0; `clippy-*.log` |
| `sh .githooks/pre-push` | Formatting and all three strict profiles pass; `hook.log` |
| `node validation/check-inventory.mjs` | 95 existing contracts and conformance paths pass; `inventory.log` |

Each new history starts from a real static three-replica split, stages two
non-serving targets and reopens source group20 with explicit member startup.
It commits joint configuration2 (voters1/2/3 to2/3, retaining1 as learner) and the
source fence, then aborts all source owners while the configuration result is
unread. Cleanup verifies the original committed result and drains transport and
native workers without resuming the failed core. WAL/joint-checkpoint recovery
retains the same operation, fence and exported bytes; the old volatile ticket is
rejected. Import provenance uses observed configuration2. Metadata publication
and activation preserve source fencing and reject premature target service.
Durable status resumes final configuration3, which is also aborted/recovered.
Every source replica stays fenced, and replica1 is non-voting. Child writes,
original retries and exactly two outbox effects survive target reopen with
source and metadata stopped. The test does not change production protocols.

An initial cleanup helper used ordinary peer polling after abort; the existing
fenced-owner guard rejected it. Cleanup now uses `PeerDriver::drain`. An initial
attempt to reapply the old joint record as a fresh proposal was correctly refused
by the configuration admission journal (`rejected-raw-reapply.log`). Recovery now
uses the existing `configuration_status`/`resume_configuration` contract rather
than weakening expected-configuration checks.

The completed macOS job
[114126028727](https://github.com/KarlLivesey/VoteBoat/actions/runs/38022391223/job/114126028727)
at older source1b460ef reported a worker-fencing error during QUIC promotion and
a shutdown timeout during a promoted-leader-loss history. Its exact failure
excerpt is `macos-1b460ef-failures.log`. Both isolated Linux cases and the full50-test Linux target pass; that does
not establish the cause or a macOS fix. No deadline or safety check was relaxed.
The older pre175 full suite is still separate historical work, not validation
of this revision. It has not been restarted because observation was slow.

Still open: broader simultaneous/new-voter/revocation/rollback schedules,
physical power-loss/provider assumptions, current macOS/separate-host evidence,
deployment integration and the original P7 performance gates.
