# [copied from target/claude/research-ci/pr4-close-root-1ac1cfc1.md; raw logs stay outside the tree]

# PR 4 close root: `Network(Session(Closed))` at `cohort/close/progress.rs:126`

## Conclusion

**Parent reproduces (timing class).** On the matrix's own E3 setup the root appeared on neither side: parent 0/40, PR 0/40 (10 whole-binary runs plus 30 `--exact` runs per side). Under a tighter CPU squeeze, which makes the test run slowly as it did in the matrix's PR E3 rep 3, the parent shows the same root at the same `file:line:col`, from the same caller line, and at a similar or higher rate: **parent 17/31 vs PR 14/31** over all squeeze runs. At the heaviest squeeze both sides fail 13/13. At the borderline squeeze it is parent 4/12 vs PR 1/12.

The failing test's code path does not include any code PR 4 changed. The single matrix occurrence is a slow-run timing failure, not a PR 4 defect.

## Test name
`cohort::close::rejection::shutdown_rejection_retains_original_cancellation_before_later_provider_fault` in the `logical_activation_driver` binary. It was the only non-poison root in the matrix log `proof-pr4-final-logs/all-logical_activation_driver-E3-3-pr-1.log`. The other 23 failures in that run were `PoisonError` cascades from `HISTORY.lock().unwrap()`. Matrix record: PR, E3 rep 3, 7 passed / 24 failed, wall 62.9 s, host 1-minute load **62.07**. The other five E3 runs of this binary had loads of 16–33.

## Revisions and builds
- Parent: `c61a2ba6` (the exact matrix parent), built from `git archive c61a2ba6 | tar -x -C target/parent-src` with `CARGO_TARGET_DIR=target/parent`.
- PR: `origin/ci/tr1b-ci-ports-accept` = `1ac1cfc1` (detached checkout), built in the worktree `target/`.
- Both builds: `CARGO_INCREMENTAL=0 taskset -c 0-3 nice -n 19 cargo +stable test --locked --offline --all-features --no-run -j 2 --test logical_activation_driver`. Both exited 0, and the executables differ (`cmp`). I built only this one test target; it is the same executable the full `--no-run` produces for it. Build logs: `pr4-close-root-logs/build-*.log`.
- The matrix ran PR `03127ec5`. `03127ec5..1ac1cfc1` changes only `tests/support/ports.rs`, three `channels.rs` files of other binaries, and docs/validation. None of them is on this test's path (see below).

## Causal reading of the code

### What PR 4 changes in this binary
`git diff c61a2ba6 1ac1cfc1 -- src` touches only `src/bin/support/route_discovery/tests.rs` (unit tests of a different binary). `tests/logical_activation_driver/` and `tests/logical_activation_driver.rs` are unchanged. The binary does include changed files through `#[path]`/`mod`:
- `tests/logical_voter_native/cluster.rs`, mounted as `cluster`. `Cluster::new` now takes ports from `support::ports::PortLease` (cross-process lock files, ports outside the ephemeral range), where it used to probe from a 20000+ counter. It also gained a `serial()` helper.
- `tests/support/mod.rs` now compiles `ports` and `serial`.

So the earlier matrix note that the PR does not touch the driver's test files is true for its directory. It is incomplete for the binary, because `cluster.rs` is compiled into it.

### The failing test does not run any changed code
- It builds its children with `cohort::members::Members::new` (`cohort/members.rs:44–60`, unchanged). That function binds `127.0.0.1:0` itself and never calls `Cluster::new`. The only thing it uses from `cluster.rs` is the `HISTORY` static (`rejection.rs:28`, same on both sides) and the `ChildNode` type alias.
- The parent-level sessions come from `cohort/channels.rs` (unchanged), on `127.0.0.1:0` listeners.
- Every test that calls `Cluster::new` (`history.rs`, `bindings.rs`, `readiness.rs`, `read_intake.rs`, `intake.rs`, via `logical_voter_native/drive.rs`) holds `HISTORY` for its whole body. None can run alongside this test, and a `PortLease` lives inside its `Cluster`, so it is gone before the lock is released. The two `preflight` tests that skip `HISTORY` are unchanged and do not use `Cluster`.
- Every library file on the failure path is unchanged: `src/native/tls.rs`, `src/logical_voter/network/poll.rs`, `src/logical_voter/node/{lifecycle,network}.rs`, `src/runtime/replica.rs`.

### How the error arises (same source on both sides)
1. Three logical parents (`sender`, `opposite`, `idle`) are joined by real TLS-over-TCP sessions. Their Raft timers are `heartbeat 250 ms, election 3000 + up to 2000 ms` (`cohort/groups.rs:134–141`), driven by `MonoTime` from a real `Instant`.
2. `progress::hold`: `sender` campaigns, and its 0→1 send is gated to 7 accepted bytes. `opposite` and `idle` are not polled.
3. `close_peer(b, opposite)` (`rejection.rs:52`): `opposite` goes Quiescing → Draining. `finish_draining` closes its sessions (`lifecycle.rs:111–120`), which sends TLS close_notify to `sender` and `idle`.
4. `close_peer(c, idle)` (`rejection.rs:53`): `idle` polls with `quiet_budget()` (plaintext `read_bytes: 0`), but TLS records are still read. Its session to `opposite` processes the close_notify (`tls.rs:562`), sets `peer_closed` and closes itself (`tls.rs:596`). If `idle` has a logical frame to `opposite` in flight at that point, `LogicalPeerSession::write` calls `write_plaintext` and gets `SessionError::Closed` (`tls.rs:624–633`). `poll.rs:138` turns that into `Network(Session(Closed))`, the normal-mode `poll_network` returns it (`node/network.rs:60`), and the `unwrap()` at `progress.rs:126:51` panics.
5. The source of that in-flight frame: closing intake keeps "peer/control input and accepted work" running (`replica.rs:956`), so timers still fire while Quiescing. `idle` is a follower that has never heard from a leader. Once its 3–5 s election deadline (counted from parent construction) passes before it drains, it campaigns and queues a RequestVote to `opposite`, whose session is already closed. The only other timer in `tls.rs` is the handshake deadline, which gives a different error.

Steps 1–4 and the caller are observed. A `RUST_BACKTRACE=1` run on each side puts the panic at `rejection.rs:53:5`, the **second** `close_peer` (`idle`), on both parent and PR (`pr4-close-root-logs/squeeze-bt-01-{parent,pr}.log`). Step 5, the election timer, is inferred from the code and from how the failures track elapsed time (below). It was not instrumented, because this task changes no source.

What the reading predicts: an unloaded run (about 0.2 s) can never fail. Failure needs roughly ≥3 s from parent construction to `idle`'s close, a slowdown of more than 15×. That is consistent with the one matrix hit coming from the run with a host load of 62 (cores 0–3 share SMT with 8–11, which the other suite was using).

## Environment
- **E3, matching the matrix:** three owned burners `taskset -c N sh -c 'while :; do :; done'` for N = 1, 2, 3. They inherit the runner shell's nice −4, as in the matrix runner. They are started before and killed and reaped after **every** run. The test runs as `taskset -c 0-3 nice -n 19 BIN [args]` (absolute nice 15, as in the matrix), with cwd at the revision's source root. Runs were interleaved parent, PR, parent, … and every run was recorded with no retries.
- **Squeeze (supplementary, not E3):** the same burners, all pinned to **cpu 1** (1, 2 or 3 of them), and the test as `taskset -c 1 nice -n N BIN <test> --exact --nocapture`. The purpose is to stretch the test's wall time on purpose and see which side fails. Calibration levels were test nice 10/14/19 with 1 burner, then nice 19 with 2 or 3 burners.
- Sampled processes: affinity `0-3` (E3) or `1` (squeeze). Absolute nice was 15 in every E3 sample taken after exec, or 6/10/15 for the squeeze calibrations. Eight E3 samples read −4: that is a race in my sampler, which read `/proc/PID/stat` before `nice` had exec'd and `comm` after. The command line always ran `nice -n 19` from the −4 shell. Burner nice was −4 throughout.
- Runner: `pr4-close-root-logs/drive.py.txt`. It ran inside this session under `taskset -c 0-3`; nothing was detached.

## Results

| phase | side | runs | close root | target test reached a real result (not poisoned) |
|---|---|---|---|---|
| E3 whole binary | parent | 10 | **0** | 9 |
| E3 whole binary | PR | 10 | **0** | 9 |
| E3 `--exact` | parent | 30 | **0** | 30 |
| E3 `--exact` | PR | 30 | **0** | 30 |
| squeeze 1 burner, nice 10/14/19 (2 each) | parent | 6 | 0 | 6 |
| squeeze 1 burner, nice 10/14/19 (2 each) | PR | 6 | 0 | 6 |
| squeeze 2 burners (cal 2 + series 10) | parent | 12 | **4** | 12 |
| squeeze 2 burners (cal 2 + series 10) | PR | 12 | **1** | 12 |
| squeeze 3 burners (cal 2 + series 10 + backtrace 1) | parent | 13 | **13** | 13 |
| squeeze 3 burners (cal 2 + series 10 + backtrace 1) | PR | 13 | **13** | 13 |

Totals: **E3: parent 0/40, PR 0/40. Squeeze: parent 17/31, PR 14/31.**

Wall time against outcome (single-test runs): 0.2 s alone in E3, always passes. About 0.6/1.4/5.6 s with one burner, always passes. 12–27 s with two burners: some runs fail. Failures took 16.2–26.5 s (parent) and 23.4 s (PR). Passes took 11.7–17.3 s, so the two overlap only around 16–17 s, and every run of 14.4 s or less passed. 27–54 s with three burners: always fails. Every failure is `progress.rs:126:51 Network(Session(Closed))`, right after the same `332 close held original bytes=Observation { accepted: 7, offered: 605, … } outbound=OutboundUsage { batches: 2, messages: 2, bytes: 6904 }` line the matrix log shows.

The full per-run table (side, run, exit, wall, passed/failed, target-test status, every distinct non-poison root, poison count, log) is in `pr4-close-root-logs/table.md`. Raw logs are in `pr4-close-root-logs/<phase>-<rep>-<side>.log`, with records in `*-records.jsonl`.

### Other E3 whole-binary roots
These are recorded for completeness; none of them is the close root.
- `learners.rs:95:5` `original bridge usage LogicalBridgeUsage { actions: 0, … }`: parent runs 1 and 2, PR run 2. This is the parent-seen phase root from the matrix.
- `cohort/members.rs:56:70` `AddrInUse`: parent runs 3, 4, 5, 6 and 7, PR run 8. This is the driver's own unchanged probe/drop bind: TCP `127.0.0.1:0`, then UDP on the same port, then the sockets are dropped and rebound later. It is not `Cluster::new` and not PR 4 code. It is a leftover port race on both sides that TR1b did not convert. Reviewers may want it tracked separately.
- In 2 runs (parent 7, PR 8) the target test was poisoned before it ran.
- Whole-binary wall times: parent 0.2–46.8 s, PR 6.5–43.5 s. The short runs ended early through AddrInUse plus poison.

## Evidence summary for the reviewers
- The root's code path (test, fixture and library) is byte-identical across parent and PR. The one changed fixture in the binary (`Cluster::new` port leases) is never reached by this test, and it cannot overlap with it because of `HISTORY`.
- The parent reproduces the identical root (`progress.rs:126:51`, `Network(Session(Closed))`, caller `rejection.rs:53:5`) whenever the test runs slowly enough, at a rate no lower than the PR's.
- E3 on today's host (load 5–8) does not slow the test enough on either side (0/40 each). The matrix hit came from the PR E3 run with load 62.
- If a fix is wanted, it belongs to TR2 real-clock pacing, not TR1b. One option is a `close_peer` that tolerates `Session(Closed)` the way `fail_running` already does. Another is a long or fixed election timeout for the non-sender parents in this test. I did not change anything; this is a suggestion only.

## Host
- `nproc` 16; AMD Ryzen 7 9800X3D. Logical cpus 0–3 have SMT siblings 8–11. Kernel `Linux core-karl 7.2.10-1-cachyos #1 SMP PREEMPT_DYNAMIC Wed, 14 Oct 2026 07:02:02 +0000 x86_64`; rustc 1.98.1 (48a229cea 2026-09-01). `ip_local_port_range` 32768–60999.
- Cores 4–15 were used by another suite and builder throughout; this work used only cores 0–3.
- Load: before builds `04:34:44 load 4.13, 4.10, 4.47`. Before the first E3 run `04:39:03 load 5.78, 5.47, 4.93`. After the E3 phases `04:49:28 load 5.30, 6.06, 5.64`. After the last squeeze run `05:12:57 load 6.53, 6.97, 6.81`. Final `05:13:55 load 5.03, 6.42, 6.63`. Date 2026-10-19 (+01:00).
