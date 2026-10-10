# Slice229 — rejected staging-preparation scheduling candidate

The native candidate prepared the unselected MANIFEST.tmp before full WAL sync,
then performed full manifest-file sync, atomic rename and full directory sync.
It changed scheduling, not file formats, ticket validation or publication rules.
JournalIo gained a default sequential combined operation in the trial; native
create/recover/barrier used it and host implementations kept their defaults.
No worker, runtime, dependency or second authoritative store was added.

The candidate is **rejected** and production code, tests and contract inventory
are restored exactly to base revision9a9f3dcb2d12dbbdcd23561c00e9378f590bfa3d.
The combined API is not an available public feature. `candidate.patch` and exact
`candidate/` source files preserve the measured trial and its tests; applying
that patch to the base reproduces the candidate. The source/binary hashes and
reference context bind what was measured, not the subsequently restored build.

| Uninstrumented original workload | Applied ops/s | p99 | Fixed250ms gate |
| --- | ---: | ---: | --- |
| Slice222 baseline | 7.890 | 471.057ms | Failed |
| Slice229 candidate | 6.253 | 681.979ms | Failed |

The candidate run completes all256 measured receipts after64 warmup, with320
recovered on all replicas, original retries, reads and full worker joins. Raw
receipt arithmetic passes; the unchanged gate exits1 with passed=false. TCP/TLS,
startup assembly, one group/window1,8-byte commands, original heartbeat/election
timers and full synchronization remain. No instrumented run replaces the result.
Different shared-host conditions prevent a causal speedup/regression claim; this
run does not justify accepting the candidate or demonstrate the required budget.

Validation at the candidate source:

- `file-cuts.log`:5 native tests pass, covering the existing41 primitive cuts
  plus18 combined cuts. Initial/legacy/reclaimed selections retain acknowledged
  hard state, committed command bytes, exclusive lock and recovery session rules.
  Missing initial selection refuses; late uncertain selection acknowledges nothing.
  These are process-level interruption checks, not hardware power-loss proof.
- `storage-all.log`: combined downstream3, native timing3, reclaim8, log-store9
  and shared provider22 pass. Default order short-circuits failed sync; a downstream
  override is actually reached by create/barrier/recovery. Failed calls fence,
  release no receipt and recover actual synced bytes. Original ticket scope,
  suffix/corruption/reclaim cases remain. `storage-native.log`:3/3/8/9 pass.
- `consensus-all.log`: Raft41 and shared-barrier8 pass. `strict-candidate.log`:
  formatting and all four strict all-target Clippy profiles finish zero.
- `services-all.log`: counter181/directory22 pass; transfer19/2 fails during
  startup. The phase history's original group20 add2/key200/delta11 returns exact
  UNKNOWN LeadershipChanged; the interruption history's one-pass metadata leader
  lookup finds none. This is not a recovered-data mismatch. Both original tests
  then pass independently (`transfer-quic-cuts-selected.log`:1, ten actual
  checkpoint phases/lost fence+publication replies; `transfer-quic-interrupt-selected.log`:
 1, actual interrupted read/split/restart/retries/joins). These passes retain the
  broad failures; they do not fix the startup assumptions or certify concurrency.
- The release build overlaps part of the broad suite, not the selected tests or
  benchmark. Before measurement, all local build/test handles are terminal and
  host inspection finds no matching cargo/rustc/native test/service/benchmark.
  Three logical replicas share one Btrfs/Sabrent NVMe/AMD9800X3D host/device.
  Other desktop activity remains; no CPU/device isolation or sustainable-capacity,
  separate-host/macOS performance claim. `host-before.log` records current context.
- Terminal preceding228 run38061320631 at9a9f3dc: Ubuntu counter179/2 fails
  original configure19770 with authenticated-read uncertainty and pending21101
  missing configuration_queued after a preparing deadline. macOS180/1 fails
  original drain19701 handoff with LeadershipChanged. Later suites are unrun.
  Neither those failures nor the local startup failures are fixed by this trial.

After rejection, `strict-final.log` checks the restored production tree with
formatting and all four strict profiles. The restored release build is recorded
separately from the candidate executable. Existing observation wrappers intentionally
used the sequential default in the trial; the measured startup assembly used
the native combined path. Manifest-work timing excluded nested WAL-sync time,
but no instrumented candidate measurement or per-operation critical path is claimed.

Full file and containing-directory synchronization remain explicit dependencies,
consistent with [Rust File::sync_all](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all)
and [Linux fsync](https://www.man7.org/linux/man-pages/man2/fsync.2.html).
Next resolve the evidenced startup/original-request boundaries, then provider
review and a measured P7 candidate. Original250ms, broader platform/provider/fault/
deployment and full P0–P7 stay open. Security review remains with Daybreak.
