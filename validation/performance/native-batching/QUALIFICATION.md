# Actual native batch diagnostic qualification

Source basis: cab9d6d plus exactly the two benchmark observer files identified by
OWNED_SOURCES.sha256. Production runtime/queue/durability policy is unchanged.

Local isolated CARGO_TARGET_DIR=target/p9/native-batching/checks:

- New diagnostic accounting tests: 4 passed, 0 failed, exact successful originals
  for synthetic widths1/4/32 and a real three-node TCP native driver workload
  (window4 eight receipts, then window1 three receipts; singleton histograms exact).
  Malformed original admissions/positions/visit ordering refuse; repeated
  operation IDs with different admissions remain counted; commit/failure-only
  steps do not count. Final direct terminal exit0; accounting-all.log archived.
- Existing real ready_native_admissions_share_persist_but_keep_every_original_result
  in effect_owner target: 1 passed, 0 failed (terminal21038 exit0), verifying
  four original owner steps share one exact VisitTicket/Persist, retain distinct
  positions/admissions and every original application result, including repeated
  operation IDs. real-batch-four.log archived.
- cargo +stable fmt --all -- --check: exit0 (terminal66779).
- Full all-target/all-feature strict Clippy: exit0, zero diagnostics after
  extracting cohesive report writing and the explicit diagnostic workload wrapper.
  No lint level/threshold changes. Original failing length logs archived.
- Additional default and no-default full strict runs passed locally. Root
  independently completed formatting and all four full strict configurations
  (default/all-feature/core-only/native-only), each exit0 with zero diagnostics;
  root-*.log are archived. Independent matched workspace qualification is
  handled by root and is not implied by primary package checks.

Original failures are retained, including the first sandbox-denied loopback bind.
The native fixture requires TCP/UDP loopback endpoints; rerun with appropriate
host permissions passed. No timed pilot, macOS accounting test, throughput,
heartbeat-causality or private queue/refusal observation is claimed here.

Live source is frozen. Release build terminal81434 passed (24.75s build wall time, not benchmark time).
Prebuilt executable is target/p9/native-batching/checks/release/examples/native_benchmark;
ignored runner target/p9/native-batching/run.sh requires that binary and a fresh
run directory. BINARY.sha256 is retained under target/p9/native-batching. No pilot
was started; root controls serialized attribution campaigns after CPU quiet and
reviewed publication. The schema and README describe observation scope/limits.

## macOS follow-up status before publication

Fresh same-source Mac qualification is pending. Its initial all-feature accounting
command exited101 before any tests: pinned raft-proto dev build could not find
protoc>=3.1.0 in the SSH PATH. /opt/homebrew/bin/protoc is installed (libprotoc36.2);
this is an environment-path failure, not an observed product/test failure. Mac
formatting already passed. Original remote log is retained at
/Users/karllivesey/voteboat-diagnostics/native-batching-cab9d6d/artifacts/initial-missing-protoc-path.log.
The focused retry adds the existing Homebrew tool directory to PATH; source and
behavior are unchanged. No Mac accounting pass, strict check, asserted build or
pilot is claimed until its actual terminal is recorded.

## Completed macOS and matched build-profile follow-up

The Mac follow-up is now qualified: terminal57578 exit0 ran four accounting
controls (4/0, actual loopback TCP endpoints), targeted strict native-example
Clippy (0 diagnostics), fresh asserted release build and exact source/example
guard. Format check had already passed. Supplementary library profile guard
terminal9869 exit0 verified fresh:false for the actual compiled library too.
No source or package versions changed. Three original environment failures are
preserved: absent protoc PATH; Homebrew cargo shadowing rustup; protobuf-build's
major==3 check rejecting Homebrew protoc36.2. Final combined preflight verifies
rustup Cargo/rustc1.98.0, explicit existing protoc3.21.12, two build jobs and the
independent snapshot target. No installation or fallback source mutation occurred.

Both hosts compiled the exact same 1353-file frozen source tree:
9bd40105b592d1d4680d0fca857c63709e412f202be2b702cf8cc33eeddf1741.
Actual compiler-artifact records require opt_level3, debug_assertions:true,
overflow_checks:true, test:false and fresh:false for library and native example.
The original Linux ordinary-release binary/hash is preserved separately and is
excluded from planned asserted-profile attribution cells. Linux asserted binary
hash is 619ed16f88f2d0d4ebd05b06af7dc5146e8142f38951515a4dc9faf3230bc8b3;
Mac asserted binary hash is 91d72a3bc47e22cd39e8f9cdffba0f293d346ff4231b331cb6adc1d3de09a025.
The raw compiler JSONL remains under target/p9/native-batching and the independent
Mac workspace; archive guards retain the actual artifact/profile/source identity.

Four attribution cells per host are prepared: TCP/QUIC x window1/32, 64 measured
commands plus the actual fixed64-command warmup. See pilot-plan.csv. These are
future observation cells, not paired throughput claims. **No pilot has run**;
root must release each host only after other qualification/timing work ends.

## Quiet Linux attribution — actual terminal

Runner76545 completed all four cells serially, exit0, with no case restarted.
Before/after source and asserted binary guards matched. Exact original samples,
weighted width counts (64 in each warmup/measurement phase), useful-write receipts,
three-replica state, duplicate retry, recovery and worker joins passed in all cells.
Every raw phase, CSV, stdout/stderr and final native store is in linux/raw.tgz;
raw-files.sha256 identifies unmodified files. Per-cell validation is in
linux/linux-analysis.json and summary.csv. These are short diagnostic observations
with instrumentation overhead, not paired protocol throughput conclusions.

Measured leader counts: window1 had64 singleton proposal visits and128 actual WAL
synchronizations/manifest publications for both TCP and QUIC. Window32 had32
singleton visits plus2 width16 visits (64 originals total), with44 actual WAL
synchronizations/publications for both protocols. Thus window32 did batch half its
original commands; the observer does not expose why remaining visits were single.
Pre-poll heartbeat deadline correlations remain explicitly unobserved queue
selection reasons. Do not infer a queued Control cause from the deadline counts.

## Quiet macOS attribution — actual terminal

SSH runner43451 completed all four fresh cells serially, exit0; no case restarted.
Source and asserted91d72a3b binary guards are byte-identical before/after. Each
warmup/measurement histogram sums to64 originals, with all useful-write receipts,
three-replica state, recovery/retry and worker joins verified. The identical
cross-host analyzer checked unique operations65..128, unique original applied
positions and expected counter values. Full raw phases/native stores/stdout/stderr
are retained in mac-pilots/raw.tgz with hash inventory; exact aggregate is in
mac-pilots/mac-analysis.json and attribution-summary.csv.

Mac structural counts matched Linux in these cells: window1 had64 singleton
proposal visits and128 leader WAL sync/manifest calls; window32 had32 singletons
plus2 width16 batches, with44 leader sync/manifest calls. Mac per-call average WAL
sync was5.4–6.5ms; manifest publication was13.8–19.0ms. Linux cells were8.4–14.9ms
and16.0–26.3ms respectively. These ranges describe the actual individual short
observation cells; they are not paired host/protocol throughput comparisons or a
statement that any specific queued class caused the singleton visits. Public
queue/refusal reasons remain unobserved in all eight cells.

All eight prepared cells are now terminal and archived. No production batching,
protected-queue order, heartbeat schedule or durability policy changed.
