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
