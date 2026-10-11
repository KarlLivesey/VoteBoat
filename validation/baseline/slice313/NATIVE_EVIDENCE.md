# Slice313 owned native acceptance and embedding

Owned source is frozen: `tests/transaction_driver.rs`, its six child modules,
`examples/embedded_transfer.rs`, host/cleanup and controlled recovery test.
`driver-owned.sha256` records exact final owned source bytes. No production,
Cargo, shared documentation or Git changes by this agent.

## Actual coverage

Fourteen distinct native network histories compose3 original physical replicas,
each hosting3 independently replicated transaction groups. Actual NativeMultiStartup,
TCP/TLS and QUIC sockets, file WAL and snapshot workers drive all authoritative
proposals/completions/quorum reads. Group leaders begin on different physical
replicas. Tests serialize native histories and reserve bounded listener candidates
outside the ephemeral dial pool.

- Commit, exact original-ID retry, reversed debit direction and durable business
  refusal. Repeated completion returns the same retained Finish first index.
- Active backpressure and whole-lifetime N=1: exact retry succeeds after capacity
  fills; N+1 has definite pre-admission Application(DedupCapacity), with fresh
  quorum absence in all groups and unchanged balances.
- Actual lost Prepare0 and Prepare1 observations, canonical participant ordering
  even for reverse direction, locked fresh quorum account reads; actual prepared
  checkpoint followed by Decide/apply suffix and cold native reopen. Captured
  coordinator checkpoint bases remain unchanged and Decide's original first index
  exceeds them. Recovered spec and original phase facts compare exactly, then
  original-ID resume completes.
- Locked refusal, absent-participant Abort tombstone, unchanged prepared lock,
  actual coordinator leader retire/drain/join/reopen and resumed original Commit.
  This is graceful replacement, not a process crash claim.
- Lost AppliedAll/Release and Finish observations; retained canonical facts survive
  fresh status reconstruction and final balances agree.
- Constructor rejects an actual nonempty Node client stream before taking its
  output; host consumes the original opaque output, then driver resume completes.
- An actual exported DriverA completion is rejected by fresh-generation DriverB
  without changing DriverB credits; old ticket progress is rejected too. Missing-ID
  fresh resume gives Unknown(NoRetainedStart), never an invented Abort.
- Two original real Node owners remain alive but unpolled. The sole remaining
  replica cannot obtain new majority read/commit acknowledgments despite actual
  Node/timer/socket progress. No terminal business outcome appears; stop_waiting
  yields Unknown and actual read ownership drains. Local status here is diagnostic;
  fresh quorum state after restored polling proves unchanged original prepared
  facts/locks, then original-ID resume completes and all original owners join.

All positive network histories finish actual Node drain and reclaim/join selected
TCP dialer, snapshot and WAL workers, then rebind original TCP+UDP listener ports.
No application or public caller injects committed phase facts. Private original
Node output ticket/node/operation mismatch controls are separately owned by the
production driver agent; public stale DriverCompletion coverage is not substituted
for those controls.

## Standalone embedding

`embedded_transfer` is explicitly single-voter/three-group and uses the existing
public NativeMultiStartup and generic TransferDriver. It is not a distributed
transaction intake service. CLI:

```
cargo +stable run --locked --offline --example embedded_transfer -- create DIRECTORY TLS_DIRECTORY submit ID AMOUNT forward|reverse
cargo +stable run --locked --offline --example embedded_transfer -- recover DIRECTORY TLS_DIRECTORY submit ID AMOUNT forward|reverse
cargo +stable run --locked --offline --example embedded_transfer -- recover DIRECTORY TLS_DIRECTORY resume ID
```

Opening account balances1000/0 and lifetime capacity1024 are fixed metadata. Phase
IDs derive deterministically from checked ID*16 plus1..10; recover submit requires
original immutable arguments, while resume only supplies ID. The CLI attaches once
per process and exposes outcomes, not opaque driver outputs; embedding hosts that
reattach under one live RuntimeOwner must allocate a fresh generation each time.
Actual fresh Account quorum reads report balances. Normal close checkpoints each
group, actualdrains and joins workers. Unknown retains authoritative files and
joins workers before reporting an unresolved error.

Focused failed-owner cleanup transfers Node::RecoveryRequired via into_recovery;
it never submits Checkpoint to a failed core. Closed nonfailed peer ownership is
driven using PeerDriver::drain(outbound), selected connector/dialer joins, and
native persistence/reclaim/snapshot terminal observations are polled without
applying them to the failed core before actual store reclaim/join. The original
run failure is preserved and cleanup failures reported separately. The focused
control accepts a real Start, explicitly aborts the Node, asserts
RecoveryRequired/Aborted, executes cleanup, cold reopens same files and normally
joins them. This is an owner-abort cleanup control, not injected storage I/O fault
or power-loss testing. A fenced peer provider cannot be declared drained; its
explicit recovery refusal is reported rather than bypassed.

## Actual commands and results

All commands use isolated `CARGO_TARGET_DIR=target/divide-shutdown/build` and
sequential feature builds. Root owns final integrated Linux/Mac four-profile
qualification on one frozen image; the records below are local agent preflight.

- `cargo +stable test --locked --offline --all-features --test transaction_driver -- --test-threads=1`:
  initial12/0 (`driver-all.log`), repeated12/0 after checkpoint suffix assertion
  (`driver-all-final.log`). Requested unavailable history was then added.
- `cargo +stable test --locked --offline --all-features --test transaction_driver quorum_unavailable -- --test-threads=1`:
 2/0 (`driver-unavailable.log`). Together the distinct all-feature histories14/0.
- `cargo +stable test --locked --offline --test transaction_driver -- --test-threads=1`:
  initial6/0 (`driver-default.log`), final7/0 including unavailable quorum
  (`driver-default-final.log`).
- `cargo +stable test --locked --offline --no-default-features --features native --test transaction_driver`:
  compiles cleanly and0tests (`driver-native.log`); TLS-gated target intentionally
  has no native-only socket tests. Core-only provider conformance belongs to root.
- `cargo +stable test --locked --offline --all-features --example embedded_transfer`:
 1/0 (`driver-demo-recovery-test.log`), name
 `tests::accepted_start_abort_reclaims_real_workers_and_cold_reopens`.
- `cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings`:
  final0 (`driver-strict-final.log`).
- `cargo +stable fmt --all -- --check`: final0 (`driver-fmt-final.log`, zero bytes).

Actual CLI create/retry/resume using TLS fixtures and
`target/divide-shutdown/transfer-demo` reports Commit original first_index5,
balances983/17 and workers_joined=true (`driver-demo-create-fixed.log`,
`driver-demo-retry.log`, `driver-demo-resume.log`). Missing-ID resume99 reports
Unknown(NoRetainedStart), workers_joined=true, then expected exit1 while retaining
files (`driver-demo-absent.log`). These CLI logs precede the focused recovery-path
correction; root integrated qualification reruns CLI against final frozen source.

## Failed checks retained

Initial owned target compilation showed Start encoder returns Result; added exact
unwrap for the already checked test spec. Initial standalone create rejected
configuration because multi-group requires wire8; selected with_wire_version(8),
then actual create passed. Original failure remains `driver-demo-create.log`.
First global strict check encountered production agent's new private-test wrong
TlsCredentials import/TCP variant (`driver-strict-all.log`); agent corrected its
owned test, global strict then passed (`driver-strict-all-fixed.log`). Initial fmt
showed only shared module ordering; their owners corrected it before final0.
No lints, thresholds, original provider deadlines or ownership guards weakened.
