# Slice228 — original receipt positions in sealed native journals

This offline analysis maps the original slice222 reference and diagnostic receipts
to physical append and commit records on all three replicas. It uses the existing
public native codec, recovery and apply contracts; it changes no production log,
provider contract, durability barrier, deadline or benchmark workload.

Each of the six journals has651 frames/87525 bytes: bootstrap1, control6,
append-only322 and commit-only322. Every original measured receipt (operations
65–320, indices66–321) has an exact earlier append and later commit-only record:
512 selected physical batches per replica, with2 records per original receipt.
The first pair is133/134 and the last643/644. Both measurement modes have identical
physical rows per replica. Recovery also records later requests319/320 at indices
323/324 in term2; those cannot replace the original term1 receipt positions.
The322 physical command commits are not322 useful application operations: the
original benchmark's deduplication/recovery check still reports320.

`reference.json` and `diagnostic.json` retain all256 exact receipt-to-three-replica
batch mappings, original recorded latency, CSV hashes and counts. Source manifest,
WAL and LOCK hashes plus both original samples/summary files match before/after
(`source-journals-before.sha256`, `source-journals-after.log`). Native codec/storage
source is unchanged since the measurement revision (`codec-since-measurement.diff`
is empty). Inspector source and actual native-only binary hashes are retained.
The owned in-memory recovery copy is an analysis fixture, not a durable backend;
its incremented session never escapes as client, voter or lifecycle evidence.

The inspector holds the existing source lock, reads bounded bytes and refuses
selected CURRENT, missing LOCK, snapshots, mismatched identities, corrupt data,
complete written tails and torn tails. Native recovery validates the copied
format/state before the source's original manifest boundary is inspected. Replay
tracks newly appended and newly committed entries independently, including a
combined batch that commits an earlier command and suffix replacement generations.
Only fully validated bounded records are emitted; output flushing is checked.

Validation:

- `example-all.log`, `example-native.log`:5 actual native-file checks pass in each
  configuration. Combined batches, discarded suffix lineage, unbarriered/torn
  tails, wrong identity/checksums, actual CURRENT selection, existing-lock
  contention and lock release are checked without source modification.
- `checker-tests.log`:6 artifact checks pass on both actual corpora. Mutations
  remove an original commit while leaving a later duplicate, change its term,
  move a receipt to a Noop, break physical offsets/event ownership, and mislabel
  append-only as combined. All are refused rather than credited to the receipt.
- `example-initial-failed.log`:4/1, because a bootstrap-only reclaim correctly
  made no CURRENT selection. The fixture now creates enough history to perform
  actual reclaim; missing LOCK is independently tested on a legacy fixture.
- `checker-tests-sandbox.log`: sandbox worker launch failed without a specific
  cause in the retained output. Permission-enabled workers then exposed one
  fixture assumption (`checker-tests-initial-failed.log`:5/1): changing only a
  batch header failed the event-ownership check before the intended class check.
  The mutation now changes both owned header/events; validator code is unchanged.
- `strict-final.log`: formatting and default/all-feature/core-only/native-only
  strict all-target Clippy finish at zero. Unaffected service suites were not
  repeated locally for this offline example/tool change. Push repeats the hook.
- Terminal preceding227 run38059461105 at exact source
  `b448fffed6160ab91552830afd511453a5e4698d`: Ubuntu counter181/directory22/
  transfer21 pass. macOS counter179/2 fails: assignment group7/3 original add42/5
  has no eligible leader within routing deadline (sampled term47); pending
  configuration21101 misses configuration_queued and reaches its preparing
  deadline after quorum loss. Later macOS suites are unrun. Logs and terminal
  metadata remain retained; neither failure is fixed or attributed by this slice.

This establishes physical attribution, not synchronization count, per-command
file latency or a critical path. No new performance measurement was run. The
unchanged original250ms gate still fails at471.057ms p99; the instrumented
872.815ms run does not replace it. Next choose a bounded candidate under the same
public storage contract, with meaningful crash/ownership checks, then run the
original gate. P7, broader provider/platform/fault/deployment and full P0–P7
remain open. Daybreak owns the broader security review.
