# Slice314A independent promise/capsule models

Scope: tests/logical_voter_promises.rs and its child modules only. Source frozen after the final focused run, repository formatting check
and strict all-target/all-feature Clippy passed. No production, shared docs,
design-pack, dependencies or git changes were made by this agent.

The pre-edit schema is target/divide-membership/LOGICAL_VOTER_MODEL_SCHEMA.md.
Reviewed archived sources are validation/baseline/slice313/
PLANNED_LOGICAL_VOTER_MAPPING.md and PLANNED_LOGICAL_VOTER_MODEL.md; root's
Schema314 supplies latest-accepted child eligibility and parent control-only
lifecycle serialization to the other profiles. This profile has static members.

## Exact selected graph

Five separate finite fixed semantic workload pools; actual physical action
interleavings are discovered by BFS, not supplied as complete schedules. All
start from three empty logical cells with three physical child stores each.
Epoch1 is acquired through original admit/write/sync/commit/apply/observe.
Terms1/2, candidatesA/B, entriesX/Y at parent positions1/2, epochs1/2;48 retained
original commands per cell; one actively driven child mutation globally, one
held original output, at most two response packets. Each accepted child action
allows at most one unsynchronized physical power cut; one original inner-quorum
outage is available per workload, with real repair of that same affected cell.

Next semantic request begins only after all three physical prefixes catch up.
Actual 2/3 commitment and observation before third-store catch-up remain
reachable and counted below. There is no indefinitely lagging replica across
multiple completed requests and no full Cartesian child/parent Raft RPC graph.

An immutable original command arena identifies actual physical prefix bytes.
Per-physical written versus synchronized contiguous prefixes are separate from
canonical child committed/applied authority and original output observations.
Recover decodes the selected physical store's own synchronized applied prefix,
with an actual fresh live matching quorum. Apply also requires the selected
original physical applier's synchronized prefix. The surviving ordinary child
consensus preservation contract is assumed, not re-proved: commit/applied flags
are not silently persisted physical metadata. Complete synchronized-store loss,
child elections, suffix rollback/checkpoints and membership transitions belong
to other profiles/native acceptance. No oracle image restores protocol state.

The independent oracle replays original retained records separately, checks
original action/scope/revision/content, durable parent term/vote/prefix, actual
child quorum and own-store provenance, exact output/credit correlation and
original authorized write/read responses before cleanup. Historical parent
grants and acknowledged-write/read invocation history survive cuts but are
never used to recover protocol state. Source uses no production/model helpers.

## Positive exhaustion and terminals

Declared caps per workload: depth384,500000 states,5000000 directed edges.
All five graphs exhaust; all three cap flags false, queue frontier0, deliberate
unexpanded depth frontier0. Terminal states are independently checked.

| Graph | States | Edges | Maximum depth | Terminals | Commit before third | Observe before third |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Promises |146148|397527|120|76|12000|23960|
| Repeated |70110|193434|108|38|5664|11876|
| Read |316692|864081|293|76|26208|51700|
| Written |56050|160612|95|38|4320|10128|
| StaleCompletion |86450|244095|119|152|6048|15980|

Every graph reaches actual output loss, unsynchronized cut, own-prefix recovery,
inner quorum loss and original-cell repair. Read has76 actual freshY completed
read terminals. Promises/Repeated/Read have76/38/76 refusal terminals; Written
has19 refusal terminals and19 paths which avoid the early-output probe. The
fixed semantic request pool means this is finite selected-protocol acceptance,
not arbitrary client history or full-engine safety/liveness proof.

## Required actual weakened controls

Each mutation changes a protocol transition/authority rule; no oracle flags,
prefabricated committed images or fabricated completion are supplied. Exact
Step traces are printed in the raw test log and replayed from empty state. Each
safe counterpart is checked after every real transition, refuses/protects the
missing dependency, then finds and checks an actual terminal continuation.

| Weakening | Search states | Edges | Trace length | Independently observed failure |
| --- | ---: | ---: | ---: | --- |
| Worker-only cached response |273631|745640|261|New context5 read after acknowledgedY returnsX|
| Written output escape |42171|117924|81|Original owned result released before child sync/commit/apply|
| Physical vote counting |43892|121759|78|Two physical representatives of one logical leaf elect with one logical vote|
| Stale effect completion |53173|146982|102|Held real epoch1 original completion frees epoch2's fresh wait|
| Replacement clears vote |58022|158549|74|Same logical cell durably grantsA andB in term1|

The cached-reader control creates the read only AFTER Y's real quorum-authorized
term2 acknowledgment, using the partitioned former parent's term1 view and a
new read context5. This cannot reuse an old read capsule. Safe old-epoch capsule
admission refuses; fresh term2/context6 capsules supply an actual currentY read.
Negative exploration continues past missing capsule/erased image until the
required concrete stale-client-read/double-vote event, then evaluates the same
independent oracle. Other controls stop at their exact concrete event. Negative
searches stop on a counterexample and do not claim full graph exhaustion.

## Actual validation and focused fixes

All builds use CARGO_TARGET_DIR=target/divide-membership/build. First focused
default target run passed6/0,65.42s. A subsequent all-feature6/0,64.70s has explicit
frontier reporting. Final qualification after independently checking original
Resolve quorum and write/read success-time packet provenance is recorded below.

Initial compilation exposed child module path resolution and ambiguous logical
mask integer types, corrected directly. Initial strict Clippy exposed5 own
diagnostics: admission/parent oversized functions, capsule argument count,
control complexity and checked-then-unwrapped pending. Split cohesive admission,
election and protected-state assertions, introduced typed ResponseData, and
matched pending ownership directly. No lint threshold/allow/dependency changes.
The parent review's own-prefix recovery and original-cell Repair findings were
fixed before the initial6/0 and are independently asserted in the oracle.

Logs: logical314a-preflight.log, logical314a-strict-preflight.log,
logical314a-strict-fixed.log, logical314a-final-all.log,
logical314a-qualified-all.log, logical314a-fmt-final.log and
logical314a-strict-final.log in this directory. The source manifest is
logical314a-source.sha256.

Final terminal commands:

- `CARGO_TARGET_DIR=target/divide-membership/build cargo +stable test --locked --offline --all-features --test logical_voter_promises -- --nocapture`:6 passed/0 failed,64.02s; final oracle included, all5 graphs exhausted, every frontier0.
- `cargo +stable fmt --all -- --check`:exit0,zero formatting differences.
- `CARGO_TARGET_DIR=target/divide-membership/build cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings`:exit0,zero diagnostics.
- `sha256sum -c target/divide-membership/logical314a-source.sha256`:all10 source files match.

Root owns combined frozen Linux/macOS four-profile qualification; no extra
unchanged feature-profile runs were performed by this agent. No native extension
or overall P8 completion is certified by this target.
