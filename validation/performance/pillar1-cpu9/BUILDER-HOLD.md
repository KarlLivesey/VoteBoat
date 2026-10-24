# CPU-9 builder: terminal HOLD before publication

24 October 2026. Base264522d5a2531098395fc4209dd485be1a8c82fa;
production baseline0bf9271ecb5a34c4089c6bb10381303232624170 was the input.
This is an uncommitted implementation patch, not a checked published candidate.

The builder accidentally passed `payload_bootstrap.py` as its own payload in
the attempted `ancestor-oracles` read. The nested invocation failed parsing
`ancestor-oracles` as the affinity argument (ValueError, real exit1). This is
explicitly forbidden by the implementation release. Implementation and all
dependent tests/publication stopped. No commit, push or PR was created. The
outer receipt records nice19/cores0–3 before and after; that does not excuse
the launcher-protocol violation or clear any historical visibility HOLD.

The already-running normal-api controller completed and joined normally. Its
final source passed188 Raft tests,11 public volatile tests,2 queue doctests
(including downstream exclusive-borrow compile-fail), and47 matched engine
tests. The matched fixture ran widths1/231/515 through real default checkpoints,
follower snapshot catch-up, resumed originals, checkpoint/tail reconstruction
of all nine final actor states, and normal owned shutdown.

An earlier source state also passed the broad selected all-feature ownership,
snapshot, membership/recovery, logical activation/payload and transfer matrix,
plus125 public provider/runtime/worker/quorum/volatile tests. The final source
change made raw queue views crate-internal and adapted only the historical
test observer to explicit owned export. The earlier matrix must not be described
as a fresh full qualification of the final source.

The188-test normal source includes the966-case independent old-VecDeque capacity
and FIFO oracle, exact-one cold adoption, all mixed typed variants, drop/forget,
wrap/growth and token/durable-bridge checks. Sizes: queue32, slot272, front16,
guard8, token24, Drain40; largest payload256; old/new cold Result headers32.
This supports the source ownership ledger in BUDGET.md, not machine elimination.

Before the final public-view visibility change, a compiled VALID restoration
of the real original common Effect staging join and actual full-Effect Actor
pop passed188+47 tests. Six separate source mutants compiled and failed their
intended FIFO/rollback/state/capacity/byte-bound assertions. Full normal source
closure and actual normal test executables were hash-restored after each.
Those controls were not rerun after the final visibility/test-observer change.
Existing provider conformance also executed its accepted-credit early-release
negative controls. No provider or persistence algorithm was changed.

The new history fixture initially failed an incorrect universal4999-retained
assertion. The original policy retains1000 between checkpoints spaced5000
apart, so stopping at24000 can retain5001. Its failed full input patch/raw101
were frozen before correcting the oracle to the original <6000 policy bound
and exact snapshot/base/prefix relation. The corrected all-width histories
passed. Mechanical compilation/navigation failures and their raw receipts
remain in the durable builder directory; earliest compile iterations have
retained transformer/hash provenance rather than a contemporaneous full patch.

Outstanding: final strict7 with zero diagnostics, actual enabled-hook push,
the stopped-ancestor routed histories, current-source control requalification,
and final publication. An intermediate all-feature Clippy pass is not a clean
final repository. The original R3 overlay/addition hashes were independently
verified and an in-memory unchanged-body placement/queue-adapter map saved;
no diagnostic driver build or six optional driver controls were run. The
independent adapter gate remains unresolved.

Parent gates remain unchanged: fresh dual exact-head source reviews, original
adapter audit, Apple complete-flow physical removal/VALID/semantic/restoration,
original nine pairs at each width, conditional64, full5 and integration. No
Apple build/disassembly/profile, scored timing, full5, merge or speed claim.
Physical removal, gain and CPU7 attribution remain UNKNOWN. CPU8 failed515;
PR31 and all older rejections/HOLDs remain unchanged.

The complete patch, working-tree/input manifests, controls/restorations/raw
faults, build-consumer hashes/modes/locks and cleanup receipts are retained in
MAINignored `target/claude/pillar1-20261022/cpu9-builder/`. The owned target,
checkout, registration and unpublished seed are to be swept before terminal
delivery; the durable REPORT.md records the actual cleanup outcome.
