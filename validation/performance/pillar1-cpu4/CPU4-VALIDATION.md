# CPU4 focused candidate evidence

### CPU-4 A1 adapter schema before edits (R6, 23 October 2026)

Finding A1 on exact e85 concerns generated observations on rejected enqueue,
not consensus or a healthy denominator failure. Old A constructs each Original
and FIFO wait and advances next before try_send. The B cursor already owns all
originals before send; move generated-ID accounting to that same ownership
boundary. Keep submitted_including_warmup's original generated-observation
meaning; it does not count accepted queue transfers.

Data/API: Pool::enqueue receives &mut next, checks shape/ID arithmetic, calls
the actual normal Client::prepare/public begin, and validates the Port observer.
Only after the owned invocation enters the global FIFO does it advance next,
before the unchanged try_send. Pre-cursor preparation/begin failure leaves next
and FIFO unchanged. Failed send retains exactly those cursor originals and
propagates the same error. Caller/test traces no longer advance separately.
No pumping, pacing, retry, request-boundary, Actor or public API change.

Ownership/failure: available-prefix carry, count-minus-next remaining ORIGINALS,
all original outcomes, domain/port cleanup and restart contracts are unchanged.
The failed request drops its unsubmitted producer ownership; retained observers
are NotSubmitted rather than Applied. No recovery state or extra owner is added.
Acceptance: real bounded-full and closed queues at1/4/final3, sparse full-u128
IDs, real prepare/begin failures before cursor, source-anchored old-A comparison,
and final3 failed conjuncts using actual three-Actor state. A compiled corrected-
API old-ordering mutant must fail the generated-count assertion, then restore.
Complete source overlay applies/compiles/runs original9 plus new untimed controls
and reverses exactly. Normal matched default/all-feature focused tests, all3
width/checkpoint/snapshot controls, sequential strict7 and actual enabled hook
are required. Normal root/matched RELEASE inputs stay byte-exact e85.

Current evidence only: target/claude/pillar1-20261022/cpu4-adapter-a1-fix-r6.
Original R5 evidence/report are immutable. Report pin:28998 UTF-8 bytes,
28960 Unicode characters, SHA2560558c758bcaa5b152a9d3cecef76cfaf8e43caebb5ad9bd44b41d00204fb9673.
No historical pre-R5 byte hash is asserted. Parent retains exact-head source
confirmations, independent adapter re-audit, original9pairs/conditional64/full5.
No CPU4 timing or historical resource-HOLD clearance follows this source fix.


23 October 2026. This is one preserved, bounded public-write-port candidate.
It is not CPU qualification or a full-suite result. Production starts at
`ffeaeab05dbe61848a780cad3a789d6f6a9b78f4`; CPU1/2/3 remain rejected and the
CPU3 resource HOLD remains unresolved. R2 evidence and its limitations remain
historical and immutable.

### A1 R6 actual focused evidence

Real bounded-full/closed controls pass for1/4/final3, ordinary and full-u128 IDs;
the failed cursor yields matching real NotSubmitted envelopes, old-A oneshots
close, counts/FIFO remaining originals and propagated errors match. Zero-ID
Client::prepare and real public port.begin after domain close leave count/FIFO
unchanged; an already-open idle second slot ensures the latter reaches begin.
Final3 faults at231/515 retain generated231/515 versus old-B successful-prefix
228/512. With actual three-Actor campaign frontiers1, both A and corrected B
fail exactly reply_count, unreplied, commit_frontiers, applied_frontiers,
reported_frontiers and propagated_error. Old B additionally fails expected_count.
This is a bounded one-round/no-warmup enqueue-fault closure with a source-anchored
old-A segment, not a full old-engine campaign or complete consensus proof.

The corrected-API old-ordering mutant compiled and failed the intended
failed-send generated-count assertion (2 versus3); exact source/full manifest
restored. Final normal matched default59/all-feature59 pass, including original
56; complete installed overlay12 (original9+new3) passes and reverses/checks
byte-exact. All3 real owned applications/receipts, healthy12000/12012/12360,
two default5000 checkpoints each, suffix/pin/lagging real snapshot and resumed
1/231/515 delivery remain checked. Telemetry-only/skipped-count failed conjuncts
remain reported/reply_count. No timing entrypoint ran.

Frozen actual input manifest: 1555 paths, 248286 bytes,
SHA256 ea7ed1c28273da125aeefc3fcb1597c4f4f680c732491ddd0c1f129c3d8394f2.
Normal release source (including P2 Actor), root module/Counter/example,
Cargo/dependencies/hooks/native/core/logical/application remain e85-equivalent.
Unchanged root/semantic and old mutation receipts are reused by source equality,
not claimed rerun. Parent e85 source approvals remain historical; exact new-head
confirmations and independent adapter re-audit/performance/full5 remain pending.

R6 limits: initial fixed-bootstrap intake readers were audited by the loader,
but preceded the sampled outer observer and own stock guard. Later controlled
checks cannot certify those early reads or short-lived/reparented/shared hosts.
One optional .cargo/config.toml read failed; one guessed Cargo executable did
not exist and no build started. Logs persist; actual /usr/bin/cargo was then
observed and used. The first passing closed-domain test reached cold registration;
it was tightened before acceptance to use an existing idle port and reach begin.
No live resource mismatch was observed in controlled samples; original histories
and HOLDs remain unchanged and unattributed. Strict7/actual hook follow separately.

R6 final local acceptance: normal default59/all-feature59, complete overlay12,
compiled intended old-ordering kill/exact restoration and all seven sequential
strict checks pass with zero diagnostics on the frozen1555-input manifest.
Actual enabled nonforce push-hook acceptance/publication is recorded separately
in cpu4-adapter-a1-fix-r6/publication-command.log. No hook/Cargo/lint change.
Parent confirmations/re-audit/performance/full5 and resource HOLDs remain pending.

## P2 repair acceptance (planned before source edits)

Review A on ec6052 withheld source approval for whole-result prevalidation.
R4 must reject short/extra/duplicate/wrong-tail/reversed current receipt vectors
with no new Applied and no pending removal, in actual borrowed and owned
committed contexts with sparse full-u128 IDs. Earlier Applied intervals remain;
valid complete application followed by a closed middle consumer retains its
successful prefix. Existing prior Send/application-failure ordering stays.
Compile old-behavior restoration against the corrected controls and require
the intended no-new-Applied failure, then restore exact fixed inputs. Execute
focused final matched default/all-feature controls and widths1/231/515 with
all3 two5000 checkpoints, real lagging snapshot and resumed delivery. Update
source-only overlay provenance, check/install/compile its final asset, run
untimed guards and restore exact sources. Finish sequential strict7 and actual
enabled hook. Evidence: parent-safe `cpu4-p2-fix-r4`, never prior evidence edits.

Unchanged root public-port/Counter/example and native/core/application paths
reuse the R3 receipts above by source equivalence; they are not new R4 runs.
The nine R3 mutants target unaffected branches and are preserved rather than
rerun. The new malformed-result mutant supplements their missing boundary.
Exact source approval, independent old/new audit, CPU performance and full5
remain pending parent gates; no prior resource HOLD is cleared.

## Actual R4 P2 executions

Evidence directory: parent-safe `cpu4-p2-fix-r4`. Final normal matched engine
default56 and all-feature56 pass with0 failed/ignored, after overlay restoration
and asset freeze. Both have the exact same1554-input SHA256/size manifest as
the seven strict checks:
`847bcf24d93d07dcef69d60fce4441c8eec07b561fa00ba29f152e3e5a2b6263`.
Every build records before/after equality and archived source objects.

The corrected malformed-result test covers six shapes (short, extra, duplicate,
wrong full-u128 tail, reversed, empty) in both actual borrowed EntryRange and
owned committed contexts. It checks no new Applied before error, all current
pending producers retained, an earlier interval still Applied, and owner-drop
Unknown cleanup. Sparse IDs share low bits while differing high bits. A compiled
restoration of the old delivery method fails exactly at
`malformed current receipt set must publish no new Applied: owned=false kind=0 ordinal=0`;
the fixed source and all build inputs restore exactly. This is one intended
semantic kill, not a compiler failure. Existing valid whole-interval closed-middle
consumer prefix and prior Send/application-failure controls still pass.

Every final engine configuration and the updated overlay retain all3 real owned
applications/receipts at widths1/231/515, healthy originals12000/12012/12360,
two default5000 checkpoints on every replica, historical1000 suffix/pins,
actual lagging snapshot installation and resumed1/231/515 originals. Updated
patch application/reversal and before/after hashes verify exact restoration;
the complete overlay compiles and passes9 untimed public-path controls. Actual
telemetry-only/skipped-original traces retain their original failed conjuncts,
chunks1/4/final3, FIFO carry and original cardinality. Driver/pool/steps/reference
are byte-identical to ec6052. The final explanatory adapter README was added
after that functional manifest; no executable asset differs. No timer ran.

Root `src/`, `tests/`, `examples/`, native/core/application/Cargo/hooks and all
Actor code outside `deliver_results` are source-equivalent to R3. Root module15,
Counter4/all4 configurations/example and semantic receipts above are historical
reuse, not new tests. Nine preserved mutant impacts: generation/old-owner reuse,
content/full-ID, readiness and registry-capacity branches stay in unchanged
public producer/invocation/state/domain code; one-is-batch and fallback triggers
stay in unchanged matched client code; pre-apply trigger and its existing control
stay in unchanged `apply_entries`/test code. Their downstream delivery boundary
is now strengthened; R3 kills are not claimed as fresh R4 executions. The new
old-behavior mutation specifically qualifies the changed boundary.

Strict7 passed with zero diagnostics and sequential actual command execution:
rootfmt, default/all/no-default/native-only strict locked-offline all-target
Clippy, matchedfmt and strict matched Clippy. Same input manifest for all7;
`FINAL-seven-strict-checks.json` records exact commands and join observations.
The native-only and matched-format outer envelopes briefly overlap in a stock
guard, with the prior check joined before the next check starts. Actual enabled nonforce hook runs on
the ensuing publication; its receipt is separate. Initial bootstrap-interface
failure/unsampled contract inspection, failed read paths and truncated historical
displays remain explicit limits in docs/IMPLEMENTATION.md and the R4 report.
Later controlled samples do not certify early reads or unknown infrastructure,
attribute the earlier parent priority change, or clear any resource HOLD.

## Actual R3 executions

Evidence directory in the parent checkout:
`target/claude/pillar1-20261022/cpu4-builder-r3`.
Each controlled build/test has a contemporaneous source SHA256/size inventory,
source objects, command log/result, bootstrap identity, sampled owned
descendant/thread identities, join result and unchanged-input comparison.
These manifests describe their actual cuts, not reconstructed R2 history.

| Control | Actual evidence |
| --- | --- |
| Public ownership module | 15/15 in each of default, all features, no default features, native only; includes unknown ordinal, repeated noncancelling polling, ABA/renewal, cancellation/live owners, poison, nested R, reentrant wake/drop and fixed capacity witnesses |
| Downstream public integration | 4/4 in each configuration, including real nonempty sparse Counter results, retry/conflict/overflow, owned control, independent groups, nested receipt bounds and actual snapshot resumption |
| Actual example | `public-example-actual`: no-default-features execution, two groups, individual receipts and authorized reads, surviving group after other closes |
| Normal matched consumer | `matched-final-consumer`: 56/56 after R2 lint/helper extraction and formatting; nine controls use the changed public path |
| All3 checkpoints/snapshots | Width1:12000, width231:12012, width515:12360 healthy originals; every replica crosses two default5000 checkpoints with1000 historical suffix/pin policy; existing lagging follower installs a real snapshot, then1/231/515 port originals resume |
| Adapter controls | Exact chunks1;57x4+3;128x4+3, actual all3 application intervals and owned receipt counts, available global FIFO prefix/carry and remaining ORIGINALS; telemetry-only fails only reported, skipped-original fails only reply count |
| Unchanged-feature focused controls | Application23, directory31, membership32, quorum6; logical-voter persistence2 verifies exact Written ticket and consumed DURABLE release |
| Complete source-only overlay | Final formatted driver/engine hook/modules compiled with overlay installed; nine untimed public-path controls pass; engine/Actor restored byte-exact afterward. Timing entry point was compiled, not invoked |

## Compiled semantic mutations

All nine mutate changed public or normal-consumer branches and compile. Each
fails the intended behavioral assertion, then restores the exact saved source.
Root mutations run in no-default-features; normal-consumer mutations use the
matched workspace. No compiler-only or unchanged legacy-arm failure is counted.

| Mutation | Intended control |
| --- | --- |
| Bypass generation/epoch validation | Stale public publication must return an error without changing current ownership |
| Permit begin with live old owners | Cancellation must retain BusyDrain |
| Ignore immutable content | Wrong-content publication returns exact rejected R |
| Ignore full operation ID | Wrong full-ID publication returns exact rejected R |
| Miss readiness after clone/registration interleaving | Deterministic completion-before-registration is Ready |
| Release registry capacity at cancellation | Live producer reservation remains charged |
| Treat first receipt as batch success | Write remains Pending until every original result |
| Publish before whole application | Failing actual interval exposes no new receipt |
| Route configured writes to owned channels | Real public Request/observer witness rejects fallback |

The generation mutation genuinely failed at `unwrap_err()` before the later
custom assertion message. The evidence helper initially expected that later
message. Its original classifier failure and restored source are retained;
`mutant-generation/validated-semantic-result.json` validates the existing
compiled failure without rerunning it. Other failed read searches and all
earlier R2 failures remain recorded; they are not candidate failures or erased
resource history.

## Semantic feature preservation matrix

| Implemented capability | Changed consumer / evidence | Remaining boundary |
| --- | --- | --- |
| Recursive/weighted/joint quorum; current-term commit | Core/policy/log code unchanged; quorum6 and membership32 exercise strict predicates, joint activation, rollback, durable/committed status and recovery | Whole feature histories in parent full5; no new quorum protocol claim |
| Logical-voter child-consumed DURABLE evidence | Logical/application/bridge code unchanged; two actual persistence controls distinguish Written from consumed DURABLE | Parent broader logical/native histories |
| Membership, learner readiness, retirement and transfer | Membership/native/runtime paths unchanged; membership32 covers matching learner promotion, identity, configuration/snapshot/recovery; directory31 covers intent/fencing/lineage contracts | Unexercised retirement/transfer end-to-end histories remain parent full5 |
| Reads and authority | Read APIs/core/native unchanged; real Counter example/integration use actual read barrier; directory controls preserve scoped ownership views | Native/read failover full histories remain parent |
| Responsibility hierarchy, ownership, dedup/outbox/lineage | Directory/routing/transfer/application APIs unchanged; directory31 and application23, plus two independent live Counter groups sharing caller resources | No new online split/merge implementation or global order claim |
| Full original IDs, immutable command, owned arbitrary R | New public module15 and public integration4 in all4 configs; exact normal consumer/partial/FIFO controls; content/ID/cardinality mutants | Host remains responsible for real commit, whole apply and truthful nested bound; routing is not a commit certificate |
| Default retention, recovery, snapshots | Actual all3 two5000 checkpoints and lagging snapshot/resumed port delivery at all3 widths; Counter actual state/dedup checkpoint histories | Volatile examples do not promise crash durability |
| Bounded multi-group/public/native composition | New std-only no-default seam; independent bounded factories/ports and domains; native ClientRouter/ApplicationRouter/EffectOwner untouched; owned oversized fallback retained | Native/TLS/QUIC full histories remain parent; Cargo all-feature compilation is not their functional proof |
| Native/logical/durable provider interfaces, wire and scheduler | Release source unchanged, Cargo/deps/locks/hooks unchanged; exact source inventory/diff fences and strict feature configurations | Parent full5/exhaustive relay/docs gate |

## Ownership and cost ledger

The existing StateMachine owned Vec API, all3 actual applications/results,
PendingReplies full-ID algorithm, original Actor scheduling/FIFO and normal
owned shutdown remain. A successful configured write reuses registered cells
and descriptor capacity, replacing fresh per-original oneshot control ownership
and per-call transport/wait vectors. Larger supported calls select the actual
owned path before admission.

Costs include cold domain registry/port/epoch/cell allocations, retained command
capacity and immutable copy-in, ordinary core-input Vec/CommandBytes copies,
two shared/epoch key copies at begin, per-original producer key copies,
temporary shared keepalives at retirement/publication, and observer key copies
even on Pending try_take/poll. Bind/check/publish/observe/cancel/reap take bounded
per-port locks; ordinary writes never take the domain registry mutex. Waker
clone is outside the lock; replacement/drop/wake and arbitrary R destruction
are outside locks. Output/error R and whole-application Vec overlap are charged
by the documented retained-byte reservations and caller/host budgets.

The adapter additionally charges retained Slot/Cursor/scratch capacities,
normal clients/descriptors and startup domains/registration. Healthy traces use
1/58/129 live ports; bounded carry can open more within the declared ceiling.
Pool exhaustion fails rather than pacing, pumping or erasing old outcomes.
No source allocation expression, pointer/capacity witness or inclusive sample is
an allocator count, measured CPU saving or a qualification result.

## Publication and remaining gates

Exact sequential strict7 and the actual enabled nonforce push hook are required
before publication; final receipts are recorded in docs/IMPLEMENTATION.md and
the parent-safe evidence directory. Parent owns independent old-A/new-B adapter
audit, two fresh exact-head reviews, original9pairs, conditional64 and full5 with
normal-debug exhaustive relay/docs. No performance, full suite, Mac build,
browser, subagent or idle watcher belongs to this builder task.

Final local strict7 PASS, sequential on an unchanged input manifest with zero diagnostics. Actual enabled push hook/publication receipt follows separately. R3 initial POSIX read bridges lack complete persisted identities/descendant observations, so their absolute priority remains UNKNOWN; later controlled checks are not whole-turn resource certification. Full limits: parent-safe cpu4-builder-r3/R3-LIMITS.md.
