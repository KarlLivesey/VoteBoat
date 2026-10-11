# Profile B implementation schema314

Independent local types and transitions; oracle imports no production helpers or
model transition/recovery decoder. Fixed immutable command program (18 actions):
claim1, promiseA, exact X at parent1, commit1, metadata/bytes/publish/anchor of
parent snapshot0, tentative X at parent2, replace parent2 with exactY under new
suffix generation, commit2, metadata/bytes/publish/anchor of snapshot1, old-pin
reclaim, claim2, serve2. Ordinary parent indices remain1/2; child positions are
separate and advance per accepted action. Each admission captures original ID,
representative epoch, suffix generation and revision. Two parent cells have an
explicitly executed matching synced witness prelude for X/Y on two physical child
stores; only cell0's nested snapshots/interleavings vary in BFS.

Three physical cell0 stores track written versus synchronized contiguous suffix,
staged/published/anchored own child checkpoints. Child Commit is the declared
qualified ordinary-child consensus abstraction, requiring an independently
matched synchronized majority; Apply and caller observation remain separate.
Commit mutates no persisted marker. At most one power-or-process cut may
occur at the selected physical representative, not simultaneous loss of all child
runtimes. Surviving ordinary child authority retains the committed/applied prefix.
Fresh child quorum observation is explicit before any representative recovery;
selected own synchronized child checkpoint+suffix must reconstruct that boundary.
Missing own data/quorum refuses; canonical or oracle images never initialize it.
Child checkpoint opportunities are fixed at parent anchor0, staged metadata1 and
old-pin reclaim, with stage/sync/anchor interleavings on selected physical1 in the
graph and all three physical paths in controls. A recovered epoch2 service must
hold its own exact parent image/bytes after the committed claim.

Parent snapshots keep two slots; publish requires full child-backed bytes, anchor
requires published complete slot, and old pin is reclaimable only after replacement
anchor. Parent view recovers from selected anchored parent checkpoint plus suffix.
Lost output does not undo committed application. Exact original output can resolve
from a fresh quorum observation; stale owner/suffix binding cannot serve a newer
effect. Witness/history oracle records acknowledged X/Y and decodes physical own
checkpoints+suffix independently, comparing exact byte arrays rather than tags.

BFS bounds384depth/500000states/5000000accepted edges, with all cap/frontier flags
reported. Fixed program bounds workload, not a claim over every payload/RPC. Real
counterexamples execute all child write/sync/commit/apply steps: missing X body but
matching tag; parent anchor/reclaim after metadata-only new snapshot, compact into
actual physical child checkpoint, then cut/recover to expose acknowledged loss.
Safe counterparts refuse early action and then progress through full dependencies.

## Declared caller-scheduled refinement

The first graph exhausted its 500000-state resource allowance before terminal
reachability:2773215 accepted edges,11371744 attempts,depth89,26177 queued states,
zero terminals. Its source/schema/log remain in payload-unqualified; that graph
is unqualified. No safety oracle failure was observed before its cap.

The selected graph now requires every prior original physical delivery to reach
all three synchronized stores before the caller admits its next original action.
This workload scheduling rule applies equally to Admit and typed Submit edges;
it does not change Model.step or its two-store majority commit guard. Within each
round, commit/application/output/checkpoint/cut can still precede third-store sync.
Accumulated lag across multiple original admissions is excluded and remains a
native315/316 recovery obligation. Constants384/500000/5000000 remain unchanged.
The selected child-checkpoint schedule explores physical1; controls exercise all
three physical checkpoint paths. At most one selected power-or-process cut occurs
with surviving ordinary child authority; all-child simultaneous loss is excluded.
The parent term1/configuration1 workload contains fixed18 original operations,
two typed early submissions; control-only FillX supplies missing original bytes
through a new actual child command. Independent recovery checks include contiguous
parent commit boundary and parent snapshot boundary, distinct from child positions.
