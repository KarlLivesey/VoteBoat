# Slice438 schema — bounded pending-table reserve slack

Selected before source editing. Baseline is published437 restoration407d9942,
whose non-test implementation is exactly retained435. Reuse immutable435
artifacts and profiles; do not recapture unchanged code. This is one admission
experiment, not a new prerequisite or a claimed improvement.

Evidence: Linux435 bulk map-insert inclusive333/6498 samples (5.125%), physical
self221 (3.401%); Mac insert inclusive826380/8564928 observed CPU microseconds
(9.648%), physical self597007 (6.970%). Hashing intersects those contexts and
remains unchanged. Profiles do not isolate probing, rehashing or allocation;
unknown/inlined frames and platform sampling biases remain. Proposal growth is
smaller and not selected. Saved lookup queries are in slice437/profile-contexts.

Data/API: change only the private matched Actor's initial HashMap reserve from
P=clients*4+1 to2P. Validated clients1..4096 gives P5..16385 and reserve10..32770;
no arithmetic overflow. Std may round bucket allocation and reports available
insertion capacity, not allocated bytes. Logical outstanding-original bound P,
RandomState, full128-bit keys, Reply values and every original channel stay exact.
No public/provider/production/native-service/wire/storage/lock/profile change.

Transitions/order: reserve once at actor construction, before warmup and timed
clients. Keep shape gate, whole-request logical budget, ordered insert-and-replace
on duplicate, proposal push, deferred request and peer priority unchanged.
Delivery still removes by exact full ID before send and stops at the first missing
or canceled result. There is no FIFO assumption, new token, watermark or generation.
No hot reserve, shrink, cleanup scan, delayed admission or altered checkpoint rate.

Ownership/failure/restart: same actor owns map and Senders. Duplicate replacement,
partial admission/delivery, receiver cancellation and actor drop retain existing
outcomes. Extra reserve allocates earlier under the existing allocation-failure
policy; no new fallible protocol action. Actor-local volatile state is reconstructed
empty on cluster creation; no migration/recovery format or durability change.

Acceptance: existing full128 IDs, arbitrary/reversed receipts, duplicate replacement,
shape-before-budget, full-bound rejection, partial rotation/churn, cancellation and
borrowed-completion/FIFO controls must pass. Record actual initial/high-water
capacity and tuple layout without equating capacity to RSS. Root production checks
can be reused only through exact source scope; matched formatting/strict and the
full enabled publication hook remain required. Publish a checked source slice,
build genuinely fresh guarded asserted artifacts, qualify Mac, then one rotating
three-trial bulk20M plus singleton diagnostic campaign on each host against435.
Keep raw failures/tails; do not rerun unchanged cases or tune another reserve factor
if the result fails. Retain only with useful both-host evidence and acceptable
singleton/latency/memory tradeoff. Reprofile a retained changed artifact when useful.

Expected limit: this may reduce probe/table-maintenance work. It removes neither
SipHash nor per-original channels and cannot explain all insert-inclusive time.
Extra table memory/cache pressure can erase the gain. No predicted rate is accepted.
Full P9 seven-cell margins, sustained memory/debt/refusals, durable applications,
headroom/fairness/placement and P8 composition remain active and unachieved.
