# Slice440 schema — compact lossless pending-reply key

Written before source editing. Retained source is published439 restoration
75f6093f with qualified metadata2585cf5d. Immutable435 artifacts/profiles remain
its exact non-test performance baseline.438 reserve expansion is rejected; no
reserve factor,436 wait representation,432 inline hint or420 lookahead is retried.

Measured stage: saved435 admission map insert inclusive5.125% Linux sample weights
and9.648% Mac observedCPU intervals; physical self3.401%/6.970%. Hashing intersects
these contexts and is retained. Actual key OperationId wraps NonZeroU128; key16
with alignment16 and Reply8 gives a32-byte tuple on the two qualified hosts.
The experiment removes its8 bytes of padding through a16-byte,8-aligned key.
These profile contexts are not instruction/probe counts or an allocator cause.

Data/API: private ReplyKey is a transparent two-u64 array holding every bit of a
validated OperationId. Constructor splits low/high halves; hash reconstructs the
same fullu128 and delegates to its standard Hash implementation. Test exact Hasher
method/byte stream and same RandomState hash_one output against OperationId,
including128 one-bit values, u128::MAX, same-low64/different-upper96 and repeated
IDs. Derive equality over both halves. No low-ID assumption, truncation, new
hasher, crypto/dependency change, unsafe/packed access, new nonce or digest.
Measure key/tuple size and alignment on Linux and Mac: intended bucket payload
24 instead32 bytes; available capacity and std allocation rounding stay separate.

Transitions/ownership: only private HashMap key type and insert/remove key
conversion change. Initial requested reserve stays P=clients*4+1, logical gateP
stays. Keep shape→whole-request budget→ordered insert/replace→duplicate error→
Proposal push, deferred ownership, first failure and exact receipt order.
Remove by complete ID before send, with the same missing/canceled first-error
and untouched tail. Original proposals, log/wire/application IDs and every
per-original completion channel retain OperationId. No public composition seam,
Core/provider/storage/format/profile/maintenance/queue/timer change.

Failure/cleanup/restart: same map owns and drops senders, with no new allocation
or persistent state. Duplicates replace the old sender before the same error;
partial admission/delivery, canceled and arbitrary/reversed receipts preserve
outcomes. Empty map reconstructed on actor creation. Key is only created from a
validated nonzero ID. No new durability authority, watermark or generation.

Acceptance: prove lossless full128 bits, equality, identical Hash method stream
and randomized hash output; actual smaller tuple layout. Existing replies/budget/
churn, borrowed completion/FIFO/error, bytes/receipts, client cancellation and
maintenance controls must pass; standalone formatting/strict and the full enabled
publication hook stay at zero. Reuse unaffected root checks only through exact
source scope. Publish the complete checked source, build genuinely fresh guarded
asserted artifacts, qualify Mac and run one rotating three-trial bulk20M plus
singleton diagnostic campaign on both hosts against immutable435. Every raw
failure/regression/tail stays; no type/layout variant tuning follows rejection.

Limits: less padding does not remove hashing, map probes or per-original channels.
Conversions, equality codegen or cache-line crossings can erase the gain. No rate
is predicted or accepted from layout alone. Native service performance is not
claimed from this private host change. Full seven-cell margins, sustained memory/
debt/refusals, durable apps/headroom/fairness/placement/P8 remain active and unmet.
