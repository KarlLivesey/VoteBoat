#429 exact zero-byte entry equality

Measured hypothesis: actual Linux428 Samply JSON/symbolication identifies
LogEntry equality inclusive52.65% of selected on-CPU sample weight, zero-byte
libc bcmp native52.62%, with owning update correlation24.31% and checkpoint
comparison22.15% nested inclusive scopes. These are sampled CPU weights, not
additive CPU/wall totals or a sustained capacity promise. The smallest candidate
avoids a byte comparator only where BOTH command byte vectors are empty.

Data/API unchanged: LogEntry still owns index/term/EntryPayload; Eq remains and
manual PartialEq exhaustively destructures every field. Cheap index and term
checks precede an exhaustive payload match. Noop matches only Noop, Configuration
uses the unchanged derived full ConfigurationRecord equality, and Command
compares the entire arbitrary OperationId then checks both vectors empty OR full
Vec equality (including length and every byte). Capacity does not affect equality.
No pointer/digest/revision shortcut, byte representation or outcome/channel change.
Adding any field or payload variant must make exhaustive matching fail to compile
until reviewed. EntryPayload and GroupLog derived equality remain independent.

No ownership, persistent format, transition, admission, failure, restart, cleanup,
fencing, durability/quorum, retention/checkpoint frequency or pipeline changes.
Raw matches_state and protected checkpoint verification remain exact; owning
mutable LogUpdate correlation still checks every original field/content. Release
profile gate, assertions and all build inputs remain unchanged.

Acceptance: independent tuple(index,term,&payload) derived oracle over deterministic
headers/arbitrary128-bitIDs, all variants and Configuration changes, empty vectors
with distinct capacities, equal/different nonempty bytes/length and symmetric Eq/
ne relations. Existing full-store/public composition controls must stay exact.
Fresh isolated source only; prior held427 implementation/tests are design material,
its incomplete checks are NOT qualification. Run affected all/core libraries plus
log_store/provider/snapshot/Raft/effect_owner, actual TCP/QUIC service controls,
matched harness, root fmt/fourstrict and standalonefmt/strict sequentially.
Approved socket escalation prevents sandbox-only bind errors. Keep every command,
original handle, exit, failure/log and before/after source inventory/patch. No
main/docs edits, commit/push, profile build or timing during this qualification.

Source-compatibility tradeoff: Rust's compiler-issued structural-match marker is
provided by derived PartialEq. A manual implementation no longer permits const
LogEntry values in structural const-pattern matches. Public fields and Eq truth
table/on-disk shape remain unchanged; ordinary Eq callers are unaffected. User
permits interface changes, and this narrow source compatibility change is explicit
rather than claimed absent. No new public helper or configurable equality mode.

Post-original-boundary codegen refinement: explicit #[inline] on the public
manual Eq method supports ordinary cross-crate embeddings without depending on
ThinLTO. It is a hint, not inline(always), and changes no Eq body byte after
removing that attribute. Existing optimized426 bitcode has no standalone Eq
definition from which its derived annotation could be recovered, so no stronger
actual-original-annotation claim is made. Preserve pre-inline broad386 library
plus201 owner/2 timeout/2 quiet-pass evidence; do not claim that broad scope ran
on the final exact file hash. Final focused primitives plus unrun all/core
integrations/service/matched and all strict profiles qualify the final source.
