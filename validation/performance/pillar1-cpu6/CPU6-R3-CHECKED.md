# CPU6 R3 checked implementation — qualification pending

23 October 2026. Base `5fda857d2f827a47925c152f227bce7cbc1ced98`; production baseline `0bf9271ecb5a34c4089c6bb10381303232624170`. This finishes the existing CPU6 candidate and the explicitly authorized capsule-owner amendment. Gain UNKNOWN. CPU1–CPU5 stay rejected. Parent owns main and every subsequent qualification gate.

## Implemented shape and costs

EntryBatch has Empty/Inline(real LogEntry)/One/Many at the batch boundary. Direct Propose and exact singleton selection own Inline without an entry Vec. Canonical spans remain Owned(Vec) or Shared(Arc, range): installation moves Inline into the last owned piece or the necessary first canonical allocation. Canonical growth remains a cost; there is no descriptor per original. Independent pending, provider, outbound and application holders retain actual payloads.

The private CommandEntries wrapper owns the ProposalBatch's same Vec. The receiving core checks current role/fence/transfer/limits/count/largest/retention/conversion peak and checked index/term range before consuming bind. Wide binding assigns index/term and folds intrinsic facts in one traversal. Mixed prefix indexing remains a required second pass. Generic/decoded/recovered/configuration/edit input still inspects actual values. Existing spare-capacity shrink may reallocate; no universal zero-allocation claim is made. Legacy Vec<Proposal> conversion still creates staging entries.

Only the independent logical capsule changes to Protocol(Box<Message>). Each construction/decode owns a complete Message allocation; value cloning introduces another allocation; consuming moves its Message out. Authorization/frame/queue/publication compare borrowed complete Message values. Logical allocation/clone/decode work is introduced cost. Physical Message/Rpc/Pending remain inline. Wire/WAL formats, DURABLE release, queue lifecycle, limits, Cargo/locks/hooks/lints/toolchain and the original design pack are unchanged.

## Ownership and accounting ledger

| Owner/boundary | Charge and lifecycle |
| --- | --- |
| Inline batch | Actual nested retained payload; the fixed LogEntry lives inside the enlarged batch. Data/configuration classification and application slices inspect the real value. |
| Canonical log | One actual LogEntry plus payload for Inline installation; existing Owned logical charge and whole Shared backing/index charge remain. Empty ranges retain nothing. |
| Prepared wide backing | Original Vec capacity × sizeof(LogEntry), every retained payload, and the real mixed prefix allocation. Independent raw-owner ledger covers uniform/mixed widths 1/2/15/16/17/231/515/1024. |
| Message/effect/update/provider | Actual compiled enclosing sizes feed existing sizeof-based reservations, sole-message envelope, decoded storage and lease/pending ownership. Native accepted overlays and owned plans retain their existing independent ownership. |
| Release capture | Per capsule: sizeof(ExpectedCapsule) + sizeof(LogicalReleaseRequest) + 3 × sizeof(Message) + sizeof(Command), beside the existing four capsule-byte windows; plus typed Pending. Expected/request/child-command owners can coexist. Checked overflow and unchanged control/data reserves apply. This is conservative reservation, not allocator RSS. |
| Provenance | sizeof(LogicalSendProvenance) + sizeof(Command) + actual encoded-command capacity + full message_cost, including its boxed Message and nested backing/payload. It no longer subtracts Message as inline Command storage. |
| Logical queue | Existing message cost + full provenance cost + sizeof(AuthorizedLogicalSend) + 512 headroom, retained through dispatch/partial I/O until exact completion/cancellation. Existing conservative fixed overlap remains. Failed submit retains the original owner. |
| Capsule decode | Generic wire decode charges a Message slot, real entry Vec/payload/index. It pops that Message and drops the Vec before Box allocation, transferring the charged slot without overlapping two Message heap allocations. Frame decode then consumes the Box into the existing inline received owner. |

New reservations can reject inputs that the old smaller fixed shapes fit. No ceiling was raised to hide that difference. Exact-limit, one-byte-under and overflow controls exercise the affected paths.

Normal Linux compiled bytes: EntryBatch64, Span24, LogEntry64, EntryPayload48, CommandBytes16, Suffix80, Rpc96, Message272, LogUpdate288, OwnedNextLog720, Pending1168, Effect288. CapsuleBody64, Capsule432, CommandBody528, Command640, LogicalSendProvenance416, AuthorizedLogicalSend1072, CommittedLogicalAction416, ReleasePermit416, ExpectedCapsule864, LogicalReleaseRequest960, release Pending64 and EffectLease416. These sizes and larger transfers/register pressure are costs, not CPU proof; Apple observations remain pending.

## Actual focused evidence

[CPU6-CHECKED-MATRIX.json](CPU6-CHECKED-MATRIX.json) contains exact commands, separate child/observer/outer exits and raw-log/result hashes. The selected groups record 1,377 passing test events, not unique tests: 436 default/matched events; 938 all-feature/no-default/native-only events; three new boundary/checkpoint events. Two existing ignored exhaustive selections remain ignored. These are focused selections, not full suites.

| Feature/consumer | Actual coverage |
| --- | --- |
| Entry contents/facts/ranges/lifetime | Raw vector/fresh charge oracles; bounded subranges and widths 0/1/2/15/16/17/231/515/1024/max; sparse/high IDs, mixed/configuration/malformed/edit paths; exact double-ended iteration; held/drop/compaction/replacement/empty retention. |
| Receiver authority and persistence | Current/foreign/stale builder limits and index/term exhaustion; exact tickets/predecessors/incarnations; Written insufficient for application/quorum; owned-store and native accepted-overlay validation. |
| Public Rust Counter | Actual volatile and durable nonempty +1 commands at B1/B231/B515; exact all-three receipts/IDs/outcomes/applied prefixes; two 5000-entry checkpoint intervals, lagging snapshot install and continued writes. The public fixture triggers the ordinary checkpoint boundary; it does not claim matched retain1000 policy. |
| Normal matched embedding | All three normal Actor collection/application/maintenance paths, exact width statistics and original replies, two actual default5000 checkpoints on each replica with retain1000, lagging snapshot catch-up, continued writes and empty owned queues. Only cfg(test) fixtures changed. |
| Logical capsules/authority | Independent Box clone/borrow/move/drop and full high-ID payload equality at 1/16/231/515, generic/prepared identical bytes, exact decode budget, stale/altered representative/revision/generation/configuration/peer refusals; real proof queue credits, failed submit, partial I/O, exact flush/completion. |
| Quorums, membership and reads | Weighted/recursive/joint and learner/final rules, current read authority, logical Written versus consumed child-DURABLE release; TCP original client/read results, checkpoint/suffix restart, and three logical voters with nine native child owners. |
| Native recovery and codecs | Restart identity/hard state/suffix/commit; atomic interrupted replacement, short write/sync/publication fencing, torn configuration/WAL tails and snapshot crashes; entry-byte goldens and exact decoded memory/index budget; focused QUIC framing/credits/three-replica commits. |
| Lifecycle/FIFO/independent groups | Actual matched predecessor/partial delivery/client-drop consumers; retirement/transfer/merge/split lineage/retry/outbox controls; held fleet work does not stop ship/department, full IDs remain independent, dropped ship does not stop department. No ancestor-per-write/global counter was introduced. |

All seven strict checks passed with zero diagnostics: root fmt; locked/offline all-target Clippy default/all/no-default/native-only; matched fmt and its own locked/offline all-target Clippy. The actual enabled nonforce push hook and publication/cleanup identities are recorded separately in the terminal R3 receipt; this prepublication source record does not assert those later actions succeeded.

## Structural controls and incorrect mutants

Two compiled old representations remain VALID: restoring only actual singleton producer/selector Vec routes returns one allocated entry slot instead of zero, with equal content; restoring separate wide assignment plus generic freeze returns an additional width-sized inspection pass. Candidate uniform work is [width,0,0,0,0], mixed [width,0,width,0,0]; old uniform [width,width,0,0,0], mixed [width,width,width,0,0]. Observed downstream counters stay zero. These bounded visits are structural evidence, not CPU/allocator-count measurements.

Thirteen incorrect compiled mutants failed at their intended assertions: wrong index, term, mixed range, retained payload charge, stale edit seal, omitted Inline application slice, payload charge, held lifetime, boxed provenance owner, Inline data classification, capture fixed owners, decoded Message owner and queued proof charge. Full actual input manifests matched after every restoration. The first retained-charge mutant SURVIVED a dependent cached-charge comparison; its raw failure remains preserved. An added independent raw backing/payload ledger then killed that same error. The two valid old representations are not mutant kills.

## Failures, observation limits and pending work

R1/R2 patch/manifest/report evidence stays immutable and is hashed before/after. Exact R2 reconstruction matched all 27 changed/new paths and 2,608 actual build/consumer inputs before edits; 63,328 unchanged archived documents were not rehashed.

R2's unrecorded failing PID and ProcessLookupError cause remain UNKNOWN/HOLD. Compiler0 with observer/bootstrap1 remains incomplete. R3's own corrected handler records exact PID/TID/start/operation/path/raw errno before propagation and verifies typed absence or reuse; live/unknown/permission/malformed errors remain fatal. The finite exited-and-joined ESRCH control was allowed only with absence proof; the exact-live synthetic ESRCH control produced the expected fatal verdict. Both real helpers inherited the required resources and joined. These controls do not diagnose R2.

R3 preserved and narrowly corrected its own initial import/absent-target/record-key setup errors, a new capsule fixture's missing claim, helper visibility, test-helper lint limits, and old direct-consumer Vec/pointer/accounting assertions. The original pointer oracle accidentally selected Inline while trying to inspect a shared backing; it now selects the actual message range. No content, pointer-lifetime, byte-budget, authority or context check was removed. Read/inventory/path and truncated-display mistakes remain raw evidence, including a later guessed nonexistent command-bytes path. No unexplained product/resource failure was retried unchanged.

Known owned launches used cores0–3, absolute nice19, max4 Cargo jobs, incremental0, debug0/opt0/assertions true and one explicitly shared root/matched normal target below15GiB. The unchanged stock actual-Mac guard ran before required launches. Completed post-control Rust/check/control receipts had zero actual sampled resource violations/observation errors and joined children; the earlier own setup errors remain failures. Initial/read-only bootstrap coverage, T3/tool/short-lived/reparented/inaccessible gaps and historical HOLDs prevent a whole-task or whole-machine containment claim. The expected queue mutant panic left three exact source/PID/start-backed fixture directories, registered for terminal own cleanup; shared test ports remain untouched.

Macro review: the prepared-entry implementation and focused checks are complete; P1 gain/qualification remains UNKNOWN and P2/P3 performance work stays paused. The linked current/next-two plan remains in docs/IMPLEMENTATION.md. Parent still owns fresh dual exact-head Sol reviews, normal Apple mechanism/lifetime proof, the original nine paired primary CPU runs with EACH width median B/A<1 and at least2/3 wins, then only conditionally original64 canonical and full5/exhaustive relay/docs. No performance, Apple, full-suite, exhaustive relay, new research, subagent or successor was launched here.

