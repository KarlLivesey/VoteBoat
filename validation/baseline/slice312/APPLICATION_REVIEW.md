# Slice312 settled-source review

Read-only review of `src/transaction.rs`, every `src/transaction/**` module,
`tests/transaction_application.rs`, the existing application trait contracts,
and Schema312/refinements in `docs/IMPLEMENTATION.md`. No source edits, builds,
tests, formatter or lint runs were performed for this review; root owns frozen
Linux/macOS execution evidence. No concrete correctness blocker or contradictory
completion claim was found within the declared application-only scope.

| Contract | Source and inspected evidence |
| --- | --- |
| Existing six traits | `traits.rs:35–159` implements StateMachine, BoundedStateMachine, ReadableStateMachine, BoundedReadableStateMachine and CheckpointStateMachine; `admission.rs:106–134` implements ProposalAdmission. Receipt identity/nested accounting uses ApplicationReceipt. Contiguous positive-term batches clone state, emit one ordered receipt per Command, skip other payloads while advancing the boundary, and publish only on total success. Public downstream tests exercise these contracts. |
| Bounded whole-transfer reservation | `admission.rs:24–94` compares immutable retained and distinct pending specs, reserves coordinator and both participant phase namespaces by destination, and rejects transaction N+1. Equal numeric operations in different destinations remain valid. Whole retained records contain every phase slot; no eviction exists. Terminal phases and identical retries do not consume another record. Public and internal admission tests cover pending duplicates/capacity and participant-destination aliases. |
| Locks, refusal and Abort | `state.rs:191–259` prepares without changing balances, changes a committed balance once while retaining its lock, permits Abort from Absent (including retained refusal), and releases only a lock owned by this transaction. A late Prepare after decision/application/release returns Closed unless its earlier Prepare outcome is already retained, in which case that original outcome is returned. Refusal tests cover another owner's lock, absent-peer Abort and late Prepare, insufficient funds and overflow. |
| Original positions and immutable assertions | `state.rs:84–101` returns the original phase index/outcome on identical retry; a changed full command assertion is an operation conflict and a changed spec is a transaction conflict. The enclosing receipt still carries this invocation's current applied index. `command.rs` checks expected fact group/incarnation, operation and nonzero position; admission rejects changed retained/pending assertion content. Internal controls change the fact index and spec amount and check no reapplication. |
| Boxed reads | `traits.rs:99–142` requires the requested boundary to be applied and performs immutable reads. A present status owns exactly one fixed-size boxed TransferStatus, accounted as nested bytes; bounds include the inline TransferRead plus that allocation. Account and absent status return zero nested bytes. Public tests check actual charge, undersized nested limit, future-boundary refusal and unchanged state. |
| Restore and suffix semantics | `codec/checkpoint.rs:78–250` checks schema, exact role/group/incarnation/profile/limits/opening accounts, bounded counts and framing. Reconstruction starts from configured openings, sorts retained first commands by checked unique local positions, validates role/spec/slot/bindings, reruns transitions and compares the entire rebuilt application with parsed state before publication. Thus lock ownership/current arithmetic and later balances are reconstructed without equating released historical balances with current balances. Applied gaps accommodate Noops/configuration and nonmutating duplicate/conflict invocations; no oracle balance/state is injected. Recovery tests cover active locks, duplicate commit after restore, later transfers, truncations and selected semantic mutations. |
| Safe P4 membership remains available | `admission.rs:107–115` validates the owning group and command, with no frozen configuration predicate. Deployment requirements report lifetime command/checkpoint bounds. No phase record binds voter ConfigurationId. Immutable own-group applied truth remains valid across the existing safe membership protocol; ownership/profile incarnations remain fixed in this slice. |

The capacity guarantee across three applications requires the documented trusted
driver preflight: each participant must provision at least the coordinator's
whole retained transaction capacity and contain the selected fixed account.
Slice312 cannot establish that cross-application condition by itself.

Canonical participant ordering is encoded by TransferProfile and used by the
internal fixture's indexed prepare sequence, independently of debit direction.
Actually sequencing scoped Node observations is still313, as are fresh quorum
read authority, ownership of original Node completion tickets and native
partition/restart histories. The internal phase bytes/facts validate structure
and immutable binding; they are trusted host assertions, not authenticated
foreign certificates. The application tests and module documentation explicitly
state that boundary. This review accepts no claim of native transaction
provenance, a complete usable distributed transfer, or all P8 completion.
