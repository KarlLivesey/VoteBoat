# Slice458: bounded offered-workload group weights

Pre-edit schema, approved by root after Linux pilot32499 actually ended. This is a workload-fixture extension for explicit P9 hot/cold/skew evidence, not quorum weights or a production scheduling change. Existing uniform native445-to455 measurements remain usable. No performance improvement or fairness result is claimed by adding the input.

## Data/API and exact schedule

Add optional `--group-weights W1,...,Wn` only with `--offered`, with exactly GROUPS positive decimal weights and checked total<=256. Omission preserves the current CLI/default mapping. The private immutable schedule stores at most256 precomputed GroupIdentity slots, ordered by group then repeated weight. Measured operation `op` chooses slot `(op-1)%total`; the absolute phase includes WARMUP64, rather than restarting at the measured range. All-one weights must therefore exactly match `(op-1)%groups+1`. Warmup remains the existing uniform64 operations.

The CLI extracts only the new option, then preserves existing positional validation order (groups, protocol, count/window, offered rate, maintenance) before parsing weights and checking their offered-only constraint. Reject duplicate/missing options, invalid/zero/overflow weights, count mismatch and excessive total before root creation. Add no count/window/default/election/queue/ready-quantum ceiling changes.

Weighted preflight counts actual measured offers for each group at the documented absolute phase, adds that group's uniform warmup count and the existing64 control reserve, and compares with the unchanged default per-group log capacity. Do not apply the old uniform ceiling to skew. Omitted input keeps the old preflight unchanged; all-one weighted acceptance has the same maximum bound as the old uniform prefix.

## Ownership, transitions and failure/restart

RunConfig owns the schedule; the offered ledger borrows it for the finite run. Each row captures its selected group before admission, so pending tickets, history, application verification and failure persistence use the actual group. No offer or per-original outcome is removed. Retain unchanged global/per-group pending limits and their refusal order; weighted hot groups can therefore refuse while cold groups retain their own credits.

Warmup retries remain uniform. Recovery of the last successfully applied measured operation must retry its retained row.group, not reconstruct a uniform group. The no-applied-offer fallback uses the last uniform warmup sample's group. Audit every operation_group/retry call; normal/shared/lanes modes remain unchanged.

No new durable state or protocol field is introduced. The CSV already contains the actual group; run.txt and summary add canonical workload weights and the absolute phase convention when explicitly requested. Recovery reopens the same WAL/checkpoints and validates each group's actual applied history. Existing worker shutdown/join and partial ledger/error cleanup stay intact, including refusals and no-leader outcomes. Invalid inputs cannot create the root.

## Independent validation and checks

Extend the existing offered CSV validator with an independent cumulative-weight oracle (not the Rust lookup implementation). Missing weight metadata accepts historical uniform datasets; explicit metadata requires positive canonical decimal weights, exact group count, total<=256 and the documented phase. Verify every CSV row's actual group while retaining every existing operation/ticket/status/window/value/rate/latency/recovery assertion. Array counting independently checks weighted per-group preflight arithmetic against the unchanged default log bound. No quorum-weight interpretation.

Focused pure Rust controls: flag extraction/parsing/bounds; all-one and omitted mappings including groups not dividing64; deterministic skew and bounded absolute operation phase; exact weighted per-group totals versus literal counts; max-log boundary and hot-group overflow that uniform ceil would miss. Pure JS fixtures: historical uniform pass; weighted valid history; changed weights/phase/CSV group/value, excessive total/zero/mismatch and overfull hot-group mutants fail. No new live workload, broad suites, test/build or capture until SOURCE_DONE and root coordination. Existing zero fmt/strict requirements remain.

## File scope

- `examples/native_benchmark.rs`: option extraction, RunConfig schedule and pre-root budget validation; unchanged legacy path.
- `examples/benchmark/offered.rs`: ledger group selection, metadata and actual-group retry.
- Private `examples/benchmark/offered_groups.rs` and its focused tests: bounded immutable workload schedule, parsing and preflight only.
- `validation/check-offered.mjs` and `validation/check-offered.test.mjs`: independent schedule/budget and CSV controls.
- This schema. Root owns master plans, documentation, publication and timing grants. No production core/provider/public API/default change; no new runner or dependency.
