# Slice 421 implementation map

This reviewed map preceded implementation; paths identify existing propagation
joints rather than a line-stable final diff. Actual checks are in README.md.

## Reviewed source facts

| Path | Current role / change point |
| --- | --- |
| `src/native/startup.rs:58` | Add explicit ready_visit_items scalar beside NodeLimits; current config has no quantum selection |
| `src/native/startup.rs:533` | validate_stores pure configuration path used by static/member and multi; put early selected runtime validation here |
| `src/native/startup.rs:588` | legacy open validates config, then TCP bind; it bypasses prepare_with_options, so must not miss early checks |
| `src/native/startup.rs:836` | ordinary/member/rotation/discovery prepare_with_options validates storage/timers/identity/application before TCP/UDP bind |
| `src/native/startup.rs:968` | validate_storage_selection checks default-derived worker/shard envelopes before ordinary resource opening |
| `src/native/startup.rs:993` | startup_shard_limits(log,groups) starts from default visit16 and derives byte floors; common propagation point |
| `src/native/startup.rs:1181` | prepare_runtime selects ShardLimits, derives owner reservation envelope and registers all recovered cores |
| `src/native/startup/multi.rs:19` | multi validate checks whole exact group/app inventory before resource opening; selected group cap/quantum validation belongs here too |
| `src/native/startup/multi.rs:195` | multi prepare_as calls validate, then binds Socket and recovers files; it bypasses ordinary prepare_with_options |
| `src/native/startup/multi/storage.rs:52` | each recovered member Core retains configured_core features and current default BatchLimits64; no count change required |
| `src/native/startup/peer_rotation.rs` | public rotation entry points delegate existing preparations; inherit selection rather than add option overloads |
| `src/runtime.rs:222` | actual ShardLimits joint; default max_groups4096/group_items256/visit_items16/visit_ms2 |
| `src/runtime.rs:348` | per-visit item/deadline bounds and persistent Control,Control,Data,Background cursor |
| `src/runtime/ready_batch.rs:25` | original-count/byte/Core64/app/control-background gates; selected quantum is already honored |
| `src/runtime/effects/ready.rs:8` | adaptive owner reservation reduces selected prefix against current free/global/bulk space |
| `src/runtime/node.rs:91` | NodeLimits is replica-driver, peer and configuration routing; do not reinterpret it as ready-queue limits |
| `src/runtime/replica.rs:67` | poll steps128/effects256 are distinct per-poll limits |

No runtime-policy rewrite is needed. Existing root-owned batching policy and private
slice 419 representation stay untouched. Propagation and early-validation changes
are concentrated in native startup plus its current config literals.

## Benchmark files

- `examples/native_benchmark.rs:697`: existing main parser strips optional journal
  timing flag then validates fixed positional modes. Add one bounded flag parser
  before mode length checks; preserve old modes and reject unsupported lane use.
- `examples/native_benchmark.rs:123`: open_recorded builds real NativeStartup for
  each replica. Pass the same selection on Create and Recover, journal-on/off.
- `examples/native_benchmark.rs:793`: RunConfig carries requested selection; report
  settings with existing output at report_run, not an undocumented process env var.
- `examples/benchmark/shared.rs:240`: manual shared timed_shard independently selects
  default16. Propagate the explicit CLI choice here as well rather than print a
  requested value that was ignored by the actual assembly.
- Existing offered/maintenance/reopen closures must retain the same choice. Lane mode
  has its own CLI; reject this opt-in there initially rather than change multiple
  unrelated benchmark entry points. No timed campaign until source is qualified.

## Existing fixtures to extend

- `tests/startup.rs`, `tests/startup/log_limits.rs`: pure/no-resource refusal and
  early invalid limits, creation/recovery and actual file cleanup.
- `tests/native_member_startup.rs`, `tests/startup/multi.rs`, discovery and rotation
  children: verify every assembly branch gets the selection before resources.
- `tests/runtime.rs:318` and its singleton continuing-control fairness history:
  parameterize bounded1/16/32/64 controls; preserve exact fairness assertions.
- `tests/client_admission/ready_batch*`: real owner/app admission, per-original
  positions/receipts, app opt-out, budget-shrinking and Unknown/retry controls.
- `src/runtime/ready_batch/tests*`: bytes/deadline/Core count and protected-tail gates;
  use existing fixtures to exercise quota remaining after earlier control items.
- Existing native service TCP/QUIC and ModelIO controls provide actual durability/
  restart proof; test selection changes without removing maintenance or original IDs.

## Effort and decision

Reviewed API: one public NativeStartup.ready_visit_items scalar, existing literal default
updates selecting16, common helper propagation into ShardLimits.visit_items, pure
range validation before resources in legacy/ordinary/member/multi/enrollment paths,
and one benchmark flag threaded through startup/shared/recovery settings.
No new storage or scheduler implementation is necessary. The wider quantum's main
tradeoff is more original work per Data opportunity, increasing possible cross-group
and control latency until the existing count/byte/time/dependency limits yield.

The scalar leaves existing derived log/byte/owner floors unchanged. Full ShardLimits
exposure is excluded because it could imply hard ceilings that native startup
auto-raises. Node::from_parts/manual Shard assembly remains the full replacement
joint for hosts. Default16, Core64 and all disk protocols remain intact; measure
actual widths/barrier counts on both hosts rather than assume a selected batch width.
