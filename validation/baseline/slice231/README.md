# Slice231 — shared buffer provider obligations

One test-only live-reservation, owner-registration and byte oracle runs unchanged
against the independent downstream HostPool and NativeBufferPool. Each executes
32 seeds of256 sequential actions plus deterministic boundaries using168 total
bytes/seven leases,24-byte/one-lease control headroom and two72-byte/three-lease
owner quotas. Production providers and public contract version3 are unchanged.

The checker independently predicts refusal and usage from held buffers and live
views. It checks stable declarations, default Bulk acquisition, exact byte/lease
limits, protected Control capacity, unbound/closed view refusal, invalid/rebound
owner refusal and reconnect sharing. Both Bulk and Control buffers retain owner
registration after original views are dropped; Control charges only total budget.
Exact final Drop permits registration reuse. Resize retains reservation and
credits, preserves prefixes, zeroes growth and cannot expose truncated stale
bytes. Oversize growth preserves contents. Mutable and immutable views agree,
and capacity stays within the owned reservation. Closing/dropping views does not
revoke accepted mutable buffers or close unrelated siblings.

The same checker rejects a deliberately broken provider reporting zero usage
while a buffer is held, and a wrapper filling newly grown bytes with255. These
negative controls target the live-reservation and byte-preservation assertions;
they do not simulate an operating-system allocation failure or concurrent race.
Existing native overflow/thread and transport-lifetime checks remain separate.

Validation at `source.sha256`:

- `provider-all.log`:26 shared provider tests pass, including the four new
  buffer tests (two providers and two broken-provider checks). An earlier selected
  pass is retained as `buffer-all-initial.log`; deterministic quota boundaries
  were expanded before the final full run.
- `integration-all.log`: buffer13, transport35, wire15 and QUIC15 pass. These
  retain native allocation overflow, concurrent reconnect/control quotas and
  actual buffer use across frame/transport ownership. No service-spawning test
  or unrelated storage/consensus suite is rerun for this test-only addition.
- `provider-default.log` and `provider-native.log`: buffer13/provider26 each
  pass. `provider-core.log`: downstream buffer6/provider11 pass with native
  disabled. Feature configurations run sequentially.
- `strict-final.log`: formatting and all four strict all-target Clippy profiles
  finish zero. No allowance, limit change or hook bypass. `strict-initial.log`
  retains an earlier all-feature strict pass. `git diff --check` and source
  hash validation pass.
- `inventory.log`:108 existing contract paths pass. `conformance.json`:9
  partial reviews/68 operations,99 contracts unreviewed. `conformance-tests.log`:
  all24 named metadata checks execute and pass. These reference-integrity checks
  do not establish the Rust behavior; the tests above supply selected evidence.
  The initial sandbox Node runner produced only a file-level one-test summary,
  retained in `conformance-tests-sandbox-summary.log`. It is not counted as24
  executed checks. The explicit permitted child-worker run supplies the final
  named results. A child-git formatting helper initially received EPERM; direct
  file input then preserved original metadata formatting without another child.

`docs/provider-conformance.json` now names all11 BufferPool/FrameBuffer operation
obligations, concrete assertions and remaining limits. Shared profiles assume
cloneable views with the selected host/native semantics. These finite sequential
traces and selected existing thread histories do not certify arbitrary providers,
every capacity, bounded callback execution, physical allocation/RSS, storage
destruction ordering, arbitrary concurrent lifetimes or system-wide shutdown.
Accounting identities and volatile leases grant no durability or authority.

Previous-source operator CI38063611738 at7a1f7102924608d9fea6ed70824c0bdc2c09623c
is in progress on Ubuntu and macOS at the saved `ci230-observed.json` observation.
It is not waited on or retriggered, and no current-source platform pass is claimed.
Original250ms P7, operator/platform/deployment and broader generated/combined
fault requirements remain open; the full P0–P7 goal stays active. The static
service remains usable while that work continues. Daybreak owns security review.
