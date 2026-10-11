# Parallel functional qualification

Integrated source based on `11b57629`, preserving the ignored design pack.
The 694-input manifest covers the exact runtime/test/build source on both hosts.
Linux and Apple Silicon macOS each pass **810 affected test executions**:

| Configuration | Passed | Failed |
| --- | ---: | ---: |
| All features | 253 | 0 |
| Default | 231 | 0 |
| No default features | 97 | 0 |
| Native only | 229 | 0 |

Each runs complete Raft, member recovery, runtime, QUIC, transport and peers
targets. Formatting and four strict all-target Clippy configurations have zero
diagnostics on both hosts. Source guards match before/terminal; publication and
post-original-regression Linux guards also match. Mac uses the existing hashed
temporary ARM protoc helper. Feature configurations run sequentially per host.
The provider metadata checker passes 33 cases and retains 15 partial reviews /
98 operations. This is selected acceptance, not a clean full-suite certificate.

Three workers independently delivered timer conformance, old-checkpoint recovery
and clean-close ownership; root delivered interrupted reference comparison and
integrated qualification. Independent reviews found no concrete blocker.

- Native original WAL tickets/barriers and reference original Ready use explicit
  accepted/written/synchronized/completed phases. Eighteen leader-command cuts,
  six acknowledged-command restarts, held follower acknowledgements, own-image
  replay, original retries/reads and stale refusal pass. Deliberately false sync
  loses original op702 after power loss and reduces to the preserved schedule;
  the corrected host passes it. Reference images are Entry/HardState models,
  not serialized/torn physical bytes. Snapshots/arbitrary follower cuts remain.
- Verified old checkpoint plus real Final-to-Joint rollback uses 139 native
  append/sync/manifest fault schedules, actual recovered-policy ballots,
  original retries and real file reclaim/reopen. No policy fixture mutation
  substitutes for rollback. Accepted policy and volatile role are not receipts.
- Shared exact-owner/budget/order/refusal/lateness timer assertions run on
  independent host/native implementations and detect five defective hosts.
  Sequence exhaustion and arbitrary host execution remain unqualified.
- Production QUIC retains clean-close protocol work and its original timer;
  framed EOF retains that session until actual closure. Independent before-fix
  controls fail on outbound encrypted response progress and early owner loss.
  Tests preserve original sends/results/credits and unread input. They observe
  datagrams, not decoded proof of a specific CONNECTION_CLOSE frame.

The exact original checkpoint QUIC lifecycle test **fails 0/1 in 57.88s** after
Activate22. Unlike older 285/288, this image captures the actual remaining
provider: Closing, flushed, not protocol-drained, and deadline60926 at time57874.
Its original deadline is still 3052ms beyond the fixture cutoff. See
`linux-original-checkpoint.log`. This is retained evidence, not a claim that
older unobserved provider states had the same cause. Checking the fixture's
cleanup expectation is separate next work; no production timer was extended.

Preflight failures and worker concurrency observations remain archived. Focused
contract/type/visibility/helper fixes precede the integrated clean image; no
lint waiver or assertion removal. The zero-test initial original filter is
retained separately and is not counted as acceptance.

`runtime.patch` is the zero-context source delta over frozen296; apply with
`git apply --unidiff-zero`. The separate metadata patch records the obligation
updates. `raw.sha256` hashes original captured bytes, `published.sha256` hashes
the normalized text archive; README is excluded from the latter. No build
executables, archives, design-pack copies or credentials are published here.
