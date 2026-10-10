# Bounded recursive-lookup authentication attempts

Base: `71da217`. RPL-1.5. Local results are Linux only. `source.sha256` records
final source, manifest and test content, including the new test module.

The route client used a1.5-second connection-attempt deadline inside its original
ten-second invocation budget. An expired authentication attempt incorrectly
terminated the whole lookup, including when a stale manifest floor required
further bounded observations. The exact authentication timeout now releases the
socket and returns to existing endpoint rotation if time remains. Overall expiry
is terminal; other authentication failures remain terminal. Poll errors retain
the recursive-lookup context and underlying reason. No deadline is enlarged.

Two regression tests hold real TCP listeners without answering TLS. They verify
that the socket closes, no active source or observation appears, the original
deadline is retained, and a full-budget expiry remains terminal.

| Evidence | Actual outcome |
| --- | --- |
| `deadline-build.log` | Initial test compile failed: ManifestObservation does not implement PartialEq. Assertions were corrected to match the error variant; no production type was changed. |
| `deadline-before.log` | Both regression tests fail against unchanged production code. |
| `deadline-after.log` | Both regression tests pass after the fix. |
| `directory-service.log` | Full all-feature directory target:12 pass,1 fail. The original root-floor history and wrong-TLS-identity rejection pass. A separate direct QUIC manifest lookup times out after checkpoint recovery. |
| `manifest-recovery-diagnostic.log` | That exact failed history passes on a focused rerun, after failure diagnostics only. It remains an intermittent unresolved observation. |
| `pre-push-checks-final.log` | Formatting and default/all/core/native-only strict Clippy pass, zero diagnostics. |

Commands:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-directory route_discovery::tests:: -- --nocapture
cargo +stable test --locked --offline --all-features --test directory_service -- --nocapture
cargo +stable test --locked --offline --all-features --test directory_service directory_publication_remote_reads_and_checkpoint_recovery_quic -- --exact --nocapture
.githooks/pre-push
git diff --check
```

The full failure invokes `directory_client::lookup`, which does not call the
modified recursive route code. The new diagnostic includes its chosen node,
process output and all service logs if it recurs. A focused pass is not a
full-target pass and does not prove the intermittent cause is repaired.

`macos-job.log` is completed job114159916607 from run38033786328 at01d5190,
with ANSI escapes and trailing whitespace removed. It predates71da217 and
records115 passing and seven failing counter-service histories:

- Assignment retry: empty output, as in the previous macOS run.
- Replacement drain: expected readiness preparation never appears.
- Group membership: a grouped write returns LeadershipChanged.
- Group leadership: a stale leader refuses the next move-leader command.
- Grouped data: a grouped write returns LeadershipChanged.
- Multi-group runner: final read finds no eligible leader within its deadline.
- Interrupted multi-group runner: the expected partial joint cut is not reached.

The three grouped caller paths were subsequently corrected in71da217. This old
result cannot establish those fixes on macOS. Its current run38034032131 has
Ubuntu job114160726976 and macOS job114160727069 in progress at final inspection.
No full-platform or P0–P7 completion is claimed.
