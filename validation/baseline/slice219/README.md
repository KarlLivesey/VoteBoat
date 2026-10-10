# Slice219 — bounded same-visit framed session progress

NativePeerTransport previously polled session I/O only before plaintext work,
deferring newly queued output and released receive/stream credit to another owner
visit. It now follows actual plaintext progress with one session poll using only
the original remaining call/read/write allowance. Both results are validated and
summed. Zero/exhausted session call budgets add no poll. All original frame bytes
and actual local flush/QUIC ACK still precede Sent; original queue credit remains
owned until exact completion. Failure drops transport buffers before returning
the original Failed batch and preserves already validated input.

Actual encrypted UDP/frame checks use production default session/transport
limits and three distinct original group heartbeat messages/tickets. Existing
256-byte session fixtures retain their original explicit limit selection.

| Selected virtual owner profile | Before | After |
| --- | --- | --- |
| 1ms, three separate frames,50ms horizon | All3 delivered/completed by10ms | All3 by5ms |
| 6ms, three separate frames,50ms horizon | All3 delivered; only2 completed by48ms; fails original profile | All3 delivered/completed by30ms |
| 6ms, one existing multiplexed frame,50ms horizon | All3 messages/original batch complete by12ms | Complete by6ms |

This is a finite virtual-time encrypted loopback schedule. It does not measure
macOS polling intervals, wall-clock/network latency, sustainable capacity or the
original P7 fixed250ms p99 benchmark. No additional stream, ACK configuration,
timer, overall budget, public batching seam or ticket rewrite is installed.

Validation and retained failures:

- `framed-before.log`:2 pass/1 fail; `framed-after.log`:same3 profiles pass.
  `before.sha256` identifies the source/test state before the production edit.
- `host-before.log`:1 pass/3 fail. Independent buffered HostSession originally
  sees no follow-through, no second-call failure and no overreport refusal.
  `host-after.log`:all4 checks pass, including each counter dimension on either
  poll, exact remaining/summed budgets, zero/exhausted calls and original cleanup.
- `transport-all.log`/`transport-default.log`:35 pass each, including actual
  TCP/TLS, zero/small budgets, prefix/partial framing, shared lifetimes, closed/
  failed slots and218's independent transport ownership trace.
  `transport-native-only.log`:34 pass without TLS/QUIC dependencies.
- `quic-all.log`:15 pass; `connector-all.log`:15 pass, including selected actual
  loss/close/credit and core replication histories.
- `counter-all.log`:all166 pass. `directory-all.log`:18 pass/1 failure in TCP
  peer reload on manifest lookup deadline; retained unresolved. Its helper samples
  a leader and pins one endpoint; raw evidence does not identify which of four
  lookup phases expired or prove a stale sample. `transfer-all.log`:all21 pass.
  Service targets ran sequentially with the same all-feature executables. No
  complete local operator pass or diagnosis of that directory failure is claimed.
- `ledger-tests.log`:21 metadata cases pass; ledger remains8 partial contracts/
  57 operations and100 unreviewed contracts. Scope references are not certification.
  Inventory paths pass; final source/artifact hashes retain exact validation.
- Initial all-target/all-feature strict Clippy passes; final formatting/four
  strict configurations pass with zero diagnostics in `strict-final.log` through
  the unchanged enabled hook. No lint allowance or threshold change.

Preceding d5244df
[run38052027362](https://github.com/KarlLivesey/VoteBoat/actions/runs/38052027362)
is terminal: Ubuntu166/19/21 passes; macOS counter162/4 fails, so later targets
do not run. Raw logs plus transitional/terminal JSON remain. macOS failures are
assignment routing deadline, configuration preparing expiry before queued,
original19770 TCP setup NOT_LEADER and absent local TCP drain record at expected
Active observation. They do not contain this production change; inherited passes
and differences from earlier run counts do not prove current platform acceptance.

Macro progress: a reproduced P2 owner-cycle delay is corrected through the
existing public transport/session contracts. Current work is matching-platform
and retained directory failure diagnosis; next is exact administration caller/
preparation recovery, followed by the unchanged P7 profile. Full P0–P7 remains
active; wider security stays with the user's Daybreak work.
