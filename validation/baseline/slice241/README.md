# Slice241 — recover the original planned configuration after role loss

Base672cc11502eefd4cbb6503ba955f72b627ec4f13. The replacement fixture previously
sampled a leader and asserted one configure reply. Its caller now retains the
unchanged provisioned plan and operation ID through exact current-authority/
known uncertainty replies, using the existing bounded leader_request path.
Successful replies must identify the original operation. A Joint receipt is not
Final: the caller observes finalize_requires_authorization and explicitly requests
the original trusted Final record. Completion still requires durable completed
status on every replica. CLI auto remains data-only; no production/provider/core/
format or deadline change. Wrong/conflicting/malformed and unrelated errors stop.

Actual TCP/QUIC histories establish an original handoff/campaign gate, lose both
followers, send the configuration unread, observe its accepted uncommitted Joint
and kill the leader. The native stopped store contains that exact operation above
its committed prefix. Reopened survivors elect another eligible voter; the first
request uses the previously sampled, now gated follower. Both old one-shot paths
fail NOT_LEADER. Both corrected paths preserve the original plan/operation through
Final on all replicas, data7 plus fresh3, original data duplicates and historical
handoff receipt. WAL/TCP and actual checked-prefix QUIC checkpoints survive cold
recovery, with the same terminal configuration reply and value10. Services/client
sockets are closed and workers joined. This selected accepted-prefix election
history is finite evidence, not a general rollback or distributed liveness proof.

Initial compilation calls an inaccessible checkpoint fixture helper; the existing
checkpoint_after helper replaces it before native evidence. before.log retains
0/2; after.log passes3 including exact classification refusals. All8 replacement
histories and the full194-test all-feature counter suite pass. The full default136-test counter suite passes sequentially afterward. Formatting/four strict profiles finish zero;
inventory108/conformance metadata retain9 partial reviews/68 operations.

Preceding239 run38071895206 is terminal: Ubuntu114270825119 passes counter190,
directory22 and transfer33; macOS114270824821 ends counter189/1 on original group7/
inc3 add42 delta5 with UNKNOWN authentication deadline expired. Later macOS targets
are unrun. These exact preceding-source results do not certify current241 macOS.
Original write/status-wait recovery and broader P0–P7 exits remain open. Functional
Linux/macOS features stay ahead of tuning/security, Windows/P8 deferred, goal active.
