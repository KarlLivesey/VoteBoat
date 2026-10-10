# Slice237 — fresh transfer readiness and observed cancellation cleanup

Base cee252df008f81ffedf78630f1c8e0df5e8dc20b. Test fixtures combine role discovery
with a fresh authenticated intent observation for metadata group1/inc1, checking
bounded encoding and exact query. Only empty stdout plus exact group-unavailable
CLI error permits reselection; malformed/authentication/wrong-query failures stay
terminal. After an accepted wait's client is killed/joined, status must show
actual pending usage0. Usage1 waits; duplicate/missing/invalid fields fail.

The existing wait is generic over its observed value. Its15s readiness and5s
cleanup guards are between probes, with each current CLI ask retaining its12s
budget. These are not new stricter end-to-end wall-clock guarantees. Production
CLI/read cancellation, provider APIs, core, storage formats and lifecycle rules
remain unchanged. Role/status alone is not a transfer or quorum acknowledgement.

Actual TCP/QUIC histories pause the sampled metadata authority, establish a live
replacement, then force the pinned first read unavailable. Old preparation fails2;
old immediate cleanup fails the independent usage1->0 script. Final selected5 pass,
including deadline/terminal/malformed checks and a real wrong-publication-query
refusal. Original profile/IDs/data, complete split/retries and final worker joins
remain. Full all-feature transfer33 and default24 pass sequentially; formatting
and four strict profiles finish zero; inventory108/conformance metadata pass.
Initial response-shape compile errors are retained with the direct Result fix.

Preceding source235: Ubuntu181/5, macOS counter186 pass then directory21/1 AddrInUse;
transfer unrun. Source236: Ubuntu184/2 ERR InvalidCommand on original19769; macOS
183/3 fails two planned-drain historical-target checks and sampled19901 NOT_LEADER.
Later targets are unrun. Source234's earlier macOS26/2 transfer failures select
this correction; native reproductions do not prove identical event timing.
No matching237 macOS/general P6/P0–P7 completion. Feature/platform work continues;
performance/security are later milestones.
