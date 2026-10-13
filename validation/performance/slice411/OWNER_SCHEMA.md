# Written append overlap: owner and worker integration

Purpose/P9.1: let bounded immutable ordinary leader append bytes reach followers
while the local barrier runs, preserving exact original durable quorum and result
contracts. One Core persistence dependency remains; no new outstanding local
revision, application success, vote or commitment is authorized by Written.

Data/API: keep public worker apply_to_shard/apply_to_timed and Raft::admitted
semantics. The physical EffectOwner chooses an explicit internal Written
continuation only after previewing Core eligibility/output bound. The logical
owner retains serialization. Active visits gain exact leased-effect byte charge;
queued early sends and terminal effects share their existing original Visit.

State: sole Persist lease -> worker request waiting -> exact Written admission ->
optional early sends while waiting -> exact Durable installs original terminal
continuation behind retained sends -> all effects/leases drain -> finish visit.
Core acknowledgements stay queued behind the existing dependency. Failed storage
fences immediately even if a downstream send lease is held.

Reservation: retain the original continuation allowance and reserve additional
early-output space before choosing overlap. If global/bulk space cannot fit,
admit the exact ticket without early sends and use the original serialized path.
Before installing a terminal continuation, count retained deque capacity, exact
leased bytes and the incoming allocation; preserve the lease ticket and order.
No event-wide retained terminal queue or coupling of unrelated worker units.

Failure/restart: no new persisted format/generation. Existing worker identity,
visit scope, exact ticket, fencing and restart reconstruction remain mandatory.
Core early request origin must retire on failure/suffix/configuration/session
changes and remain exact when the same bytes become durable. Held outbound sends
remain charged until transfer/discard; failure cannot invent successful results.

Acceptance: early follower persist while leader barrier held with no local/client
success; Written/Durable same poll; terminal completion while send lease held;
all original results after both durable dependencies; space fallback; wrong and
stale completion; failure after early send; queued term/configuration controls;
unrelated-group progress; original raw-worker compatibility. Run affected Core,
owner, worker and real TCP/QUIC controls, four strict profiles, Linux/M2 Max
qualification and paired attributed measurements before claiming a gain.
