# C12 separate diagnostic wall clock

Purpose: complete the explicitly required C12 operation from original chapter17
and component-contracts.json. Existing Clock supplies only monotonic time; the
inventory correctly admits the missing diagnostic wall-clock API. Earlier scope
limits do not satisfy this functional requirement. This advances P0 composition
and full P0–P8 closure, not performance or security work.

Shape: additive DiagnosticWallTime(i128) signed nanoseconds since Unix epoch and
an independent public DiagnosticClock::diagnostic_wall_time contract. Clock and
MonoTime/election_deadline stay unchanged. Native MonotonicClock implements the
additional trait using SystemTime only when explicitly asked for diagnostics.
No automatic conversions to MonoTime, new owner/thread or constructor resource.

Transitions/ownership: diagnostics are caller-sampled local values, allowed to
move backwards; no term/lease/quorum/durability authority. Existing clones retain
the same monotonic epoch. The host owns injection and provider lifetime.
A small private conversion is needed to handle pre-epoch SystemTime correctly
and deterministically test signed conversion without changing the machine clock.

Failure/cleanup: no queues, retained data or cancellation protocol; all system
Duration seconds/nanos fit signed128 nanoseconds. No core wall-clock reads or
fallback from consensus timing. Preserve exact current Mac398 source/job; its
research histories are not qualification of this new diagnostic API.

Acceptance: meaningful downstream double varies wall time backwards/forwards
while unchanged monotonic election inputs produce unchanged deadlines and never
read diagnostics. Native epoch/pre-epoch/subsecond conversion controls. Complete
runtime target all/default/core/native sequentially, formatter/allfour strict
zero; selected native/injection clock tests on Mac after398 terminal handback.
Update inventory/conformance obligations and current macro/mini plan honestly.
