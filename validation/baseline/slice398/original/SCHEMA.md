# Retired representative receive drain

Purpose: P8 repeated logical-voter replacement must keep every original transport
owned and progressing while its former peer shuts down. Three obsolete sessions
are retained after authenticated route retirement, but Lower.advance only polls
them and keeps their never-admitted received outputs until AFTER Middle shutdown.
LogicalPeerSession read stops with received output held. Native QUIC's bounded
stream credit can then obstruct later accepted remote sends. Actual Mac394
observer shows Node203 outbound3 and all other owners/lower bridges drained, but
has no retired-receiver state; it does not prove that this gap caused that run.

Shape: existing Retired { session, authorization } owns both the exact old
session and original RuntimeAuthorization. After each existing normal poll,
consume only its unadmitted received output with that stored authorization.
Reuse the exact take_received operation already used during final cleanup.
No new helper/API, admission, quorum proof, route or session replacement.

Transitions/ownership: route retirement -> same three original sessions retained
-> normal poll + discard never-admitted received frame -> original Middle/full
lower drain -> take_retired_for_close -> original channels/provider terminal
close/join/rebind. Do not close obsolete sessions early while Node203 is Running.
Keep their entire owned provider lifetimes and accepted send completion checks.

Failures/cleanup: authorization mismatch remains fatal; no fabricated ACK,
consumer result or success. Existing no-retired-send-output and full resource
handbacks remain. The host already owns this local cancellation responsibility;
there is no delivery to the current core or changed protocol/clock. Original15s
shutdown waits and provider timers are unchanged.

Acceptance: fmt/all four strict Clippy profiles zero. Original complete repeated
TCP/QUIC replacement histories must retain both genuine replacements, exact
Joint/Final/Outcome/Release/business/read results and all15 ownership handbacks.
Guard source/artifacts/profile and preserve every failure. Matching Mac uses a
frozen changed source after the current exclusive393/396/397 reservations.
