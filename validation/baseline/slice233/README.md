# Planned drain after source leadership loss

Base `9dfb2ffe57e35715d961964a2ab7af5adb1005bf`. Forced native TCP and QUIC
handoff between preparation and drain-run reproduces the original19701 admission
failure: ERR NOT_LEADER before a drain record exists. Separate before logs retain
both failures. The new planned single-group composition reuses the existing
group-drain journal/assignment/leader workflow, accepting a follower source while
preserving exact plans and the existing readiness and stop checks.

After checks require unchanged source plan, typed journal owner/sequence/operation/
phase/configuration binding, original values/retries, source shutdown and worker
joins. Single drain-run refuses more than one assignment before remote actions;
its128-request/45-second/5-second bounds are unchanged. Retained-replica admission
keeps its original handoff path. Planned status uses multi=true/groups=1 and one
bounded assignment row. No new provider, consensus protocol or journal format.

Final all-feature counter183/unit41 pass; default drain28/unit23 pass. Both
all-feature and native-only journal11/membership9 pass. Formatting/four strict
profiles finish zero; inventory108 and conformance metadata remain9 partial
reviews/68 operations. First broad counter182/1 fails the changed-plan refusal's
old label; its actual startup refusal is retained, and the final exact expectation
passes. `commands.txt` and source hashes identify the checks; published logs omit
absolute workspace paths and retain test results, errors and generated identities.

Preceding-source232 operator run38066100713: macOS job114253943898 finishes
counter177/4, with directory/transfer unrun. Configuration preparation21101,
group7/3 handoff40001 Busy replay, joint conflict-refusal assertion and trickled
socket WouldBlock failures are retained in a redacted public-job excerpt.
Ubuntu114253944056 remains live at observation; neither outcome validates233.
No candidate macOS, general fault proof or full P4/P0–P7 completion is claimed.
Next is functional route/platform recovery. Performance and security remain later.
