# Slice246 — native shutdown retains the host clock

The shared native shutdown helper invented10000ms plus a fresh elapsed clock.
A running owner already beyond that time correctly refused TimeWentBack. The
helper now receives each caller's original host epoch. Cleanup still has its
separate unchanged10-second deadline. Fresh/recovered unpolled owners use a fresh
epoch consistent with their initial MonoTime0. No production, core, provider,
public API, stored/wire format, quorum, synchronization or timeout change.

The actual native regression starts an owner at20000ms, polls in that matching
domain, drains/joins, rebinds the exact TCP/UDP address, reopens the same directory
under a newer store session and drains again. Old fixed-epoch semantics fail0/1;
the newly passed clock argument is deliberately ignored for that before check.
An initial compilation missed three nested helper callers; those diagnostics are
retained and every caller now supplies its actual clock.

Clock-only Linux startup48/default34 and strict checks pass. Mac becomes40/8 and
31/3, with the new regression passing and TimeWentBack absent. Remaining native
failures involve maintenance/election/read assumptions. The disk-backed wire and
maintenance fixtures still selected50/150–300ms defaults, and shared groups50/
500–1000ms. These assemblies now explicitly use the declared throughput profile,
as do the service and Rust embedding recipe. Low-level defaults/custom-timer tests
remain unchanged. No assertion, requested voter, operation ID, data/dedup/recovery
history, caller deadline or synchronization rule is weakened.

Final Linux and Mac: all48 and default34 startup tests pass at original concurrency;
formatting/four strict configurations are zero. All8 changed file hashes bind the
Mac run to immutable9c952aa plus recorded clock/profile patches. Inventory108 and
conformance metadata9 partial reviews/68 operations remain limited metadata checks.
These are finite functional results, not statistical timing attribution or a
latency guarantee. Original raw logs and failed stores remain for diagnosis.

Mac245 counter191/5 and default135/2 remain separate; no production service behavior
changed here. Next is original configuration readiness/admission and bounded drain
runner progress, then required operator integration. Full P0–P7 stays active;
features/Linux/macOS first, performance/security later; Windows/P8 deferred.
