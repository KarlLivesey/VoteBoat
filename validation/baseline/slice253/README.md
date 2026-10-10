# Slice253 — controlled native QUIC input readiness

Native production, public interfaces, dependencies, persistent/wire formats,
session timers, resource budgets and flush/Sent gates are unchanged. This change
corrects test fixtures which advance virtual time while real UDP input has not
yet arrived. Optional owned socket observers peek without consuming packets and
check actual source addresses. They close their own handles on Drop; native
session polling still consumes and authenticates the input. Ordinary pair
construction requests no observers.

For the controlled raw/frame schedules, observe peer input only after an actual
native UDP write, before the peer visit/next virtual tick. Original poll order,
8-call budgets,6ms/50ms coarse and1ms/150ms fine horizons, payloads, tickets,
returned credits and final acknowledgement assertions remain. The fixture waits
at most two physical seconds per observed arrival; this explicitly changes the
physical input condition and supplies no physical latency guarantee. Bounded
per-tick traces format after execution, and can perturb physical timing.

Foreign-input and handshake-byte-ceiling fixtures establish actual arrival before
their original single native poll. The foreign1200 bytes preserve absent binding
and10500 timeout. The ceiling case requires actual emitted bytes above128 then
requires exact HandshakeTooLarge and absent binding at MonoTime0.

| Focused all-feature QUIC target | Linux | Mac |
| --- | --- | --- |
| Unchanged isolated source |15 passed|14 passed,1 failed|
| Foreign arrival correction |15 passed|13 passed,2 failed|
| Traced coarse selection |3 passed|3 passed|
| Traced whole target |Not run|14 passed,1 failed|
| Controlled delivery before ceiling arrival correction |15 passed|14 passed,1 failed|
| Final accepted source |15 passed|15 passed|
| Final formatting/default/all/core/native strict Clippy |Zero diagnostics|Zero diagnostics|
| Final build inputs |651 verify|651 verify|

The completed original251 Mac QUIC target failed13/2. Its target-only extract is
retained; the enclosing full suite remains live and is not claimed terminal.
The failed trace shows send18/read24 and send42/read48 with acknowledgement
deadline73 outside the original50 horizon. Native flush correctly remains false.
This identifies an unmodelled physical input schedule, not a kernel diagnosis.
No old unobserved delivery schedule, broad platform success or P7 gain is claimed.
Mac arm64/macOS27.0 uses installed stable Rust1.98.0 and child-shell4096 file
descriptors. Linux/Mac focused runs use isolated source/build directories, separate
from original251. Mac routing failures in that full run remain independent work.

Final-source.sha256 binds651 build inputs (same contents on both hosts); sourcebase
records c585f8d. Earlier source manifests bind retained candidates where available.
Final.patch is zero-context: use git apply --unidiff-zero; reverse checking verifies
the candidate installed change. Logs normalize task paths/trailing whitespace;
raw.sha256 records original unnormalized local log digests. No growing whole-suite
log is archived as a terminal result. Full P0–P7 stays active.

Reproduce in a socket-enabled environment:

```sh
cargo +stable test --locked --offline --all-features --test quic
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.mjs
```
