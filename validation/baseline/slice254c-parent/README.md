# Slice254c parent — preserve original retry and store ownership

The original TCP/QUIC parent-offline WAL/checkpoint histories fail0/2 on Mac
at the first child operation1 write: Unknown(LeadershipChanged). Backtraces
identify IndependentChild::exercise after every metadata owner has shut down.
Use the existing four-attempt original-ID/content recovery helper and its accepted
uncertainty witness. Exact7/11 values remain; first duplicate requires observed
accepted uncertainty, and the deliberate next retry still requires duplicate.
Known NotLeader alone cannot justify a duplicate first receipt.

This reaches Mac1/1: TCP's final frozen-parent log comparison uses synthetic
Vec positions, although campaign may put another actual winner in position0.
Capture each Node's exact StoreIdentity before closing and compare its returned
full GroupLog with its own id-named files. Vector lengths must match. No search
for any matching replica, ignored fields or fixture poll-order change.

Final original parent histories pass2/0 on Linux and Mac, covering both TCP/QUIC
and WAL/checkpoint. Original cache/parent independence, keys/hints, bootstrap,
read/fencing/binding refusals, cold duplicate retries, stopped logs and joined
cleanup remain. No production/core/provider/interface/timer/quota/dependency change.
Formatting and default/all/core/native strict Clippy are zero on both.652 build
inputs verify. Earlier raw-outcome and wrong-owner failures remain archived.

Installed stable1.98.1 Linux and stable1.98.0 arm64/macOS27.0 conditions match254.
Mac uses child-shell4096 descriptors and a separate source/build from the ongoing
unchanged original251 full suite. This closes these two selected parent cases,
not original namespace/delete/other lifecycle failures or full platform acceptance.
Full P0–P7 remains active; performance/security/P8/Windows remain separate.
Raw.sha256 records original logs; publication normalizes task paths/trailing
whitespace. Source manifests retain before/first/final inputs. Final.patch is
zero-context relative to cb45344 (git apply --unidiff-zero). Failed Drop is not
proof of joins; successful histories use explicit drain/reclaim assertions.
