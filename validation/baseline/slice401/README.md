# Separate diagnostic clock401

Original chapter17 C12 requires diagnostic wall time independently of monotonic
time. This slice closes the previously absent public operation with
DiagnosticClock/DiagnosticWallTime and explicit native SystemTime sampling.
Consensus Clock/MonoTime and election timing stay unchanged.

Linux full runtime target: all54/0, default54/0, core37/0, native54/0,
run sequentially in Cargo's normal test profile. Native signed epoch/subsecond
and shared-monotonic-epoch controls:2/0. Formatter and strict all-target
Clippy default/all/core/native:0 diagnostics. See actual commands/results/logs.
Provider metadata integrity passes145 contracts/41 partial reviews/389 operations;
references establish no universal provider certification.

Matching Mac clock execution is pending after the separate398 native run hands
back the machine. Its pre401 binaries do not qualify this new API. No full moving
repository test matrix, release profile, or before/after artifact guard is claimed.
source_after_tests.json records only post-test implementation/inventory hashes.

Original design pack is unchanged; performance P9 and security P10 remain deferred.

Checker controls initially failed their stale exact review count (41 vs40);
updated counts and replaced last-row buffer selection with explicit contract
selection, then the actual direct Node checker run passes39/0. The original
failed --test wrapper log is retained; it is not a passing result.
