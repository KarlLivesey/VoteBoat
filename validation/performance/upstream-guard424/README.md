# Pinned-engine guard-extended reference

ExactOpenRaft e88bb40707f2e511d8ecc44d31edcb1d814a11a8 source, same independent
196-package lock and default benchmark features. All1357 source inputs are guarded;
only bench.rs changes WAIT_TIMEOUT400→3600. The same constant also bounds optional
delay validation; actual default network/append/flush delays remainzero. This is
explicitly a guard-extended driver. Original400s source/artifacts remain unchanged.

Fresh asserted Linux opt3/debuginfo2/thinLTO/codegen1 library+driver compile records
are fresh:false, assertions/overflow enabled, actual source paths map into the
frozen identity. Build runner3353 exits0; before/after1357 hashes match. Copied
artifacts have exact original hashes and file modes. Small default three-voter
batch1/4 controls complete128 originals,128/32 samples after4 warmups with default
stats, no pacing/delay/config overrides. These functional rates qualify no floor.
Pinned driver's completion semantics remain; it does not add our shared driver's
post-clock all-follower verification.

Both setup failures are retained: nesting the excluded upstream benchmark under
VoteBoat's workspace failed before compilation; moving exact files to/tmp fixes
the layout without manifest changes. Initial artifactcopy omitted executable mode;
copying the original mode fixes permission without changing bytes. No engine,
store/network/workload change, dependency fetching or discarded timing occurred.
Full compiler logs remain locally hashed inBUILD.json; source/artifact/profile,
actual root compiler records, functional output and original failures are published.
No20M campaign, sustained comparison or healthy-margin pass is claimed here.
