# Publication redactions

Local machine name, absolute workspace paths, mount paths, unrelated device
inventory and unrelated host memory/process details are omitted from published
logs. Originals remain in ignored local target/slice232-unpublished-logs/.
Source copies, patch, raw synthetic counter receipts, test counts, errors, build
results and workload hashes are unchanged. Benchmark data contains only generated
operation/group identities and timing measurements, with no user data.

Affected logs:

- ci231-ubuntu.log
- file-final.log
- host-before.log
- journal-file-initial.log
- journal-file.log
- legacy-file.log
- reference-gate.log
- release-build.log
- restored-bins-build.log
- restored-release-build.log
- services-all.log
- storage-core-all.log
- storage-filename-assumption-failed.log
- storage-legacy-open-failed.log
- storage-native.log
- strict-candidate.log
- strict-final.log
- strict-initial.log
