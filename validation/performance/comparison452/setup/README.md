# Linux450 versus452 paired setup

Immutable450 source d6395be7ec56af68f346a72308e90a20200fb731 is the baseline, copied from comparison450's actual candidate CONFIG. Candidate is published5201bfeae8f891b8c3ea71aa75717c691a61fd50. Both source/mode/compiler/lock/profile/library/executable/DWP guards passed; actual452 three CLI outputs pass the unchanged checker. Prior checker/rounding negative controls are reused by exact driver hashes, not repeated. No new negative-control framework.

Prepared combined command: `bash target/p9/comparison452/linux-run.sh`. Three rotations each baselineOFF/candidateOFF/candidateON/OpenRaft: bulk12 cases requested20M/actual19,988,480 and singleton12 cases1,048,576, with1024warmups and unchanged timer/runtime/maintenance settings. Bulk then singleton serially, no interim processing. Timing has not started and awaits joint quiet grant. No improvement, retention or full P9 acceptance claim.

All files regular; SHA256SUMS covers each regular payload except itself. No nested manifests/binaries/source/buildtree/resultcampaign copies.
