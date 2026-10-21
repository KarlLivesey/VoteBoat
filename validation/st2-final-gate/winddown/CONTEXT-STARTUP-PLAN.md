# One full startup context diagnostic — schema before launch

Parent released exactly one context diagnostic, not a retry series or gate.
No source or docs edit: current complete git diff matches the captured
diag-absent-once.patch byte-for-byte, SHA256
7bfa83cb786169884304c8203fc3d06c265d4fe17f65fa57dc3b9c9819817c8d.
Published base remains3aed5d671e735f7cd622b4ae4441a54ca5b3d5e7.
The existing branch schema and diagnostics are reused without any code change.

Data/ownership/transitions: existing optional diagnostic metadata observes
election/write/drain, issued/applied/protocol progress, last real Applied host
elapsed/age and aggregate existing-predicate poll/gating counts. Existing
dump includes authoritative persistence/snapshot/connector usage. No new
request/socket/credit ownership bookkeeping, polling changes or per-poll logs.
The unchanged600s guard's lazy failure arguments call diagnostic_report()
and dump(nodes): phase/counts/progress age and usage will emit if it fires.
Cleanup/restart/partial-progress semantics and all acceptance predicates remain
unchanged, including49152 Applied/window64/5000ms protocol/600s wall/term/budget/
checkpoint/caps. No setup-noop, yield, admission, serialization or bound changes.

| Property | Original failed3aed full-default startup | This one context diagnostic |
| --- | --- | --- |
| Executable | own target/debug/deps/startup-d667aaa7d934dd31 | Same path, already built diagnostics-only input from previous released run |
| argv after executable | none | none |
| Affinity |13,14,15 |13,14,15 |
| Nice | audited actual19 | require actual19 for controller/descendants/all observed threads |
| RUST_TEST_THREADS | inherited; runner did not set/unset it or use --test-threads | inherited, verified currently absent; do not introduce an override |
| Historical environment limit | original audit did not snapshot this variable's literal value | no invented historical value; current value is explicitly recorded as unset |
| Libtest concurrency | normal default/inherited environment | normal default/inherited environment under same3CPU affinity |
| Output capture | default (no --nocapture/--show-output) | same; passing receipts remain captured, failing stdout is printed by libtest |
| Feature/build semantics | default features, unoptimized debug, incremental0, dev/test debug symbols0 | existing same diagnostic binary, no build or feature change |
| Cargo jobs / target / scratch | jobs4, own root target, own target/scratch | same environment, no Cargo launch needed |
| Environment setup | run-gate.py copies current environment then sets documented gate vars | same relevant setup; only audit label identifies this diagnostic |
| Competing work | exhaustive relay alone12; other startup tests run concurrently within binary13–15 | full startup normal within-binary concurrency13–15; NO relay/burners/extra jobs |

Acceptance/audit: guard before launch with own60GiB cap/global caps unchanged;
launch existing binary ONCE, no filter or extra libtest flags, synchronous
joined controller with resource scans. Compare all56 test result names against
the original startup log, retain every result/rc and aggregate failure if any.
Store executable hash, input patch hash, original/current argv/affinity/env,
actual resources and cleanup. A pass is inconclusive for the original failure;
no cause/gate clearance. On either result, report only observed classification
and minimal next proposal or inability to classify; hold all further edits/runs.
No main lease, main writes, new source repair, review launch or matrix reset.
