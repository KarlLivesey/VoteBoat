# One full startup context diagnostic — INCONCLUSIVE

Exactly ONE parent-released execution of the existing instrumented startup
binary; no build, source/docs edit, relay, burners or extra competitors.
Input patch7bfa83cb786169884304c8203fc3d06c265d4fe17f65fa57dc3b9c9819817c8d
on base3aed5d671e735f7cd622b4ae4441a54ca5b3d5e7 remains byte-for-byte
unchanged before/after. Binary SHA256
ce00bcd2e5aec70c33cff47135157c3b239724f61b24113a4431232031d74d48 also unchanged.
See CONTEXT-STARTUP-PLAN.md, written before launch, and input/audit/terminal JSONs.

Result:56 passed /0 failed /0 ignored /0 filtered, binary rc0, aggregate rc0,
484.24s libtest (484.53s supervised). All56 original startup test names occur
exactly once, missing/extra/duplicate0. Absent follower, all other backpressure,
retention, restart and cleanup assertions passed; no acceptance change or retry.

Exact original/current executable argv is the same startup-d667aaa7d934dd31
path with NO arguments, filter, --test-threads, --nocapture or --show-output.
Original/current affinity13–15. RUST_TEST_THREADS is currently absent and was
inherited unchanged, with no new override; original runner also inherited it
without setting/unsetting it. Its literal historical value was not snapshotted,
so we do not invent one. Three concurrent test threads were directly observed.
Same relevant original environment: max4 Cargo jobs, incremental0, dev/test
debug symbols0, own root target and own target/scratch. No Cargo build launched.

No hang guard fired. Default libtest capture suppresses passing test stdout,
as in the original invocation. Consequently absent-follower phase/quarter
counts/last-Applied host age and instantaneous worker usage are NOT available
from this passing context log. Do not import isolated-run values as if they
belonged to this run. The unchanged lazy600s failure report contains all those
fields and authoritative persistence/snapshot/connector usage, but was not
exercised. Existing49152 Applied/window64/5000ms protocol/600s wall/term/budget/
checkpoint/cap/cleanup predicates all remain in the exact input and passed.

Classification: INCONCLUSIVE for the original full3aed FAILED2667/1/1.
This one context pass does not identify the original failed phase, whether it
had continuing real Applied progress at600s, a stopped clock/predicate, or
terminal cleanup work. No source cause, host-noise explanation or causal
repair/control is established. It does not justify clock/admission/yield/noop/
serialization/timeout changes, and is not qualification. No minimal cause-
specific repair can responsibly be selected from these observations. No
unchanged retry series or further context experiment is launched/proposed
as established proof; parent direction is needed for any further action.

Resource audit:331 sampled task entries, zero violations; controller/children/
all observed threads actual nice19 and affinity13–15. All test processes and
controller joined/exited; no owned executable/controller remains, including
deleted-cwd scan. Pre/post diskguardPASS: free2852GiB,main64/150GiB,shared2/120GiB,
live2/5,Linux242/300 and1/10GiB,Mac10/40GiB and2/3dirs. Own target2,003,886,080B.

The two original modified files remain the same uncommitted diagnostics-only
patch; published branch remains3aed. No source edits after the run. No new
strict checks, hook/push, source review, full configuration or main write.
Original failed source/log/audit/control history is intact. Further edits and
runs HELD pending parent direction; no integration lease exists.
