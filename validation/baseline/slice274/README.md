# Slice274: endpoint retry outcome qualification

The endpoint fixture discarded the existing recovery helper's accepted-unknown
flag and required every fresh final result to be nonduplicate. Mac269 reported
operation3 duplicate=true at that assertion on its older662-input image.

A controlled real TCP history reproduces the extracted original rule: a command
reaches the original native WAL uncommitted while peers are not polled; Campaign
produces an actual Unknown(LeadershipChanged); the original completion and credit
are released; fresh authority commits that exact request before retry. Its actual
duplicate receipt fails the original rule0/1. All668 before-source inputs verify
before/after this failure. This demonstrates the invalid caller assumption; the
original Mac269 emission did not retain its discarded uncertainty flag.

The shared private receipt assertion now permits a fresh duplicate only with
actual accepted leadership uncertainty from the existing proposal helper.
Known original retries still require duplicate=true, and operation/value checks
remain. No production, public provider, wire or persistence format change.

Two controlled TCP/QUIC tests each run WAL and checkpoint histories, retaining
original IDs/content, committed values5 then12, one then two outbox items, and
exact cold duplicates. The fresh operation4 client result also preserves observed uncertainty. Its
first apply receipt is recovered by replaying the captured replica’s actual
contiguous committed tail; the entire application checkpoint must equal the
live replica. Negative calls use those actual receipts and must
panic for an unexplained fresh duplicate and a missing required original
duplicate; caught expected panics are visible in otherwise passing logs. No
ClientUnknown or DurableLog object is fabricated.

Both hosts pass the original endpoint migrations plus controls: 6 all-feature
and 3 default-feature family checks, plus 2 all-feature and 1 default regression
checks, for 12 executions per host. Formatting and all four strict Clippy
profiles are clean; all 668 frozen inputs verify before checks, after the
regressions and at family terminal. Family times are Linux 35.16s/11.06s and
Mac 274.08s/120.56s in all/default order; these are validation runtimes, not
performance measurements. The initial source and matrices remain preliminary
local evidence; these published results qualify only the final manifest.
Native/core profiles exclude this TLS fixture and receive strict compile/lint
coverage. Raw and normalized artifact hashes are separate.

The imported-retirement timeout is the next concrete deliverable. Frozen Mac269/
262 and terminal old Mac251 failures remain separate; selected qualification
does not close the full platform/combined-fault/provider/P0–P7 gates.
