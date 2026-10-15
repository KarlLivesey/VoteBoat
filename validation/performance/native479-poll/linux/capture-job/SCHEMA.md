# 479: locate and reduce work inside a native ready batch

Before capture or implementation.478's paired ready16/64 histories establish
useful bulk barrier amortization on Btrfs and APFS, with explicit singleton,
moderate and cold-group regressions. Defaults remain unchanged. Linux bulk
leader barrier elapsed falls from roughly91% to65% of the native measurement;
Mac remains roughly85–89% under bulk/skew load. These nested wall scopes are not
CPU attribution. Linux ready64 max host polls reach26–35ms despite the nominal
2ms visit deadline. Host poll covers an entire fixture round over replicas and
progress; it is not one Shard visit, so the numerical difference does not prove
a visit overrun. A larger batch exposes work that must be understood before
raising batching limits again. This advances P9.0/P9.1/P9.2/P9.3.

## Existing shape and ordering

`ready_batch::window` selects a bounded already-ready prefix. The owner retains
every original, checks application admission, and constructs one owned proposal.
The supplied MonoTime visit deadline is cooperative: `collect` does not refresh
wall time between candidates. `check_ready_execution` constructs unapplied-log,
pending-router and selected-prefix iterators for each candidate; Counter rebuilds
its exact reserved-operation BTreeSet on every validation. It also rescans pending
receipts. Repetition is source-grounded, but its sampled cost is not yet known.
BucketCounter's history cloning is a separate, unmeasured application path.

First capture the current native Counter workload with existing476 artifacts;
do not change admission, proposal limits, defaults, checkpoint frequency, storage
format, syncs, outcomes or workload boundaries to improve the profile. A capture
is the smallest necessary diagnostic because the saved framework profiles use
the no-op application and cannot attribute this Counter admission work.

## Bounded headless capture

One TCP/TLS capture per host:8192 offered8B Counter originals at1024/s,
window4096, ready64, WitnessLogIo,64 warmups, three voters,2s active maintenance
and the existing120s drain guard. Linux selects the one-group bulk case where
the long host polls were observed. Mac selects the existing two-group31:1 skew
case, with2048/group fixture pending ceiling, because q64 one-group bulk is
offered-capped with max polls about2.6ms/whole-child CPU about.196 core, whereas
skew exposes roughly20ms/.4core and the cold-tail regression. Preserve actual
group-specific outcomes; an absent hot frame in offered-capped bulk would not
disprove backlog-sensitive work. These are workload-specific diagnostics, not
matched cross-host throughput or directly comparable sample shares.
Reuse exact published476 native
opt3/debug0/assertions/overflow/default LTO/codegen artifacts, source inventories,
compiler and same-volume placement. No rebuild or baseline recapture. Record
original command, environment, tool/artifact/source hashes and process ownership.

Linux uses the existing extracted perf with user-space `cycles:u`,1000Hz,
DWARF16KiB and128 mmap pages under unchanged kernel policy2/2/516. Mac uses the
existing samply `record --save-only --rate 1000` and saved JSON, with browser
opening disabled. Reuse matching available symbols only. Native debug0 can limit
inline/source resolution; preserve unresolved names, raw addresses and warnings
rather than inferring a cause. No installation, sudo, sysctl, browser or SVG.

Finish setup, archive work, publication and all competing host work before the
quiet recording window. Prepared -> Recording -> Actual terminal -> Conversion
retains the original handle; never restart for a polling timeout. On interruption,
terminate and wait for the owned child/profiler, preserve partial output and mark
failure. Convert only after both captures actually terminate. Own and stop any
headless symbol server if needed; do not start a browser to resolve symbols.

## Acceptance and next change

Validate original conservation, recovery64+applied, retries, joins, maintenance
and selected settings with the existing frozen validators. Profiled rates and
latencies are diagnostics, not unprofiled paired improvements or acceptance.
Read compact numerical self/inclusive/caller summaries from text, folded stacks
or JSON. Report sample weights, missing/lost/unresolved/truncated information,
whole-launch versus measurement boundaries and platform sampling limitations.
Do not add inclusive contexts or reinterpret sampled counts as elapsed CPU time.

Choose one bounded change from the largest remaining measured removable work.
If repeated admission is confirmed, sketch the exact batch/context API, borrowed
ownership, successful-prefix/error ordering, duplicate identities/content,
receipt bounds, cancellation, configuration/leadership transitions and cleanup
before editing. Keep the existing per-command behavior as an independent oracle.
If the samples do not support it, retain that finding and select the supported
ordering/batching stage instead; do not implement the speculation anyway.
No committed shortcut, speculative durability, rejected470/472 pipeline or461
fusion is revived by this schema. Existing public batch ceilings and all owned
byte/control budgets remain separate requirements for any later wider batch.

Publish checked evidence and each meaningful implementation slice through the
enabled zero-diagnostic hook; qualify fresh published artifacts and paired useful
rates/tails/refusals/fairness before retention. Btrfs/APFS results cannot close
the pending real ext4/XFS acceptance. Full seven-cell both-host sustained healthy
margin, memory/debt, durable headroom, placement/shared physical barriers and P8
composition remain required.8,422,500 originals/s is the framework minimum, not
a native Counter target, a ceiling or a reason to stop P9.
