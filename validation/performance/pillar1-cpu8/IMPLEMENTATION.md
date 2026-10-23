# CPU8 compact checked append and commit plans

This P1 candidate replaces the hot plan's complete next summary with checked
changed scalars and three summary facts. It retains the complete predecessor,
suffix ownership, one Pending, exact completion correlation, existing sole
in-place installer, stamp fallback and cold full plans. This is an implementation
candidate, not CPU acceptance or a complete Raft engine.

The authority is CPU8-DESIGN.md SHA256
`2bd007a6293f1ad8c1591086370b2ef164e006a27aaac955ba0986f1557d53f5`
and the bounded implementation release plus R2 addendum. Exact base:
`423966e882fbd163f35a6f61a62bea3b0e8fa1ea`; production baseline:
`0bf9271ecb5a34c4089c6bb10381303232624170`. The untested R1 patch SHA256
`4cb624e45cc93e29ec9547c393bdc103498bb0a8e0d456f196473c763c0bacd6`
reconstructed archival tree `2904f2c43b67d3f3a13a678a872706e24c4c6668`;
all 2,603 frozen file hashes, sizes and modes matched before refinement.
Original R1 evidence remains immutable and unqualified.

## Schema and boundaries

`OwnedAppend` holds `LogMetadata` predecessor, `AppendDelta` (revision, hard
state, ballot origin, commit, last index/term, count), `SummaryFacts` (plain,
retained bytes, clone stability), and its existing suffix. `CommitAdvance` holds
the predecessor, checked revision/commit and facts. `PlannedMetadata` borrows
those fields for core scalar reads. The original validation order and errors
remain, including ballot origin, suffix bounds, commit checks and exhaustion.

The installer preflights the exact stamp or complete predecessor before taking
the suffix. Actual canonical entries establish tail/count; checked outcomes
update the paired canonical summary, header and facts. The stamp advances once
and Pending clears only after success. Raw refusal changes nothing; public
failure keeps its existing fence and completed-prefix behavior. Restart creates
no persisted compact plan and still rejects old completion domains.

Native accepted overlays and prepared by-value boundaries still materialize
their required owned metadata and summaries. This retained work is not counted
as removed. Durable provider input remains independent; narrow normalized
entry owners and frozen shared backing/adjacency/charges are unchanged. No new
heap owner, effect representation, queue, lock, unsafe code or production budget
is introduced. Membership, snapshot, truncation and compaction retain cold plans.

## Independent controls

The direct paired-install fixture compares complete logs against the existing
full `stage_update` oracle and summaries rebuilt from actual entries. It covers
widths1/231/515, full128 IDs and bytes, plain/configured/checkpointed histories,
matching/missing/foreign stamps, append and commit. Equal-revision mutations of
configuration, generation, snapshot/base, ballot, commit and tail refuse without
spending the plan. Existing stamp exhaustion/renewal and public fencing fixtures
remain independent controls.

The added matched fixture uses the unchanged actual actors, public persistence,
application and snapshot helpers. For each width it checks all three applications
through two default5000 checkpoints, retained-entry ceilings, follower snapshot
catch-up, resumed writes, full128 reply IDs, executed proposal-width diagnostics
and joined normal shutdown. The smallest existing client setting that admits
each request width is used; no production capacity or workload limit changes.
Existing recovery/fault schedules separately exercise durable application replay,
deduplication, snapshot refusal/failure, quorum and configuration histories.

[The valid old-work patch](controls/valid-old-work.patch) restores the exact old production flow in four source
files, including predecessor+next summary, old validation output, materializing
core access and full-summary assignment. It adds no dummy work. Its semantic
checks and exact restoration, and the compiled semantic mutants, are recorded
in the ignored evidence packet. Apple must independently establish that this
valid control restores the targeted compiled movement before any timings.

## Layout, qualification and limitations

Linux x86_64 observed bytes: OwnedAppend480, CommitAdvance352, OwnedChange480,
OwnedNextLog480, Pending912, OptionPending912, PreparedOwnedLog672. Original
sizes are respectively672,640,672,672,1104,1104,688. Native prepared installation
is a different boundary from the staged plan. Existing production reservations
and budgets remain unchanged. Sizes alone prove neither executed copy removal
nor performance improvement.

Executed-check receipts and input identities are in [CHECKS.json](CHECKS.json)
and [BUILD-INPUTS.json](BUILD-INPUTS.json). Normal candidate checks passed:
38 transition tests,184 Raft tests,23 native accepted-store tests,11 public
volatile/durable helper tests,229 affected-feature tests across nine integration
binaries,42 native logical-voter tests,47 matched-consumer tests and one routed
history that stops all ancestors/source workers before driving two child groups.
The restored candidate again passed transition/Raft/matched checks. Counts are
per selected invocation, not a claim that every feature configuration ran a
complete test suite. Seven strict checks passed with zero diagnostics: root
formatting, default/all/no-default/native-only all-target Clippy, and matched
formatting/all-target Clippy with its own locked dependencies. The enabled
actual nonforce push hook must also pass; its post-commit receipt and final head
are supplied by the publication packet and PR body.

The valid old-work control passed38 transition,184 Raft,23 native accepted,
11 public helper and the all-width application control, then restored exact
normal source hashes. Ten compiled mutants each failed an independent semantic
assertion (Cargo exit101): revision-only predecessor, wrong count, undercharged
retention, wrong plain/clone facts, lost ballot origin, omitted full ID/payload
comparison, wrong completion correlation, aliased narrow provider owner and
misplaced FIFO commit. Their patches and exact test commands are saved; none
is present in candidate sources.

Original R2 faults remain in ignored `cpu8-builder-r2`: a navigation read used
the wrong parent path; another used a nonexistent inline-test filename, and a
helper named `inspect.py` shadowed Python's library during that traceback (the
owned helper was renamed). New fixtures initially used derived base accessors
as fields, accessed private diagnostics and omitted an explicit integer type.
The first all-width run used a one-client result budget for a231-entry request;
the fixture now selects the minimum existing client setting, with production
admission unchanged. The first valid control passed37 semantics but failed a
new size probe that incorrectly applied the staged672 bound to the original
native prepared688 type. Per-type measured baselines corrected that probe;
no production budget, hook, lint or guard threshold changed. Publication's first
staged whitespace check also stopped before commit on blank context markers in
three saved patch lines. Tracked control patches now use verified zero-context
format, with identical changed-source hashes; raw executed patches and the
original staged failure remain unchanged. Original failing
sources/logs/returns and exact control restorations are retained. Several early
displays truncated; critical source/contracts were reread in bounded ranges and
the full frozen manifest was checked mechanically. R1 faults remain separate.

Parent gates remain dual fresh exact-head source reviews,
Apple compiled-work/valid-control/restoration, original9 pairs at all three
widths, conditional64 canonical, exclusive full5 and integration. None of those
gates is supplied by these Linux focused checks. Gain and CPU7 attribution remain
UNKNOWN; all historical resource/identity HOLDs remain unresolved.

R2 uses verified `/usr/bin/cargo` and `/usr/bin/rustc` proxies (stable1.98.1),
nice19, cores0–3, max4 Cargo jobs, incremental0 and only its own target (15GiB
ceiling). Stock guard SHA256
`649f0e53f3cc3b3f9456feb1f600fd173131e74b75a50bb45843738a9075abd9`
and its Mac-inclusive checks/caps are unchanged. No Mac build/write, timing,
profile, full suite, subagent or successor is authorized here. Sampling does
not cover inference/tool/T3/ancestor overhead or every short-lived/reparented/
inaccessible process, and clears no historical failure.
