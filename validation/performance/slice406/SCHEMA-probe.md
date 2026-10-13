# P9 native staging diagnostic schema

Purpose: isolate the cost of one native log transition while unrelated retained
groups increase. This is a volatile simulated-journal diagnostic with the real
native codec, transition validation and barrier path. It measures neither
physical durability, consensus throughput nor application throughput.

Shape: fresh output CSV, unrelated group count 0..32, fixed retained history
2..4096 per group, measured iterations 1..2000; separate 16-iteration warm-up.
The changed group is group1. Every group initially retains exactly HISTORY
eight-byte commands and commits HISTORY-1. Each measured transition replaces
only group1's last uncommitted command, then acknowledges one barrier. There is
no growing retained suffix and no full-state lookup within the timed region.

Ownership: one exclusive NativeLogStore over a shared observer containing a
bounded in-memory append buffer. Simulated sync records its length; simulated
manifest publication copies the real codec's manifest. Capacity is reserved
before timing, with an explicit native WAL ceiling. Mutation construction,
ticket checking, state checking, restart and CSV output are outside timing.

Transitions: create exact store/group identities; populate fixed histories;
warm up; replace and publish one suffix each iteration; verify every original
group exactly; drop the store; simulate loss of unsynchronized volatile bytes;
recover the native journal; require same store identity, incremented session,
exact group equality and refusal of a pre-restart ticket.

Failure/cleanup: errors stop the run before a successful CSV is published.
Fresh-path creation refuses overwrites. All state is process-local and dropped
on exit. No filesystem journal or simulated evidence may escape as actual
durability evidence. The only disk output is the explicitly labelled CSV.

Acceptance: small tests exercise bounded argument parsing, real native replay,
fixed retained histories and exact restart identity/session. Each timed row
must contain exactly one physical encoded append, simulated sync and manifest
publication; include their byte counts. Parent performs baseline/candidate
measurement only after checks finish, using byte-identical harness and equal
CLI cases, with raw CSV and explicit volatile scope.
