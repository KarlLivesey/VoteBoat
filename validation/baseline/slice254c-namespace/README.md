# Namespace fixture leadership recovery

Original ee38796 inputs: race TCP/QUIC passes2/0 on Linux and fails1/1 on Mac.
The QUIC failure is exact NotLeader at positive creation reservation10002, before
any intentional race fault. Fixing that call alone passes Linux creation10/0
and strict profiles, but Mac0/2 exposes the same NotLeader at manifest setup10001
and the explicit post-recovery decision retry. These failed bodies remain.

Review the positive namespace service/race/cancellation callers together and use
the existing four-attempt original-ID/content recovery and quorum-read helpers.
First data20000 must return Value(7) with its exact operation; a duplicate first
receipt needs observed accepted LeadershipChanged. A committed race winner must
always return duplicate; a new winner may be duplicate only with that witness.
Known NotLeader alone cannot justify it. Explicit/cold duplicate retries remain.
All deliberate raw unread submissions and negative admission checks remain, as
do two-voter commit/third-voter absence cuts, decision/status equality, competing
publication/cancellation refusal, readiness/activation, checkpoints, metadata
outage, complete frozen logs and successful explicit close/reclaim assertions.

Final affected creation family10/0 passes on Linux and Mac, including all eight
schedules per TCP/QUIC race. Formatting and all four strict profiles are zero
on both;652 final build inputs match. Inventory108 and13 partial reviews/81
operations metadata remain unchanged; these checks do not certify full providers.
No production/public API/persistence/protocol/timer/quota/dependency changes.
Linux additional deletion4/0 and created-namespace split4/0 pass; their Mac runs
are separate pending checks and are not certified here. The preserved original
251 Mac full run remains independent and live, with its old failures retained.
This closes selected namespace callers, not full P5/P6/platform or P7 acceptance.

Before/reservation/final source manifests bind each candidate. Raw digests record
original logs; publication normalizes workspace paths, trailing whitespace and trailing blank lines.
final.patch is zero-context relative to ee38796. Failed Drop is not a worker-join
certificate; failed roots remain. Mac uses the isolated repo252 source/build,
stable1.98.0 arm64 and scoped4096 descriptors. Linux stable1.98.1.
