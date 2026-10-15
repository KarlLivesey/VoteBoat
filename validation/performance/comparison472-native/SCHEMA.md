# Native472 ordering comparison, before setup

Purpose: test whether the checked whole-poll pump removes470's source-identified
ordering restriction usefully. The primary selected-path comparison is retained
470 depth4 versus fresh472 depth4. Include both releases at default depth1 to
measure common-path regressions, rather than splice results from old campaigns.
This is the existing finite native campaign with four explicit arms, not a new
benchmark implementation or permission to retune durability/quantum/checkpoints.

## Workload and comparison shape

Reuse comparison470's controller/resource collector and committed current raw
validator closure (including the published snapshot-gauge correction).24 serial
cases: TCP/TLS and QUIC ×three trials ×four arms:
A=470 default1, B=472 default1, C=470 selected4, D=472 selected4.
Within each protocol use ABCD/BADC/CDAB. Each old/new pair stays adjacent; its
order reverses in trial2 and selected/default pair priority reverses in trial3.
This is three paired trials, not a fully position-balanced four-trial Latin
square. Report every pair and tail rather than rely on medians or claim a
universal causal effect from this finite campaign.

Every case uses one group/three voters,8-byte Counter,64 warmups,15000 offers
at500/s,window64,ready16,active2smaintenance,no pause and the same30s offered
horizon. Default arms omit append-window; selected arms use --append-window4
with the existing4unit/64original/1MiB limits. Same native asserted profile,
compiler/locks/fixture/validator settings within host; native opt3/debug0/default
LTO/codegen remains separate from the framework profile. Retain470 fresh artifact
268ea24412df55c7c8ca61d503b5d2a0614da196a1fcdeb601366d574e56e940; bind472
only after its published-source fresh root/executable build and actual controls.
No framework floor is inferred from these real disk-backed measurements.

## Ownership, failures and measurement

Each case owns a fresh absent disk-backed WAL root and separate process-resource
metadata. The original request retains its ordinary receipt/application/retry/
recovery lifetime. No replacement of a failed or slow case, no warm-case restart,
no hidden checkpoint/window/offer retuning. Record original stdout/stderr/exit,
partial offer ledger and terminal markers. Finish all workload processes before
CSV validation, analysis, source copies or archive work on that host.

Report completed originals during the offered horizon and through final drain,
service/intended/end-to-end latency, admitted/refused/not-proposed/unknown outcomes,
recovery/retry/worker joins, maintenance and per-replica storage/sync/publication
counts. Require actual Core/worker depth readback on all three replicas at both
create/recovery. Synchronized same-group sharing needs exact known dependencies
and nested1sync/1publication;0/0 and ambiguous calls remain separate. Ticket suffix
command counts include replica observations and are not client successes. Retain
missing/dropped/error correlations and overflow-bin limitations. Nested elapsed
wall times are not additive CPU; whole-process peak RSS includes warmup/recovery/
cleanup and does not establish a continuous physical-memory or debt bound.

Both releases retain the same observation path; grouping changes observer work,
so this measures instrumented end-to-end usefulness rather than observer-free
pump CPU cost. No claim that four queued units become one four-unit physical
barrier. The original470 failures/revalidation remain immutable and separate.

## Gates and decisions

Before timing: verify both source inventories/modes, original/copied executable
and root-library hashes, actual fresh:false/profile/compiler/lock records,
committed validator closure, exact24 argv/order, absent case roots and authorized
TCP/UDP socket preflight. Finish all build/control/copy/check/setup work first;
root grants one quiet window. Poll the original live controller handle and never
restart solely because observation times out. Collect only after actual terminal.

Afterwards: every valid case must pass exact outcome/conservation/recovery/retry/
join/maintenance/storage and selected/default readbacks. Compare all three C/D
rate/tail pairs and barriers per useful completion; A/B controls common-path cost.
Claim improvement only with useful measured benefit and explicit tradeoffs;
activation alone does not qualify. Keep failures, refusals, regressions and tails.
An ineffective change is rejected without another tuning family. Fresh framework
bulk/singleton and Mac qualification/measurements remain separate required work.
Full P9 stays active: all seven required cells on both hosts, sustained physical
memory/debt/headroom, durable applications, fairness/skew/placement/shared barriers
and P8 composition remain open. Three inherited owner timeout failures remain
outstanding and are not relabeled as passes by this campaign.
