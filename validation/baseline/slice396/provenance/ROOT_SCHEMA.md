# Membership-drain cooperative host

Purpose: complete P4 startup membership-drain qualification. The remaining
QUIC failure in full startup395 is an unprinted post-close client outcome;
its cause remains unknown. Source review independently finds the same host
polling gap: after removing the source, the host waits for its shutdown while
its two still-serving voters are not polled.

Shape: reuse the existing private drain_wire_nodes_with callback. No new
production API or helper. Supply normal h.poll for the two disjoint survivors.

Transitions/ownership: retain the original durable Joint/Final, checkpoint
reopen, recovered draining state, election and source removal. Poll survivors
while the same source drains and its original dialer/log/snapshot workers join.
Keep one original10s cleanup clock and all final survivor shutdown checks.

Failure/restart: no retries, extra campaigns, fabricated outcomes, weaker
assertions or timer changes. Preserve operation19723 Applied and operation19722
original duplicate receipt. Print the actual outcome only on assertion failure.
No retrospective diagnosis of the old unprinted failure.

Acceptance: formatting and all four strict Clippy profiles zero; qualify the
complete startup target with all/default features sequentially and record
actual outcomes. Mac startup qualification follows the exclusive current run.
