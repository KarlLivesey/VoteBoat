# Parent shutdown diagnostic288 — failed Linux history

The original QUIC delegated split checkpoint history fails0/1 on Linux in67.79s.
Its tracked fixture diagnostic identifies the exact close_parent call at
delegated.rs723 after Activate22. Parent nodes1/2 are Drained; node3 is Draining
with clients, reads, application results, owner and outbound drained, zero
read/result usage and one retained peer connection to1. This narrows the failure
to peer shutdown without establishing whether roster, frame transport or secure
session is responsible. Original full285 reports the same retained connection.

Mac passes the matching isolated history1/0 in495.84s. Formatting/four strict
Clippy profiles are zero on both hosts, and669 input guards verify before/terminal;
the Linux publication source also verifies. The only tracked code change
is deadline diagnostics and close caller tracking. Production behavior, budgets,
shutdown predicates and protocol formats remain unchanged. The selected Mac
pass does not explain the failed Linux parent drain. Original Mac285 is still
running independently; no complete Mac lifecycle qualification is claimed here.

The separate temporary instrumented Linux289 image passes1/0 in40.44s, with
its original source manifest and terminal guard. Its observed protocol closing
states retain no send/write/datagram and reach close timers normally. Instrumented
timing does not explain or erase the original failed histories. Its logging was
never added to production. The next deliverable is a causal regression/fix at
the retained peer boundary, rather than another deadline or error waiver.
