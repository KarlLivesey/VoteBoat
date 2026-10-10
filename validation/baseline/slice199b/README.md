# Slice199b — automatic reads skip draining replicas

The original Ubuntu operator job114158445225, run38033281575 at78382c0,
completed with121 counter histories passing and the QUIC retired-learner history
failing: an automatic read stopped on `ERR Draining` from the deliberately
reopened old source. `ubuntu-job.log` retains that job's output, with ANSI and
trailing whitespace removed only for repository hygiene.

The extended socket-level test fails before the fix with the same response;
see `routing-before.log`. Automatic reads now try the next configured replica
for that exact response. Direct node reads still refuse, writes are unchanged,
and malformed/extended responses remain terminal. No deadline is increased.

All122 counter-service tests pass after the fix in
../slice200/services-before.log, including both drain-retirement histories and
the positive/negative client routing assertions. The same invocation later
failed two new transfer tests for a separate fixture issue documented there.
This does not establish successful macOS or current full-platform execution.
