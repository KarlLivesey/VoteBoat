# Resource correction, 21 October 2026

Initial focused runs requested `taskset -c 10-11 nice -n 19`, max two Cargo
jobs, CARGO_INCREMENTAL=0, own target only. Their processes had exited when the
parent resource correction arrived. Their kernel niceness was not sampled;
argv is not proof of actual19. This task observed inherited -4 in its own tool
helpers (and16 in another helper); relative+19 from -4 yields15. Earlier test
values cannot be recovered and are not retrospectively labelled actual19.
Historical N1c logs likewise retain their original requested-priority wording.

Only exact own-checkout cwd processes/threads were corrected; no other owner
was touched. `resource-correction.json` records those live tool-helper threads.
The focused launcher now sets absolute19 and affinity10-11, rechecks before
submission, verifies each child's kernel nice19 at launch and records resource
JSON per case. Its children inherit these values; two Cargo jobs remain fixed.
The same bounded focused default/all set is repeated under this launcher with
prefix `absolute19-`; no full suite or new workload is added.

A live audit additionally found the outer sequence controller back at -4 after
its start assertion (cause unverified). The per-case controller, Cargo and all
live test threads were already19. Corrected the outer controller again and
added a per-case reset. `resource-absolute19-live.json` records before/after:
four processes,13 threads, all actual19 and affinity10-11 after correction.
The launcher resets/verifies priority immediately before every child launch,
including lint/hook/push work, rather than relying on inherited shell priority.
