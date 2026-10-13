# Fresh Linux matched426 artifact preparation

Purpose: compile published799f2cf once for subsequent paired425/426 measurements.
No timing/tests occur in this job. Each artifact retains original and copied hashes,
actual Cargo JSON and verbose compiler commands.

Exact archived tracked src/tests/examples/matched sources, build/config/lock inputs
and hooks; comparison and contract/conformance documents are supplemental evidence.
Whole inventory and22 changed source/input hashes guarded before and after build.
Source archive preserves Git modes; no build trees or generated state transferred.

Fresh target, stable Linux compiler; offline pinned lock. opt3/debug2/assertions/
overflow/thinLTO/codegen1. Inherited rustflags and compiler wrappers unset.
Require fresh:false root library/executable and pinned upstream openraft/bench_minimal.
Native profile is separate and untouched.

Failures retain logs, status and original runner handle; no restart on timeout.
Do not overlap builds/copies/checks with host timing.
