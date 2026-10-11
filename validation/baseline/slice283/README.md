# Forced unavailable-witness delivery283 — failed intermediate source

Member fixture paths add a process-local atomic sequence to avoid concurrent
wall-clock name collisions. The witness outage now requires an actual native
Failed send to the absent witness and drained outbound credits before executing
cancellation. A fresh context is queued after the witness store reopens.

Full member matrices fail on both hosts:48/all passed with2 retained-witness
timeouts;26/default passed with1 retained-witness timeout. Actual failed sends
and cancellation pass, and no directory collision occurs. Reopening the store
alone does not complete the authenticated connection; the previous disconnected
send expiry still applies until attach.284 corrects that host retry ordering.

Formatting/four strict Clippy profiles are zero, and all668 source inputs verify
before/terminal on Linux and macOS. Original budgets, error checks and cold
recovery remain. This is failed evidence, not acceptance of284 or all membership.
