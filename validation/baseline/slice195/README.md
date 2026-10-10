# Slice195 — executable split recovery cuts

Base revision:97a26a7d2be6c974e856ca8a70f5fd6a18d8f5e9. RPL-1.5. Source hashes
identify the exact production/test inputs. The only production change adds a
payload-free local proposal-admission log line; no authorization, durability,
consensus, wire or persistence format changes.

The tests use `voteboat-transfer`, real TLS command connections and twelve
native replica processes per history: three each for metadata1, source20 and
targets21/22. The original generated profile and operation IDs200/201 remain
unchanged through every restart.

| Boundary | Expected next decision | Data serving |
| --- | --- | --- |
| Before intent | RecordIntent | Source only |
| Intent committed | Stage21 | Source only |
| First target staged | Stage22 | Source only |
| Both targets staged | Fence20 | Source only |
| Source fenced | Export21 | None |
| First import | Export22 | None |
| Both imports | Publish | None |
| Publication | Activate21 | None |
| First activation | Activate22 | Target21 only |
| Both activations | Complete | Both targets |

Every row checks serving/refusal, kills all role processes, reopens original
stores, compares the exact next-action response and repeats serving/refusal
checks. TCP uses WAL recovery. Before each QUIC boundary kill, the test captures
each group's committed prefix, waits for every replica to commit it, requests
each checkpoint and waits until each local durable base reaches that prefix.
These are checked checkpoint boundaries rather than assumed timer delays.

Before the fence and publication commands, both followers of the receiving
group are killed. The original authenticated command is submitted to its leader.
The test waits for Node proposal admission, kills the waiting client, then kills
and reopens all remaining processes. It observes the phase again and resumes
only the original operation when still pending. Publication payloads come from
authenticated completed observations and the public TransferOperation API.

Final checks stop metadata/source, read both children's original values, retry
original writes for duplicate results, write new operations and read back the
new values. Normal shutdown must report joined workers. Child-process guards
kill and wait on owned children when an assertion fails.

## Validation scope

`cuts.log`: both new phase/unknown-outcome histories pass in69.54s, covering20
phase boundaries and4 additional lost-reply cuts. All four unknown commands
were retained and completed during recovery in these runs; the alternative
discarded-command outcome is not claimed as observed.

`regression.log`: all3 original executable/authentication/accepted-read-disconnect
tests pass in20.26s after the fixture extraction. `fmt.log` and
`clippy-{all,default,minimal}.log`: format and all three strict configurations
pass with zero diagnostics. `inventory.log`:98 contracts pass shape/path checks,
not consensus correctness. Exact commands and source hashes are adjacent.

These are process termination histories at observed boundaries, plus admitted
commands without quorum acknowledgement. They are not physical power cuts or
exhaustive scheduling inside writes/fsyncs. They do not cover simultaneous
membership changes, arbitrary storage faults, recursive/retained/merge executable
profiles, later ownership retirement, macOS execution or separate-host operation.
They do not establish the full P0–P7 goal or the original P7 performance gate.
