# CPU8 causal controls

These patches change only the frozen candidate's named source files. Apply one
at a time with `git apply --unidiff-zero <patch>`; reverse with
`git apply --unidiff-zero --reverse <patch>`, then verify all normal hashes in
`../BUILD-INPUTS.json` before rebuilding the candidate. The raw executed patches
remain in the ignored evidence packet. Tracked patches use zero context so
blank context markers do not become trailing whitespace in an added patch file.
`FORMATS.json` binds each raw and tracked patch to identical normal/result
source hashes. All11 tracked patches passed `git apply --check --unidiff-zero`.
No compiled source, control meaning or whitespace-check setting changed.

`valid-old-work.patch` restores the exact original four production files from
`423966e8`, including the full next-summary representation, validation output,
core metadata accesses and canonical full-summary assignment. It passed the
same transition/Raft/native/public/all-width application semantics. It is the
VALID work-restoration control for the parent's Apple gate, not an incorrect
semantic mutant or a speed claim.

The ten `mutant-*.patch` files are separately compiled incorrect controls. Exact
commands and actual expected Cargo exit101 assertions are in `../CHECKS.json`.
They cover predecessor, delta count, retained charge, plain/clone summary facts,
ballot origin, full-ID/payload equality, ticket correlation, narrow provider
independence and FIFO placement. Every raw control restored exact candidate
bytes before the next launch. Parent Apple execution and restoration remain
required on its own exact prepared packet.
