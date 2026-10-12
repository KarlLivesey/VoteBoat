# Slice397: recovered checkpoint coverage qualified on Linux

The exact 1,306-input cut adds the immutable applied-frontier checkpoint predicate
and its real recovered-store regression. Production is unchanged from396.

| Original selection | Actual passed/failed | Seconds |
| --- | ---: | ---: |
| Maintenance checkpoint controls |2/0|0.09|
| Complete two-level cold history, TCP |1/0|48.23|
| Complete two-level cold history, QUIC |1/0|76.90|
| Same regression with exact old checkpoint predicate |0/1|0.06|

The negative restores only cluster.rs while keeping the new regression. It fails
at the actual `state.base_index() >= frontier` assertion after the fresh Claim.
This demonstrates the recovered snapshot false-publication boundary, not the
cause of old Mac394 final-close failure. Successful cold histories retain warm
and recovered original Read5, checkpoints/suffix/dedup, same closed stores/fresh
sessions and all 27 original provider joins/port and directory cleanup.

[Original logs and command/profile records](original), [negative logs and exact
old source](negative), [source scope](original/CAPTURE.json) and [checkpoint
patch](checkpoint.patch) preserve the actual evidence. Both library and fixture
Cargo records prove all features,opt 3,debug assertions and overflow checks.
No executable/library/cache is copied. Original source and artifact hashes were
checked read-only before and after archival, with no mismatch.

397 formatting/four strict Clippy checks and the initial wrong-cwd precompile
refusal existed only in tool output. [Transcribed execution evidence](transcribed-tool-evidence.json)
is explicitly retrospective; no contemporaneous raw log or timestamp is invented.
The refusal happened before Cargo/native launch. Corrected mutant build and its
single genuine failing native invocation are retained, never counted as zero.
The exact 885d572 push-hook source is included to inspect the four check commands.

[Manifest](original/SOURCE.json), [copy/hash map](COPY_MAP.json) and
[evidence fingerprints](evidence.sha256) identify this portable packet. Included
fixture sources explain acceptance/reclaim scope; the manifest covers all inputs.
No Mac397 success, whole current-repository acceptance, performance or deadlock
claim follows from this finite historical slice. Parent publication is separate.
