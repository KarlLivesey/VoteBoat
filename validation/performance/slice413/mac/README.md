# Scoped 413 macOS qualification

The frozen 1349-input configured-delta 413 source passed formatting, strict Clippy for default/all/core/native configurations and genuine fresh:false compilation from its separate Mac snapshot. All three source guards matched 1349 inputs. SOURCE.json and SOURCE_SHA256.txt identify the tested bytes; PARENT.txt records the earlier parent commit, not the dirty snapshot's complete identity.

Actual all-feature scope: 530 tests across library, log_store, raft, snapshot, membership, member_recovery, log_owned and framework benchmark, plus one automatic hundred-group checkpoint/reopen control and one hundred-group worker/transport catch-up control: 532 passed, zero failed. Core scope passed 338 tests. The two selected effect-owner controls are native-gated and executed zero tests in core, with 190 filtered each. This is the requested affected scope, not the complete 840/595 Linux suite or the full effect-owner suite.

RESULTS.json records per-target counts and all command exits. The 45 original hashed files plus unchanged EVIDENCE.json preserve commands, logs, runner, source manifests and compilation proof. The fresh isolated target avoided stale-cache qualification. No timed 413 performance measurements were run or claimed.
