# Slice400: both complete default nested targets qualified on Linux

Exact frozen398 source,1,306 unchanged compilation inputs. The actual DEFAULT
Cargo invocation supplies no feature flags. Its two complete affected targets
ran once each sequentially:

| Original target | Passed/failed | Ignored/filtered | Seconds |
| --- | ---: | ---: | ---: |
| logical_nested_native |7/0|0/0|90.39|
| logical_activation_nested |8/0|0/0|91.66|

[Original commands, lists and logs](original) retain every actual selection.
Nested-native includes complete warm/cold27-owner paths, handoff, checkpoint
controls and selected-profile contracts. Activation includes Joint cold recovery,
both replacements, three cold import modes, first activation and profile controls.
All original business/read/authority/ownership/worker/channel/join/rebind/cleanup
assertions remain. Isolated test-temp is empty at terminal.

Actual [compiler artifacts](original/BINARY.json) contain ONLY default/native/tls,
opt3,debug assertions and overflow checks. The two fixture executables compile
fresh (Cargo fresh=false); the default library is cached (fresh=true), with exact
original398 default-library/rmeta hashes and compiler profile in
[cache provenance](original/DEFAULT_LIBRARY_CACHE_PROVENANCE.json). No fresh
library compiler invocation is claimed. Default no-run returned0 in27.47s.

[Formatting/four strict Clippy reuse](original/CHECKS_REUSE.json) is explicit:
original398 checks returned0 and every current copied compilation input matches
that exact checked source. No new lint run is invented. Before/after/terminal
source/new default executables/library/rmeta and original398 artifact guards0.
This directly executes the default configuration; all-feature TCP is not its
substitute. No broader tests, arbitrary provider permutations or Mac acceptance.

[Manifest](original/SOURCE.json), [copy/hash map](COPY_MAP.json) and
[evidence fingerprints](evidence.sha256) seal the finite result and provenance.
Binaries, libraries and caches remain outside this portable archive. Main source,
Git, Mac and root documentation were untouched by publication.
