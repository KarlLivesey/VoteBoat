# Slice466 immutable canonical entry ownership qualification

The committed base is `{{base_commit}}` ({{initial_inputs}} selected inputs).
Final qualification selects {{final_inputs}} inputs. This archive preserves
actual commands, stdout/stderr, compiler JSON, statuses, source modes and focused
source versions for the immutable EntryBatch/EntryHistory migration. See
TERMINAL_SCOPE.json for original handles and exact source/check boundaries;
ACTUAL_TEST_RESULTS.json extracts each log's original counts without relabeling
different attempts as one final-source passing run.

{{scope_notes}}

Entry batches are retained across pending effects, canonical Core history and
native accepted/durable histories. Ownership tests check actual LogEntry backing
identity, explicit replacement isolation, delayed and wrong durability receipts,
subset barriers, multi-chunk application as one commit batch, and full physical
backing/descriptor charges after partial prefix and repeated suffix cuts.
Borrowed contiguous application ranges and owned coalescing fallback preserve
the application invocation/error boundary. These are qualification scopes, not
a measured reduction in copies, a speedup, or physical RSS/headroom acceptance.

Failed compiler/fixture/capacity checks, sandbox socket refusals and subsequent
focused corrections keep their original logs, exits and source scopes. Successful
permission-qualified retries do not replace the initial failures. Reuse receipts
apply only to their exact stated unchanged sources and configurations. No test
counts or unexecuted checks are inferred from a source review.

Only changed/new/deleted-path source versions are selected. SOURCE_VERSIONS.json
records each version's path, raw SHA256, original physical mode and deduplicated
content object. Source objects are archival data with mode0600; their original
mode is recorded in that index. Full source trees, binaries, build trees and
generated WAL/storage data are excluded. Raw files larger than1MiB are losslessly
gzip-compressed; COMPRESSION.json records original and archived bytes/hashes.

Every copied inventory or nested manifest is an ordinary payload. The outer
SHA256SUMS covers every regular file recursively except the outer manifest
itself; verification rereads that exact set and every compressed/source object.
Historical absolute command paths remain unchanged. Fresh performance artifacts,
both-host paired measurements and sustained P9/durable/headroom/fairness/P8
acceptance remain separate work.
