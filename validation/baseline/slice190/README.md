# Slice190 — executable recursive metadata lookup

Adds a bounded authority map and `voteboat-directory route`, composed through
public remote manifest discovery, cache and resolver contracts. Scope is a
route hint: no ownership activation, data write or automatic placement execution.
The9-process fixtures run three independently replicated metadata authorities.

Validation commands and terminal results are in the neighboring logs.
`initial-failure.txt`, `directory-second-failure.log` and
`counter-startup-failures.log` preserve failed checks and focused corrections.
The original QUIC unpublished-lookup failure did not print its actual error;
the diagnostic-only change and subsequent passes do not establish its cause.

No full-roadmap, current macOS or separate-host result is claimed. The existing
P7 latency gate remains unmet. The original baseline175 run is a different
source revision and remained live in the latest host observation; it cannot
certify these changes. Local source hashes identify the final validated files.

Final results:

- All features, Directory:13 passed,25.19s.
- Default features, Directory:11 passed,22.50s.
- All features, counter:65 passed,66.52s.
- Formatting and strict all-target Clippy default/all/core-only: zero diagnostics.
- Inventory:95 contract records, shape and paths only.

All-feature Directory logs include an earlier13-test pass in25.48s. The final
run also covers the simplified fixture key selection and closed-provider guard.
