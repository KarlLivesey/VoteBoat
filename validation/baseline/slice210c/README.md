# Slice210c — restore operator workflow context scope

Run38043992630 at45fa31a was rejected before creating any jobs. The new
runner.temp expression was placed in job-level env, whose allowed contexts do
not include runner. Move only TMPDIR to the operator test step's env, where
runner is supported. The test command, artifact path, matrix and deadlines are
unchanged. GitHub's context-availability table establishes the distinction:
https://docs.github.com/en/actions/reference/workflows-and-actions/contexts

`workflow-rejected.json` records the original failed run with no jobs.
`pre-push.log` records formatting and all four strict Clippy profiles passing.
Normal job creation after push must still be observed; this is CI repair,
not local QUIC liveness or platform acceptance. Slice210b's failed QUIC evidence
and the full P0–P7 goal remain active work.
