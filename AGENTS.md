# VoteBoat implementation

Follow `VoteBoat-Design-Pack/AGENTS.md` for all production implementation.
Markdown in the design pack is the design source; preserve the original pack.
Implement bounded, tested vertical slices. Record progress, differences and
actual validation in `docs/IMPLEMENTATION.md`. The public package is RPL-1.5.
Do not claim a complete Raft engine from the durable voting slice.

Target macOS and Linux; Windows is deferred. CI provides background feedback,
not a required merge check or a reason to stop implementation. Run relevant
local checks and keep making progress while remote CI runs.

Before each slice or material redesign, sketch a small schema plan: data/API
shape, state transitions, ownership and failure/cleanup paths, and the acceptance
checks. Check the existing contracts and likely restart/partial-progress cases
before editing. Use failed checks to identify the cause and make a focused fix;
avoid cycling through broad rewrites or repeating unchanged verification.

Prioritize a usable networked service and Rust embedding before completing the
full roadmap. Keep that full goal active, but do not make online membership or
split/merge prerequisites for a usable static-membership release.
