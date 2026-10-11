# Terminal original Mac delegation family

The preserved91576fa/653 build finishes21passed,44failed, zero ignored,4031.40s.
The wrapper records delegation_family=101 and verifies original inputs at terminal.
All44 captured failure bodies are mapped in mac-family-failure-map.json:

- 36 fail the same immediate requests1/0 assertion at unread_phase.rs:76 after
  Unknown(LeadershipChanged).
- 8 fail startup with AlreadyExists in the reused delegation history directories.

The259/260 controlled actual native negatives reproduce each assumption on both
hosts, and their corrected regressions pass. This diagnosis does not transform
the original failures into successes: the complete corrected67-history family
still needs fresh acceptance. Original failed stores and the independent original
full251 Mac run remain preserved. The parent scope retains its own strict checks;
these family logs add no new full-platform or performance claim.

Raw.sha256 hashes the input bytes; published.sha256 hashes path-normalized text
with trailing whitespace removed. The failure map classifies exact terminal
assertion bodies, not hypothetical runtime causes beyond those bodies.
