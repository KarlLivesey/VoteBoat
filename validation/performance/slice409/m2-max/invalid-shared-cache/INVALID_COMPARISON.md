# Invalid paired comparison: shared Cargo cache selected reference artifacts

The first attempted96-case campaign completed every executable invocation and
source/hash guard. However, the candidate build reused the shared target
fingerprints: it finished in0.04s with fresh artifacts, and all candidate
executable/library hashes were exactly identical to reference despite different
production source. Thus these96 raw cases measured reference artifacts twice;
they are not candidate409 performance evidence. Preserve them as a failed
qualification attempt, not a zero-improvement finding.

Cause: both frozen source trees were prepared before the first variant build,
and sharing a target directory allowed Cargo to consider the second tree fresh.
Stable source bytes and copied-artifact hashes did not establish that Cargo had
actually compiled the second source tree.

Correction: new remote campaign directory; a distinct Cargo target directory
per variant; require non-fresh compilation of library and both examples; refuse
identical reference/candidate artifact hashes before any timed cases. Original
source snapshots and workload shapes remain unchanged. Do not mix these cases
with the corrected campaign's three interleaved pairs.
