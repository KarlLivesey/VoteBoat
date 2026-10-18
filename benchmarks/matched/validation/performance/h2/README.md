# H2 ready-snapshot collection: quiet-Mac evidence (18–19 October 2026)

Main `30a1f6c5` against H2 `65007ae2` (harness-only; `c61a2ba6` on main between
them changes AGENTS.md only). Apple M2 Max, idle on two checks 15 s apart before
the run (`environment.txt`). Release builds from `git archive` of each revision
with the matched lock offline.

- `fwmac-pairs.txt`: `fwmac.sh 30a1f6c5 65007ae2 5` output, five interleaved
  pairs per cell; medians and per-pair candidate/base ratios of rusage CPU per
  completed write. Per-run lines were not retained by the tool.
- `counters.txt`: three more interleaved runs per cell and revision through the
  researcher's `run2.py` (`/usr/bin/time -l`): user/sys CPU, switches, retired
  instructions, request p99, proposal rounds and mean width. Rounds include the
  1,024 warmup singletons; width is operations / rounds.
- `attribution/attr-<rev>-<cell>.json`: full stdout of one `--attribution true`
  run per cell and revision.

Cells: bulk = 4096 clients × 4 entries, 4,000,000 requested; single = 1 × 1,
131,072; the `1` suffix adds `--server-workers 1`. All runs use 1,024 warmups,
default retention, upstream-default envelopes and campaign timeout 600 s.

## CPU per completed write (five pairs)

| Cell | main ns/write | H2 ns/write | Ratio | Per-pair ratios |
| --- | ---: | ---: | ---: | --- |
| bulk 16 workers | 248 | 250 | 1.008 | 1.01 1.01 1.00 0.99 1.02 |
| bulk 1 worker | 219 | 217 | 0.991 | 0.97 0.99 1.02 0.98 1.00 |
| single 16 workers | 23,281 | 23,120 | 0.993 | 1.01 0.99 1.00 0.98 1.02 |
| single 1 worker | 13,300 | 13,419 | 1.009 | 0.99 1.01 1.02 1.01 1.01 |

Rates: bulk 8.56 M → 8.45 M/s, bulk-1 7.12 M → 7.12 M/s, single 80.2 k → 80.6 k/s,
single-1 84.0 k → 83.3 k/s. All within ±1.3%; no cell moves outside its pair spread.

## Counters (medians of three runs)

| Cell | Metric | main | H2 | Ratio |
| --- | --- | ---: | ---: | ---: |
| bulk | proposal rounds | 7,791 | 7,760 | 0.996 |
| bulk | mean width | 513.1 | 515.2 | 1.004 |
| bulk | user / sys ns/write | 223 / 35 | 224 / 32 | — |
| bulk | request p99 µs | 2,370 | 2,312 | 0.975 |
| bulk1 | proposal rounds | 13,040 | 15,865 | **1.217** |
| bulk1 | mean width | 306.6 | 252.0 | **0.822** |
| bulk1 | user / sys ns/write | 204 / 16 | 202 / 18 | — |
| bulk1 | request p99 µs | 2,574 | 2,687 | 1.044 |
| single | user / sys ns/write | 13,609 / 10,145 | 13,754 / 10,414 | — |
| single | request p99 µs | 27.5 | 28.8 | 1.047 |
| single1 | user / sys ns/write | 9,670 / 4,232 | 9,675 / 4,243 | — |
| single1 | request p99 µs | 24.5 | 22.8 | 0.931 |

At 16 workers H2 leaves rounds and width unchanged on main, as the researcher
found. At one worker it narrows passes by about 18% (rounds +22%) with neutral
CPU. Main's one-worker passes were never cut by a peer message; they ended on an
empty queue and so also absorbed requests that clients enqueued during the pass.
H2 leaves those for the next pass. This is the rule change itself, not noise:
the attribution run shows the same direction.

## Attribution controls (one run per cell and revision)

| Cell | Revision | Leader proposals | Max width | Mean width | Leader persists | Follower persists | Persists/original | Sends/original | Phase failures |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| single | main | 132,096 | 1 | 1.00 | **264,195** | 242,635 / 248,110 | 5.715086 | 7.431580 | 0 |
| single | H2 | 132,096 | 1 | 1.00 | **264,195** | 242,657 / 248,524 | 5.718387 | 7.438196 | 0 |
| single1 | main | 132,096 | 1 | 1.00 | **264,195** | 245,983 / 261,151 | 5.839155 | 7.679688 | 0 |
| single1 | H2 | 132,096 | 1 | 1.00 | **264,195** | 245,538 / 260,814 | 5.833235 | 7.667848 | 0 |
| bulk | main | 7,717 | 3,424 | 597.30 | 12,789 | 5,548 / 5,571 | 0.005979 | 0.007439 | 0 |
| bulk | H2 | 8,133 | 2,544 | 562.34 | 13,228 | 5,493 / 5,544 | 0.006068 | 0.007631 | 0 |
| bulk1 | main | 12,650 | 2,384 | 343.86 | 17,637 | 5,583 / 5,729 | 0.007240 | 0.007019 | 0 |
| bulk1 | H2 | 15,717 | 1,908 | 272.08 | 20,663 | 5,363 / 5,915 | 0.007988 | 0.007047 | 0 |

Every run completed all originals (3,997,696 bulk, 131,072 single) with all three
frontiers equal (3,998,721 / 132,097). Mean width is completed originals over
leader `proposal_batches − warmup`; persists sum all replicas' `persistence_units`;
sends sum attribution `send` calls; both divide by completed + warmup.

Singleton parity floor: identical on both revisions (leader persists exactly
264,195, width 1, 132,096 proposals); follower persists and sends differ by
under 0.2%, inside run-to-run spread. Bulk-16 attribution differs by one run's
noise (counters above show unchanged width). Bulk-1 shows the narrower passes:
width −21%, leader persists +17%, persists/original +10%, sends/original +0.4%.
