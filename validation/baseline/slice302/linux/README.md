# Terminal Linux functional matrix302

All four original feature configurations complete successfully on the frozen
705-input source. Every input matches Git commit
`77c07ae67aae861b34d07edf6878f49595e7ab53`; before/terminal guards also match.
The later service shutdown correction is qualified separately in
[305](../../slice305/README.md), including complete Linux/Mac affected targets.

| Configuration | Passed | Failed | Cargo exit |
| --- | ---: | ---: | ---: |
| All features | 1,952 | 0 | 0 |
| Default features | 1,708 | 0 | 0 |
| No default features | 775 | 0 | 0 |
| Native only | 1,226 | 0 | 0 |

Total:5,661 executed tests over355 target/configuration summaries, including
zero-test/doc summaries. Repeated configurations are repeated executions,
not distinct tests or coverage claims. Formatting and all four strict all-target
Clippy profiles have zero diagnostics. `linux-stage.txt` is terminal=0.

`checks.sh` records the commands: each complete test profile uses locked/offline
Cargo, no-fail-fast and nocapture, sequentially so its executables remain owned
by that configuration. No quiet process was restarted. No additional runtime
tests were run for publication. Raw test logs retain intentional diagnostic
output and exact target results; Cargo exits and Rust summaries establish success.

The original Mac run remains live, with its original counter shutdown failure
retained. This directory contains completed Linux results only. It does not
claim whole-platform feature qualification, the original Mac cause, arbitrary
provider certification, exhaustive failure coverage or P7 performance acceptance.

`source.sha256` hashes compilation/test inputs; `linux-base77c-source.log`
verifies them against the Git tree. `raw.sha256` records retained bytes and
`published.sha256` verifies files after trailing-whitespace normalization.
No build cache or design pack is included.
