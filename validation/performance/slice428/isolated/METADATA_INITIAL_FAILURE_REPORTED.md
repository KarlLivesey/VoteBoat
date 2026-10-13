# Reported initial metadata checker failure

The root/review agent reported an actual tool invocation (chunk5f54fd) ending
exit1 because the existing checker accepts only tests/*.rs assertion paths and
the new ledger referenced src/native/log_store/accepted/tests/checkpoint.rs
(and other private/matched anchors). The exact raw terminal output was unavailable
for this archive. This paragraph is a provenance report, not a reconstructed log.

METADATA_LEDGER_BEFORE.json retains the invalid ledger. The focused correction
retains valid integration anchors without broadening the checker. Final actual
metadata-final.command/log/stderr/status records checker0,41 contracts and391
operations against the corrected isolated source. No production change or test
rerun was made for the documentation-only correction.
