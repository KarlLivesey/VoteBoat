# Mac402 full QUIC cold history: failed final cleanup

Source: same frozen402 source as the adjacent passing mac_tcp record (9d3e863).
The exact single test ran843.17s and failed at CloseDrained after15.000760792s.
Checkpoint publication, cold reopen and original history assertions had progressed
to final cleanup. All three group813 nodes had base892, commit/applied893 and real
snapshot generation2/index892. All application/worker queues were drained. Node8
was Drained; Node7 and Node9 retained one original QUIC transport each. No actual
secure-session deadline was captured by that old diagnostic, so this record does
not establish the timer cause or a completed cleanup.

Raw log, exact command, aggregate exits and before/after original artifact/source
guards are retained here. The full terminal working capture is under ignored
`target/root315/mac402/terminal/results`. Do not report402 QUIC as passing.

Follow-up405 reuses the existing strictly checked protocol-only close window from
routed tests and adds actual peer drain diagnostics. It must still pass original
worker joins and listener rebinding; no production change is established by this
failure. TCP402 and native100 successes are not rerun to hide this failure.
