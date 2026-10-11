# Temporary QUIC closing trace289 — isolated Linux pass

The unchanged original QUIC checkpoint split history passes1/0 in40.44s with
once-per-second closing diagnostics in a separate frozen image. The trace records
only local/peer node IDs, accepted stream/write flags, pending datagram size,
protocol closed/drained flags and next timer delay. It logs no payload,
credentials or crypto state, and changes no protocol condition or timeout.

Its669-input manifest was generated before compilation; the terminal guard
verifies those exact bytes. There is no separately executed before-run guard
or strict four-profile qualification of this temporary probe. This is diagnostic
evidence, not an accepted provider implementation. The zero-context runtime patch
applies to the tracked native QUIC provider; use288's fixture patch separately.

Observed closing states have no accepted stream/write or pending datagram and
drain normally. The Linux288 and full285 failures remain unresolved. Logging
affects timing, so this passing schedule does not establish the reason for their
retained peer connection or justify a production fix. Probe logging was not
added to production source.
