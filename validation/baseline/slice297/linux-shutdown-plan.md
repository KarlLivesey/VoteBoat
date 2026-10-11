# Parallel shutdown slice schema

Purpose: advance P5/P6 usable lifecycle shutdown/restart acceptance. The retained
Linux285/288 parent timeout has one transport and no outstanding driver/outbound
work; it does not capture its session predicate and remains unresolved until a
matching failed state proves the cause.

Data/API: keep the existing SecureSession/PeerTransport public surface, bounded
poll budgets, exact authenticated binding and retained QUIC connection. Test
through public methods and observe actual UDP readiness without consuming data.

Hypothesis: authenticated clean peer close currently sets SessionState::Closed
before Quinn's queued close response or real protocol drain. A final-budget read
can therefore make subsequent polls skip retained close output and timer work.
Prove this with one-call polling and actual encrypted UDP before changing code.

Transitions: receiving clean close with no unresolved sending stream marks
peer_closed and starts Closing; bounded polling must retain response/datagrams
and real protocol timer until Quinn is_drained and pending output is empty. No
close budget/RTT/timer extension. Unread plaintext remains readable until EOF;
accepted original send ownership remains until existing completion contracts.

Ownership/failure: session owns its original connection/socket and ciphertext;
transport must not drop it merely because clean EOF arrived while protocol work
remains. Session/authentication/identity/protocol errors preserve existing
failure behavior. No consensus/durability changes or new external effects.

Checks: regression fails before fix for unsent close response; after fix actual
UDP response emitted under bounded budgets, timer remains authoritative,
unread plaintext preserved and original local sends completed exactly once.
Run complete QUIC/transport/peers tests and strict Clippy with isolated target
cache; parent integrates metadata/docs and both-host qualification. Keep285/288
causal limitation explicit, do not repeat unchanged healthy original probes.

## Implemented evidence and limitations

- Confirmed actual one-call encrypted UDP failure against isolated original
  production code: clean peer close strands Quinn's close response on all later
  public polls (`before-response.log`, 0/1). A prior original-state assertion
  also records Closed instead of Closing (`before-peer-close-authorized.log`).
- Confirmed a second, independent composition failure: with repaired session but
  original transport, clean framed EOF drops its protocol owner before drain
  (`before-transport-owner.log`, 0/1). This control keeps root production fixed.
- Repair only changes authenticated clean peer-close to Closing, leaving Quinn
  responsible for its original timer, and retains the original framed session
  after clean EOF until its actual Closed. No deadline, RTT, budget, authority,
  accepted-send or durability changes. Unknown/failed sends preserve Failed
  ownership; real send completion and queue credit release are unchanged.
- New tests: `tests/quic/peer_close.rs` actual last-call receive then bounded
  queued-response emission/timer drain; `tests/quic/frame_progress/shutdown.rs`
  original frame ticket/result and queue credits, unread decoded input through
  peer close, then EOF/session retention and actual response/drain release.
- Relevant complete local contract run passed77 executions (21QUIC,37transport,
  19peers) after production fixes. Final mechanical test-refactor repeat is
  recorded separately in `after-contracts-final.log`.
- Strict all-target/all-feature Clippy passed after extracting the focused
  received-close observation helper and replacing clone-to-slice assertions
  (`clippy-all-final.log`). Later fmt/default/native checks were interrupted by
  the parent concurrently registering reference/native_host before writing its
  file; core strict passed. Parent integrates full-source checks after all
  contributions finish; no clean-whole-repository claim from this transient.
- Initial sandbox run could not bind local UDP; authorized escalation ran the
  actual negative control. First new framing test mistakenly tried to consume
  the unread original input twice; focused fix checks typed received presence
  and separates peer-close observation from EOF retention. No assertion waived.
- Exact frozen285/288/295 Cargo.lock all resolve quinn-proto0.11.19. An initial
  0.11.17 claim based on another cached source was corrected; no dependency
  change exists in this repair.
- Original285/288 did not capture the retained provider predicate. This is a
  proved shutdown correctness fix, not proof that it caused those older runs.
  Their timeout remains independently retained until meaningful qualification.
