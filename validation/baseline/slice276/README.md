# Native default timing consistency276: implementation and partial qualification

Five native convenience constructors now select NativeTimingProfile::default,
matching the runnable service's existing throughput profile:100ms heartbeat,
elections1000–2000ms. Generic TimerConfig::default and explicit host timer
settings are unchanged. No wire/persistent format or provider seam changes.

Two actual native default tests fail0/2 before the production change: the core
is already Candidate at950ms. The before manifest was captured/verified at
terminal, not before that invocation. The retained test source and source image
identify that older rule. Afterward all five timing tests pass, including the
five constructor paths, static/member/rotated recovery, TCP/QUIC, actual native
worker commitment/application and cleanup, plus explicit legacy timing.

Linux's frozen668-input matrix is terminal and passes190 executions: startup
51/all and37/default, member50/all and27/default, selected retirement/endpoint/
imported-partial16/all and9/default. Formatting/four strict profiles are clean,
and before/terminal source guards verify. Core/native profiles provide compile/
lint coverage rather than runtime coverage of these TLS-only fixtures.

The independent Mac276 matrix is terminal:187 passed/3 failed over190 executions.
Its all-feature startup target is
50 passed/1 failed (actual final configuration Unknown(LeadershipChanged) versus
Committed-only fixture assertion), and member target48 passed/2 failed (TCP
compacted-joint snapshot repair ingress InvalidMessage; TCP retained-witness
progress deadline). Timing tests individually pass. Formatting/four strict
profiles and before/terminal668 guards verify. Remaining routed/default histories
pass. The earlier capture guard is retained separately.277 separately
reproduces and corrects the drain assertion while requiring original durable
Final on every replica. The witness-loss fixture uses a convenience constructor;
its timing transition needs investigation. Snapshot repair supplies explicit
settings. Neither failure is waived by the constructor checks.

The full two-platform source276 startup/member gate is not accepted. These
finite observations do not close retirement timeout cause, platform/P0–P7 or
broader fault/provider obligations. Raw and normalized artifact hashes differ;
original images, failed stores and frozen Mac source remain preserved.
