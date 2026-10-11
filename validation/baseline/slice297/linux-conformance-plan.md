# TimerService shared-contract slice

Purpose: advance P0 / VB-011 replacement-provider qualification with executable exact-owner and bounded-poll obligations already required by the public contract. No production semantics, persistence, protocol, runtime dependency or timer policy changes.

Data/API: public RuntimeOwner, StoreBinding, GroupIdentity, TimerToken, Expiration and TimerService only. Independent HostTimers uses Vec storage; native DeadlineQueue uses ordered maps. Shared checks compare exact caller-retained tokens, live counts and returned expirations; tie order remains unspecified.

State transitions: simultaneous owners register otherwise identical timers; foreign cancellation refuses without consuming either owner. Zero-budget polling observes time without transfer; regressed polling refuses without consuming deadlines; due transfer is bounded, earliest first and exactly once. Capacity is freed only by exact cancellation or returned expiration. Include MonoTime u64 boundary arithmetic.

Ownership/failure: original tokens remain with the caller until the actual expiration or cancellation. Owner identity varies store ID/incarnation/session, execution lane ID and runtime generation independently. The lane has no separate incarnation field. Scope assertions detect store-blind cancellation. Poll assertions detect budget bypass, destructive regression refusal, incorrect lateness and reversed ordering through deliberately defective host fixtures.

Acceptance: host/native shared suite passes with native features; host and negative fixtures pass without default features. Focused runtime tests use CARGO_TARGET_DIR=target/divide-conformance/build. Formatting and strict all-target/all-feature Clippy clean; parent performs four profiles and macOS frozen integration. Metadata anchors exact shared functions and named runners, remains partial, and retains sequence exhaustion, arbitrary host responsiveness and broader owner/timer integration gaps.

Dependencies: existing public seam and independently implemented fixtures. Current deliverable: exact owner isolation + refusal-preserving poll checks. Next: parent integrates and qualifies platform matrix. Following: broader functional baseline gates remain the parent macro plan; no claim this slice certifies TimerService generally.
