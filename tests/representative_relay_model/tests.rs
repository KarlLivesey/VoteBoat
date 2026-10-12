// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Finite protocol gate; no native transport, authentication or Raft proof claim.
use super::{oracle, protocol, search, types::*};
#[test]
fn strict_physical_voter_relay_graphs_exhaust_without_unsafe_release() {
    for p in [
        Profile::Election,
        Profile::Freshness,
        Profile::Append,
        Profile::Read,
        Profile::Configuration,
        Profile::Group,
        Profile::Store,
    ] {
        let graph =
            search::explore(p, Rule::Strict).unwrap_or_else(|failure| panic!("{p:?}: {failure:?}"));
        eprintln!("relay340 strict {p:?}: {graph:?}");
        assert_eq!(graph.frontier, 0);
        assert!(graph.terminals > 0 && graph.loss > 0 && graph.replay > 0);
        assert!(graph.relay_restart > 0 && graph.source_crash > 0);
        assert!(graph.cancelled > 0 && graph.backpressure > 0 && graph.stall > 0);
        assert!(graph.requests_issued > 0 && graph.requests_consumed > 0);
        assert!(graph.requests_returned > 0 && graph.requests_retired > 0);
    }
}
#[test]
fn weakened_relay_and_durability_rules_replay_original_counterexamples() {
    for (profile, rule, expected) in [
        (
            Profile::Election,
            Rule::RelayVote,
            Violation::ImaginaryVoter,
        ),
        (
            Profile::Read,
            Rule::DuplicateVote,
            Violation::DuplicateVoter,
        ),
        (
            Profile::Election,
            Rule::WrittenAck,
            Violation::WrittenBeforeSync,
        ),
        (
            Profile::Freshness,
            Rule::FreshnessOmitted,
            Violation::StaleCandidate,
        ),
        (Profile::Append, Rule::OldCompletion, Violation::Ownership),
    ] {
        negative(profile, rule, expected);
    }
}
#[test]
fn stale_original_context_and_store_outputs_cannot_certify_new_scope() {
    for profile in [
        Profile::Election,
        Profile::Append,
        Profile::Read,
        Profile::Configuration,
        Profile::Group,
        Profile::Store,
    ] {
        negative(profile, Rule::ScopeOmitted, Violation::Scope);
    }
}
fn negative(p: Profile, rule: Rule, expected: Violation) {
    let failure = search::explore(p, rule).expect_err("actual weakened trace must fail");
    eprintln!(
        "relay340 negative {p:?}/{rule:?}: {:?} trace={:?}",
        failure.violation, failure.trace
    );
    assert_eq!(failure.violation, expected);
    assert_eq!(search::replay(p, rule, &failure.trace), expected);
}

#[test]
fn retired_store_original_replay_fails_before_publication() {
    use Action::*;
    let trace = [
        Receive(0),
        Write(0),
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Receive(1),
        Write(1),
        Sync(1),
        Emit(1),
        Receive(2),
        Write(2),
        Sync(2),
        Emit(2),
        Capture(0),
        Capture(1),
        Export,
        Deliver,
        Complete,
        Release,
        RetireRequest,
        Next,
        Receive(0),
        Write(0),
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Replay(1),
        Capture(3),
        Export,
        Deliver,
        Release,
    ];
    assert_eq!(
        search::replay(Profile::Store, Rule::ScopeOmitted, &trace),
        Violation::Store
    );
}
#[test]
fn original_append_ack_before_sync_cannot_release_matching_prefix() {
    use Action::*;
    let trace = [
        Receive(0),
        Write(0),
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Receive(1),
        Write(1),
        Sync(1),
        Emit(1),
        Receive(2),
        Write(2),
        Sync(2),
        Emit(2),
        Capture(0),
        Capture(1),
        Export,
        Deliver,
        Complete,
        Release,
        RetireRequest,
        Next,
        Receive(0),
        Write(0),
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Receive(1),
        Write(1),
        Emit(1),
        Capture(3),
        Capture(4),
        Export,
        Deliver,
        Release,
    ];
    assert_eq!(
        search::replay(Profile::Append, Rule::WrittenAck, &trace),
        Violation::WrittenBeforeSync
    );
}

#[test]
fn request_fanout_restarts_from_the_original_retained_parent_request() {
    use Action::*;
    let p = Profile::Read;
    let mut state = State::initial(p);
    assert!(!protocol::actions(&state, p, Rule::Strict).contains(&Receive(1)));
    for action in [Receive(0), Write(0)] {
        step(&mut state, action, p);
    }
    assert!(!protocol::actions(&state, p, Rule::Strict).contains(&IssueRequest));
    for action in [
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Receive(1),
        Write(1),
    ] {
        step(&mut state, action, p);
    }
    let original = state.requests[0].unwrap();
    assert_eq!(state.request_ticket.unwrap().original, original);
    assert!(!protocol::actions(&state, p, Rule::Strict).contains(&ReturnRequest));
    step(&mut state, RestartRelay, p);
    assert!(state.request_ticket.is_none());
    assert_eq!(state.requests[0], Some(original));
    let enabled = protocol::actions(&state, p, Rule::Strict);
    assert!(!enabled.contains(&Receive(2)) && !enabled.contains(&Capture(0)));
    for action in [
        Sync(1),
        Emit(1),
        ConsumeRequest,
        Capture(1),
        Cancel(1),
        ReturnRequest,
    ] {
        step(&mut state, action, p);
    }
    assert_eq!(state.requests[0], Some(original));
    for action in [ConsumeRequest, Receive(2)] {
        step(&mut state, action, p);
    }
    let ticket = state.request_ticket.unwrap();
    assert_eq!(ticket.generation, 2);
    assert_eq!(ticket.original, original);
}

#[test]
fn restart_does_not_erase_original_transmitted_wire_replay() {
    use Action::*;
    let p = Profile::Election;
    let mut state = State::initial(p);
    for action in [
        Receive(0),
        Write(0),
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Capture(0),
        Export,
        RestartRelay,
    ] {
        step(&mut state, action, p);
    }
    let packet = state.obsolete.unwrap();
    assert_eq!(packet.ids, [Some(0), None]);
    assert!(state.request_ticket.is_none());
    assert!(protocol::actions(&state, p, Rule::Strict).contains(&Replay(0)));
    step(&mut state, Replay(0), p);
    assert_eq!(state.claims, vec![0]);
    assert_eq!(state.obsolete, Some(packet));
}

fn step(state: &mut State, action: Action, p: Profile) {
    assert!(protocol::actions(state, p, Rule::Strict).contains(&action));
    *state = protocol::apply(state, action, p, Rule::Strict);
    assert_eq!(oracle::check(state, p), None);
}

#[test]
fn nonce_projection_keeps_original_counters_and_owned_packet_values() {
    use Action::*;
    let (mut aggregate, mut split) = packet_histories();
    assert_eq!((aggregate.sequence, split.sequence), (1, 2));
    assert_ne!(aggregate, split);
    assert_eq!(search::seen_key(&aggregate), search::seen_key(&split));
    search::assert_equivalent(&aggregate, &split, Profile::Read, Rule::Strict);
    for action in [Capture(2), Export] {
        step(&mut aggregate, action, Profile::Read);
        step(&mut split, action, Profile::Read);
    }
    let left = aggregate.packet.unwrap().ticket;
    let right = split.packet.unwrap().ticket;
    assert_eq!((left.sequence, right.sequence), (2, 3));
    assert_ne!(left, right);
    assert_eq!(aggregate.credit, Some(left));
    assert_eq!(split.credit, Some(right));
    assert_eq!(search::seen_key(&aggregate), search::seen_key(&split));
    search::assert_equivalent(&aggregate, &split, Profile::Read, Rule::Strict);
    assert_eq!((aggregate.sequence, split.sequence), (2, 3));
    assert_eq!(aggregate.packet.unwrap().ticket, left);
    assert_eq!(split.packet.unwrap().ticket, right);
}

#[test]
fn nonce_projection_preserves_a_foreign_actual_credit_mismatch() {
    use Action::*;
    let (mut aggregate, mut split) = packet_histories();
    for action in [Capture(2), Export] {
        step(&mut aggregate, action, Profile::Read);
        step(&mut split, action, Profile::Read);
    }
    // Deliberately invalid projection input, using another genuine packet nonce.
    // This is not an enabled protocol action or source durability evidence.
    aggregate.credit = split.credit;
    assert_ne!(aggregate.packet.unwrap().ticket, aggregate.credit.unwrap());
    let key = search::seen_key(&aggregate);
    assert_ne!(key.packet.unwrap().ticket, key.credit.unwrap());
    assert_eq!(
        oracle::check(&aggregate, Profile::Read),
        Some(Violation::Ownership)
    );
    assert_eq!(
        oracle::check(&key, Profile::Read),
        Some(Violation::Ownership)
    );
}

fn packet_histories() -> (State, State) {
    use Action::*;
    let p = Profile::Read;
    let mut base = State::initial(p);
    for action in [
        Receive(0),
        Write(0),
        Sync(0),
        IssueRequest,
        ConsumeRequest,
        Emit(0),
        Receive(1),
        Write(1),
        Sync(1),
        Emit(1),
        Receive(2),
        Write(2),
        Sync(2),
        Emit(2),
    ] {
        step(&mut base, action, p);
    }
    let mut aggregate = base.clone();
    let mut split = base;
    for action in [Capture(0), Capture(1), Export, Deliver, Complete] {
        step(&mut aggregate, action, p);
    }
    for action in [
        Capture(0),
        Export,
        Deliver,
        Complete,
        Capture(1),
        Export,
        Deliver,
        Complete,
    ] {
        step(&mut split, action, p);
    }
    (aggregate, split)
}
