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
//! Capsules below originate from actual recovered Raft Send effects.
use super::*;
use crate::{
    membership::Configuration,
    raft::{Effect, Event, Message, Raft, RequestContext, Rpc},
};
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        cid(id),
        Policy::new(
            Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
            PolicyLimits::default(),
        )
        .unwrap(),
        voters
            .iter()
            .map(|n| (node(*n), store(u128::from(*n))))
            .collect(),
        learners
            .iter()
            .map(|n| (node(*n), store(u128::from(*n))))
            .collect(),
    )
    .unwrap()
}
fn fixture() -> LogicalApplication {
    let initial = configuration(1, &[1, 3], &[]);
    let mut a = LogicalApplication::new(
        LogicalStoreMapping::new(group(1), node(1), store(1), group(2)).unwrap(),
        bootstrap(group(2), 2, store(2)),
        Bootstrap {
            group: group(1),
            configuration: initial.id(),
            policy: initial.policy().clone(),
            voter_stores: initial.voter_stores().clone(),
        },
        LogicalLimits::default(),
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_authority(WireLimits::default()).unwrap(),
    )
    .unwrap();
    claim(&mut a);
    a
}
fn recovered(a: &LogicalApplication) -> Raft {
    Raft::recover_member(
        node(1),
        owner().store,
        a.state.parent.clone().unwrap(),
        a.limits.parent_log,
    )
    .unwrap()
}
fn emitted(a: &LogicalApplication, event: Event) -> Message {
    let mut core = recovered(a);
    let before = core.state().clone();
    let effects = core.step(event).unwrap();
    assert_eq!(
        core.state(),
        &before,
        "pure control cannot mutate durable promises"
    );
    let [Effect::Send(message)] = effects.as_slice() else {
        panic!("one actual Send effect")
    };
    message.clone()
}
fn request(a: &LogicalApplication, requested: u64) -> Message {
    emitted(
        a,
        Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(3),
                store: store(3),
            },
            candidate: PeerIdentity {
                node: node(4),
                store: store(4),
            },
            configuration: cid(requested),
        },
    )
}
fn incoming(requested: u64) -> Message {
    let sender = StoreBinding {
        identity: store(3),
        session: StoreSession::new(7).unwrap(),
    };
    Message {
        group: group(1),
        configuration: cid(1),
        from: node(3),
        sender,
        to: node(1),
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 9,
        },
        rpc: Rpc::AuthorityRequest {
            candidate: PeerIdentity {
                node: node(4),
                store: store(4),
            },
            configuration: cid(requested),
        },
    }
}
fn authorize(a: &LogicalApplication, id: u128, message: Message) -> Command {
    let parent = a.state.parent.as_ref().unwrap();
    command(
        id,
        1,
        CommandBody::Authorize(LogicalCapsule {
            expected_revision: parent.revision,
            expected_generation: parent.generation,
            local_configuration: parent.membership().unwrap().id(),
            origin: LogicalCapsuleOrigin::Protocol(EffectTicket {
                visit: visit(),
                sequence: 1,
            }),
            body: LogicalCapsuleBody::Protocol(message),
        }),
    )
}
fn authorized(a: &mut LogicalApplication, id: u128, message: Message) -> Command {
    let parent = a.state.parent.clone();
    let c = authorize(a, id, message);
    assert_eq!(apply(a, &c).outcome, LogicalOutcome::CapsuleAuthorized);
    assert_eq!(a.state.parent, parent);
    c
}
fn refused(a: &mut LogicalApplication, id: u128, message: Message) {
    let parent = a.state.parent.clone();
    let c = authorize(a, id, message);
    assert_eq!(
        apply(a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
    );
    assert_eq!(a.state.parent, parent);
}
#[test]
fn term_zero_actual_request_and_denial_keep_distinct_configuration_scopes() {
    let mut a = fixture();
    let query = request(&a, 2);
    assert_eq!(query.term, 1);
    assert_eq!(query.configuration, cid(1));
    assert!(matches!(query.rpc,Rpc::AuthorityRequest {configuration,..} if configuration==cid(2)));
    authorized(&mut a, 2, query);
    let reply = emitted(&a, Event::Receive(incoming(2)));
    assert_eq!(reply.term, 1);
    assert_eq!(reply.configuration, cid(1));
    assert!(
        matches!(reply.rpc,Rpc::AuthorityReply { configuration,committed_index:0,committed_term:0,granted:false,..} if configuration==cid(2))
    );
    let c = authorized(&mut a, 3, reply);
    let original = a.state.records[&c.operation].receipt;
    let replace = command(
        4,
        2,
        CommandBody::Claim {
            expected_epoch: Some(representative(1).epoch),
        },
    );
    assert_eq!(apply(&mut a, &replace).outcome, LogicalOutcome::Claimed);
    let duplicate = apply(&mut a, &c);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.outcome, original.outcome);
    assert_eq!(duplicate.first_index, original.first_index);
    assert_eq!(a.state.parent.as_ref().unwrap().hard_state.term, 0);
}
#[test]
fn authority_exception_does_not_relax_normal_promise_or_original_request_scope() {
    let mut a = fixture();
    let mut query = request(&a, 2);
    query.term = 2;
    refused(&mut a, 2, query);
    let reply = emitted(&a, Event::Receive(incoming(2)));
    let mut wrong_origin = reply.clone();
    wrong_origin.context.origin.identity = store(9);
    refused(&mut a, 3, wrong_origin);
    let mut missing_base = reply.clone();
    missing_base.configuration = cid(2);
    if let Rpc::AuthorityReply { configuration, .. } = &mut missing_base.rpc {
        *configuration = cid(3);
    }
    refused(&mut a, 4, missing_base);
    let mut false_grant = reply;
    if let Rpc::AuthorityReply {
        granted,
        committed_index,
        committed_term,
        ..
    } = &mut false_grant.rpc
    {
        *granted = true;
        *committed_index = 1;
        *committed_term = 1;
    }
    refused(&mut a, 5, false_grant);
    let mut vote = request(&a, 2);
    vote.rpc = Rpc::Vote {
        last_index: 0,
        last_term: 0,
    };
    refused(&mut a, 6, vote);
    let mut append = request(&a, 2);
    append.rpc = Rpc::Append {
        previous_index: 0,
        previous_term: 0,
        entries: vec![].into(),
        leader_commit: 0,
    };
    refused(&mut a, 7, append);
}
mod promotion;

mod snapshot_ack;
