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
//! Real follower admissions are observations, never promotion authority.
use super::*;
use voteboat::raft::{RaftError, ReadinessRequirements};
#[test]
fn original_readiness_and_cancel_admissions_match_real_follower_steps_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn original_readiness_and_cancel_admissions_match_real_follower_steps_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let mut host = cluster::Cluster::simulated(protocol, fixture::child_application());
    let leader = host.leader().unwrap();
    let follower = (0..3).find(|i| *i != leader).unwrap();
    let peer = host.nodes[leader]
        .local()
        .owner
        .core(setup::mapping().child)
        .unwrap()
        .local_node();
    host.wait(|h| {
        h.nodes[follower]
            .peers()
            .unwrap()
            .roster()
            .binding(peer)
            .is_some()
    });
    let requirements = ReadinessRequirements {
        application_schema: 1,
        inner_profile: voteboat::raft::InnerProfile::LEAF,
        command_bytes: 1,
        snapshot_bytes: 1,
    };
    let ticket = host.nodes[follower]
        .request_learner_readiness_tracked(setup::mapping().child, peer, requirements)
        .unwrap();
    assert_eq!(ticket.owner, host.nodes[follower].local().owner.identity());
    let step = original_step(&mut host, follower, ticket);
    assert_eq!(step.error, Some(RaftError::NotLeader));
    assert!(host.nodes[follower]
        .local()
        .owner
        .core(ticket.group)
        .unwrap()
        .pending_learner_readiness()
        .is_none());
    assert!(host.nodes[follower]
        .local()
        .owner
        .core(ticket.group)
        .unwrap()
        .ready_learner()
        .is_none());
    let cancel = host.nodes[follower]
        .cancel_learner_readiness_tracked(ticket.group)
        .unwrap();
    assert_ne!(cancel, ticket);
    assert_eq!(cancel.owner, ticket.owner);
    let step = original_step(&mut host, follower, cancel);
    assert_eq!(step.error, None);
    assert_eq!(step.admission, Some(cancel));
    assert!(host.nodes[follower].configuration_requests_are_drained());
    host.finish();
}
fn original_step(
    host: &mut cluster::Cluster<harness::SimClock>,
    selected: usize,
    ticket: AdmissionTicket,
) -> OwnerStep {
    let end = harness::Wait::new(&host.clock, harness::LIVENESS_MS);
    loop {
        let now = harness::FixtureClock::round(&host.clock, harness::Observed::nodes(&host.nodes));
        let mut observed = None;
        for (i, n) in host.nodes.iter_mut().enumerate() {
            let p = n.poll(now, NodePollBudget::default()).unwrap();
            if i == selected {
                observed = p
                    .replica
                    .and_then(|r| r.steps.into_iter().find(|s| s.admission == Some(ticket)));
            }
        }
        if let Some(s) = observed {
            return s;
        }
        assert!(
            end.within(&host.clock),
            "exact original admission must execute"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
