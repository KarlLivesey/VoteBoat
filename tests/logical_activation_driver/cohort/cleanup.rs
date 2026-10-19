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
//! Keep every original facade/bridge/provider until its own terminal drain.
use super::*;
use voteboat::outbound::{LocalSendCompletion, OutboundBinding, OutboundQueue};
#[derive(Default)]
struct Observed {
    completions: Vec<LocalSendCompletion>,
    closures: Vec<LogicalPeerClosure>,
}
impl Observed {
    fn retain(&mut self, progress: LogicalNodeShutdownProgress, binding: OutboundBinding) {
        for completion in progress.completions {
            assert_eq!(completion.ticket.binding, binding);
            assert!(!self
                .completions
                .iter()
                .any(|old| old.ticket == completion.ticket));
            self.completions.push(completion);
            let limits = groups::storage_limits();
            let retained_actions = limits.max_actions + limits.control_actions;
            assert!(self.completions.len() <= retained_actions * 2);
        }
        self.closures.extend(progress.peer_closures);
        assert!(self.closures.len() <= 2);
    }
}
pub fn close(
    mut peers: progress::Peers<'_>,
    leader: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
) -> [fixture::Parts; 2] {
    leader.close_intake().unwrap();
    for p in &mut peers.parents {
        p.close_intake().unwrap();
    }
    let bindings = [
        leader.local().outbound.binding(),
        peers.parents[0].local().outbound.binding(),
        peers.parents[1].local().outbound.binding(),
    ];
    let mut observed: [Observed; 3] = std::array::from_fn(|_| Observed::default());
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        for (i, done) in peers.shutdown_step(leader, target).into_iter().enumerate() {
            observed[i].retain(done, bindings[i]);
        }
        if leader.is_drained()
            && peers.parents.iter().all(fixture::FacadeNode::is_drained)
            && target.is_drained()
            && peers.bridges.iter().all(aliases::Bridge::is_drained)
        {
            break;
        }
        assert!(
            end.within(&peers.clock),
            "original independent parent and bridge cleanup"
        );
    }
    for (i, done) in observed.iter().enumerate() {
        eprintln!(
            "334 original close parent={:?} completions={:?} closures={:?}",
            bindings[i].node, done.completions, done.closures
        );
    }
    // leader is borrowed separately, so its actual original parts are reclaimed
    // by the caller. Return the other two here; no resources are fabricated.
    for b in &mut peers.bridges {
        b.close();
        assert!(b.is_drained());
    }
    target.close();
    assert!(target.is_drained());
    let [left, right] = peers.parents;
    [
        left.into_parts()
            .unwrap_or_else(|_| panic!("original left drain")),
        right
            .into_parts()
            .unwrap_or_else(|_| panic!("original right drain")),
    ]
}
