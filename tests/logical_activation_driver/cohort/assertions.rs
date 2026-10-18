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
//! Inspect original committed state only after the consumed driver observations.
use super::*;
use voteboat::application::StateMachine;
pub fn terminal(
    parent: &fixture::FacadeNode,
    peers: &progress::Peers<'_>,
    target: &aliases::Bridge<'_>,
    intent: &ChildActivationIntent,
    first: ActivationPosition,
    representative: RepresentativeBinding,
) {
    let original = parent.local().applications[&intent.mapping().parent]
        .status(intent.operations().begin)
        .unwrap();
    assert_eq!(original.intent, *intent);
    assert_eq!(original.begin.first, first);
    assert!(original.outcome.is_some());
    assert!(original.release.is_some());
    for peer in &peers.parents {
        assert_eq!(
            peer.local().applications[&intent.mapping().parent].status(intent.operations().begin),
            Some(original.clone())
        );
    }
    for index in [0, 1, 3] {
        target
            .inspect_backing(index, |local| {
                let core = local.owner.core(intent.mapping().child).unwrap();
                assert_eq!(core.membership().id(), intent.target_configuration());
                assert!(core.membership().joint().is_none());
                assert!(core.membership().is_voter(support::node(4)));
                assert!(!core.membership().is_voter(support::node(3)));
                assert!(
                    local.applications[&intent.mapping().child].applied_index()
                        >= original.child_final.unwrap().index
                );
                assert_eq!(
                    local.applications[&intent.mapping().child]
                        .metadata()
                        .representative,
                    Some(representative)
                );
            })
            .unwrap();
    }
}
pub fn phase(
    driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
    peers: &mut progress::Peers<'_>,
    target: &mut aliases::Bridge<'_>,
    phase: ChildActivationDriverPhase,
) {
    let end = harness::hang_guard();
    loop {
        peers.step(driver, target);
        if driver.phase() == phase && target.is_drained() {
            return;
        }
        assert!(
            Instant::now() < end,
            "original target phase {:?} expected {phase:?}",
            driver.phase()
        );
    }
}
