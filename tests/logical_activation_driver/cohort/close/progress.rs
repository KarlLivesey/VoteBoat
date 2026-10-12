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
//! Original lower work settles before the deliberate real channel closure.
use super::*;
use voteboat::{
    outbound::OutboundQueue, snapshot_worker::SnapshotWorker, transport::TransportPollBudget,
    worker::PersistenceWorker,
};
pub fn now(clock: Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
pub fn quiet_budget() -> LogicalNodePollBudget {
    LogicalNodePollBudget {
        transport: TransportPollBudget {
            read_bytes: 0,
            ..TransportPollBudget::default()
        },
        ..LogicalNodePollBudget::default()
    }
}
pub fn step(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: Instant,
    budget: LogicalNodePollBudget,
) -> Result<ReplicaProgress, LogicalNodeError> {
    bridge.advance(now(clock), fixture::budget(4)).unwrap();
    super::super::progress::pump(bridge, node);
    node.poll(now(clock), budget)
}
pub fn hold(
    bridge: &mut aliases::Bridge<'_>,
    sender: &mut fixture::FacadeNode,
    clock: Instant,
    observed: &gate::Observed,
) {
    sender.campaign().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while observed.borrow().accepted != 7 {
        step(
            bridge,
            sender,
            clock,
            LogicalNodePollBudget {
                dispatch: 1,
                sessions: 1,
                ..LogicalNodePollBudget::default()
            },
        )
        .unwrap();
        assert!(Instant::now() < end, "actual authorized seven-byte send");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(observed.borrow().offered > 7);
    while !lower_drained(sender, bridge) {
        step(
            bridge,
            sender,
            clock,
            LogicalNodePollBudget {
                dispatch: 0,
                sessions: 0,
                ..LogicalNodePollBudget::default()
            },
        )
        .unwrap();
        assert!(
            Instant::now() < end,
            "original lower work must settle before failure"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(sender.state(), NodeState::Running);
    assert!(sender.local().outbound.usage().messages >= 1);
    eprintln!(
        "332 close held original bytes={:?} outbound={:?}",
        observed.borrow(),
        sender.local().outbound.usage()
    );
}
pub fn lower_drained(node: &mut fixture::FacadeNode, bridge: &aliases::Bridge<'_>) -> bool {
    let mut clear = false;
    node.pump_backing(|p| {
        clear = p.persistence.is_drained()
            && p.releases.is_drained()
            && p.snapshots.is_none_or(|s| s.is_drained());
    });
    let local = node.local();
    clear
        && local.owner.usage().persistence_requests == 0
        && local.persistence.is_drained()
        && local
            .snapshots
            .as_ref()
            .is_some_and(|s| s.router.is_drained() && s.worker.is_drained())
        && local.clients.is_drained()
        && local.reads.is_drained()
        && local.results.is_drained()
        && bridge.is_drained()
}
pub fn close_peer(
    bridge: &mut aliases::Bridge<'_>,
    peer: &mut fixture::FacadeNode,
    clock: Instant,
) {
    assert_eq!(peer.state(), NodeState::Running);
    assert!(peer.local().clients.is_drained());
    assert!(peer.local().reads.is_drained());
    assert!(peer.local().results.is_drained());
    peer.close_intake().unwrap();
    assert_eq!(peer.state(), NodeState::Quiescing);
    let end = Instant::now() + Duration::from_secs(15);
    while !peer.is_drained() {
        step(bridge, peer, clock, quiet_budget()).unwrap();
        assert!(Instant::now() < end, "original opposite endpoint close");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(peer.state(), NodeState::Drained);
}
pub fn fail_running(
    bridge: &mut aliases::Bridge<'_>,
    sender: &mut fixture::FacadeNode,
    clock: Instant,
) {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        assert_eq!(sender.state(), NodeState::Running);
        match step(
            bridge,
            sender,
            clock,
            LogicalNodePollBudget {
                dispatch: 0,
                ..LogicalNodePollBudget::default()
            },
        ) {
            Ok(_) => (),
            Err(error) => {
                assert!(
                    matches!(
                        error,
                        LogicalNodeError::Network(
                            LogicalNetworkError::Closed
                                | LogicalNetworkError::Session(
                                    voteboat::secure::SessionError::Closed
                                )
                        )
                    ),
                    "actual close error {error:?}"
                );
                assert_eq!(sender.failure(), Some(&error));
                eprintln!(
                    "332 close original running error={error:?} state={:?} outbound={:?}",
                    sender.state(),
                    sender.local().outbound.usage()
                );
                break;
            }
        }
        assert!(
            Instant::now() < end,
            "real opposite close must terminate original send"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(sender.state(), NodeState::RecoveryRequired);
    assert!(matches!(
        sender.poll(now(clock), LogicalNodePollBudget::default()),
        Err(LogicalNodeError::RecoveryRequired)
    ));
}
