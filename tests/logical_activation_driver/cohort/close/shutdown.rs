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
//! Genuine requested shutdown reports exact original terminal send observations.
use super::*;
use voteboat::{
    outbound::{LocalSendCompletion, LocalSendResult, OutboundQueue},
    snapshot_worker::SnapshotWorker,
    worker::PersistenceWorker,
};
#[derive(Default)]
struct Observed {
    completions: Vec<LocalSendCompletion>,
    closures: Vec<LogicalPeerClosure>,
}
impl Observed {
    fn retain(&mut self, progress: LogicalNodeShutdownProgress) {
        for completion in progress.completions {
            assert!(!self
                .completions
                .iter()
                .any(|old| old.ticket == completion.ticket));
            self.completions.push(completion);
        }
        self.closures.extend(progress.peer_closures);
    }
    fn terminal(&self, binding: voteboat::outbound::OutboundBinding) {
        assert_eq!(self.completions.len(), 2);
        assert!(self.completions.iter().all(|c| c.ticket.binding == binding));
        let failed = self
            .completions
            .iter()
            .find(|c| c.ticket.peer == support::node(12))
            .unwrap();
        assert_eq!(failed.ticket.sequence, 1);
        assert_eq!(failed.result, LocalSendResult::Failed);
        let cancelled = self
            .completions
            .iter()
            .find(|c| c.ticket.peer == support::node(13))
            .unwrap();
        assert_eq!(cancelled.result, LocalSendResult::Cancelled);
        assert_eq!(self.closures.len(), 1);
        assert_eq!(self.closures[0].peer, support::node(12));
        assert!(matches!(
            self.closures[0].reason,
            LogicalNetworkError::Closed
                | LogicalNetworkError::Session(voteboat::secure::SessionError::Closed)
        ));
        eprintln!(
            "332 requested close originals={:?} peer_closures={:?}",
            self.completions, self.closures
        );
    }
}
#[test]
fn requested_logical_shutdown_settles_original_partial_send_on_real_native_peer_close() {
    let _serial = cluster::HISTORY.lock().unwrap();
    let clock = Instant::now();
    let mut first = members::Members::new(NativePeerProtocol::TcpTls, 0, clock);
    let mut second = members::Members::new(NativePeerProtocol::TcpTls, 1, clock);
    let mut third = members::Members::new(NativePeerProtocol::TcpTls, 2, clock);
    let mut a = bridge(&mut first, 0);
    let mut b = bridge(&mut second, 1);
    let mut c = bridge(&mut third, 2);
    let images = [
        drive::claim_in(&mut a, clock, 33901, false),
        drive::claim_in(&mut b, clock, 33902, false),
        drive::claim_in(&mut c, clock, 33903, false),
    ];
    let gate = gate::Observed::default();
    let (channels, [mut sender, mut opposite, mut idle]) =
        assembly::parents(&images, progress::now(clock), gate.clone());
    refuse_running(&mut sender, clock, &gate);
    progress::hold(&mut a, &mut sender, clock, &gate);
    let binding = sender.local().outbound.binding();
    let mut observed = begin(&mut sender, clock, &gate);
    progress::close_peer(&mut b, &mut opposite, clock);
    drain(&mut sender, &mut a, clock, &mut observed);
    observed.terminal(binding);
    assert_eq!(gate.borrow().accepted, 7);
    assert!(progress::lower_drained(&mut sender, &a));
    assert!(sender.local().owner.is_drained());
    assert!(sender.local().outbound.is_drained());
    let repeated = sender
        .poll_shutdown(progress::now(clock), LogicalNodePollBudget::default())
        .unwrap_or_else(|e| panic!("drained shutdown {:?}", e.reason));
    assert!(repeated.completions.is_empty() && repeated.peer_closures.is_empty());
    progress::close_peer(&mut c, &mut idle, clock);
    let sender = sender
        .into_parts()
        .unwrap_or_else(|_| panic!("original requested sender drain"));
    let opposite = opposite
        .into_parts()
        .unwrap_or_else(|_| panic!("original opposite drain"));
    let idle = idle
        .into_parts()
        .unwrap_or_else(|_| panic!("original idle drain"));
    a.close();
    b.close();
    c.close();
    assert!(a.is_drained() && b.is_drained() && c.is_drained());
    drop((a, b, c));
    channels.close(
        sender
            .sessions
            .into_values()
            .chain(opposite.sessions.into_values())
            .chain(idle.sessions.into_values()),
        progress::now(clock),
    );
    first.finish();
    second.finish();
    third.finish();
}
fn refuse_running(sender: &mut fixture::FacadeNode, clock: Instant, gate: &gate::Observed) {
    let owner = sender.local().owner.usage();
    let queue = sender.local().outbound.usage();
    let rejected = sender
        .poll_shutdown(progress::now(clock), LogicalNodePollBudget::default())
        .expect_err("running shutdown poll requires real prior close_intake");
    assert_eq!(rejected.reason, LogicalNodeError::Closed);
    assert!(rejected.progress.completions.is_empty());
    assert!(rejected.progress.peer_closures.is_empty());
    assert_eq!(sender.state(), NodeState::Running);
    assert_eq!(sender.local().owner.usage(), owner);
    assert_eq!(sender.local().outbound.usage(), queue);
    assert_eq!(gate.borrow().accepted, 0);
}
fn begin(sender: &mut fixture::FacadeNode, clock: Instant, gate: &gate::Observed) -> Observed {
    let before = sender.local().outbound.usage();
    assert_eq!(before.messages, 2);
    sender.close_intake().unwrap();
    assert_eq!(sender.state(), NodeState::Quiescing);
    let result = sender
        .poll_shutdown(
            progress::now(clock),
            LogicalNodePollBudget {
                sessions: 0,
                ..LogicalNodePollBudget::default()
            },
        )
        .unwrap_or_else(|e| panic!("requested close transition {:?}", e.reason));
    let mut observed = Observed::default();
    observed.retain(result);
    assert_eq!(sender.state(), NodeState::Draining);
    assert_eq!(observed.completions.len(), 1);
    assert_eq!(observed.completions[0].result, LocalSendResult::Cancelled);
    assert_eq!(observed.completions[0].ticket.peer, support::node(13));
    assert!(observed.closures.is_empty());
    assert_eq!(sender.local().outbound.usage().messages, 1);
    assert_eq!(gate.borrow().accepted, 7);
    assert_eq!(gate.borrow().terminal_writes, 0);
    observed
}
fn drain(
    sender: &mut fixture::FacadeNode,
    bridge: &mut aliases::Bridge<'_>,
    clock: Instant,
    observed: &mut Observed,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while !sender.is_drained() {
        bridge
            .advance(progress::now(clock), fixture::budget(4))
            .unwrap();
        super::super::progress::pump(bridge, sender);
        let output = sender
            .poll_shutdown(progress::now(clock), LogicalNodePollBudget::default())
            .unwrap_or_else(|e| {
                panic!(
                    "original shutdown rejection {:?} completions={:?} closures={:?}",
                    e.reason, e.progress.completions, e.progress.peer_closures
                )
            });
        observed.retain(output);
        assert_ne!(sender.state(), NodeState::RecoveryRequired);
        assert!(Instant::now() < end, "original requested sender close");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(sender.local().persistence.is_drained());
    let snapshots = sender.local().snapshots.as_ref().unwrap();
    assert!(snapshots.router.is_drained() && snapshots.worker.is_drained());
}
