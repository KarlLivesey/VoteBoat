// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{identity::OperationId, runtime::*, snapshot::SnapshotRef, snapshot_worker::*};
pub struct Held {
    pub owner: RuntimeOwner,
    pub request: SnapshotWorkTicket,
    pub usage: SnapshotWorkUsage,
    pub snapshot: Option<SnapshotRef>,
    pub frontier: u64,
    visit: Option<EffectVisitInspection>,
    operation: OperationId,
    attempt: LogicalBridgeAttemptInspection,
    bytes: Vec<u8>,
}
impl Held {
    pub fn assert_same(&self, host: &mut Host<'_, '_>) {
        host.upper[0]
            .inspect_backing(0, |p| {
                assert_eq!(p.owner.identity(), self.owner);
                assert_eq!(
                    p.owner.inspect_visit(setup::middle(0).group).unwrap(),
                    self.visit
                );
                assert_eq!(p.snapshots.as_ref().unwrap().worker.usage(), self.usage);
                assert_eq!(
                    p.owner
                        .core(setup::middle(0).group)
                        .unwrap()
                        .state()
                        .snapshot,
                    self.snapshot
                );
            })
            .unwrap();
        host.upper[0]
            .pump_backing(0, |p| {
                let port = p.unwrap().snapshots.unwrap();
                assert_eq!(port.pending_request(), Some(self.request));
                assert_eq!(port.operation(), Some(self.operation));
            })
            .unwrap();
        host.lower.bridges[0].inspect_work(|work| {
            let work = work.expect("same original lower checkpoint work");
            assert_eq!(work.operation(), self.operation);
            assert_eq!(work.attempt(), Some(self.attempt));
            assert_eq!(work.command_bytes(), Some(self.bytes.as_slice()));
        });
    }
}
pub fn capture(host: &mut Host<'_, '_>) -> Held {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        host.poll();
        let retained = host.upper[0]
            .pump_backing(0, |p| {
                let p = p.unwrap().snapshots.unwrap();
                p.pending_request().zip(p.operation())
            })
            .unwrap();
        if let Some((request, operation)) = retained {
            let original = host.lower.bridges[0].inspect_work(|work| {
                let work = work?;
                if work.operation() != operation {
                    return None;
                }
                let attempt = work.attempt()?;
                let LogicalBridgeChildTicket::Client(ticket) = attempt.ticket() else {
                    panic!("original checkpoint child proposal");
                };
                assert_eq!(ticket.operation, operation);
                assert_eq!(ticket.group, setup::bottom(0).group);
                assert_eq!(ticket.binding.owner, attempt.owner());
                Some((attempt, work.command_bytes().unwrap().to_vec()))
            });
            if let Some((attempt, bytes)) = original {
                let (owner, usage, snapshot, frontier, visit) = host.upper[0]
                    .inspect_backing(0, |p| {
                        let state = p.owner.core(setup::middle(0).group).unwrap().state();
                        (
                            p.owner.identity(),
                            p.snapshots.as_ref().unwrap().worker.usage(),
                            state.snapshot,
                            state.commit_index,
                            p.owner.inspect_visit(setup::middle(0).group).unwrap(),
                        )
                    })
                    .unwrap();
                assert_eq!(usage.requests, 1);
                return Held {
                    owner,
                    request,
                    usage,
                    snapshot,
                    frontier,
                    visit,
                    operation,
                    attempt,
                    bytes,
                };
            }
        }
        assert!(
            Instant::now() < end,
            "original child checkpoint association unavailable"
        );
    }
}
