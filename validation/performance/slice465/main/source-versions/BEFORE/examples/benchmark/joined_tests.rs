// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn step(visit: u64, original: Option<u64>) -> OwnerStep {
    let owner = RuntimeOwner {
        store: StoreBinding {
            identity: store(1),
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    OwnerStep {
        visit: VisitTicket {
            owner,
            group: group(),
            sequence: visit,
        },
        admission: Some(AdmissionTicket {
            owner,
            group: group(),
            sequence: original.unwrap_or(99),
        }),
        operation: original.map(|_| OperationId::new(u128::MAX).unwrap()),
        proposed: original.map(|index| ProposalPosition { index, term: 1 }),
        read: None,
        error: None,
    }
}
#[test]
fn counts_actual_joined_visits_and_each_repeated_operation_original() {
    let mut totals = Totals::default();
    totals
        .observe(
            0,
            &[
                step(1, None),
                step(1, None),
                step(1, Some(7)),
                step(1, Some(8)),
                step(2, None),
                step(3, Some(9)),
                step(3, Some(10)),
                step(4, None),
                step(4, Some(11)),
            ],
        )
        .unwrap();
    assert_eq!(
        totals.replicas[0],
        Counts {
            transitions: 2,
            replies: 3,
            originals: 3
        }
    );
    assert_eq!(
        totals.summary().unwrap(),
        "joined_transitions=2 joined_reply_inputs=3 joined_originals=3"
    );
    assert_eq!(totals.replicas[1], Counts::default());
}
#[test]
fn ordinary_failed_and_reversed_steps_never_count_as_joined() {
    let mut failed = step(1, Some(1));
    failed.error = Some(voteboat::raft::RaftError::Busy);
    let mut unproposed = step(2, Some(2));
    unproposed.proposed = None;
    for steps in [
        vec![step(1, None), failed],
        vec![step(2, None), unproposed],
        vec![step(3, Some(3)), step(3, None)],
        vec![step(4, None)],
        vec![step(5, Some(5)), step(5, Some(6))],
        vec![step(6, None), step(7, Some(7))],
    ] {
        let mut totals = Totals::default();
        totals.observe(2, &steps).unwrap();
        assert_eq!(totals.replicas, [Counts::default(); 3]);
    }
}
