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
//! Fault injection into completion correlation, not child authority evidence.
use super::*;
use crate::runtime::{RuntimeOwner, VisitTicket};

fn fixture() -> (
    LogicalPersistenceWorker,
    SyncSender<WorkerEvent>,
    VisitTicket,
    LogTicket,
) {
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let store = StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    };
    let binding = WorkerBinding {
        store,
        generation: StorageWorkerGeneration::new(1).unwrap(),
    };
    let visit = VisitTicket {
        owner: RuntimeOwner {
            store,
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        },
        group,
        sequence: 1,
    };
    let log = LogTicket {
        binding: store,
        batch: 1,
        group,
        revision: LogRevision::new(2).unwrap(),
        generation: LogGeneration::new(1).unwrap(),
        last_index: 5,
        term: 3,
    };
    let (sender, events) = mpsc::sync_channel(2);
    let worker = LogicalPersistenceWorker {
        binding,
        limits: WorkerLimits::default(),
        mapping: LogicalStoreMapping {
            parent: group,
            voter: NodeId::new(1).unwrap(),
            store: store.identity,
            child: GroupIdentity {
                id: GroupId::new(2).unwrap(),
                ..group
            },
        },
        sender: None,
        events,
        retained: Some(RetainedPersist {
            ticket: WorkerTicket {
                binding,
                sequence: 1,
            },
            visits: vec![visit],
            usage: WorkerUsage {
                requests: 1,
                units: 1,
                bytes: 128,
            },
            written: None,
        }),
        sequence: 1,
        fenced: false,
        outstanding: Arc::new(AtomicBool::new(true)),
    };
    (worker, sender, visit, log)
}

fn written(
    worker: &mut LogicalPersistenceWorker,
    sender: &SyncSender<WorkerEvent>,
    visit: VisitTicket,
    log: LogTicket,
) {
    let request = worker.retained.as_ref().unwrap().ticket;
    sender
        .try_send(WorkerEvent::Written {
            request,
            admissions: vec![(visit, log)],
        })
        .unwrap();
    assert!(matches!(
        worker.poll(1).pop(),
        Some(WorkerEvent::Written { .. })
    ));
    assert_eq!(worker.usage().bytes, 128);
    assert!(worker.outstanding.load(Ordering::Acquire));
}

#[test]
fn durable_matches_every_original_written_ticket_field() {
    for field in 0..8 {
        let (mut worker, sender, visit, original) = fixture();
        written(&mut worker, &sender, visit, original);
        let request = worker.retained.as_ref().unwrap().ticket;
        let mut altered = original;
        match field {
            0 => altered.binding.identity.id = StoreId::new(2).unwrap(),
            1 => altered.binding.session = StoreSession::new(2).unwrap(),
            2 => altered.batch += 1,
            3 => altered.group.incarnation = GroupIncarnation::new(2).unwrap(),
            4 => altered.revision = LogRevision::new(3).unwrap(),
            5 => altered.generation = LogGeneration::new(2).unwrap(),
            6 => altered.last_index += 1,
            7 => altered.term += 1,
            _ => unreachable!(),
        }
        sender
            .try_send(WorkerEvent::Durable {
                request,
                visits: vec![visit],
                completion: DurableLog {
                    tickets: vec![altered],
                },
            })
            .unwrap();
        let result = worker.poll(1).pop();
        let Some(WorkerEvent::Failed {
            request: failed,
            visits,
            error,
        }) = result
        else {
            panic!("mismatched durable field {field} escaped");
        };
        assert_eq!(failed, request);
        assert_eq!(visits, vec![visit]);
        assert!(matches!(error, StorageError::Uncertain(_)));
        assert!(worker.fenced);
        assert_eq!(worker.usage(), WorkerUsage::default());
        assert!(!worker.outstanding.load(Ordering::Acquire));
    }
}

#[test]
fn original_written_ticket_releases_only_at_consumed_durable() {
    let (mut worker, sender, visit, log) = fixture();
    written(&mut worker, &sender, visit, log);
    let request = worker.retained.as_ref().unwrap().ticket;
    sender
        .try_send(WorkerEvent::Durable {
            request,
            visits: vec![visit],
            completion: DurableLog { tickets: vec![log] },
        })
        .unwrap();
    assert_eq!(worker.usage().bytes, 128);
    let result = worker.poll(1).pop();
    assert!(matches!(result, Some(WorkerEvent::Durable { .. })));
    assert!(!worker.fenced);
    assert_eq!(worker.usage(), WorkerUsage::default());
    assert!(!worker.outstanding.load(Ordering::Acquire));
}
