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
//! Completion-stream fault controls; these do not mint child authority.
use super::*;
use crate::{
    quorum::{Limits, Policy, Tree},
    runtime::RuntimeOwner,
};

struct Fixture {
    worker: LogicalSnapshotWorker,
    requests: Receiver<AcceptedSnapshot>,
    events: SyncSender<SnapshotWorkEvent>,
    visit: VisitTicket,
}
fn fixture() -> Fixture {
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let store = StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(101).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    };
    let images = SnapshotLimits {
        max_application_bytes: 4096,
        max_metadata_bytes: 4096,
        max_chunk_bytes: 1024,
    };
    let (sender, requests) = mpsc::sync_channel(1);
    let (events, outputs) = mpsc::sync_channel(1);
    let worker = LogicalSnapshotWorker {
        binding: SnapshotWorkerBinding {
            store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        },
        mapping: LogicalStoreMapping {
            parent: group,
            voter: NodeId::new(11).unwrap(),
            store: store.identity,
            child: GroupIdentity {
                id: GroupId::new(2).unwrap(),
                ..group
            },
        },
        limits: SnapshotWorkLimits::default(),
        images,
        allowance: snapshot_load_reservation(images).unwrap(),
        sender: Some(sender),
        events: outputs,
        retained: None,
        sequence: 0,
        fenced: false,
        outstanding: Arc::new(AtomicBool::new(false)),
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
    Fixture {
        worker,
        requests,
        events,
        visit,
    }
}
fn wrong_snapshot(f: &Fixture) -> Snapshot {
    let group = GroupIdentity {
        id: GroupId::new(2).unwrap(),
        ..f.visit.group
    };
    let node = f.worker.mapping.voter;
    Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: Bootstrap {
                group,
                configuration: ConfigurationId::new(1).unwrap(),
                policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
                voter_stores: BTreeMap::from([(node, f.worker.mapping.store)]),
            },
            membership: None,
            index: 1,
            term: 1,
            application_schema: 1,
        },
        application: vec![7; 32],
    }
}
#[test]
fn foreign_snapshot_rejection_returns_original_buffer_and_no_credits() {
    let mut f = fixture();
    let snapshot = wrong_snapshot(&f);
    let original = snapshot.application.as_ptr();
    let error = f
        .worker
        .submit(SnapshotWork {
            visit: f.visit,
            job: SnapshotJob::Publish {
                snapshot,
                durable: None,
            },
        })
        .unwrap_err();
    assert_eq!(error.reason, SnapshotWorkError::WrongBinding);
    let SnapshotJob::Publish { snapshot, .. } = error.work.job else {
        panic!("original publication lost");
    };
    assert_eq!(snapshot.application.as_ptr(), original);
    assert_eq!(snapshot.application, vec![7; 32]);
    assert_eq!(f.worker.usage(), SnapshotWorkUsage::default());
    assert!(!f.worker.outstanding.load(Ordering::Acquire));
    assert!(matches!(f.requests.try_recv(), Err(TryRecvError::Empty)));
}
#[test]
fn original_readiness_credit_survives_queued_output_and_close_until_poll() {
    let mut f = fixture();
    let request = f
        .worker
        .submit(SnapshotWork {
            visit: f.visit,
            job: SnapshotJob::Readiness { reference: None },
        })
        .unwrap();
    let accepted = f.requests.try_recv().unwrap();
    assert_eq!(accepted.request, request);
    assert_eq!(accepted.work.visit, f.visit);
    f.events
        .try_send(SnapshotWorkEvent {
            request,
            visit: f.visit,
            result: Ok(SnapshotOutput::Readiness {
                snapshot: None,
                limits: f.worker.images,
            }),
        })
        .unwrap();
    f.worker.close();
    assert!(f.worker.poll(0).is_empty());
    assert_eq!(f.worker.usage().requests, 1);
    assert!(f.worker.outstanding.load(Ordering::Acquire));
    let events = f.worker.poll(1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].request, request);
    assert!(matches!(
        events[0].result,
        Ok(SnapshotOutput::Readiness { snapshot: None, .. })
    ));
    assert!(f.worker.is_drained());
    assert!(!f.worker.outstanding.load(Ordering::Acquire));
}
#[test]
fn foreign_completion_fences_and_returns_original_uncertainty() {
    let mut f = fixture();
    let request = f
        .worker
        .submit(SnapshotWork {
            visit: f.visit,
            job: SnapshotJob::Readiness { reference: None },
        })
        .unwrap();
    let _accepted = f.requests.try_recv().unwrap();
    f.events
        .try_send(SnapshotWorkEvent {
            request: SnapshotWorkTicket {
                sequence: request.sequence + 1,
                ..request
            },
            visit: f.visit,
            result: Ok(SnapshotOutput::Readiness {
                snapshot: None,
                limits: f.worker.images,
            }),
        })
        .unwrap();
    let mut events = f.worker.poll(1);
    let event = events.pop().unwrap();
    assert_eq!(event.request, request);
    assert_eq!(event.visit, f.visit);
    assert!(matches!(event.result, Err(StorageError::Uncertain(_))));
    assert!(f.worker.fenced);
    assert!(f.worker.is_drained());
    assert!(!f.worker.outstanding.load(Ordering::Acquire));
}
