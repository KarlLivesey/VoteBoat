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
use std::{cell::RefCell, rc::Rc};
use voteboat::{identity::*, observability::*, runtime::*};
#[path = "observability/events.rs"]
mod events;
#[path = "observability/timing.rs"]
mod timing;
fn owner(id: u128) -> RuntimeOwner {
    RuntimeOwner {
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(id).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn empty(owner: RuntimeOwner) -> CounterSnapshot {
    CounterSnapshot {
        owner,
        sampled_at: None,
        state: None,
        counters: NodeCounters::default(),
        closed: false,
    }
}
fn counters(n: u64) -> NodeCounters {
    NodeCounters {
        polls: n,
        failed_polls: n,
        owner_steps: n,
        step_errors: n,
        worker_events: n,
        snapshot_events: n,
        snapshot_installs: n,
        snapshot_send_refusals: n,
        persistence_batches: n,
        applications: n,
        peer_sends: n,
        peer_received: n,
        ingress_blocked: n,
        connection_failures: n,
    }
}
fn sample(owner: RuntimeOwner, time: u64, n: u64) -> NodeObservation {
    NodeObservation {
        owner,
        sampled_at: MonoTime(time),
        state: NodeState::Running,
        counters: counters(n),
    }
}
// An independently owned host resource with two scoped views and no native types.
struct HostObserver {
    slots: Rc<RefCell<[CounterSnapshot; 2]>>,
    slot: usize,
}
impl Observer for HostObserver {
    fn record_bounded(&mut self, s: NodeObservation) -> Result<(), ObservationError> {
        let mut slots = self.slots.borrow_mut();
        let dst = &mut slots[self.slot];
        if dst.closed {
            return Err(ObservationError::Closed);
        }
        if dst.owner != s.owner {
            return Err(ObservationError::WrongOwner);
        }
        if dst.sampled_at.is_some_and(|t| t > s.sampled_at) {
            return Err(ObservationError::ClockRegressed);
        }
        dst.counters = dst.counters.saturating_add(s.counters);
        dst.sampled_at = Some(s.sampled_at);
        dst.state = Some(s.state);
        Ok(())
    }
    fn snapshot_counters(&self) -> CounterSnapshot {
        self.slots.borrow()[self.slot]
    }
    fn close(&mut self) {
        self.slots.borrow_mut()[self.slot].closed = true;
    }
}
fn conformance(mut observer: impl Observer) {
    let binding = observer.snapshot_counters().owner;
    assert_eq!(observer.snapshot_counters(), empty(binding));
    observer.record_bounded(sample(binding, 10, 2)).unwrap();
    let before = observer.snapshot_counters();
    assert_eq!(before.counters, counters(2));
    let mut wrong = binding;
    wrong.store.session = StoreSession::new(2).unwrap();
    let mut incarnation = binding;
    incarnation.store.identity.incarnation = StoreIncarnation::new(2).unwrap();
    let mut generation = binding;
    generation.generation = RuntimeGeneration::new(2).unwrap();
    let mut lane = binding;
    lane.lane = ExecutionLaneId::new(2).unwrap();
    for foreign in [owner(2), wrong, incarnation, generation, lane] {
        assert_eq!(
            observer.record_bounded(sample(foreign, 11, 1)),
            Err(ObservationError::WrongOwner)
        );
        assert_eq!(observer.snapshot_counters(), before);
    }
    assert_eq!(
        observer.record_bounded(sample(binding, 9, 1)),
        Err(ObservationError::ClockRegressed)
    );
    assert_eq!(observer.snapshot_counters(), before);
    // Equal millisecond times are legitimate for distinct polls; no dedup claim.
    observer.record_bounded(sample(binding, 10, 3)).unwrap();
    assert_eq!(observer.snapshot_counters().counters, counters(5));
    assert_eq!(before.counters, counters(2)); // Exported values have independent lifetime.
    observer
        .record_bounded(sample(binding, 11, u64::MAX))
        .unwrap();
    observer.record_bounded(sample(binding, 12, 1)).unwrap();
    assert_eq!(observer.snapshot_counters().counters, counters(u64::MAX));
    observer.close();
    observer.close();
    let closed = observer.snapshot_counters();
    assert!(closed.closed);
    assert_eq!(
        observer.record_bounded(sample(binding, 13, 1)),
        Err(ObservationError::Closed)
    );
    assert_eq!(observer.snapshot_counters(), closed);
}
#[test]
fn downstream_observer_conformance_and_shared_scope_close() {
    let slots = Rc::new(RefCell::new([empty(owner(1)), empty(owner(2))]));
    conformance(HostObserver {
        slots: slots.clone(),
        slot: 0,
    });
    let mut other = HostObserver {
        slots: slots.clone(),
        slot: 1,
    };
    other.record_bounded(sample(owner(2), 1, 7)).unwrap();
    assert_eq!(other.snapshot_counters().counters, counters(7));
    drop(other);
    assert!(!slots.borrow()[1].closed); // Dropping a view does not close host resources.
}
#[cfg(feature = "native")]
#[test]
fn native_observer_conformance_has_fixed_storage_and_independent_instances() {
    use voteboat::native::observability::NativeCounterObserver;
    assert!(std::mem::size_of::<NativeCounterObserver>() < 512);
    conformance(NativeCounterObserver::new(owner(1)));
    let mut other = NativeCounterObserver::new(owner(2));
    other.record_bounded(sample(owner(2), 1, 7)).unwrap();
    assert_eq!(other.snapshot_counters().counters, counters(7));
}
#[test]
fn capture_counts_returned_work_and_never_fabricates_progress_for_failed_polls() {
    let progress = Ok(NodeProgress {
        state: NodeState::Running,
        replica: Some(ReplicaProgress {
            steps: vec![OwnerStep {
                admission: None,
                proposed: None,
                visit: VisitTicket {
                    owner: owner(1),
                    group: GroupIdentity {
                        id: GroupId::new(1).unwrap(),
                        incarnation: GroupIncarnation::new(1).unwrap(),
                    },
                    sequence: 1,
                },
                operation: None,
                read: None,
                error: Some(voteboat::raft::RaftError::NotLeader),
            }],
            worker_events: 2,
            snapshot_events: 3,
            snapshot_installs: 1,
            snapshot_send_refusals: 2,
            persistence_batches: 4,
            applications: 5,
            ..Default::default()
        }),
        peers: Some(PeerDriverProgress {
            sends: 6,
            received: 7,
            ingress_blocked: 8,
            connection_failures: 9,
            ..Default::default()
        }),
    });
    let sample = NodeObservation::from_poll(owner(1), MonoTime(12), NodeState::Running, &progress);
    assert_eq!(
        sample.counters,
        NodeCounters {
            polls: 1,
            failed_polls: 0,
            owner_steps: 1,
            step_errors: 1,
            worker_events: 2,
            snapshot_events: 3,
            snapshot_installs: 1,
            snapshot_send_refusals: 2,
            persistence_batches: 4,
            applications: 5,
            peer_sends: 6,
            peer_received: 7,
            ingress_blocked: 8,
            connection_failures: 9
        }
    );
    assert!(progress.is_ok());
    let failure = Err(NodeError::InvalidLimits);
    let sample = NodeObservation::from_poll(owner(1), MonoTime(13), NodeState::Running, &failure);
    assert_eq!(
        sample.counters,
        NodeCounters {
            polls: 1,
            failed_polls: 1,
            ..Default::default()
        }
    );
    assert_eq!(sample.state, NodeState::Running); // A rejected budget is not a fenced core.
    assert!(matches!(failure, Err(NodeError::InvalidLimits)));
}
