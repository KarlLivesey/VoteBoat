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
mod support;
use support::{admission::HostPolicy, *};
use voteboat::{admission::*, identity::*, outbound::*};
fn binding(id: u64) -> OutboundBinding {
    OutboundBinding {
        node: node(id),
        store: StoreBinding {
            identity: identity(id as u128),
            session: StoreSession::new(1).unwrap(),
        },
        generation: OutboundGeneration::new(1).unwrap(),
    }
}
fn max(batches: usize) -> OutboundUsage {
    OutboundUsage {
        batches,
        messages: 128,
        bytes: 65536,
    }
}
fn request() -> AdmissionRequest {
    AdmissionRequest {
        owner: binding(1),
        peer: node(2),
        class: MessageClass::Data,
        cost: OutboundUsage {
            batches: 1,
            messages: 1,
            bytes: 1024,
        },
        node_usage: OutboundUsage::default(),
        peer_usage: OutboundUsage::default(),
    }
}
#[test]
fn downstream_owned_lease_survives_shared_view_close_and_clone_without_native() {
    let mut first = HostPolicy::new(max(1));
    let other = first.clone();
    let lease = first.reserve(request()).unwrap();
    let extra_owner = lease.clone();
    assert_eq!(other.usage(), request().cost);
    first.close();
    assert!(matches!(
        first.reserve(request()),
        Err(AdmissionError::Closed)
    ));
    drop(first);
    drop(lease);
    assert!(matches!(
        other.reserve(request()),
        Err(AdmissionError::Overloaded)
    ));
    drop(extra_owner);
    assert_eq!(other.usage(), OutboundUsage::default());
    drop(other.reserve(request()).unwrap());
    assert_eq!(other.usage(), OutboundUsage::default());
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::{admission::NativeAdmissionPolicy, outbound::NativeOutbound};
    use voteboat::raft::*;
    fn limits() -> OutboundLimits {
        let mut limits = OutboundLimits {
            max_peers: 2,
            ..OutboundLimits::default()
        };
        for budget in [&mut limits.node, &mut limits.peer] {
            budget.max.batches = 4;
            budget.control.batches = 1;
            budget.background.batches = 1;
        }
        limits
    }
    fn message(owner: u64, peer: u64, data: bool) -> Message {
        let owner = binding(owner);
        Message {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            from: owner.node,
            to: node(peer),
            sender: owner.store,
            term: 1,
            context: RequestContext {
                origin: owner.store,
                sequence: 1,
            },
            rpc: if data {
                Rpc::Append {
                    previous_index: 0,
                    previous_term: 0,
                    entries: vec![entry(1, 1, 7)],
                    leader_commit: 0,
                }
            } else {
                Rpc::ReadProbe
            },
        }
    }
    fn finish(queue: &mut impl OutboundQueue) {
        for batch in queue.poll(100) {
            queue.complete(batch, LocalSendResult::Sent).unwrap();
        }
        assert!(queue.is_drained());
    }
    #[test]
    fn host_policy_in_native_queue_preserves_rejection_scope_and_last_owned_batch() {
        let policy = HostPolicy::new(max(1));
        let mut first = NativeOutbound::with_policy(binding(1), limits(), policy.clone()).unwrap();
        let mut second = NativeOutbound::with_policy(binding(3), limits(), policy.clone()).unwrap();
        first.submit(vec![message(1, 2, true)]).unwrap();
        assert_eq!(policy.last().owner, binding(1));
        assert_eq!(policy.last().peer, node(2));
        assert_eq!(policy.last().node_usage, OutboundUsage::default());
        let mut messages = Vec::with_capacity(8);
        messages.push(message(3, 2, true));
        let original_pointer = messages.as_ptr();
        let original_capacity = messages.capacity();
        let rejected = second.submit(messages).unwrap_err();
        assert_eq!(rejected.reason, OutboundError::Overloaded);
        assert_eq!(rejected.messages.as_ptr(), original_pointer);
        assert_eq!(rejected.messages.capacity(), original_capacity);
        let mut batch = first.poll(1).pop().unwrap();
        assert!(batch.admission.is_some());
        let original_ticket = batch.ticket;
        batch.ticket.binding.generation = OutboundGeneration::new(2).unwrap();
        let rejected = first.complete(batch, LocalSendResult::Failed).unwrap_err();
        assert_eq!(rejected.reason, OutboundError::WrongBinding);
        assert_eq!(policy.usage().batches, 1);
        let mut batch = *rejected.batch;
        batch.ticket = original_ticket;
        first.close();
        drop(first); // dispatched batch still owns reservation
        assert_eq!(policy.usage().batches, 1);
        assert_eq!(
            second.submit(vec![message(3, 2, true)]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        second.submit(vec![message(3, 2, false)]).unwrap();
        finish(&mut second);
        assert_eq!(policy.usage().batches, 1);
        drop(batch);
        assert_eq!(policy.usage(), OutboundUsage::default());
        second.submit(vec![message(3, 2, true)]).unwrap();
        finish(&mut second);
        assert_eq!(policy.usage(), OutboundUsage::default());
    }
    #[test]
    fn permissive_policy_cannot_cross_node_peer_background_or_control_reserves() {
        let policy = HostPolicy::new(max(128));
        let mut queue = NativeOutbound::with_policy(binding(1), limits(), policy.clone()).unwrap();
        for _ in 0..3 {
            queue.submit(vec![message(1, 2, true)]).unwrap();
        }
        assert_eq!(policy.calls(), 3);
        assert_eq!(
            queue.submit(vec![message(1, 2, true)]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        assert_eq!(policy.calls(), 3); // hard refusal before optional policy
        queue.submit(vec![message(1, 2, false)]).unwrap();
        assert_eq!(policy.calls(), 3); // control does not depend on policy
        assert_eq!(
            queue.submit(vec![message(1, 3, false)]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        finish(&mut queue);
        assert_eq!(policy.usage(), OutboundUsage::default());
        // Peer reserve remains protected even when the node has more capacity.
        let mut peer_limited = limits();
        peer_limited.node.max.batches = 8;
        let mut queue =
            NativeOutbound::with_policy(binding(1), peer_limited, policy.clone()).unwrap();
        for _ in 0..3 {
            queue.submit(vec![message(1, 2, true)]).unwrap();
        }
        assert_eq!(
            queue.submit(vec![message(1, 2, true)]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        queue.submit(vec![message(1, 3, true)]).unwrap();
        let calls = policy.calls();
        assert_eq!(
            queue.submit(vec![message(1, 4, true)]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        assert_eq!(policy.calls(), calls); // peer-count refusal before reserve
        finish(&mut queue);
        let mut background = message(1, 2, true);
        background.rpc = Rpc::Snapshot {
            snapshot: Box::new(voteboat::snapshot::Snapshot {
                metadata: voteboat::snapshot::SnapshotMetadata {
                    membership: None,
                    bootstrap: bootstrap(1, 3),
                    index: 1,
                    term: 1,
                    application_schema: 1,
                },
                application: vec![7; 64],
            }),
        };
        queue.submit(vec![background.clone()]).unwrap();
        let calls = policy.calls();
        assert_eq!(
            queue.submit(vec![background]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        assert_eq!(policy.calls(), calls);
        finish(&mut queue);
        assert_eq!(policy.usage(), OutboundUsage::default());
    }
    #[test]
    fn closed_policy_refuses_bulk_but_cannot_block_control_or_close_another_queue() {
        let shared = NativeAdmissionPolicy::new(max(1)).unwrap();
        let mut closed = shared.clone();
        closed.close();
        let mut queue = NativeOutbound::with_policy(binding(1), limits(), closed).unwrap();
        assert_eq!(
            queue.submit(vec![message(1, 2, true)]).unwrap_err().reason,
            OutboundError::Overloaded
        );
        queue.submit(vec![message(1, 2, false)]).unwrap();
        let batch = queue.poll(1).pop().unwrap();
        assert!(batch.admission.is_none());
        queue.complete(batch, LocalSendResult::Sent).unwrap();
        queue.close();
        let mut other = NativeOutbound::with_policy(binding(3), limits(), shared.clone()).unwrap();
        other.submit(vec![message(3, 2, true)]).unwrap();
        assert_eq!(shared.usage().batches, 1);
        other.close();
        finish(&mut other);
        assert_eq!(shared.usage(), OutboundUsage::default());
    }
    #[test]
    fn native_shared_budget_rolls_back_each_dimension_and_preserves_last_owner() {
        let policy = NativeAdmissionPolicy::new(OutboundUsage {
            batches: 2,
            messages: 1,
            bytes: 1024,
        })
        .unwrap();
        let mut bad = request();
        bad.cost.messages = 2;
        assert!(matches!(
            policy.reserve(bad),
            Err(AdmissionError::Overloaded)
        ));
        assert_eq!(policy.usage(), OutboundUsage::default());
        bad = request();
        bad.cost.bytes = 1025;
        assert!(matches!(
            policy.reserve(bad),
            Err(AdmissionError::Overloaded)
        ));
        assert_eq!(policy.usage(), OutboundUsage::default());
        bad = request();
        bad.class = MessageClass::Control;
        assert!(matches!(
            policy.reserve(bad),
            Err(AdmissionError::InvalidRequest)
        ));
        let lease = policy.reserve(request()).unwrap();
        let last = lease.clone();
        drop(lease);
        assert_eq!(policy.usage(), request().cost);
        assert!(matches!(
            policy.reserve(request()),
            Err(AdmissionError::Overloaded)
        ));
        drop(last);
        assert_eq!(policy.usage(), OutboundUsage::default());
        assert!(NativeAdmissionPolicy::new(OutboundUsage::default()).is_err());
    }
    #[test]
    fn abandoned_batch_holds_queue_reservation_until_owner_queue_is_dropped() {
        let policy = NativeAdmissionPolicy::new(max(1)).unwrap();
        let mut queue = NativeOutbound::with_policy(binding(1), limits(), policy.clone()).unwrap();
        queue.submit(vec![message(1, 2, true)]).unwrap();
        drop(queue.poll(1).pop().unwrap());
        assert_eq!(policy.usage().batches, 1);
        assert!(!queue.is_drained());
        queue.close();
        assert_eq!(policy.usage().batches, 1);
        drop(queue);
        assert_eq!(policy.usage(), OutboundUsage::default());
    }
    #[test]
    fn failed_queue_construction_never_reserves_or_closes_the_host_policy() {
        let policy = HostPolicy::new(max(1));
        let mut invalid = limits();
        invalid.max_peers = 0;
        assert!(matches!(
            NativeOutbound::with_policy(binding(1), invalid, policy.clone()),
            Err(OutboundError::InvalidLimits)
        ));
        assert_eq!(policy.calls(), 0);
        assert_eq!(policy.usage(), OutboundUsage::default());
        let mut queue = NativeOutbound::with_policy(binding(1), limits(), policy.clone()).unwrap();
        queue.submit(vec![message(1, 2, true)]).unwrap();
        finish(&mut queue);
        assert_eq!(policy.usage(), OutboundUsage::default());
    }
    #[test]
    fn concurrent_native_policy_views_never_leak_partial_reservations() {
        let policy = NativeAdmissionPolicy::new(OutboundUsage {
            batches: 8,
            messages: 8,
            bytes: 8192,
        })
        .unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let policy = policy.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        match policy.reserve(request()) {
                            Ok(lease) => {
                                let usage = policy.usage();
                                assert!(
                                    usage.batches <= 8
                                        && usage.messages <= 8
                                        && usage.bytes <= 8192
                                );
                                std::thread::yield_now();
                                drop(lease);
                            }
                            Err(AdmissionError::Overloaded) => {}
                            Err(error) => panic!("{error:?}"),
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(policy.usage(), OutboundUsage::default());
    }
}
