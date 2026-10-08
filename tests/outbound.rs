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
use support::outbound::HostOutbound;
use support::*;
use voteboat::{identity::*, outbound::*, raft::*};
fn binding() -> OutboundBinding {
    OutboundBinding {
        node: node(1),
        store: HostLogStore::new(1).binding,
        generation: OutboundGeneration::new(1).unwrap(),
    }
}
fn small() -> OutboundLimits {
    OutboundLimits {
        max_peers: 2,
        node: OutboundBudget {
            max: OutboundUsage {
                batches: 8,
                messages: 16,
                bytes: 64 * 1024,
            },
            control: OutboundUsage {
                batches: 2,
                messages: 4,
                bytes: 4 * 1024,
            },
            background: OutboundUsage {
                batches: 2,
                messages: 4,
                bytes: 8 * 1024,
            },
        },
        peer: OutboundBudget {
            max: OutboundUsage {
                batches: 4,
                messages: 8,
                bytes: 16 * 1024,
            },
            control: OutboundUsage {
                batches: 1,
                messages: 2,
                bytes: 2 * 1024,
            },
            background: OutboundUsage {
                batches: 1,
                messages: 2,
                bytes: 4 * 1024,
            },
        },
        batch_messages: 4,
        batch_bytes: 4 * 1024,
    }
}
fn message(peer: u64, class: MessageClass) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        sender: binding().store,
        to: node(peer),
        term: 1,
        context: RequestContext {
            origin: binding().store,
            sequence: 1,
        },
        rpc: match class {
            MessageClass::Control => Rpc::ReadProbe,
            MessageClass::Data => Rpc::Append {
                previous_index: 0,
                previous_term: 0,
                entries: vec![entry(1, 1, 7)],
                leader_commit: 0,
            },
            MessageClass::Background => Rpc::Snapshot {
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
            },
        },
    }
}
fn conformance(mut queue: impl OutboundQueue) {
    let original = message(2, MessageClass::Data);
    queue.submit(vec![original.clone()]).unwrap();
    queue.submit(vec![original.clone()]).unwrap();
    queue.submit(vec![original.clone()]).unwrap();
    let rejected = queue.submit(vec![original.clone()]).unwrap_err();
    assert_eq!(rejected.reason, OutboundError::Overloaded);
    assert_eq!(rejected.messages, [original]);
    queue
        .submit(vec![message(2, MessageClass::Control)])
        .unwrap();
    queue
        .submit(vec![message(3, MessageClass::Control)])
        .unwrap();
    assert_eq!(queue.peer_usage(node(2)).batches, 4);
    assert_eq!(
        queue
            .submit(vec![message(4, MessageClass::Control)])
            .unwrap_err()
            .reason,
        OutboundError::Overloaded
    );
    let usage = queue.usage();
    assert!(queue.poll(0).is_empty());
    let mut dispatched = queue.poll(100);
    assert_eq!(dispatched.len(), 5);
    assert_eq!(queue.usage(), usage); // still held by the downstream sender
    assert!(queue.poll(100).is_empty());
    assert_eq!(
        queue
            .submit(vec![message(2, MessageClass::Data)])
            .unwrap_err()
            .reason,
        OutboundError::Overloaded
    );
    let mut batch = dispatched.pop().unwrap();
    let original_ticket = batch.ticket;
    batch.ticket.binding.generation = OutboundGeneration::new(2).unwrap();
    let rejected = queue.complete(batch, LocalSendResult::Sent).unwrap_err();
    assert_eq!(rejected.reason, OutboundError::WrongBinding);
    assert_eq!(queue.usage(), usage);
    let mut batch = *rejected.batch;
    batch.ticket = original_ticket;
    let replay = OutboundBatch {
        ticket: original_ticket,
        messages: vec![],
    };
    assert_eq!(
        queue
            .complete(batch, LocalSendResult::Failed)
            .unwrap()
            .result,
        LocalSendResult::Failed
    );
    assert_eq!(
        queue
            .complete(replay, LocalSendResult::Sent)
            .unwrap_err()
            .reason,
        OutboundError::UnknownTicket
    );
    queue
        .submit(vec![message(
            original_ticket.peer.get(),
            MessageClass::Control,
        )])
        .unwrap();
    queue.close();
    assert_eq!(
        queue
            .submit(vec![message(2, MessageClass::Control)])
            .unwrap_err()
            .reason,
        OutboundError::Closed
    );
    assert!(!queue.is_drained());
    for batch in dispatched {
        queue.complete(batch, LocalSendResult::Cancelled).unwrap();
    }
    // Closing must still dispatch accepted queued work, as well as resolve
    // batches already held by an external sender.
    for batch in queue.poll(100) {
        queue.complete(batch, LocalSendResult::Sent).unwrap();
    }
    assert!(queue.is_drained());
    assert_eq!(queue.peer_usage(node(2)), OutboundUsage::default());
}
fn rejection_and_background(mut queue: impl OutboundQueue) {
    let mut wrong = message(2, MessageClass::Control);
    wrong.sender.session = StoreSession::new(2).unwrap();
    assert_eq!(
        queue.submit(vec![wrong]).unwrap_err().reason,
        OutboundError::WrongBinding
    );
    assert_eq!(
        queue
            .submit(vec![
                message(2, MessageClass::Control),
                message(3, MessageClass::Control)
            ])
            .unwrap_err()
            .reason,
        OutboundError::WrongPeer
    );
    assert_eq!(
        queue
            .submit(vec![message(1, MessageClass::Control)])
            .unwrap_err()
            .reason,
        OutboundError::WrongPeer
    );
    let mut oversized = Vec::with_capacity(5);
    oversized.push(message(2, MessageClass::Control));
    assert_eq!(
        queue.submit(oversized).unwrap_err().reason,
        OutboundError::BatchTooLarge
    );
    let mut payload = message(2, MessageClass::Data);
    if let Rpc::Append { entries, .. } = &mut payload.rpc {
        let voteboat::log::EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
            panic!()
        };
        *bytes = Vec::with_capacity(8192);
        bytes.push(7);
    }
    assert_eq!(
        queue.submit(vec![payload]).unwrap_err().reason,
        OutboundError::BatchTooLarge
    );
    assert_eq!(queue.usage(), OutboundUsage::default());
    queue
        .submit(vec![message(2, MessageClass::Background)])
        .unwrap();
    assert_eq!(
        queue
            .submit(vec![message(2, MessageClass::Background)])
            .unwrap_err()
            .reason,
        OutboundError::Overloaded
    );
    queue.submit(vec![message(2, MessageClass::Data)]).unwrap();
    queue
        .submit(vec![message(2, MessageClass::Control)])
        .unwrap();
    for batch in queue.poll(100) {
        queue.complete(batch, LocalSendResult::Sent).unwrap();
    }
    assert!(queue.is_drained());
}
#[test]
fn host_outbound_conformance_without_native_resources() {
    conformance(HostOutbound::new(binding(), small()).unwrap());
    let mut limits = small();
    limits.node.max.batches = 5;
    conformance(HostOutbound::new(binding(), limits).unwrap());
    rejection_and_background(HostOutbound::new(binding(), small()).unwrap());
}
fn bytes_and_exact_completion(mut queue: impl OutboundQueue) {
    let data = message(2, MessageClass::Data);
    queue.submit(vec![data.clone()]).unwrap();
    queue.submit(vec![data.clone()]).unwrap();
    assert_eq!(
        queue.submit(vec![data]).unwrap_err().reason,
        OutboundError::Overloaded
    );
    let ticket = queue
        .submit(vec![message(2, MessageClass::Control)])
        .unwrap();
    let usage = queue.usage();
    let premature = OutboundBatch {
        ticket,
        messages: vec![],
    };
    assert_eq!(
        queue
            .complete(premature, LocalSendResult::Sent)
            .unwrap_err()
            .reason,
        OutboundError::NotDispatched
    );
    assert_eq!(queue.usage(), usage);
    let mut batches = queue.poll(100);
    let mut batch = batches.pop().unwrap();
    let original = batch.ticket;
    batch.ticket.peer = node(3);
    let rejected = queue.complete(batch, LocalSendResult::Sent).unwrap_err();
    assert_eq!(queue.usage(), usage);
    let mut batch = *rejected.batch;
    batch.ticket = original;
    queue.complete(batch, LocalSendResult::Failed).unwrap();
    queue.close();
    for batch in batches {
        queue.complete(batch, LocalSendResult::Sent).unwrap();
    }
    assert!(queue.is_drained());
}
fn byte_limits() -> OutboundLimits {
    let mut limits = small();
    let data = vec![message(2, MessageClass::Data)];
    let (_, _, cost) = batch_cost(binding(), &data, data.capacity(), limits).unwrap();
    limits.peer.max.bytes = cost.bytes * 3;
    limits.peer.control.bytes = cost.bytes;
    limits.peer.background.bytes = cost.bytes;
    limits.batch_bytes = cost.bytes * 3;
    limits
}
#[test]
fn host_byte_reserve_and_completion_scope() {
    bytes_and_exact_completion(HostOutbound::new(binding(), byte_limits()).unwrap());
}
#[test]
fn invalid_reserve_cannot_construct_a_queue_and_mixed_batches_cannot_claim_control() {
    let mut limits = small();
    limits.peer.control.bytes = 1;
    assert_eq!(limits.validate().unwrap_err(), OutboundError::InvalidLimits);
    assert!(HostOutbound::new(binding(), limits).is_err());
    #[cfg(feature = "native")]
    assert!(voteboat::native::outbound::NativeOutbound::new(binding(), limits).is_err());
    let messages = vec![
        message(2, MessageClass::Control),
        message(2, MessageClass::Data),
    ];
    assert_eq!(
        batch_cost(binding(), &messages, messages.capacity(), small())
            .unwrap()
            .1,
        MessageClass::Data
    );
    let messages = vec![
        message(2, MessageClass::Control),
        message(2, MessageClass::Background),
    ];
    assert_eq!(
        batch_cost(binding(), &messages, messages.capacity(), small())
            .unwrap()
            .1,
        MessageClass::Background
    );
    let mut a = HostOutbound::new(binding(), small()).unwrap();
    let mut next = binding();
    next.generation = OutboundGeneration::new(2).unwrap();
    let mut b = HostOutbound::new(next, small()).unwrap();
    a.close();
    b.submit(vec![message(2, MessageClass::Control)]).unwrap();
    for batch in b.poll(1) {
        b.complete(batch, LocalSendResult::Sent).unwrap();
    }
    assert!(b.is_drained());
}
#[cfg(feature = "native")]
#[test]
fn native_outbound_conformance_and_fair_dispatch() {
    use voteboat::native::outbound::NativeOutbound;
    conformance(NativeOutbound::new(binding(), small()).unwrap());
    let mut limits = small();
    limits.node.max.batches = 5;
    conformance(NativeOutbound::new(binding(), limits).unwrap());
    rejection_and_background(NativeOutbound::new(binding(), small()).unwrap());
    bytes_and_exact_completion(NativeOutbound::new(binding(), byte_limits()).unwrap());
    let mut queue = NativeOutbound::new(binding(), OutboundLimits::default()).unwrap();
    for _ in 0..10 {
        queue
            .submit(vec![message(2, MessageClass::Control)])
            .unwrap();
    }
    queue.submit(vec![message(2, MessageClass::Data)]).unwrap();
    queue
        .submit(vec![message(2, MessageClass::Background)])
        .unwrap();
    queue
        .submit(vec![message(3, MessageClass::Control)])
        .unwrap();
    let batches = queue.poll(5);
    assert_eq!(batches[0].ticket.peer, node(2));
    assert_eq!(batches[1].ticket.peer, node(3));
    assert_eq!(
        message_cost(&batches[2].messages[0], usize::MAX).unwrap().0,
        MessageClass::Data
    );
    assert_eq!(
        message_cost(&batches[4].messages[0], usize::MAX).unwrap().0,
        MessageClass::Background
    );
    for batch in batches {
        queue.complete(batch, LocalSendResult::Sent).unwrap();
    }
    for batch in queue.poll(100) {
        queue.complete(batch, LocalSendResult::Sent).unwrap();
    }
    assert!(queue.is_drained());
    // Empty peer metadata cannot exhaust the roster under sequential use.
    for peer in 2..2000 {
        queue
            .submit(vec![message(peer, MessageClass::Control)])
            .unwrap();
        let batch = queue.poll(1).pop().unwrap();
        queue.complete(batch, LocalSendResult::Sent).unwrap();
    }
}
