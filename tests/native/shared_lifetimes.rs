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
use super::*;
use voteboat::{admission::*, buffer::*};

type SendTransport<B> = NativePeerTransport<HostSession, NativeWireCodec, B>;
struct Link<P: AdmissionPolicy, B: BufferPool> {
    queue: NativeOutbound<P>,
    sender: SendTransport<B>,
    receiver: NativePeerTransport<HostSession, NativeWireCodec>,
    wire: Arc<Mutex<Channel>>,
}
impl<P: AdmissionPolicy, B: BufferPool> Link<P, B> {
    fn new(policy: P, pool: B, from: u64, to: u64) -> Self {
        let (mut send, mut receive) = sessions(7);
        send.binding = binding(from, to);
        receive.binding = binding(to, from);
        let wire = send.outgoing.clone();
        let owner = send.binding.local;
        let queue = NativeOutbound::with_policy(
            OutboundBinding {
                node: owner.node,
                store: owner.store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            OutboundLimits::default(),
            policy,
        )
        .unwrap();
        let sender = NativePeerTransport::with_buffers(
            send,
            codec(),
            &queue,
            TransportLimits::default(),
            pool,
        )
        .unwrap();
        let receiver = NativePeerTransport::new(
            receive,
            codec(),
            &super::queue(to),
            TransportLimits::default(),
        )
        .unwrap();
        Self {
            queue,
            sender,
            receiver,
            wire,
        }
    }
    fn send_and_receive(&mut self, expected: Message) {
        self.sender
            .submit(batch(&mut self.queue, vec![expected.clone()]))
            .unwrap();
        for _ in 0..1000 {
            poll(&mut self.sender);
            poll(&mut self.receiver);
            if let Some(received) = self.receiver.take_received() {
                assert_eq!(received.messages, [expected]);
                return;
            }
        }
        panic!("bounded partial-I/O transfer did not complete");
    }
    fn complete(&mut self) {
        // Receiver delivery is distinct from local flush and queue release.
        for _ in 0..1000 {
            if let Some(done) = self.sender.take_send() {
                assert_eq!(done.result, LocalSendResult::Sent);
                self.queue.complete(done.batch, done.result).unwrap();
                assert!(self.queue.is_drained());
                return;
            }
            poll(&mut self.sender);
        }
        panic!("bounded local flush did not complete");
    }
    fn abort(&mut self) {
        self.sender.abort();
        self.receiver.abort();
    }
}
fn data(from: u64, to: u64, sequence: u128) -> Message {
    let mut data = message(from, to, sequence);
    data.rpc = Rpc::Append {
        previous_index: 0,
        previous_term: 0,
        entries: vec![support::entry(1, 1, 7)],
        leader_commit: 0,
    };
    data
}
fn exercise<P: AdmissionPolicy + Clone, B: BufferPool + Clone>(
    mut policy: P,
    mut pool: B,
    usage: impl Fn() -> OutboundUsage,
) {
    let max = TransportLimits::default().send_frame_bytes;
    let blocker = pool.acquire(2 * max, 0).unwrap();
    let mut failed = Link::new(policy.clone(), pool.clone(), 1, 2);
    let mut sibling = Link::new(policy.clone(), pool.clone(), 3, 4);
    let replacement_policy = policy.clone();
    policy.close();
    pool.close();
    failed.wire.lock().unwrap().flushed = false;
    failed.send_and_receive(data(1, 2, 1));
    assert!(!failed.sender.usage().completion);
    assert_eq!(usage().batches, 1);
    let original = vec![data(3, 4, 2)];
    let rejected = sibling.queue.submit(original.clone()).unwrap_err();
    assert_eq!(rejected.reason, OutboundError::Overloaded);
    assert_eq!(rejected.messages, original);
    sibling.send_and_receive(message(3, 4, 3));
    sibling.complete();
    assert_eq!(usage().batches, 1);
    assert_eq!(pool.usage().reserved_bytes, 3 * max);
    failed.abort();
    let completion = failed.sender.take_send().unwrap();
    assert_eq!(completion.result, LocalSendResult::Failed);
    let original_ticket = completion.batch.ticket;
    assert_eq!(pool.usage().reserved_bytes, 2 * max);
    assert_eq!(usage().batches, 1);
    let mut replacement_binding = failed.queue.binding();
    replacement_binding.generation = OutboundGeneration::new(2).unwrap();
    failed.queue.close();
    drop(failed);
    // Closing the first queue has not closed the sibling's shared policy view.
    let rejected = sibling.queue.submit(original.clone()).unwrap_err();
    assert_eq!(rejected.reason, OutboundError::Overloaded);
    assert_eq!(rejected.messages, original);
    let mut replacement = NativeOutbound::with_policy(
        replacement_binding,
        OutboundLimits::default(),
        replacement_policy,
    )
    .unwrap();
    let rejected = replacement
        .complete(completion.batch, completion.result)
        .unwrap_err();
    assert_eq!(rejected.reason, OutboundError::WrongBinding);
    assert_eq!(rejected.batch.ticket, original_ticket);
    assert_eq!(rejected.batch.messages, [data(1, 2, 1)]);
    assert_eq!(usage().batches, 1);
    drop(rejected);
    assert_eq!(usage(), OutboundUsage::default());
    sibling.send_and_receive(data(3, 4, 4));
    sibling.complete();
    sibling.queue.close();
    sibling.abort();
    drop(blocker);
    assert_eq!(pool.usage(), BufferUsage::default());
    assert_eq!(usage(), OutboundUsage::default());
}

fn quota() -> OutboundUsage {
    OutboundUsage {
        batches: 1,
        messages: 16,
        bytes: 65536,
    }
}
fn reserve() -> BufferLimits {
    BufferLimits {
        reserved_bytes: TransportLimits::default().send_frame_bytes,
        leases: 1,
    }
}
#[test]
fn host_providers_keep_shared_credits_through_partial_io_abort_and_replacement() {
    let policy = support::admission::HostPolicy::new(quota());
    let observer = policy.clone();
    let pool =
        support::buffer::HostPool::with_control_reserve(4 * reserve().reserved_bytes, 4, reserve());
    exercise(policy, pool, || observer.usage());
}
#[test]
fn native_providers_keep_shared_credits_through_partial_io_abort_and_replacement() {
    use voteboat::native::{admission::NativeAdmissionPolicy, buffer::NativeBufferPool};
    let policy = NativeAdmissionPolicy::new(quota()).unwrap();
    let observer = policy.clone();
    let pool = NativeBufferPool::new_with_control_reserve(
        BufferLimits {
            reserved_bytes: 4 * reserve().reserved_bytes,
            leases: 4,
        },
        reserve(),
    )
    .unwrap();
    exercise(policy, pool, || observer.usage());
}
