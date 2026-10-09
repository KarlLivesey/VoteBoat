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
use voteboat::{runtime::MonoTime, transport::*};
#[test]
fn transport_limits_are_available_without_native_dependencies() {
    assert!(TransportLimits::default().validate().is_ok());
    assert_eq!(
        TransportLimits {
            decoded_bytes: 0,
            ..TransportLimits::default()
        }
        .validate()
        .unwrap_err(),
        TransportError::InvalidLimits
    );
    assert_eq!(
        TransportPollBudget {
            plaintext_calls: 4097,
            ..TransportPollBudget::default()
        }
        .validate()
        .unwrap_err(),
        TransportError::InvalidLimits
    );
    // A host transport can be selected through this same object-safe contract.
    struct Host;
    impl PeerTransport for Host {
        fn security(&self) -> voteboat::secure::SessionSecurity {
            voteboat::secure::SessionSecurity::SimulatorOnly
        }
        fn binding(&self) -> voteboat::secure::SessionBinding {
            binding(1, 2)
        }
        fn state(&self) -> TransportState {
            TransportState::Closed
        }
        fn limits(&self) -> TransportLimits {
            TransportLimits::default()
        }
        fn usage(&self) -> TransportUsage {
            TransportUsage::default()
        }
        fn submit(
            &mut self,
            batch: voteboat::outbound::OutboundBatch,
        ) -> Result<(), TransportRejected> {
            Err(TransportRejected {
                reason: TransportError::Closed,
                batch: Box::new(batch),
            })
        }
        fn poll(
            &mut self,
            _: MonoTime,
            budget: TransportPollBudget,
        ) -> Result<TransportProgress, TransportError> {
            budget.validate()?;
            Ok(TransportProgress::default())
        }
        fn take_send(&mut self) -> Option<TransportSend> {
            None
        }
        fn take_received(&mut self) -> Option<ReceivedBatch> {
            None
        }
        fn received_info(&self) -> Option<ReceiveInfo> {
            None
        }
        fn close(&mut self) {}
        fn abort(&mut self) {}
    }
    let mut host: Box<dyn PeerTransport> = Box::new(Host);
    assert_eq!(
        host.poll(MonoTime(0), TransportPollBudget::default())
            .unwrap(),
        TransportProgress::default()
    );
}
fn binding(from: u64, to: u64) -> voteboat::secure::SessionBinding {
    use voteboat::{identity::*, secure::*};
    let local = |id| LocalIdentity {
        node: support::node(id),
        store: StoreBinding {
            identity: support::identity(id as u128),
            session: StoreSession::new(id + 1).unwrap(),
        },
    };
    SessionBinding {
        local: local(from),
        peer: local(to),
        generation: SecureSessionGeneration::new(1).unwrap(),
        wire_version: 1,
    }
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };
    use support::{group, node};
    use voteboat::{
        identity::*,
        native::{outbound::NativeOutbound, transport::NativePeerTransport, wire::NativeWireCodec},
        outbound::*,
        raft::*,
        secure::*,
        wire::*,
    };
    /// Trusted-host test attestation, not cryptographic security evidence.
    struct Channel {
        input: VecDeque<u8>,
        clean: bool,
        broken: bool,
        fail_write: bool,
        flushed: bool,
        changed: bool,
        reads: usize,
        writes: usize,
    }
    impl Default for Channel {
        fn default() -> Self {
            Self {
                input: VecDeque::new(),
                clean: false,
                broken: false,
                fail_write: false,
                flushed: true,
                changed: false,
                reads: 0,
                writes: 0,
            }
        }
    }
    struct HostSession {
        binding: SessionBinding,
        incoming: Arc<Mutex<Channel>>,
        outgoing: Arc<Mutex<Channel>>,
        chunk: usize,
        state: SessionState,
        security: SessionSecurity,
    }
    impl Drop for HostSession {
        fn drop(&mut self) {
            self.outgoing.lock().unwrap().broken = true;
        }
    }
    impl SecureSession for HostSession {
        fn security(&self) -> SessionSecurity {
            self.security
        }
        fn state(&self) -> SessionState {
            self.state
        }
        fn binding(&self) -> Option<SessionBinding> {
            let mut b = self.binding;
            if self.incoming.lock().unwrap().changed {
                b.generation = SecureSessionGeneration::new(9).unwrap();
            }
            Some(b)
        }
        fn limits(&self) -> SessionLimits {
            SessionLimits::default()
        }
        fn poll(
            &mut self,
            _: MonoTime,
            _: SessionPollBudget,
        ) -> Result<SessionProgress, SessionError> {
            Ok(SessionProgress::default())
        }
        fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
            let mut input = self.incoming.lock().unwrap();
            input.reads += 1;
            let n = bytes.len().min(self.chunk).min(input.input.len());
            if n == 0 {
                if input.clean {
                    return Ok(0);
                }
                return Err(if input.broken {
                    SessionError::Truncated
                } else {
                    SessionError::WouldBlock
                });
            }
            for b in &mut bytes[..n] {
                *b = input.input.pop_front().unwrap();
            }
            Ok(n)
        }
        fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
            let mut output = self.outgoing.lock().unwrap();
            output.writes += 1;
            if output.fail_write {
                return Err(SessionError::Io(std::io::ErrorKind::BrokenPipe));
            }
            let n = bytes.len().min(self.chunk).min(1024 - output.input.len());
            if n == 0 {
                return Err(SessionError::WouldBlock);
            }
            output.input.extend(&bytes[..n]);
            Ok(n)
        }
        fn is_flushed(&self) -> bool {
            self.outgoing.lock().unwrap().flushed
        }
        fn close(&mut self) {
            self.outgoing.lock().unwrap().clean = true;
            self.state = SessionState::Closed;
        }
        fn revoke(&mut self) {
            self.state = SessionState::Failed;
        }
    }
    fn sessions(chunk: usize) -> (HostSession, HostSession) {
        let a = Arc::new(Mutex::new(Channel::default()));
        let b = Arc::new(Mutex::new(Channel::default()));
        (
            HostSession {
                binding: binding(1, 2),
                incoming: a.clone(),
                outgoing: b.clone(),
                chunk,
                state: SessionState::Ready,
                security: SessionSecurity::Authenticated,
            },
            HostSession {
                binding: binding(2, 1),
                incoming: b,
                outgoing: a,
                chunk,
                state: SessionState::Ready,
                security: SessionSecurity::Authenticated,
            },
        )
    }
    fn queue(id: u64) -> NativeOutbound {
        let b = binding(id, if id == 1 { 2 } else { 1 });
        NativeOutbound::new(
            OutboundBinding {
                node: b.local.node,
                store: b.local.store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            OutboundLimits::default(),
        )
        .unwrap()
    }
    fn codec() -> NativeWireCodec {
        NativeWireCodec::new(WireLimits::default()).unwrap()
    }
    fn message(from: u64, to: u64, g: u128) -> Message {
        let b = binding(from, to);
        Message {
            group: group(g),
            configuration: ConfigurationId::new(1).unwrap(),
            from: node(from),
            to: node(to),
            sender: b.local.store,
            term: 1,
            context: RequestContext {
                origin: b.local.store,
                sequence: g as u64,
            },
            rpc: Rpc::ReadProbe,
        }
    }
    fn batch(queue: &mut dyn OutboundQueue, messages: Vec<Message>) -> OutboundBatch {
        queue.submit(messages).unwrap();
        queue.poll(1).pop().unwrap()
    }
    fn poll(t: &mut dyn PeerTransport) -> TransportProgress {
        let budget = TransportPollBudget {
            plaintext_calls: 3,
            read_bytes: 29,
            write_bytes: 31,
            ..TransportPollBudget::default()
        };
        let p = t.poll(MonoTime(0), budget).unwrap();
        assert!(
            p.plaintext_calls <= budget.plaintext_calls
                && p.read_bytes <= budget.read_bytes
                && p.written_bytes <= budget.write_bytes
        );
        p
    }
    #[test]
    fn shared_host_buffers_preserve_partial_io_flush_and_independent_queue_credits() {
        use voteboat::{buffer::*, native::transport::NativeTransportFactory};
        let max = TransportLimits::default().send_frame_bytes;
        let mut pool = support::buffer::HostPool::new(4 * max, 4);
        let (a, b) = sessions(7);
        let outgoing = a.outgoing.clone();
        outgoing.lock().unwrap().flushed = false;
        let mut qa = queue(1);
        let qb = queue(2);
        let mut factory = NativeTransportFactory::new(codec(), TransportLimits::default())
            .unwrap()
            .with_buffers(pool.clone())
            .unwrap();
        let mut a = factory.build(a, &qa).unwrap();
        let mut b = factory.build(b, &qb).unwrap();
        drop(factory); // transports retain their own shared views
        let mut unrelated = pool.clone();
        unrelated.close();
        drop(unrelated);
        let expected = vec![message(1, 2, 11), message(1, 2, 12)];
        a.submit(batch(&mut qa, expected.clone())).unwrap();
        assert_eq!(pool.usage().leases, 1);
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if b.received_info().is_some() {
                break;
            }
        }
        assert!(b.received_info().is_some());
        assert!(!a.usage().completion); // channel flush still owns send bytes
        assert!(a.usage().send_frame_bytes > 0);
        // Receive has decoded and released its frame, while a's idle receive
        // and unflushed send still hold maximum reservations.
        assert_eq!(
            pool.usage(),
            BufferUsage {
                reserved_bytes: 2 * max,
                leases: 2
            }
        );
        assert_eq!(b.take_received().unwrap().messages, expected);
        outgoing.lock().unwrap().flushed = true;
        poll(&mut a);
        assert_eq!(a.usage().send_frame_bytes, 0);
        assert_eq!(qa.usage().batches, 1); // frame credits aren't queue credits
        let done = a.take_send().unwrap();
        qa.complete(done.batch, done.result).unwrap();
        a.abort();
        b.abort();
        assert_eq!(pool.usage(), BufferUsage::default());
        pool.close();
        assert!(qa.is_drained());
    }
    fn protected_control<P: voteboat::buffer::BufferPool + Clone>(mut pool: P) {
        use voteboat::{buffer::*, native::transport::NativeTransportFactory};
        let max = TransportLimits::default().send_frame_bytes;
        let (a, b) = sessions(7);
        let incoming = a.incoming.clone();
        let outgoing = a.outgoing.clone();
        outgoing.lock().unwrap().flushed = false;
        let mut qa = queue(1);
        let qb = queue(2);
        let mut factory = NativeTransportFactory::new(codec(), TransportLimits::default())
            .unwrap()
            .with_buffers(pool.clone())
            .unwrap();
        let mut a = factory.build(a, &qa).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        drop(factory);
        let blocker = pool.acquire(3 * max, 0).unwrap();
        pool.close(); // transport views and accepted blocker remain valid
                      // Unclassified receive must not borrow the free control reservation.
        poll(&mut a);
        assert_eq!(incoming.lock().unwrap().reads, 0);
        let control = message(1, 2, 11);
        let mut data = message(1, 2, 12);
        data.rpc = Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![support::entry(1, 1, 7)],
            leader_commit: 0,
        };
        let mixed = vec![control.clone(), data];
        let rejected = a.submit(batch(&mut qa, mixed.clone())).unwrap_err();
        assert_eq!(rejected.reason, TransportError::Overloaded);
        assert_eq!(rejected.batch.messages, mixed);
        assert_eq!(
            pool.usage(),
            BufferUsage {
                reserved_bytes: 3 * max,
                leases: 1
            }
        );
        assert_eq!(qa.usage().batches, 1);
        qa.complete(*rejected.batch, LocalSendResult::Cancelled)
            .unwrap();
        a.submit(batch(&mut qa, vec![control.clone()])).unwrap();
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if b.received_info().is_some() {
                break;
            }
        }
        assert_eq!(b.take_received().unwrap().messages, vec![control]);
        assert!(!a.usage().completion);
        assert_eq!(
            pool.usage(),
            BufferUsage {
                reserved_bytes: 4 * max,
                leases: 2
            }
        );
        outgoing.lock().unwrap().flushed = true;
        poll(&mut a);
        assert_eq!(
            pool.usage(),
            BufferUsage {
                reserved_bytes: 3 * max,
                leases: 1
            }
        );
        let done = a.take_send().unwrap();
        assert_eq!(done.result, LocalSendResult::Sent);
        assert_eq!(qa.usage().batches, 1);
        qa.complete(done.batch, done.result).unwrap();
        outgoing.lock().unwrap().flushed = false;
        a.submit(batch(&mut qa, vec![message(1, 2, 13)])).unwrap();
        a.abort();
        let done = a.take_send().unwrap();
        assert_ne!(done.result, LocalSendResult::Sent);
        qa.complete(done.batch, done.result).unwrap();
        b.abort();
        drop(blocker);
        assert_eq!(pool.usage(), BufferUsage::default());
        assert_eq!(qa.usage().batches, 0);
    }
    #[test]
    fn downstream_shared_pool_protects_validated_control_and_holds_flush_ownership() {
        use voteboat::buffer::*;
        let max = TransportLimits::default().send_frame_bytes;
        let pool = support::buffer::HostPool::with_control_reserve(
            4 * max,
            4,
            BufferLimits {
                reserved_bytes: max,
                leases: 1,
            },
        );
        let observer = pool.clone();
        protected_control(pool);
        assert!(observer.class_calls()[0] > 2);
        assert_eq!(observer.class_calls()[1], 2);
    }
    #[test]
    fn native_shared_pool_protects_validated_control_and_holds_flush_ownership() {
        use voteboat::{buffer::*, native::buffer::NativeBufferPool};
        let max = TransportLimits::default().send_frame_bytes;
        let pool = NativeBufferPool::new_with_control_reserve(
            BufferLimits {
                reserved_bytes: 4 * max,
                leases: 4,
            },
            BufferLimits {
                reserved_bytes: max,
                leases: 1,
            },
        )
        .unwrap();
        protected_control(pool.clone());
        assert_eq!(pool.bulk_usage(), BufferUsage::default());
    }
    #[test]
    fn shared_factory_refuses_undersized_control_or_bulk_capacity_before_leasing() {
        use voteboat::{
            buffer::*,
            native::{buffer::NativeBufferPool, transport::NativeTransportFactory},
        };
        let max = TransportLimits::default().send_frame_bytes;
        for reserve in [
            BufferLimits {
                reserved_bytes: max - 1,
                leases: 1,
            },
            BufferLimits {
                reserved_bytes: 3 * max,
                leases: 1,
            },
            BufferLimits {
                reserved_bytes: max,
                leases: 3,
            },
        ] {
            let pool = NativeBufferPool::new_with_control_reserve(
                BufferLimits {
                    reserved_bytes: 4 * max,
                    leases: 4,
                },
                reserve,
            )
            .unwrap();
            assert!(matches!(
                NativeTransportFactory::new(codec(), TransportLimits::default())
                    .unwrap()
                    .with_buffers(pool.clone()),
                Err(TransportError::InvalidLimits)
            ));
            assert_eq!(pool.usage(), BufferUsage::default());
        }
    }
    #[test]
    fn malformed_host_reserve_refuses_before_frame_use_and_drops_owned_session() {
        use voteboat::{buffer::*, native::transport::NativeTransportFactory};
        #[derive(Clone)]
        struct Declared(support::buffer::HostPool);
        impl BufferPool for Declared {
            type Buffer = support::buffer::HostBuffer;
            fn limits(&self) -> BufferLimits {
                self.0.limits()
            }
            fn usage(&self) -> BufferUsage {
                self.0.usage()
            }
            fn acquire(&self, r: usize, n: usize) -> Result<Self::Buffer, BufferError> {
                self.0.acquire(r, n)
            }
            fn control_reserve(&self) -> Option<BufferLimits> {
                Some(BufferLimits {
                    reserved_bytes: self.limits().reserved_bytes,
                    leases: 1,
                })
            }
            fn close(&mut self) {
                self.0.close();
            }
        }
        let max = TransportLimits::default().send_frame_bytes;
        let pool = Declared(support::buffer::HostPool::new(4 * max, 4));
        assert!(matches!(
            NativeTransportFactory::new(codec(), TransportLimits::default())
                .unwrap()
                .with_buffers(pool.clone()),
            Err(TransportError::Buffer(BufferError::InvalidLimits))
        ));
        let (session, _peer) = sessions(7);
        let incoming = session.incoming.clone();
        let outgoing = session.outgoing.clone();
        assert!(matches!(
            NativePeerTransport::with_buffers(
                session,
                codec(),
                &queue(1),
                TransportLimits::default(),
                pool.clone()
            ),
            Err(TransportError::Buffer(BufferError::InvalidLimits))
        ));
        assert_eq!(incoming.lock().unwrap().reads, 0);
        assert!(outgoing.lock().unwrap().broken);
        assert_eq!(pool.usage(), BufferUsage::default());
        assert_eq!(pool.0.class_calls(), [0; 2]);
    }
    #[test]
    fn exhausted_pool_returns_original_send_and_retries_receive_without_reading() {
        use voteboat::buffer::*;
        let max = TransportLimits::default().send_frame_bytes;
        let pool = support::buffer::HostPool::new(2 * max, 2);
        let blocker = pool.acquire(2 * max, 0).unwrap();
        let (a, b) = sessions(7);
        let incoming = a.incoming.clone();
        let mut qa = queue(1);
        let mut qb = queue(2);
        let mut a = NativePeerTransport::with_buffers(
            a,
            codec(),
            &qa,
            TransportLimits::default(),
            pool.clone(),
        )
        .unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        let work = batch(&mut qa, vec![message(1, 2, 1)]);
        let ticket = work.ticket;
        let rejected = a.submit(work).unwrap_err();
        assert_eq!(rejected.reason, TransportError::Overloaded);
        assert_eq!(rejected.batch.ticket, ticket);
        qa.complete(*rejected.batch, LocalSendResult::Cancelled)
            .unwrap();
        b.submit(batch(&mut qb, vec![message(2, 1, 2)])).unwrap();
        for _ in 0..10 {
            poll(&mut a);
            poll(&mut b);
        }
        assert_eq!(incoming.lock().unwrap().reads, 0);
        assert_eq!(a.state(), TransportState::Open);
        drop(blocker);
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if a.received_info().is_some() {
                break;
            }
        }
        assert_eq!(a.take_received().unwrap().messages, [message(2, 1, 2)]);
        a.abort();
        assert_eq!(pool.usage(), BufferUsage::default());
        let done = b.take_send().unwrap();
        qb.complete(done.batch, done.result).unwrap();
    }
    #[test]
    fn send_failure_and_transport_drop_release_pool_without_destroying_other_view() {
        use voteboat::buffer::*;
        let max = TransportLimits::default().send_frame_bytes;
        let pool = support::buffer::HostPool::new(2 * max, 2);
        let (a, _) = sessions(1);
        a.outgoing.lock().unwrap().fail_write = true;
        let mut qa = queue(1);
        let mut a = NativePeerTransport::with_buffers(
            a,
            codec(),
            &qa,
            TransportLimits::default(),
            pool.clone(),
        )
        .unwrap();
        a.submit(batch(&mut qa, vec![message(1, 2, 1)])).unwrap();
        assert!(a.poll(MonoTime(0), TransportPollBudget::default()).is_err());
        assert_eq!(pool.usage(), BufferUsage::default());
        let done = a.take_send().unwrap();
        assert_eq!(done.result, LocalSendResult::Failed);
        qa.complete(done.batch, done.result).unwrap();
        drop(a);
        let (a, _other) = sessions(1);
        let mut a = NativePeerTransport::with_buffers(
            a,
            codec(),
            &qa,
            TransportLimits::default(),
            pool.clone(),
        )
        .unwrap();
        a.submit(batch(&mut qa, vec![message(1, 2, 2)])).unwrap();
        poll(&mut a); // partially written bytes + idle header
        assert_eq!(pool.usage().leases, 2);
        drop(a);
        assert_eq!(pool.usage(), BufferUsage::default());
        let buffer = pool.acquire(max, 1).unwrap();
        drop(buffer);
        // Dropping a transport abandons the completion, not queue ownership.
        assert_eq!(qa.usage().batches, 1);
    }
    #[test]
    fn malformed_encoding_returns_original_batch_and_pool_credits() {
        use voteboat::buffer::*;
        let max = TransportLimits::default().send_frame_bytes;
        let pool = support::buffer::HostPool::new(2 * max, 2);
        let (a, _other) = sessions(7);
        let mut qa = queue(1);
        let mut a = NativePeerTransport::with_buffers(
            a,
            codec(),
            &qa,
            TransportLimits::default(),
            pool.clone(),
        )
        .unwrap();
        let mut malformed = message(1, 2, 1);
        malformed.term = 0;
        let work = batch(&mut qa, vec![malformed]);
        let ticket = work.ticket;
        let rejected = a.submit(work).unwrap_err();
        assert!(matches!(rejected.reason, TransportError::Wire(_)));
        assert_eq!(rejected.batch.ticket, ticket);
        assert_eq!(pool.usage(), BufferUsage::default());
        qa.complete(*rejected.batch, LocalSendResult::Cancelled)
            .unwrap();
        assert!(qa.is_drained());
        assert_eq!(a.state(), TransportState::Open);
    }
    #[test]
    fn receive_growth_failure_releases_partial_frame_and_closes_channel() {
        use support::buffer::{HostBuffer, HostPool};
        use voteboat::buffer::*;
        struct LimitedPool(HostPool);
        struct LimitedBuffer(HostBuffer);
        impl AsRef<[u8]> for LimitedBuffer {
            fn as_ref(&self) -> &[u8] {
                self.0.as_ref()
            }
        }
        impl AsMut<[u8]> for LimitedBuffer {
            fn as_mut(&mut self) -> &mut [u8] {
                self.0.as_mut()
            }
        }
        impl FrameBuffer for LimitedBuffer {
            fn reservation(&self) -> usize {
                self.0.reservation()
            }
            fn capacity(&self) -> usize {
                self.0.capacity()
            }
            fn resize(&mut self, _: usize) -> Result<(), BufferError> {
                Err(BufferError::AllocationFailed)
            }
        }
        impl BufferPool for LimitedPool {
            type Buffer = LimitedBuffer;
            fn limits(&self) -> BufferLimits {
                self.0.limits()
            }
            fn usage(&self) -> BufferUsage {
                self.0.usage()
            }
            fn acquire(&self, size: usize, len: usize) -> Result<LimitedBuffer, BufferError> {
                self.0.acquire(size, len).map(LimitedBuffer)
            }
            fn close(&mut self) {
                self.0.close();
            }
        }
        let max = TransportLimits::default().receive_frame_bytes;
        let pool = HostPool::new(2 * max, 2);
        let (a, b) = sessions(7);
        let read_channel = a.incoming.clone();
        let closed_channel = a.outgoing.clone();
        let qa = queue(1);
        let mut qb = queue(2);
        let mut a = NativePeerTransport::with_buffers(
            a,
            codec(),
            &qa,
            TransportLimits::default(),
            LimitedPool(pool.clone()),
        )
        .unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        b.submit(batch(&mut qb, vec![message(2, 1, 1)])).unwrap();
        let mut failure = None;
        for _ in 0..1000 {
            poll(&mut b);
            match a.poll(MonoTime(0), TransportPollBudget::default()) {
                Ok(_) => {}
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        assert_eq!(
            failure,
            Some(TransportError::Buffer(BufferError::AllocationFailed))
        );
        assert_eq!(a.state(), TransportState::Failed);
        assert!(a.take_received().is_none());
        assert_eq!(pool.usage(), BufferUsage::default());
        // Dropping a's session marks its outgoing half broken at b's input.
        assert!(read_channel.lock().unwrap().reads > 0);
        assert!(closed_channel.lock().unwrap().broken);
        b.abort(); // the peer may still be sending when allocation fails
        let done = b.take_send().unwrap();
        qb.complete(done.batch, done.result).unwrap();
        assert_eq!(
            a.poll(MonoTime(0), TransportPollBudget::default()),
            Err(TransportError::Buffer(BufferError::AllocationFailed))
        );
    }
    #[test]
    fn policy_lease_survives_transport_flush_abort_and_queue_drop_until_batch_release() {
        use voteboat::native::admission::NativeAdmissionPolicy;
        let policy = NativeAdmissionPolicy::new(OutboundUsage {
            batches: 1,
            messages: 16,
            bytes: 65536,
        })
        .unwrap();
        let (a, b) = sessions(7);
        let outgoing = a.outgoing.clone();
        outgoing.lock().unwrap().flushed = false;
        let mut qa = NativeOutbound::with_policy(
            queue(1).binding(),
            OutboundLimits::default(),
            policy.clone(),
        )
        .unwrap();
        let qb = queue(2);
        let mut a = NativePeerTransport::new(a, codec(), &qa, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        let mut data = message(1, 2, 1);
        data.rpc = Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![support::entry(1, 1, 7)],
            leader_commit: 0,
        };
        a.submit(batch(&mut qa, vec![data.clone()])).unwrap();
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if b.received_info().is_some() {
                break;
            }
        }
        assert!(b.received_info().is_some());
        assert!(!a.usage().completion);
        assert_eq!(policy.usage().batches, 1);
        outgoing.lock().unwrap().flushed = true;
        poll(&mut a);
        assert!(a.usage().completion);
        assert_eq!(a.usage().send_frame_bytes, 0);
        drop(qa); // completed transport batch still owns its shared policy lease
        assert_eq!(policy.usage().batches, 1);
        let completed = a.take_send().unwrap();
        assert_eq!(completed.result, LocalSendResult::Sent);
        assert_eq!(policy.usage().batches, 1);
        drop(completed);
        assert_eq!(policy.usage(), OutboundUsage::default());
        a.abort();
        b.abort();
        let (session, _peer) = sessions(7);
        session.outgoing.lock().unwrap().flushed = false;
        let mut queue = NativeOutbound::with_policy(
            queue(1).binding(),
            OutboundLimits::default(),
            policy.clone(),
        )
        .unwrap();
        let mut transport =
            NativePeerTransport::new(session, codec(), &queue, TransportLimits::default()).unwrap();
        transport.submit(batch(&mut queue, vec![data])).unwrap();
        assert!(poll(&mut transport).written_bytes > 0);
        transport.abort();
        assert_eq!(policy.usage().batches, 1);
        let failed = transport.take_send().unwrap();
        assert_eq!(failed.result, LocalSendResult::Failed);
        queue.complete(failed.batch, failed.result).unwrap();
        assert_eq!(policy.usage(), OutboundUsage::default());
        assert!(queue.is_drained());
    }
    #[test]
    fn short_io_preserves_batch_and_queue_credits_until_terminal_consumption() {
        let (a, b) = sessions(7);
        let mut q = queue(1);
        let qb = queue(2);
        let mut a = NativePeerTransport::new(a, codec(), &q, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        let messages = vec![message(1, 2, 1), message(1, 2, 2)];
        let send = batch(&mut q, messages.clone());
        let ticket = send.ticket;
        a.submit(send).unwrap();
        let other = batch(&mut q, vec![message(1, 2, 3)]);
        let rejected = a.submit(other).unwrap_err();
        assert_eq!(rejected.reason, TransportError::Overloaded);
        q.complete(*rejected.batch, LocalSendResult::Cancelled)
            .unwrap();
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if a.usage().completion && b.usage().decoded_bytes > 0 {
                break;
            }
        }
        assert!(a.usage().completion);
        assert_eq!(q.usage().batches, 1);
        assert_eq!(a.usage().send_frame_bytes, 0);
        let received = b.take_received().unwrap();
        assert_eq!(received.messages, messages);
        assert_eq!(received.connection, b.binding());
        assert_eq!(b.usage().decoded_bytes, 0);
        let done = a.take_send().unwrap();
        assert_eq!(done.batch.ticket, ticket);
        assert_eq!(done.result, LocalSendResult::Sent);
        q.complete(done.batch, done.result).unwrap();
        assert!(q.is_drained());
        assert!(a.take_send().is_none());
    }
    #[test]
    fn unread_batch_stops_next_frame_and_does_not_starve_opposite_direction() {
        let (a, b) = sessions(31);
        let mut qa = queue(1);
        let mut qb = queue(2);
        let mut a = NativePeerTransport::new(a, codec(), &qa, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        a.submit(batch(&mut qa, vec![message(1, 2, 1)])).unwrap();
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if a.usage().completion && b.usage().decoded_bytes > 0 {
                break;
            }
        }
        let done = a.take_send().unwrap();
        qa.complete(done.batch, done.result).unwrap();
        a.submit(batch(&mut qa, vec![message(1, 2, 2)])).unwrap();
        b.submit(batch(&mut qb, vec![message(2, 1, 3)])).unwrap();
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if a.usage().completion && b.usage().completion && a.usage().decoded_bytes > 0 {
                break;
            }
        }
        assert_eq!(a.take_received().unwrap().messages, [message(2, 1, 3)]);
        assert_eq!(b.take_received().unwrap().messages, [message(1, 2, 1)]);
        assert_eq!(b.usage().receive_frame_bytes, 0);
        for _ in 0..1000 {
            poll(&mut b);
            if b.usage().decoded_bytes > 0 {
                break;
            }
        }
        assert_eq!(b.take_received().unwrap().messages, [message(1, 2, 2)]);
    }
    #[test]
    fn send_completion_waits_for_channel_flush_and_failure_releases_buffers() {
        let (a, b) = sessions(1024);
        let external = a.outgoing.clone();
        let incoming = a.incoming.clone();
        let mut q = queue(1);
        external.lock().unwrap().flushed = false;
        let mut a = NativePeerTransport::new(a, codec(), &q, TransportLimits::default()).unwrap();
        a.submit(batch(&mut q, vec![message(1, 2, 1)])).unwrap();
        for _ in 0..100 {
            poll(&mut a);
        }
        assert!(a.take_send().is_none());
        assert!(a.usage().sending);
        external.lock().unwrap().flushed = true;
        poll(&mut a);
        let sent = a.take_send().unwrap();
        q.complete(sent.batch, sent.result).unwrap();
        a.submit(batch(&mut q, vec![message(1, 2, 2)])).unwrap();
        external.lock().unwrap().fail_write = true;
        assert!(matches!(
            a.poll(MonoTime(0), TransportPollBudget::default()),
            Err(TransportError::Session(SessionError::Io(_)))
        ));
        assert_eq!(a.state(), TransportState::Failed);
        assert_eq!(a.usage().send_frame_bytes, 0);
        assert_eq!(a.usage().receive_frame_bytes, 0);
        assert!(external.lock().unwrap().broken);
        let calls = incoming.lock().unwrap().reads;
        assert!(a.poll(MonoTime(0), TransportPollBudget::default()).is_err());
        assert_eq!(incoming.lock().unwrap().reads, calls);
        let failed = a.take_send().unwrap();
        assert_eq!(failed.result, LocalSendResult::Failed);
        q.complete(failed.batch, failed.result).unwrap();
        assert!(q.is_drained());
        drop(b);
    }
    #[test]
    fn malformed_or_partial_frame_never_leaks_a_message() {
        for partial in [true, false] {
            let (sender, receiver) = sessions(1024);
            let input = receiver.incoming.clone();
            let qb = queue(2);
            let mut receiver =
                NativePeerTransport::new(receiver, codec(), &qb, TransportLimits::default())
                    .unwrap();
            let mut frame = codec()
                .encode_batch(binding(1, 2).outgoing(), &[message(1, 2, 1)])
                .unwrap();
            if partial {
                frame.pop();
            } else {
                frame[40] ^= 1;
            }
            input.lock().unwrap().input.extend(frame);
            input.lock().unwrap().clean = true;
            let mut error = None;
            for _ in 0..100 {
                match receiver.poll(MonoTime(0), TransportPollBudget::default()) {
                    Ok(_) => (),
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
            assert!(matches!(
                error,
                Some(TransportError::Truncated | TransportError::Wire(_))
            ));
            assert!(receiver.take_received().is_none());
            assert_eq!(receiver.usage().receive_frame_bytes, 0);
            drop(sender);
        }
    }
    #[test]
    fn invalid_header_is_rejected_before_large_frame_allocation() {
        let (sender, receiver) = sessions(1024);
        let input = receiver.incoming.clone();
        let qb = queue(2);
        let mut receiver =
            NativePeerTransport::new(receiver, codec(), &qb, TransportLimits::default()).unwrap();
        let mut frame = codec()
            .encode_batch(binding(1, 2).outgoing(), &[message(1, 2, 1)])
            .unwrap();
        frame[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        input.lock().unwrap().input.extend(&frame[..24]);
        assert!(matches!(
            receiver.poll(MonoTime(0), TransportPollBudget::default()),
            Err(TransportError::Wire(_))
        ));
        assert_eq!(input.lock().unwrap().reads, 1);
        assert_eq!(receiver.usage().receive_frame_bytes, 0);
        drop(sender);
    }
    #[test]
    fn close_drains_send_abort_fails_it_and_completed_input_survives_failure() {
        let (a, b) = sessions(7);
        let mut q = queue(1);
        let qb = queue(2);
        let mut a = NativePeerTransport::new(a, codec(), &q, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        a.submit(batch(&mut q, vec![message(1, 2, 1)])).unwrap();
        a.close();
        for _ in 0..1000 {
            poll(&mut a);
            poll(&mut b);
            if a.state() == TransportState::Closed && b.usage().decoded_bytes > 0 {
                break;
            }
        }
        assert_eq!(a.take_send().unwrap().result, LocalSendResult::Sent);
        b.abort();
        assert_eq!(b.take_received().unwrap().messages, [message(1, 2, 1)]);
        let (a, _b) = sessions(7);
        let mut q = queue(1);
        let mut a = NativePeerTransport::new(a, codec(), &q, TransportLimits::default()).unwrap();
        a.submit(batch(&mut q, vec![message(1, 2, 2)])).unwrap();
        a.abort();
        let failed = a.take_send().unwrap();
        assert_eq!(failed.result, LocalSendResult::Failed);
        q.complete(failed.batch, failed.result).unwrap();
        assert!(q.is_drained());
        assert_eq!(a.usage(), TransportUsage::default());
    }
    #[test]
    fn construction_and_submission_reject_insecure_incompatible_and_stale_bindings() {
        let (mut a, _b) = sessions(7);
        let q = queue(1);
        let released = a.outgoing.clone();
        a.security = SessionSecurity::SimulatorOnly;
        assert!(matches!(
            NativePeerTransport::new(a, codec(), &q, TransportLimits::default()),
            Err(TransportError::Session(SessionError::InsecureProvider))
        ));
        assert!(released.lock().unwrap().broken);
        let (a, _b) = sessions(7);
        assert!(matches!(
            NativePeerTransport::new(
                a,
                codec(),
                &q,
                TransportLimits {
                    send_frame_bytes: 1,
                    ..TransportLimits::default()
                }
            ),
            Err(TransportError::IncompatibleCodec)
        ));
        let (a, _b) = sessions(7);
        let mut q = queue(1);
        let changed = a.incoming.clone();
        let mut a = NativePeerTransport::new(a, codec(), &q, TransportLimits::default()).unwrap();
        let mut wrong = batch(&mut q, vec![message(1, 2, 1)]);
        let original = wrong.ticket;
        wrong.ticket.binding.generation = OutboundGeneration::new(2).unwrap();
        let rejected = a.submit(wrong).unwrap_err();
        assert_eq!(rejected.reason, TransportError::WrongBinding);
        let mut wrong = *rejected.batch;
        wrong.ticket = original;
        q.complete(wrong, LocalSendResult::Cancelled).unwrap();
        a.submit(batch(&mut q, vec![message(1, 2, 2)])).unwrap();
        changed.lock().unwrap().changed = true;
        assert_eq!(
            a.poll(MonoTime(0), TransportPollBudget::default()),
            Err(TransportError::WrongBinding)
        );
        assert_eq!(a.take_send().unwrap().result, LocalSendResult::Failed);
    }
    #[test]
    fn zero_and_single_call_budgets_preserve_bidirectional_progress() {
        let (a, b) = sessions(7);
        let input = a.incoming.clone();
        let output = a.outgoing.clone();
        let mut qa = queue(1);
        let mut qb = queue(2);
        let mut a = NativePeerTransport::new(a, codec(), &qa, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        a.submit(batch(&mut qa, vec![message(1, 2, 1)])).unwrap();
        b.submit(batch(&mut qb, vec![message(2, 1, 2)])).unwrap();
        let zero = TransportPollBudget {
            plaintext_calls: 0,
            read_bytes: 0,
            write_bytes: 0,
            session: SessionPollBudget {
                io_calls: 0,
                read_bytes: 0,
                write_bytes: 0,
            },
        };
        assert_eq!(
            a.poll(MonoTime(0), zero).unwrap(),
            TransportProgress::default()
        );
        assert_eq!(input.lock().unwrap().reads, 0);
        assert_eq!(output.lock().unwrap().writes, 0);
        let one = TransportPollBudget {
            plaintext_calls: 1,
            read_bytes: 5,
            write_bytes: 5,
            ..TransportPollBudget::default()
        };
        for _ in 0..1000 {
            for t in [&mut a, &mut b] {
                let p = t.poll(MonoTime(0), one).unwrap();
                assert!(p.plaintext_calls <= 1 && p.read_bytes <= 5 && p.written_bytes <= 5);
            }
            if a.usage().completion
                && b.usage().completion
                && a.usage().decoded_bytes > 0
                && b.usage().decoded_bytes > 0
            {
                break;
            }
        }
        assert_eq!(a.take_received().unwrap().messages, [message(2, 1, 2)]);
        assert_eq!(b.take_received().unwrap().messages, [message(1, 2, 1)]);
    }
    #[test]
    fn excessive_original_vector_capacity_is_returned_before_encoding() {
        let (a, _b) = sessions(7);
        let mut q = queue(1);
        let mut a = NativePeerTransport::new(a, codec(), &q, TransportLimits::default()).unwrap();
        let mut send = batch(&mut q, vec![message(1, 2, 1)]);
        send.messages.reserve_exact(1024);
        let pointer = send.messages.as_ptr();
        let capacity = send.messages.capacity();
        let rejected = a.submit(send).unwrap_err();
        assert_eq!(
            rejected.reason,
            TransportError::Outbound(OutboundError::BatchTooLarge)
        );
        assert_eq!(rejected.batch.messages.as_ptr(), pointer);
        assert_eq!(rejected.batch.messages.capacity(), capacity);
        assert_eq!(a.usage(), TransportUsage::default());
    }
    #[test]
    fn public_host_queue_and_single_fixture_codec_compose_with_native_driver() {
        // This intentionally tiny host codec proves selection and prefix-only
        // frames. It does not claim alternative full-protocol conformance.
        struct HostCodec(Message);
        impl WireCodec for HostCodec {
            fn format_version(&self) -> u16 {
                37
            }
            fn header_bytes(&self) -> usize {
                4
            }
            fn limits(&self) -> WireLimits {
                WireLimits {
                    max_frame_bytes: 4,
                    max_messages: 1,
                    max_command_bytes: 1,
                    max_snapshot_bytes: 1,
                    max_decoded_bytes: 1024,
                    ..WireLimits::default()
                }
            }
            fn frame_length(&self, header: &[u8]) -> Result<usize, WireError> {
                if header == b"HOST" {
                    Ok(4)
                } else {
                    Err(WireError::Corrupt("host fixture"))
                }
            }
            fn encode_batch(
                &self,
                scope: WireScope,
                messages: &[Message],
            ) -> Result<Vec<u8>, WireError> {
                if !scope.matches(&self.0) || messages != [self.0.clone()] {
                    return Err(WireError::WrongPeer);
                }
                Ok(b"HOST".to_vec())
            }
            fn decode_batch(
                &self,
                scope: WireScope,
                frame: &[u8],
            ) -> Result<Vec<Message>, WireError> {
                self.frame_length(frame)?;
                if !scope.matches(&self.0) {
                    return Err(WireError::WrongPeer);
                }
                Ok(vec![self.0.clone()])
            }
        }
        let expected = message(1, 2, 5);
        let (mut a, mut b) = sessions(1);
        a.binding.wire_version = 37;
        b.binding.wire_version = 37;
        let mut qa =
            support::outbound::HostOutbound::new(queue(1).binding(), OutboundLimits::default())
                .unwrap();
        let qb =
            support::outbound::HostOutbound::new(queue(2).binding(), OutboundLimits::default())
                .unwrap();
        let limits = TransportLimits {
            send_frame_bytes: 4,
            receive_frame_bytes: 4,
            decoded_bytes: 1024,
        };
        let mut a = NativePeerTransport::new(
            a,
            HostCodec(expected.clone()),
            &qa as &dyn OutboundQueue,
            limits,
        )
        .unwrap();
        let mut b = NativePeerTransport::new(b, HostCodec(expected.clone()), &qb, limits).unwrap();
        a.submit(batch(&mut qa, vec![expected.clone()])).unwrap();
        for _ in 0..100 {
            poll(&mut a);
            poll(&mut b);
            if a.usage().completion && b.usage().decoded_bytes > 0 {
                break;
            }
        }
        assert_eq!(b.take_received().unwrap().messages, [expected]);
        let done = a.take_send().unwrap();
        qa.complete(done.batch, done.result).unwrap();
        assert!(qa.is_drained());
    }
    #[cfg(feature = "tls")]
    #[test]
    fn actual_tcp_tls_driver_transfers_both_directions_under_backpressure() {
        let (a, b) = support::tls::pair(binding(1, 2).local, binding(1, 2).peer, 1);
        let mut qa = queue(1);
        let mut qb = queue(2);
        let mut a = NativePeerTransport::new(a, codec(), &qa, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        let left: Vec<_> = (1..=100).map(|g| message(1, 2, g)).collect();
        let right: Vec<_> = (1..=100).map(|g| message(2, 1, g)).collect();
        a.submit(batch(&mut qa, left.clone())).unwrap();
        b.submit(batch(&mut qb, right.clone())).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !(a.usage().completion
            && b.usage().completion
            && a.usage().decoded_bytes > 0
            && b.usage().decoded_bytes > 0)
        {
            poll(&mut a);
            poll(&mut b);
            assert!(std::time::Instant::now() < deadline);
        }
        assert_eq!(a.take_received().unwrap().messages, right);
        assert_eq!(b.take_received().unwrap().messages, left);
        for (transport, queue) in [(&mut a, &mut qa), (&mut b, &mut qb)] {
            let done = transport.take_send().unwrap();
            queue.complete(done.batch, done.result).unwrap();
            assert!(queue.is_drained());
        }
        // A bounded snapshot with a nine-voter recursive policy traverses the
        // same stream driver; snapshot installation durability is tested elsewhere.
        use voteboat::{
            quorum::{Policy, Tree},
            snapshot::{Snapshot, SnapshotMetadata},
        };
        let mut bootstrap = support::bootstrap(55, 9);
        bootstrap.policy = Policy::new(
            Tree::Majority(
                (0..3)
                    .map(|branch| {
                        Tree::Majority(
                            (1..=3)
                                .map(|leaf| Tree::Voter(node(branch * 3 + leaf)))
                                .collect(),
                        )
                    })
                    .collect(),
            ),
            voteboat::quorum::Limits::default(),
        )
        .unwrap();
        let mut snapshot = message(1, 2, 55);
        snapshot.rpc = Rpc::Snapshot {
            snapshot: Box::new(Snapshot {
                metadata: SnapshotMetadata {
                    membership: None,
                    bootstrap,
                    index: 3,
                    term: 1,
                    application_schema: 1,
                },
                application: vec![42; 16 * 1024],
            }),
        };
        a.submit(batch(&mut qa, vec![snapshot.clone()])).unwrap();
        while !a.usage().completion || b.usage().decoded_bytes == 0 {
            poll(&mut a);
            poll(&mut b);
            assert!(std::time::Instant::now() < deadline);
        }
        assert_eq!(b.take_received().unwrap().messages, [snapshot]);
        let done = a.take_send().unwrap();
        qa.complete(done.batch, done.result).unwrap();
        a.close();
        b.close();
        while a.state() != TransportState::Closed || b.state() != TransportState::Closed {
            poll(&mut a);
            poll(&mut b);
            assert!(std::time::Instant::now() < deadline);
        }
    }

    #[test]
    fn explicit_membership_wire_selection_preserves_short_io_credits_and_session_fencing() {
        use voteboat::{log::*, membership::*, snapshot::*};
        let c = || NativeWireCodec::with_membership(WireLimits::default()).unwrap();
        let old = support::bootstrap(1, 3);
        let next = Configuration::new(
            ConfigurationId::new(2).unwrap(),
            old.policy.clone(),
            old.voter_stores.clone(),
            [(node(4), support::identity(4))].into_iter().collect(),
        )
        .unwrap();
        let entry = LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(100).unwrap(),
                expected: old.configuration,
                change: ConfigurationChange::Learners(next),
            })),
        };
        let membership = Membership::replay(&old, std::slice::from_ref(&entry), 0).unwrap();
        let append = Message {
            rpc: Rpc::Append {
                previous_index: 0,
                previous_term: 0,
                entries: vec![entry],
                leader_commit: 0,
            },
            ..message(1, 2, 1)
        };
        let snapshot = Message {
            configuration: membership.id(),
            rpc: Rpc::Snapshot {
                snapshot: Box::new(Snapshot {
                    metadata: SnapshotMetadata {
                        bootstrap: old,
                        membership: Some(Box::new(membership)),
                        index: 1,
                        term: 1,
                        application_schema: 1,
                    },
                    application: vec![7; 32],
                }),
            },
            ..message(1, 2, 1)
        };
        for input in [append, snapshot] {
            let (mut sender, mut receiver) = sessions(7);
            let mut q = queue(1);
            let qb = queue(2);
            // The existing construction guard refuses a codec/session mismatch.
            assert!(matches!(
                NativePeerTransport::new(sessions(7).0, c(), &q, TransportLimits::default()),
                Err(TransportError::IncompatibleCodec)
            ));
            sender.binding.wire_version = 2;
            receiver.binding.wire_version = 2;
            let mut a =
                NativePeerTransport::new(sender, c(), &q, TransportLimits::default()).unwrap();
            let mut b =
                NativePeerTransport::new(receiver, c(), &qb, TransportLimits::default()).unwrap();
            a.submit(batch(&mut q, vec![input.clone()])).unwrap();
            for _ in 0..2000 {
                poll(&mut a);
                poll(&mut b);
                if a.usage().completion && b.received_info().is_some() {
                    break;
                }
            }
            assert_eq!(q.usage().batches, 1);
            let received = b.take_received().unwrap();
            assert_eq!(received.messages, vec![input]);
            assert_eq!(received.connection.wire_version, 2);
            received
                .info(TransportLimits::default().decoded_bytes)
                .unwrap();
            let done = a.take_send().unwrap();
            assert_eq!(done.result, LocalSendResult::Sent);
            q.complete(done.batch, done.result).unwrap();
            assert!(q.is_drained());
            assert_eq!(b.usage().decoded_bytes, 0);
        }
    }
}
