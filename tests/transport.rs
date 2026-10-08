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
}
