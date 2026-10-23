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
use super::{fixture, host::Host, parent::Parent};
use voteboat::{identity::*, logical_voter::*, outbound::*, runtime::*, secure::*, transport::*};
pub(super) type Session = LogicalPeerSession<Host>;
pub(super) fn session(host: Host, plan: LogicalPeerPlan, parent: &Parent) -> Session {
    LogicalPeerSession::new(
        host,
        plan,
        parent.owner.authorization(),
        parent.outbound.binding(),
        LogicalNetworkLimits::default(),
    )
    .map_err(|e| e.reason)
    .unwrap()
}
pub(super) fn budget(read_bytes: usize, write_bytes: usize) -> TransportPollBudget {
    TransportPollBudget {
        session: SessionPollBudget {
            io_calls: 0,
            read_bytes: 0,
            write_bytes: 0,
        },
        plaintext_calls: 1,
        read_bytes,
        write_bytes,
    }
}
pub(super) fn flush(source: &mut Session, host: &Host) -> LogicalNetworkCompletion {
    for time in 2..10000 {
        source.poll(MonoTime(time), budget(0, 7)).unwrap();
        let s = host.shared.borrow();
        if s.output.len() >= 16 {
            let length = u64::from_le_bytes(s.output[8..16].try_into().unwrap()) as usize;
            if s.output.len() == length {
                break;
            }
        }
        assert!(time < 9999, "bounded short-write progress");
    }
    assert!(
        source.take_send().is_none(),
        "local plaintext acceptance is not flush"
    );
    host.shared.borrow_mut().flushed = true;
    source.poll(MonoTime(10000), budget(0, 0)).unwrap();
    source.take_send().unwrap()
}
#[test]
fn real_original_proof_short_io_flush_stale_owner_and_completion_preserve_credits() {
    fixture::history(|bridges, parents, descriptors, clocks| {
        let batch = fixture::original_send(&mut bridges[0], &mut parents[0], clocks[0]);
        let message = batch.send().message().clone();
        let credits = parents[0].outbound.usage();
        let proof = batch.send().provenance();
        let retained = proof.retained_bytes(usize::MAX).unwrap();
        assert_eq!(proof.retained_bytes(retained), Ok(retained));
        assert_eq!(
            proof.retained_bytes(retained - 1),
            Err(OutboundError::BatchTooLarge)
        );
        let (_, message_bytes) = message_cost(batch.send().message(), usize::MAX).unwrap();
        assert_eq!(
            credits.bytes,
            message_bytes + retained + std::mem::size_of::<AuthorizedLogicalSend>() + 512
        );
        let host = Host::new(fixture::binding(descriptors, 0, 1, 1));
        let mut source = session(host.clone(), fixture::plan(descriptors, 0), &parents[0]);
        source.submit(batch).map_err(|e| e.reason).unwrap();
        source.poll(MonoTime(1), budget(0, 7)).unwrap();
        let written = host.shared.borrow().output.len();
        assert!(written > 0 && written <= 7);
        assert_eq!(
            source.poll(MonoTime(0), budget(0, 7)),
            Err(LogicalNetworkError::TimeWentBack)
        );
        let invalid = TransportPollBudget {
            plaintext_calls: 4097,
            ..budget(0, 7)
        };
        assert_eq!(
            source.poll(MonoTime(2), invalid),
            Err(LogicalNetworkError::InvalidLimits)
        );
        assert_eq!(host.shared.borrow().output.len(), written);
        assert_eq!(parents[0].outbound.usage(), credits);
        source.close();
        let completion = flush(&mut source, &host);
        assert_eq!(completion.result(), LocalSendResult::Sent);
        assert_eq!(parents[0].outbound.usage(), credits);
        let mut stale = LogicalNetworkQueue::new(
            parents[0].owner.authorization(),
            OutboundGeneration::new(2).unwrap(),
            OutboundLimits::default(),
            LogicalNetworkLimits::default(),
        )
        .unwrap();
        let rejected = stale.complete_logical(completion).err().unwrap();
        assert_eq!(rejected.reason, OutboundError::UnknownTicket);
        parents[0]
            .outbound
            .complete_logical(*rejected.completion)
            .map_err(|e| e.reason)
            .unwrap();
        assert!(parents[0].outbound.is_drained());
        assert!(source.is_drained());
        receive(
            &parents[1],
            descriptors,
            host.shared.borrow().output.clone(),
            message,
        );
    });
}
fn receive(
    parent: &Parent,
    descriptors: &[LogicalPeerDescriptor],
    bytes: Vec<u8>,
    message: voteboat::raft::Message,
) {
    let host = Host::new(fixture::binding(descriptors, 1, 0, 2));
    host.shared.borrow_mut().input.extend(bytes);
    host.shared.borrow_mut().flushed = true;
    let mut receiver = session(host, fixture::plan(descriptors, 1), parent);
    for time in 0..10000 {
        receiver.poll(MonoTime(time), budget(3, 0)).unwrap();
        if receiver.has_received() {
            break;
        }
        assert!(time < 9999, "bounded short-read progress");
    }
    receiver.abort();
    assert!(
        !receiver.is_drained(),
        "complete input survives channel failure"
    );
    let physical = RuntimeAuthorization::physical(parent.owner.identity());
    assert_eq!(
        receiver.take_received(physical).err(),
        Some(LogicalNetworkError::StaleOwner)
    );
    assert!(receiver.has_received());
    let received = receiver
        .take_received(parent.owner.authorization())
        .unwrap()
        .unwrap();
    assert_ne!(received.physical().node, message.from);
    let rejected = received.into_message(physical).err().unwrap();
    assert_eq!(rejected.reason, LogicalNetworkError::StaleOwner);
    assert_eq!(
        rejected
            .received
            .into_message(parent.owner.authorization())
            .map_err(|e| e.reason)
            .unwrap(),
        message
    );
    assert!(receiver.is_drained());
}
#[test]
fn real_dispatched_rejected_batch_cancels_exact_original_after_close() {
    fixture::history(|bridges, parents, descriptors, clocks| {
        let batch = fixture::original_send(&mut bridges[0], &mut parents[0], clocks[0]);
        let message = batch.send().message().clone();
        let credits = parents[0].outbound.usage();
        let rejected = parents[0]
            .outbound
            .submit(vec![message.clone()])
            .unwrap_err();
        assert_eq!(rejected.reason, OutboundError::WrongBinding);
        assert_eq!(rejected.messages, vec![message]);
        assert!(parents[0].outbound.poll(8).is_empty());
        let host = Host::new(fixture::binding(descriptors, 0, 1, 1));
        let mut source = session(host.clone(), fixture::plan(descriptors, 0), &parents[0]);
        source.close();
        source.poll(MonoTime(0), budget(0, 0)).unwrap();
        let rejected = source.submit(batch).err().unwrap();
        assert_eq!(rejected.reason, LogicalNetworkError::Closed);
        assert!(host.shared.borrow().output.is_empty());
        let mut stale = LogicalNetworkQueue::new(
            parents[0].owner.authorization(),
            OutboundGeneration::new(2).unwrap(),
            OutboundLimits::default(),
            LogicalNetworkLimits::default(),
        )
        .unwrap();
        let wrong = stale.cancel_dispatched(*rejected.batch).err().unwrap();
        assert_eq!(wrong.reason, OutboundError::UnknownTicket);
        assert_eq!(parents[0].outbound.usage(), credits);
        parents[0].outbound.close();
        let result = parents[0]
            .outbound
            .cancel_dispatched(*wrong.batch)
            .map_err(|e| e.reason)
            .unwrap();
        assert_eq!(result.result, LocalSendResult::Cancelled);
        assert!(parents[0].outbound.is_drained());
        assert!(source.is_drained());
    });
}
