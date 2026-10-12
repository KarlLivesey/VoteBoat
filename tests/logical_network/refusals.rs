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
use super::{controls::*, fixture, host::Host, parent::Parent};
use voteboat::{logical_voter::*, outbound::*, runtime::*, transport::*};

fn original_frame(
    bridge: &mut super::aliases::Bridge<'_>,
    parent: &mut Parent,
    descriptors: &[LogicalPeerDescriptor],
    clock: std::time::Instant,
) -> Vec<u8> {
    let batch = fixture::original_send(bridge, parent, clock);
    let host = Host::new(fixture::binding(descriptors, 0, 1, 1));
    let mut source = session(host.clone(), fixture::plan(descriptors, 0), parent);
    source.submit(batch).map_err(|e| e.reason).unwrap();
    let completion = flush(&mut source, &host);
    parent
        .outbound
        .complete_logical(completion)
        .map_err(|e| e.reason)
        .unwrap();
    source.close();
    source.poll(MonoTime(10001), budget(0, 0)).unwrap();
    assert!(source.is_drained());
    let bytes = host.shared.borrow().output.clone();
    bytes
}

fn refused(
    parent: &Parent,
    descriptors: &[LogicalPeerDescriptor],
    bytes: Vec<u8>,
    expected: LogicalNetworkError,
) {
    let host = Host::new(fixture::binding(descriptors, 1, 0, 2));
    host.shared.borrow_mut().input.extend(bytes);
    host.shared.borrow_mut().eof = true;
    let mut receiver = session(host.clone(), fixture::plan(descriptors, 1), parent);
    let mut terminal = None;
    for time in 0..10000 {
        if let Err(e) = receiver.poll(MonoTime(time), budget(3, 0)) {
            terminal = Some(e);
            break;
        }
        assert!(
            !receiver.has_received(),
            "malformed input must never escape"
        );
    }
    assert_eq!(terminal, Some(expected));
    assert!(host.shared.borrow().revoked);
    assert!(receiver.is_drained());
    assert!(receiver.take_send().is_none());
}

#[test]
fn real_frame_refuses_truncation_oversize_and_original_receipt_mismatches() {
    fixture::history(|bridges, parents, descriptors, clocks| {
        let bytes = original_frame(&mut bridges[0], &mut parents[0], descriptors, clocks[0]);
        for offset in [0, 72, 88, 112] {
            let mut wrong = bytes.clone();
            wrong[offset] ^= 1;
            refused(
                &parents[1],
                descriptors,
                wrong,
                LogicalNetworkError::InvalidFrame,
            );
        }
        let mut oversized = bytes.clone();
        oversized[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        refused(
            &parents[1],
            descriptors,
            oversized,
            LogicalNetworkError::TooLarge,
        );
        for cut in [1, 15, 16, bytes.len() - 1] {
            refused(
                &parents[1],
                descriptors,
                bytes[..cut].to_vec(),
                LogicalNetworkError::Truncated,
            );
        }
    });
}

#[test]
fn constructor_and_accepted_abort_preserve_original_channel_and_queue_ownership() {
    fixture::history(|bridges, parents, descriptors, clocks| {
        let batch = fixture::original_send(&mut bridges[0], &mut parents[0], clocks[0]);
        let credits = parents[0].outbound.usage();
        for wrong_version in [1, 8] {
            let mut binding = fixture::binding(descriptors, 0, 1, 1);
            binding.wire_version = wrong_version;
            let host = Host::new(binding);
            let rejected = LogicalPeerSession::new(
                host.clone(),
                fixture::plan(descriptors, 0),
                parents[0].owner.authorization(),
                parents[0].outbound.binding(),
                LogicalNetworkLimits::default(),
            )
            .err()
            .unwrap();
            assert_eq!(rejected.reason, LogicalNetworkError::WrongBinding);
            assert!(std::rc::Rc::ptr_eq(&host.shared, &rejected.session.shared));
            assert!(!host.shared.borrow().revoked);
            assert!(host.shared.borrow().output.is_empty());
        }
        let host = Host::new(fixture::binding(descriptors, 0, 1, 1));
        let mut source = session(host.clone(), fixture::plan(descriptors, 0), &parents[0]);
        source.submit(batch).map_err(|e| e.reason).unwrap();
        source.poll(MonoTime(1), budget(0, 7)).unwrap();
        source.abort();
        assert!(host.shared.borrow().revoked);
        assert_eq!(source.state(), TransportState::Failed);
        assert!(!source.is_drained(), "original completion remains owned");
        assert_eq!(parents[0].outbound.usage(), credits);
        let completion = source.take_send().unwrap();
        assert_eq!(completion.result(), LocalSendResult::Failed);
        parents[0]
            .outbound
            .complete_logical(completion)
            .map_err(|e| e.reason)
            .unwrap();
        assert!(source.is_drained());
        assert!(parents[0].outbound.is_drained());
    });
}
