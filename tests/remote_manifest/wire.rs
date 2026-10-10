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
use std::{cell::RefCell, collections::VecDeque, rc::Rc};
type Input = Rc<RefCell<VecDeque<u8>>>;
// Pause delivery after the real responder has encoded a complete hint. This
// tests corrupt wire input through the public transport/provider boundary.
fn captured_reply() -> (
    NativeRemoteManifestDiscovery<Session, NativeManifestCache>,
    Input,
    Vec<u8>,
) {
    let (a, b) = pair(1);
    let input = a.input.clone();
    let mut c = client(a);
    let mut s = server(b);
    assert!(c.lookup(query(1), MonoTime(0)).is_err());
    for step in 0..20 {
        c.poll(MonoTime(step), SessionPollBudget::default())
            .unwrap();
    }
    for step in 20..200 {
        s.poll(MonoTime(step), SessionPollBudget::default())
            .unwrap();
    }
    let bytes: Vec<_> = input.borrow_mut().drain(..).collect();
    assert!(bytes.len() > 92);
    assert_eq!(
        u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize,
        bytes.len()
    );
    (c, input, bytes)
}
fn completion(
    c: &mut NativeRemoteManifestDiscovery<Session, NativeManifestCache>,
) -> ManifestRefreshCompletion {
    for step in 200..1000 {
        if let Some(done) = c
            .poll(MonoTime(step), SessionPollBudget::default())
            .unwrap()
        {
            return done;
        }
    }
    panic!("wire completion stalled")
}
#[test]
fn malformed_headers_scope_sequence_and_manifest_never_enter_cache() {
    // Magic, version, kind, reserved bytes, zero sequence, mismatched query,
    // zero lifetime and corrupt embedded canonical manifest.
    for (offset, value) in [
        (0, 0),
        (4, 99),
        (5, 99),
        (6, 1),
        (12, 0),
        (20, 2),
        (44, 2),
        (68, 1),
        (92, 0),
    ] {
        let (mut c, input, mut bytes) = captured_reply();
        bytes[offset] = value;
        input.borrow_mut().extend(bytes);
        assert_eq!(
            completion(&mut c).result,
            Err(RemoteManifestError::Protocol),
            "offset {offset}"
        );
        assert!(c.source_failed());
        assert_eq!(c.usage().manifests, 0);
    }
    let (mut c, input, mut bytes) = captured_reply();
    bytes[84..92].fill(0);
    input.borrow_mut().extend(bytes);
    assert_eq!(
        completion(&mut c).result,
        Err(RemoteManifestError::Protocol)
    );
}
#[test]
fn oversized_lengths_are_rejected_from_header_and_truncation_times_out() {
    for length in [0u32, 91, (93 + MAX_MANIFEST_BYTES) as u32, u32::MAX] {
        let (mut c, input, mut bytes) = captured_reply();
        bytes.truncate(92);
        bytes[8..12].copy_from_slice(&length.to_le_bytes());
        input.borrow_mut().extend(bytes);
        assert_eq!(
            completion(&mut c).result,
            Err(RemoteManifestError::Protocol)
        );
        assert_eq!(c.usage().bytes, 0);
    }
    let (mut c, input, mut bytes) = captured_reply();
    bytes.pop();
    input.borrow_mut().extend(bytes);
    for step in 200..1000 {
        assert!(c
            .poll(MonoTime(step), SessionPollBudget::default())
            .unwrap()
            .is_none());
    }
    let deadline = c.pending().unwrap().deadline;
    assert_eq!(
        c.poll(deadline, SessionPollBudget::default())
            .unwrap()
            .unwrap()
            .result,
        Err(RemoteManifestError::Timeout)
    );
    assert_eq!(c.usage().manifests, 0);
}
#[test]
fn lifetime_is_bounded_and_counted_from_request_admission() {
    let (mut c, input, bytes) = captured_reply();
    input.borrow_mut().extend(bytes);
    assert!(completion(&mut c).result.is_ok());
    // The responder selected a 50s lifetime after admission. Network delay
    // must not extend it, even if the provider's own clock differs.
    assert_eq!(
        c.lookup(query(1), MonoTime(1000)).unwrap().expires_at,
        MonoTime(50_000)
    );
    let (mut c, input, mut bytes) = captured_reply();
    bytes[84..92].copy_from_slice(&60_001u64.to_le_bytes());
    input.borrow_mut().extend(bytes);
    assert_eq!(
        completion(&mut c).result,
        Err(RemoteManifestError::Protocol)
    );
}
#[test]
fn malformed_or_wrong_authority_request_closes_selected_source() {
    for (offset, value) in [(5, 2), (20, 2), (12, 0)] {
        let (a, b) = pair(1);
        let input = b.input.clone();
        let mut c = client(a);
        let mut s = server(b);
        assert!(c.lookup(query(1), MonoTime(0)).is_err());
        for step in 0..20 {
            c.poll(MonoTime(step), SessionPollBudget::default())
                .unwrap();
        }
        input.borrow_mut()[offset] = value;
        let mut failure = None;
        for step in 20..100 {
            if let Err(error) = s.poll(MonoTime(step), SessionPollBudget::default()) {
                failure = Some(error);
                break;
            }
        }
        assert_eq!(failure, Some(RemoteManifestError::Protocol));
        assert!(s.is_closed());
        assert!(s.source_mut().closed);
        assert_eq!(s.source_mut().calls, 0);
    }
}
#[test]
fn zero_io_budget_defers_work_and_cache_capacity_preserves_existing_entries() {
    let (a, b) = pair(1);
    let outgoing = a.output.clone();
    let mut c = client(a);
    let mut s = server(b);
    s.source_mut().entries.extend([
        (3, manifest(3, 1)),
        (4, manifest(4, 1)),
        (5, manifest(5, 1)),
    ]);
    assert!(c.lookup(query(1), MonoTime(0)).is_err());
    let zero = SessionPollBudget {
        io_calls: 0,
        read_bytes: 0,
        write_bytes: 0,
    };
    assert!(c.poll(MonoTime(0), zero).unwrap().is_none());
    assert!(outgoing.borrow().is_empty());
    let (_, mut now) = drive(&mut c, &mut s, MonoTime(0));
    for id in 2..=4 {
        assert!(c.lookup(query(id), now).is_err());
        let (done, end) = drive(&mut c, &mut s, now);
        assert!(done.result.is_ok());
        now = end;
    }
    assert!(c.lookup(query(5), now).is_err());
    let (done, now) = drive(&mut c, &mut s, now);
    assert_eq!(done.result, Err(ManifestDiscoveryError::Overloaded.into()));
    assert_eq!(c.usage().manifests, 4);
    for id in 1..=4 {
        assert_eq!(c.lookup(query(id), now).unwrap().manifest, manifest(id, 1));
    }
}
