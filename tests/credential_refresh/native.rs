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
#[cfg(feature = "tls")]
use voteboat::native::authorization::{NativeServiceAccess, ServiceAccess};
use voteboat::{authorization::*, native::credentials::*};
pub(super) fn generation(n: u64) -> CredentialGeneration {
    CredentialGeneration::new(n).unwrap()
}
#[test]
fn native_replace_is_monotonic_returns_material_and_close_is_scoped() {
    let mut owner = NativeCredentialSet::new(generation(3), vec![1, 2]);
    let first = owner.lease().unwrap();
    let unrelated = NativeCredentialSet::new(generation(3), ());
    let independent = unrelated.lease().unwrap();
    for g in [1, 3] {
        let material = vec![9, 8];
        let pointer = material.as_ptr();
        let (e, returned) = owner.replace(generation(g), material).unwrap_err();
        assert_eq!(e, SessionError::InvalidCredentials);
        assert_eq!(returned.as_ptr(), pointer);
        assert!(first.validate().is_ok());
        assert_eq!(owner.material(), Some(&vec![1, 2]));
    }
    assert_eq!(owner.replace(generation(4), vec![3]).unwrap(), vec![1, 2]);
    assert_eq!(first.validate(), Err(SessionError::Revoked));
    let next = owner.lease().unwrap();
    assert_eq!(next.generation(), generation(4));
    owner.close();
    assert_eq!(next.validate(), Err(SessionError::Revoked));
    assert!(independent.validate().is_ok());
    assert!(owner.material().is_none());
    assert!(owner.lease().is_err());
    assert_eq!(
        owner.replace(generation(5), vec![4]),
        Err((SessionError::Revoked, vec![4]))
    );
    assert_eq!(owner.into_material(), vec![3]);
    drop(unrelated);
    assert_eq!(independent.validate(), Err(SessionError::Revoked));
}
#[cfg(feature = "tls")]
fn policy(g: u64, permissions: ServicePermissions) -> NativeServiceAccess {
    NativeServiceAccess::new(
        generation(g),
        10_000,
        vec![ServiceAccess {
            principal: PrincipalId::new(1).unwrap(),
            peer: PeerIdentity {
                node: local(2).node,
                store: local(2).store.identity,
            },
            scope: support::group(1),
            permissions,
        }],
    )
    .unwrap()
}
#[cfg(feature = "tls")]
pub(super) fn exercise<S: SecureSession, T: SecureSession>(
    a: S,
    b: T,
    now: MonoTime,
) -> NativeCredentialSet<NativeServiceAccess> {
    let mut credentials =
        NativeCredentialSet::new(generation(1), policy(1, ServicePermissions::WRITER));
    let mut a = GuardedSession::new(a, credentials.lease().unwrap())
        .ok()
        .unwrap();
    let mut b = GuardedSession::new(b, credentials.lease().unwrap())
        .ok()
        .unwrap();
    let context = CredentialContext {
        generation: generation(1),
        authenticated_at: now,
    };
    let access = credentials.material().unwrap();
    assert!(authorize_session(
        access,
        access,
        &a,
        context,
        support::group(1),
        ServiceAction::Write,
        now
    )
    .is_ok());
    assert_eq!(a.write_plaintext(b"before"), Ok(6));
    let old = credentials
        .replace(generation(2), policy(2, ServicePermissions::READER))
        .unwrap_or_else(|(error, _)| panic!("credential replacement: {error:?}"));
    assert_eq!(a.state(), SessionState::Failed);
    assert_eq!(b.state(), SessionState::Failed);
    assert_eq!(
        a.poll(now, SessionPollBudget::default()),
        Err(SessionError::Revoked)
    );
    assert_eq!(b.read_plaintext(&mut [0; 8]), Err(SessionError::Revoked));
    assert_eq!(a.write_plaintext(b"after"), Err(SessionError::Revoked));
    // Even the old still-owned policy cannot authorize a revoked channel.
    assert!(authorize_session(
        &old,
        &old,
        &a,
        context,
        support::group(1),
        ServiceAction::Write,
        now
    )
    .is_err());
    assert_eq!(credentials.generation(), Some(generation(2)));
    credentials
}

#[cfg(feature = "tls")]
pub(super) fn resume<S: SecureSession, T: SecureSession>(
    a: S,
    b: T,
    credentials: &NativeCredentialSet<NativeServiceAccess>,
    now: MonoTime,
) {
    let mut a = GuardedSession::new(a, credentials.lease().unwrap())
        .ok()
        .unwrap();
    let mut b = GuardedSession::new(b, credentials.lease().unwrap())
        .ok()
        .unwrap();
    assert_eq!(require_authenticated(&a).unwrap().peer.node, local(2).node);
    let context = CredentialContext {
        generation: generation(2),
        authenticated_at: now,
    };
    let access = credentials.material().unwrap();
    assert!(authorize_session(
        access,
        access,
        &a,
        context,
        support::group(1),
        ServiceAction::Read,
        now
    )
    .is_ok());
    assert_eq!(
        authorize_session(
            access,
            access,
            &a,
            context,
            support::group(1),
            ServiceAction::Write,
            now
        ),
        Err(AuthorizationError::Denied)
    );
    assert_eq!(a.write_plaintext(b"new"), Ok(3));
    for step in 0..1000 {
        a.poll(MonoTime(now.0 + step), SessionPollBudget::default())
            .unwrap();
        b.poll(MonoTime(now.0 + step), SessionPollBudget::default())
            .unwrap();
        let mut bytes = [0; 3];
        match b.read_plaintext(&mut bytes) {
            Ok(n) => {
                assert_eq!(n, 3);
                assert_eq!(&bytes, b"new");
                return;
            }
            Err(SessionError::WouldBlock) => (),
            other => panic!("fresh session failed: {other:?}"),
        }
    }
    panic!("fresh session stalled");
}

#[cfg(feature = "tls")]
#[test]
fn native_transport_returns_exact_accepted_batch_as_failed_after_revocation() {
    use voteboat::{
        native::{outbound::NativeOutbound, transport::NativePeerTransport, wire::NativeWireCodec},
        outbound::*,
        raft::{Message, RequestContext, Rpc},
        transport::*,
        wire::WireLimits,
    };
    let (a, _b) = support::tls::pair(local(1), local(2), 1);
    let binding = a.binding().unwrap();
    let mut credentials = NativeCredentialSet::new(generation(1), ());
    let a = GuardedSession::new(a, credentials.lease().unwrap())
        .ok()
        .unwrap();
    let mut queue = NativeOutbound::new(
        OutboundBinding {
            node: binding.local.node,
            store: binding.local.store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        OutboundLimits::default(),
    )
    .unwrap();
    let mut transport = NativePeerTransport::new(
        a,
        NativeWireCodec::new(WireLimits::default()).unwrap(),
        &queue,
        TransportLimits::default(),
    )
    .unwrap();
    queue
        .submit(vec![Message {
            group: support::group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            from: binding.local.node,
            to: binding.peer.node,
            sender: binding.local.store,
            term: 1,
            context: RequestContext {
                origin: binding.local.store,
                sequence: 1,
            },
            rpc: Rpc::ReadProbe,
        }])
        .unwrap();
    let batch = queue.poll(1).pop().unwrap();
    let ticket = batch.ticket;
    transport.submit(batch).unwrap();
    credentials.replace(generation(2), ()).unwrap();
    assert_eq!(
        transport.poll(MonoTime(0), TransportPollBudget::default()),
        Err(TransportError::WrongBinding)
    );
    let failed = transport.take_send().unwrap();
    assert_eq!(failed.batch.ticket, ticket);
    assert_eq!(failed.result, LocalSendResult::Failed);
    assert!(transport.take_send().is_none());
    assert_eq!(transport.state(), TransportState::Failed);
}
