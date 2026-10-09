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
use std::cell::Cell;
use support::*;
use voteboat::{authorization::*, identity::*, runtime::MonoTime, secure::*};
struct HostSession {
    binding: Cell<SessionBinding>,
    security: SessionSecurity,
    state: SessionState,
}
fn session() -> HostSession {
    let local = |n| LocalIdentity {
        node: node(n),
        store: StoreBinding {
            identity: identity(n as u128),
            session: StoreSession::new(1).unwrap(),
        },
    };
    HostSession {
        binding: Cell::new(SessionBinding {
            local: local(1),
            peer: local(2),
            generation: SecureSessionGeneration::new(1).unwrap(),
            wire_version: 1,
        }),
        security: SessionSecurity::Authenticated,
        state: SessionState::Ready,
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
        Some(self.binding.get())
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(&mut self, _: MonoTime, _: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, _: &mut [u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn write_plaintext(&mut self, _: &[u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {
        self.state = SessionState::Closed;
    }
    fn revoke(&mut self) {
        self.state = SessionState::Failed;
    }
}
fn context() -> CredentialContext {
    CredentialContext {
        generation: CredentialGeneration::new(1).unwrap(),
        authenticated_at: MonoTime(10),
    }
}
#[derive(Clone)]
struct HostCredentials {
    bound: BoundPrincipal,
    closed: bool,
}
impl PrincipalCredentials for HostCredentials {
    fn bind(
        &self,
        _: &dyn SecureSession,
        _: CredentialContext,
        _: MonoTime,
    ) -> Result<BoundPrincipal, AuthorizationError> {
        if self.closed {
            Err(AuthorizationError::Closed)
        } else {
            Ok(self.bound)
        }
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
struct HostPolicy<'a> {
    calls: Cell<usize>,
    change_session: Option<&'a HostSession>,
}
impl ServiceAuthorizer for HostPolicy<'_> {
    fn authorize(&self, request: AuthorizationRequest) -> Result<(), AuthorizationError> {
        self.calls.set(self.calls.get() + 1);
        if let Some(session) = self.change_session {
            let mut changed = session.binding.get();
            changed.generation = SecureSessionGeneration::new(2).unwrap();
            session.binding.set(changed);
        }
        if request.scope != group(1) || request.action != ServiceAction::Read {
            return Err(AuthorizationError::Denied);
        }
        Ok(())
    }
}
#[test]
fn downstream_credentials_and_policy_pass_same_checked_scope_gate_without_native() {
    let session = session();
    let mut credentials = HostCredentials {
        bound: BoundPrincipal {
            principal: PrincipalId::new(7).unwrap(),
            session: session.binding.get(),
            generation: context().generation,
            issued_at: MonoTime(10),
            expires_at: MonoTime(20),
        },
        closed: false,
    };
    let other = credentials.clone();
    let policy = HostPolicy {
        calls: Cell::new(0),
        change_session: None,
    };
    let gate = |credentials: &HostCredentials, scope, action, now| {
        authorize_session(
            credentials,
            &policy,
            &session,
            context(),
            scope,
            action,
            MonoTime(now),
        )
    };
    assert_eq!(
        gate(&credentials, group(1), ServiceAction::Read, 10)
            .unwrap()
            .principal
            .get(),
        7
    );
    assert_eq!(
        gate(&credentials, group(2), ServiceAction::Read, 10),
        Err(AuthorizationError::Denied)
    );
    assert_eq!(
        gate(&credentials, group(1), ServiceAction::Write, 10),
        Err(AuthorizationError::Denied)
    );
    credentials.close();
    assert_eq!(
        gate(&credentials, group(1), ServiceAction::Read, 10),
        Err(AuthorizationError::Closed)
    );
    assert!(gate(&other, group(1), ServiceAction::Read, 19).is_ok());
    assert_eq!(
        gate(&other, group(1), ServiceAction::Read, 20),
        Err(AuthorizationError::Expired)
    );
    assert_eq!(
        gate(&other, group(1), ServiceAction::Read, 9),
        Err(AuthorizationError::ClockRegressed)
    );
    let mut bad = other.clone();
    bad.bound.generation = CredentialGeneration::new(2).unwrap();
    assert_eq!(
        gate(&bad, group(1), ServiceAction::Read, 10),
        Err(AuthorizationError::WrongGeneration)
    );
    bad = other.clone();
    bad.bound.session.peer.store.session = StoreSession::new(2).unwrap();
    let calls = policy.calls.get();
    assert_eq!(
        gate(&bad, group(1), ServiceAction::Read, 10),
        Err(AuthorizationError::WrongBinding)
    );
    assert_eq!(policy.calls.get(), calls);
}
#[test]
fn simulator_unready_and_changed_host_sessions_never_pass_authorization() {
    let mut session = session();
    let credentials = HostCredentials {
        bound: BoundPrincipal {
            principal: PrincipalId::new(7).unwrap(),
            session: session.binding.get(),
            generation: context().generation,
            issued_at: MonoTime(10),
            expires_at: MonoTime(20),
        },
        closed: false,
    };
    let policy = HostPolicy {
        calls: Cell::new(0),
        change_session: None,
    };
    session.security = SessionSecurity::SimulatorOnly;
    assert!(matches!(
        authorize_session(
            &credentials,
            &policy,
            &session,
            context(),
            group(1),
            ServiceAction::Read,
            MonoTime(10)
        ),
        Err(AuthorizationError::Session(SessionError::InsecureProvider))
    ));
    session.security = SessionSecurity::Authenticated;
    session.state = SessionState::Handshaking;
    assert!(matches!(
        authorize_session(
            &credentials,
            &policy,
            &session,
            context(),
            group(1),
            ServiceAction::Read,
            MonoTime(10)
        ),
        Err(AuthorizationError::Session(SessionError::NotReady))
    ));
    assert_eq!(policy.calls.get(), 0);
    session.state = SessionState::Ready;
    let policy = HostPolicy {
        calls: Cell::new(0),
        change_session: Some(&session),
    };
    assert_eq!(
        authorize_session(
            &credentials,
            &policy,
            &session,
            context(),
            group(1),
            ServiceAction::Read,
            MonoTime(10)
        ),
        Err(AuthorizationError::WrongBinding)
    );
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::authorization::*;
    fn entry(session: &HostSession) -> ServiceAccess {
        ServiceAccess {
            principal: PrincipalId::new(7).unwrap(),
            peer: PeerIdentity {
                node: session.binding.get().peer.node,
                store: session.binding.get().peer.store.identity,
            },
            scope: group(1),
            permissions: ServicePermissions::READER,
        }
    }
    #[test]
    fn native_plan_scope_epoch_expiry_close_and_refresh_snapshots_are_checked() {
        let session = session();
        let mut first =
            NativeServiceAccess::new(context().generation, 10, vec![entry(&session)]).unwrap();
        let other = first.clone();
        let bound = authorize_session(
            &first,
            &first,
            &session,
            context(),
            group(1),
            ServiceAction::Read,
            MonoTime(10),
        )
        .unwrap();
        for action in [
            ServiceAction::Write,
            ServiceAction::Configure,
            ServiceAction::Checkpoint,
            ServiceAction::Shutdown,
        ] {
            assert_eq!(
                authorize_session(
                    &first,
                    &first,
                    &session,
                    context(),
                    group(1),
                    action,
                    MonoTime(10)
                ),
                Err(AuthorizationError::Denied)
            );
        }
        assert_eq!(
            authorize_session(
                &first,
                &first,
                &session,
                context(),
                group(2),
                ServiceAction::Read,
                MonoTime(10)
            ),
            Err(AuthorizationError::Denied)
        );
        assert_eq!(
            first.bind(&session, context(), MonoTime(20)),
            Err(AuthorizationError::Expired)
        ); // no automatic lifetime extension
        assert_eq!(
            first.bind(&session, context(), MonoTime(9)),
            Err(AuthorizationError::ClockRegressed)
        );
        first.close();
        assert_eq!(
            first.bind(&session, context(), MonoTime(10)),
            Err(AuthorizationError::Closed)
        );
        drop(first);
        assert!(other
            .authorize(AuthorizationRequest {
                principal: bound,
                scope: group(1),
                action: ServiceAction::Read,
                now: MonoTime(19)
            })
            .is_ok());
        let fresh = NativeServiceAccess::new(
            CredentialGeneration::new(2).unwrap(),
            10,
            vec![entry(&session)],
        )
        .unwrap();
        assert_eq!(
            fresh.authorize(AuthorizationRequest {
                principal: bound,
                scope: group(1),
                action: ServiceAction::Read,
                now: MonoTime(10)
            }),
            Err(AuthorizationError::WrongGeneration)
        );
        assert_eq!(
            fresh.bind(&session, context(), MonoTime(10)),
            Err(AuthorizationError::WrongGeneration)
        );
        let mut unknown = session.binding.get();
        unknown.peer.store.identity = identity(42);
        session.binding.set(unknown);
        assert_eq!(
            other.bind(&session, context(), MonoTime(10)),
            Err(AuthorizationError::UnknownPrincipal)
        );
    }
    #[test]
    fn invalid_native_plan_returns_original_allocation_without_changing_other_views() {
        let session = session();
        let original = vec![entry(&session), entry(&session)];
        let pointer = original.as_ptr();
        let (error, returned) = NativeServiceAccess::new(context().generation, 10, original)
            .err()
            .unwrap();
        assert_eq!(error, AuthorizationError::InvalidPlan);
        assert_eq!(returned.as_ptr(), pointer);
        let mut excess = Vec::with_capacity(4097);
        excess.push(entry(&session));
        assert!(NativeServiceAccess::new(context().generation, 10, excess).is_err());
        assert!(ServicePermissions::from_bits(0).is_none());
        assert!(ServicePermissions::from_bits(128).is_none());
        let mut context = context();
        context.authenticated_at = MonoTime(u64::MAX);
        let plan = NativeServiceAccess::new(context.generation, 10, vec![entry(&session)]).unwrap();
        assert_eq!(
            plan.bind(&session, context, MonoTime(u64::MAX)),
            Err(AuthorizationError::Expired)
        );
    }
}
