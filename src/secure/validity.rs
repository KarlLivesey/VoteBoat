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
//! Credential validity independent of cryptography and transport ownership.
use super::*;
pub const SESSION_VALIDITY_CONTRACT_VERSION: u32 = 1;
/// Bounded nonblocking host check for one immutable credential generation.
/// A failed check must remain failed; replacement requires a new validity view.
/// The provider is trusted host code, not remote input or a membership decision.
pub trait SessionValidity {
    fn validate(&self) -> Result<(), SessionError>;
}
impl<V: SessionValidity + ?Sized> SessionValidity for Box<V> {
    fn validate(&self) -> Result<(), SessionError> {
        (**self).validate()
    }
}
/// Retains the selected provider and validity lease. No callback can refresh an
/// old session's lease. Checks surround each I/O call; accepted external progress
/// before revocation cannot be rolled back. Dropping one wrapper is local.
pub struct GuardedSession<S, V> {
    session: S,
    validity: V,
    binding: SessionBinding,
    revoked: bool,
}
impl<S: SecureSession, V: SessionValidity> GuardedSession<S, V> {
    pub fn new(session: S, validity: V) -> Result<Self, (SessionError, S, V)> {
        let result = validity
            .validate()
            .and_then(|()| require_authenticated(&session))
            .and_then(|binding| {
                validity.validate()?;
                if require_authenticated(&session)? != binding {
                    return Err(SessionError::WrongPeer);
                }
                Ok(binding)
            });
        match result {
            Ok(binding) => Ok(Self {
                session,
                validity,
                binding,
                revoked: false,
            }),
            Err(error) => Err((error, session, validity)),
        }
    }
    fn check(&self) -> Result<(), SessionError> {
        if self.revoked {
            return Err(SessionError::Revoked);
        }
        self.validity.validate()?;
        if self.session.security() != SessionSecurity::Authenticated {
            return Err(SessionError::InsecureProvider);
        }
        if self.session.binding() != Some(self.binding) {
            return Err(SessionError::WrongPeer);
        }
        Ok(())
    }
    fn enforce(&mut self) -> Result<(), SessionError> {
        if let Err(error) = self.check() {
            self.revoke();
            return Err(error);
        }
        Ok(())
    }
    /// Explicitly abandons the wrapper; its lease cannot refresh the session.
    /// The underlying channel is revoked before ownership is returned.
    pub fn into_revoked_parts(mut self) -> (S, V) {
        self.revoke();
        (self.session, self.validity)
    }
}
impl<S: SecureSession, V: SessionValidity> SecureSession for GuardedSession<S, V> {
    fn security(&self) -> SessionSecurity {
        self.session.security()
    }
    fn state(&self) -> SessionState {
        if self.check().is_err() {
            SessionState::Failed
        } else {
            self.session.state()
        }
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.check().ok().map(|()| self.binding)
    }
    fn limits(&self) -> SessionLimits {
        self.session.limits()
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        self.enforce()?;
        let result = self.session.poll(now, budget);
        self.enforce()?;
        result
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        self.enforce()?;
        let result = self.session.read_plaintext(bytes);
        if let Err(error) = self.enforce() {
            bytes.fill(0);
            return Err(error);
        }
        result
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        self.enforce()?;
        let result = self.session.write_plaintext(bytes);
        self.enforce()?;
        result
    }
    fn is_flushed(&self) -> bool {
        self.check().is_ok() && self.session.is_flushed()
    }
    fn close(&mut self) {
        self.session.close();
    }
    fn revoke(&mut self) {
        if !self.revoked {
            self.revoked = true;
            self.session.revoke();
        }
    }
}
