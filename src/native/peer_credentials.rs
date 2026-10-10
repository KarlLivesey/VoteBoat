// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit prepared peer-key replacement without changing membership or routes.
use super::{credentials::*, tls::*};
use crate::{
    authorization::CredentialGeneration, connect::*, identity::NodeId, runtime::MonoTime,
    secure::*, transport::ConnectTicket,
};
use std::collections::BTreeMap;

pub struct NativePeerMaterial {
    pub tls: NativeTlsConfig,
    pub peers: BTreeMap<NodeId, TlsPeer>,
}
impl NativePeerMaterial {
    /// Bind a durable local rollout to the exact TLS bytes, wire version and
    /// ordered peer identities, certificates and names. Routes are unchanged
    /// by rotation and are deliberately not credential material.
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
        hash.update(b"voteboat-peer-material-v1");
        hash.update(&self.tls.credential_fingerprint);
        hash.update(&self.tls.wire_version().to_be_bytes());
        hash.update(&(self.peers.len() as u64).to_be_bytes());
        for (node, peer) in &self.peers {
            hash.update(&node.get().to_be_bytes());
            hash.update(&peer.identity.node.get().to_be_bytes());
            hash.update(&peer.identity.store.id.get().to_be_bytes());
            hash.update(&peer.identity.store.incarnation.get().to_be_bytes());
            for bytes in [peer.certificate.as_slice(), peer.server_name.as_bytes()] {
                hash.update(&(bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
            }
        }
        hash.finish().as_ref().try_into().unwrap()
    }
}

/// Native mechanism used by RotatingPeerConnector. A successful replacement
/// preserves identities/routes/limits and leaves cancellation receipts drainable.
/// Used alone, it does not revoke sessions already transferred to a caller.
pub trait NativePeerMaterialProvider: PeerConnector {
    fn replace_material(
        &mut self,
        material: NativePeerMaterial,
    ) -> Result<NativePeerMaterial, (ConnectError, NativePeerMaterial)>;
}

pub(crate) fn validate_material(
    material: &NativePeerMaterial,
    wire_version: u16,
    identities: impl Iterator<Item = (NodeId, PeerIdentity)>,
) -> Result<(), ConnectError> {
    let mut count = 0;
    for (id, identity) in identities {
        count += 1;
        if material
            .peers
            .get(&id)
            .is_none_or(|p| p.identity != identity)
        {
            return Err(ConnectError::WrongBinding);
        }
    }
    if material.peers.len() != count || material.tls.wire_version() != wire_version {
        return Err(ConnectError::WrongBinding);
    }
    let mut bytes = 0usize;
    for peer in material.peers.values() {
        bytes = bytes
            .checked_add(peer.certificate.capacity())
            .and_then(|v| v.checked_add(peer.server_name.capacity()))
            .ok_or(ConnectError::InvalidRequest)?;
        if peer.certificate.is_empty()
            || peer.certificate.len() > 65536
            || peer.server_name.is_empty()
            || peer.server_name.len() > 253
            || rustls::pki_types::ServerName::try_from(peer.server_name.as_str()).is_err()
            || bytes > 1024 * 1024
        {
            return Err(ConnectError::InvalidRequest);
        }
    }
    Ok(())
}

/// Adds generation-bound session revocation to an explicitly owned connector.
/// Loading/validating TLS and recording rollout durability happen outside poll.
/// Only sessions established through this wrapper carry its validity leases.
pub struct RotatingPeerConnector<C: NativePeerMaterialProvider> {
    inner: C,
    credentials: NativeCredentialSet<()>,
    pending: BTreeMap<NodeId, (ConnectTicket, CredentialLease)>,
    capacity: usize,
    closed: bool,
}
impl<C: NativePeerMaterialProvider> RotatingPeerConnector<C> {
    pub fn new(inner: C, generation: CredentialGeneration) -> Result<Self, C> {
        if !inner.is_drained() || inner.limits().validate().is_err() {
            return Err(inner);
        }
        let capacity = inner.limits().requests;
        Ok(Self {
            inner,
            credentials: NativeCredentialSet::new(generation, ()),
            pending: BTreeMap::new(),
            capacity,
            closed: false,
        })
    }
    /// Reclaim the provider after close and all accepted receipts have drained.
    pub fn into_inner(self) -> Result<C, Self> {
        if self.closed && self.pending.is_empty() && self.inner.is_drained() {
            Ok(self.inner)
        } else {
            Err(self)
        }
    }
}
impl<C: NativePeerMaterialProvider> PeerCredentialControl for RotatingPeerConnector<C> {
    type Credentials = NativePeerMaterial;
    fn credential_generation(&self) -> Option<CredentialGeneration> {
        self.credentials.generation()
    }
    fn replace_peer_credentials(
        &mut self,
        expected: CredentialGeneration,
        replacement: CredentialGeneration,
        material: NativePeerMaterial,
    ) -> Result<NativePeerMaterial, (ConnectError, NativePeerMaterial)> {
        if self.closed {
            return Err((ConnectError::Closed, material));
        }
        if self.credentials.generation() != Some(expected) || replacement <= expected {
            return Err((
                ConnectError::Session(SessionError::InvalidCredentials),
                material,
            ));
        }
        let previous = self.inner.replace_material(material)?;
        // Checked above under exclusive ownership; this publication cannot race.
        self.credentials
            .replace(replacement, ())
            .expect("validated credential generation");
        for (ticket, _) in self.pending.values() {
            self.inner.cancel(*ticket);
        }
        Ok(previous)
    }
}
impl<C: NativePeerMaterialProvider> PeerConnector for RotatingPeerConnector<C> {
    type Endpoint = C::Endpoint;
    type Session = GuardedSession<C::Session, CredentialLease>;
    fn local(&self) -> LocalIdentity {
        self.inner.local()
    }
    fn limits(&self) -> ConnectLimits {
        self.inner.limits()
    }
    fn usage(&self) -> ConnectUsage {
        self.inner.usage()
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.inner.next_deadline()
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        self.inner.supports_peer(peer)
    }
    fn submit(
        &mut self,
        request: ConnectRequest<C::Endpoint>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<C::Endpoint>> {
        if self.closed
            || self.pending.len() >= self.capacity
            || self.pending.contains_key(&request.ticket.peer.node)
        {
            return Err(ConnectRejected {
                reason: if self.closed {
                    ConnectError::Closed
                } else {
                    ConnectError::Overloaded
                },
                request: Box::new(request),
            });
        }
        let ticket = request.ticket;
        let lease = self
            .credentials
            .lease()
            .expect("open connector credentials");
        self.inner.submit(request, now)?;
        self.pending.insert(ticket.peer.node, (ticket, lease));
        Ok(())
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        self.pending
            .get(&ticket.peer.node)
            .is_some_and(|(t, _)| *t == ticket)
            && self.inner.cancel(ticket)
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        let budget = budget.validate()?;
        let completions = self.inner.poll(now, budget)?;
        if completions.len() > budget.completions || completions.len() > self.pending.len() {
            return Err(ConnectError::ProviderViolation);
        }
        completions
            .into_iter()
            .map(|completion| {
                let Some((ticket, lease)) = self.pending.get(&completion.ticket.peer.node) else {
                    return Err(ConnectError::ProviderViolation);
                };
                if *ticket != completion.ticket {
                    return Err(ConnectError::ProviderViolation);
                }
                let lease = lease.clone();
                self.pending.remove(&completion.ticket.peer.node);
                let result = match (lease.validate(), completion.result) {
                    (Err(error), result) => {
                        if let Ok(mut session) = result {
                            session.revoke();
                        }
                        Err(ConnectError::Session(error))
                    }
                    (Ok(()), Ok(session)) => {
                        GuardedSession::new(session, lease).map_err(|(error, mut session, _)| {
                            session.revoke();
                            ConnectError::Session(error)
                        })
                    }
                    (Ok(()), Err(error)) => Err(error),
                };
                Ok(ConnectCompletion {
                    ticket: completion.ticket,
                    result,
                })
            })
            .collect()
    }
    fn close(&mut self) {
        self.closed = true;
        self.credentials.close();
        self.inner.close();
    }
}
