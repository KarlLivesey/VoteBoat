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
//! Bounded command-channel assembly over the existing public TLS/auth contracts.
use super::setup::{checked, Failure};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::TcpStream,
    path::Path,
};
use voteboat::{
    authorization::*,
    identity::*,
    native::{authorization::*, tls::*},
    runtime::MonoTime,
    secure::*,
};
fn material(path: &Path, max: u64) -> Result<Vec<u8>, Failure> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(max + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > max {
        return Err("invalid service-access material size".into());
    }
    Ok(bytes)
}
fn transport_identity(number: u64, client: bool) -> PeerIdentity {
    let namespace = if client { 1u64 << 63 } else { 1u64 << 62 };
    PeerIdentity {
        node: NodeId::new(namespace | number).unwrap(),
        store: StoreIdentity {
            id: StoreId::new((1u128 << 127) | u128::from(namespace | number)).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    }
}
pub fn server_local(number: u64, session: StoreSession) -> LocalIdentity {
    let peer = transport_identity(number, false);
    LocalIdentity {
        node: peer.node,
        store: StoreBinding {
            identity: peer.store,
            session,
        },
    }
}
pub struct Access {
    pub policy: NativeServiceAccess,
    tls: NativeTlsConfig,
    peers: BTreeMap<PrincipalId, TlsPeer>,
}
impl Access {
    pub fn load(path: &Path, directory: &Path, tls: NativeTlsConfig) -> Result<Self, Failure> {
        let bytes = material(path, 4096)?;
        let text = std::str::from_utf8(&bytes)?;
        let mut lines = text.lines();
        let header = lines
            .next()
            .ok_or("missing service access header")?
            .split_whitespace()
            .collect::<Vec<_>>();
        let ["voteboat-service-access-v1", generation] = header.as_slice() else {
            return Err("expected voteboat-service-access-v1 GENERATION".into());
        };
        let generation = CredentialGeneration::new(generation.parse()?)
            .ok_or("invalid credential generation")?;
        let mut entries = Vec::new();
        let mut peers = BTreeMap::new();
        let mut pin_bytes = 0usize;
        for line in lines {
            if entries.len() == 64 {
                return Err("service access exceeds 64 grants".into());
            }
            let words = line.split_whitespace().collect::<Vec<_>>();
            let [number, role, group, incarnation] = words.as_slice() else {
                return Err("expected PRINCIPAL reader|writer|admin GROUP INCARNATION".into());
            };
            let number: u64 = number.parse()?;
            if !(1..=super::setup::MAX_NODE).contains(&number) {
                return Err("invalid service principal".into());
            }
            let principal = PrincipalId::new(number).unwrap();
            let scope = GroupIdentity {
                id: GroupId::new(group.parse()?).ok_or("invalid access group")?,
                incarnation: GroupIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid access incarnation")?,
            };
            let permissions = match *role {
                "reader" => ServicePermissions::READER,
                "writer" => ServicePermissions::WRITER,
                "admin" => ServicePermissions::ADMIN,
                _ => return Err("invalid service role".into()),
            };
            let identity = transport_identity(number, true);
            if let std::collections::btree_map::Entry::Vacant(entry) = peers.entry(principal) {
                let certificate = material(&directory.join(format!("node{number}.der")), 65536)?;
                pin_bytes = pin_bytes
                    .checked_add(certificate.capacity())
                    .filter(|n| *n <= 1024 * 1024)
                    .ok_or("service pin budget")?;
                entry.insert(TlsPeer {
                    identity,
                    certificate,
                    server_name: format!("node{number}.voteboat.test"),
                });
            }
            entries.push(ServiceAccess {
                principal,
                peer: identity,
                scope,
                permissions,
            });
        }
        let policy = NativeServiceAccess::new(generation, 30_000, entries)
            .map_err(|(error, _)| format!("{error:?}"))?;
        Ok(Self {
            policy,
            tls: checked(tls.with_wire_version(1))?,
            peers,
        })
    }
}
pub struct ClientAccess {
    tls: NativeTlsConfig,
    local: LocalIdentity,
    peers: BTreeMap<u64, TlsPeer>,
    pub principal: PrincipalId,
}
impl ClientAccess {
    pub fn load(directory: &Path, principal: u64, targets: &[u64]) -> Result<Self, Failure> {
        if !(1..=super::setup::MAX_NODE).contains(&principal)
            || targets.is_empty()
            || targets.len() > 3
        {
            return Err("invalid service client selection".into());
        }
        let tls = checked(NativeTlsConfig::new(TlsCredentials {
            roots: vec![material(&directory.join("ca.der"), 65536)?],
            certificate_chain: vec![material(
                &directory.join(format!("node{principal}.der")),
                65536,
            )?],
            private_key: material(&directory.join(format!("node{principal}-key.der")), 65536)?,
        }))?;
        let peer = transport_identity(principal, true);
        let local = LocalIdentity {
            node: peer.node,
            store: StoreBinding {
                identity: peer.store,
                session: StoreSession::new(1).unwrap(),
            },
        };
        let peers = targets
            .iter()
            .map(|&number| {
                Ok((
                    number,
                    TlsPeer {
                        identity: transport_identity(number, false),
                        certificate: material(&directory.join(format!("node{number}.der")), 65536)?,
                        server_name: format!("node{number}.voteboat.test"),
                    },
                ))
            })
            .collect::<Result<_, Failure>>()?;
        Ok(Self {
            tls,
            local,
            peers,
            principal: PrincipalId::new(principal).unwrap(),
        })
    }
}
pub enum Channel {
    Plain(TcpStream),
    Selecting {
        stream: Option<TcpStream>,
        bytes: [u8; 32],
        len: usize,
    },
    Tls {
        session: Box<NativeTlsSession<TcpStream>>,
        context: Option<CredentialContext>,
        generation: CredentialGeneration,
    },
}
impl Channel {
    pub fn server(stream: TcpStream, authenticated: bool) -> Self {
        if authenticated {
            Self::Selecting {
                stream: Some(stream),
                bytes: [0; 32],
                len: 0,
            }
        } else {
            Self::Plain(stream)
        }
    }
    /// Caller has already sent the bounded public selector on this owned stream.
    pub fn client(
        stream: TcpStream,
        config: &ClientAccess,
        target: u64,
        now: MonoTime,
    ) -> Result<Self, Failure> {
        let session = checked(NativeTlsSession::client(
            stream,
            &config.tls,
            config.local,
            config.peers[&target].clone(),
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits::default(),
            now,
        ))?;
        Ok(Self::Tls {
            session: Box::new(session),
            context: None,
            generation: CredentialGeneration::new(1).unwrap(),
        })
    }
    pub fn poll(
        &mut self,
        access: Option<&Access>,
        local: LocalIdentity,
        generation: SecureSessionGeneration,
        now: MonoTime,
    ) -> Result<bool, Failure> {
        if let Self::Selecting { stream, bytes, len } = self {
            // Read only through newline: a coalesced ClientHello stays in TCP.
            for _ in 0..32 {
                if *len == bytes.len() {
                    return Err("invalid principal selector".into());
                }
                match stream.as_mut().unwrap().read(&mut bytes[*len..*len + 1]) {
                    Ok(0) => return Err("closed principal selector".into()),
                    Ok(_) => {
                        *len += 1;
                        if bytes[*len - 1] == b'\n' {
                            let principal =
                                std::str::from_utf8(&bytes[..*len - 1])?.parse::<u64>()?;
                            let principal =
                                PrincipalId::new(principal).ok_or("invalid principal selector")?;
                            let access = access.ok_or("missing access policy")?;
                            let peer = access
                                .peers
                                .get(&principal)
                                .ok_or("unknown principal selector")?
                                .clone();
                            let session = checked(NativeTlsSession::server(
                                stream.take().unwrap(),
                                &access.tls,
                                local,
                                peer,
                                generation,
                                SessionLimits::default(),
                                now,
                            ))?;
                            *self = Self::Tls {
                                session: Box::new(session),
                                context: None,
                                generation: access.policy.generation(),
                            };
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                    Err(error) => return Err(error.into()),
                }
            }
        }
        match self {
            Self::Plain(_) => Ok(true),
            Self::Selecting { .. } => Ok(false),
            Self::Tls {
                session,
                context,
                generation,
            } => {
                checked(session.poll(now, SessionPollBudget::default()))?;
                match session.state() {
                    SessionState::Handshaking => return Ok(false),
                    SessionState::Closing | SessionState::Closed | SessionState::Failed => {
                        return Err("closed service channel".into())
                    }
                    SessionState::Ready => (),
                }
                if context.is_none() {
                    *context = Some(CredentialContext {
                        generation: *generation,
                        authenticated_at: now,
                    });
                }
                Ok(true)
            }
        }
    }
    pub fn poll_client(&mut self, now: MonoTime) -> Result<bool, Failure> {
        match self {
            Self::Plain(_) => Ok(true),
            Self::Tls { session, .. } => {
                checked(session.poll(now, SessionPollBudget::default()))?;
                Ok(session.state() == SessionState::Ready)
            }
            Self::Selecting { .. } => Err("invalid client channel".into()),
        }
    }
    pub fn authorize(
        &self,
        access: Option<&Access>,
        scope: GroupIdentity,
        text: &str,
        now: MonoTime,
    ) -> Result<(), String> {
        let Some(access) = access else {
            return Ok(());
        };
        let action = match text.split_whitespace().next() {
            Some("status" | "metrics" | "events" | "maintenance" | "configuration-status") => {
                ServiceAction::Inspect
            }
            Some("read") => ServiceAction::Read,
            Some("add") => ServiceAction::Write,
            Some("checkpoint") => ServiceAction::Checkpoint,
            Some("quit") => ServiceAction::Shutdown,
            Some("configure" | "configure-record") => ServiceAction::Configure,
            _ => return Err("unknown authorized command".into()),
        };
        let Self::Tls {
            session,
            context: Some(context),
            ..
        } = self
        else {
            return Err("AUTHORIZATION".into());
        };
        authorize_session(
            &access.policy,
            &access.policy,
            &**session,
            *context,
            scope,
            action,
            now,
        )
        .map(|_| ())
        .map_err(|_| "AUTHORIZATION".into())
    }
    pub fn is_flushed(&self) -> bool {
        match self {
            Self::Plain(_) => true,
            Self::Tls { session, .. } => session.is_flushed(),
            Self::Selecting { .. } => false,
        }
    }
}
fn session_error(error: SessionError) -> io::Error {
    match error {
        SessionError::WouldBlock => io::ErrorKind::WouldBlock.into(),
        _ => io::Error::other(format!("{error:?}")),
    }
}
impl Read for Channel {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(bytes),
            Self::Tls { session, .. } => session.read_plaintext(bytes).map_err(session_error),
            Self::Selecting { .. } => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
}
impl Write for Channel {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(bytes),
            Self::Tls { session, .. } => session.write_plaintext(bytes).map_err(session_error),
            Self::Selecting { .. } => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
