// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit peer material selection and host-side publication of durable reloads.
use super::{
    credential_worker::{self as worker, Source as _},
    setup::{checked, Failure},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use voteboat::{
    application::{
        ApplicationReceipt, BoundedReadableStateMachine, CheckpointStateMachine, ProposalAdmission,
    },
    authorization::CredentialGeneration,
    identity::*,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::NativeNode,
        peer_credentials::NativePeerMaterial,
        startup::*,
        tls::*,
    },
    secure::PeerIdentity,
};
const RECORD: &str = "PEER-CREDENTIAL-RELOAD";
#[cfg(test)]
#[path = "peer_credentials_tests.rs"]
mod tests;
#[derive(Clone)]
struct Source {
    path: PathBuf,
    node: NodeId,
    wire: u16,
    peers: BTreeMap<NodeId, (StoreIdentity, String)>,
}
impl worker::Source for Source {
    type Material = NativePeerMaterial;
    fn load(&self) -> Result<worker::Loaded<NativePeerMaterial>, Failure> {
        let bytes = super::service_setup::material(&self.path, 4096)?;
        let text = std::str::from_utf8(&bytes)?;
        let words = text.split_whitespace().collect::<Vec<_>>();
        let ["voteboat-peer-credentials-v1", generation, "tls", directory] = words.as_slice()
        else {
            return Err(
                "expected voteboat-peer-credentials-v1 GENERATION followed by tls DIRECTORY".into(),
            );
        };
        let generation =
            CredentialGeneration::new(generation.parse()?).ok_or("invalid peer generation")?;
        let directory = self.path.parent().unwrap_or(Path::new(".")).join(directory);
        let read = |name: String| super::service_setup::material(&directory.join(name), 65536);
        let tls = checked(NativeTlsConfig::new(TlsCredentials {
            roots: vec![read("ca.der".into())?],
            certificate_chain: vec![read(format!("node{}.der", self.node.get()))?],
            private_key: read(format!("node{}-key.der", self.node.get()))?,
        }))?;
        let mut bytes = 0usize;
        let peers = self
            .peers
            .iter()
            .map(|(node, (store, name))| {
                let certificate = read(format!("node{}.der", node.get()))?;
                bytes = bytes
                    .checked_add(certificate.capacity())
                    .and_then(|n| n.checked_add(name.capacity()))
                    .filter(|n| *n <= 1024 * 1024)
                    .ok_or("peer credential budget")?;
                Ok((
                    *node,
                    TlsPeer {
                        identity: PeerIdentity {
                            node: *node,
                            store: *store,
                        },
                        certificate,
                        server_name: name.clone(),
                    },
                ))
            })
            .collect::<Result<_, Failure>>()?;
        let material = NativePeerMaterial {
            tls: checked(tls.with_wire_version(self.wire))?,
            peers,
        };
        Ok(worker::Loaded {
            generation,
            digest: material.digest(),
            material,
        })
    }
}
pub struct Peers {
    reload: worker::Reload<Source>,
    generation: CredentialGeneration,
}
pub fn command(peers: &mut Option<Peers>, text: &str) -> Option<Result<String, String>> {
    let words = text.split_whitespace().collect::<Vec<_>>();
    if !matches!(
        words.first(),
        Some(&"reload-peers" | &"peer-credential-status")
    ) {
        return None;
    }
    Some(
        peers
            .as_mut()
            .ok_or_else(|| "peer credential rotation disabled".to_string())
            .and_then(|p| p.command(&words)),
    )
}
impl Peers {
    pub fn validate_profile(
        root: &Path,
        path: Option<&Path>,
        authenticated: bool,
    ) -> Result<(), Failure> {
        if path.is_some() && !authenticated {
            return Err("--peer-credentials requires --service-access".into());
        }
        if path.is_none() && root.join(RECORD).try_exists()? {
            return Err("existing peer credential journal requires --peer-credentials".into());
        }
        Ok(())
    }
    pub fn load(config: &mut NativeMemberStartup, path: &Path, wire: u16) -> Result<Self, Failure> {
        let source = Source {
            path: path.to_owned(),
            node: config.startup.node,
            wire,
            peers: config
                .startup
                .peers
                .iter()
                .map(|(node, peer)| {
                    config
                        .provisioned_stores
                        .get(node)
                        .copied()
                        .map(|store| (*node, (store, peer.server_name.clone())))
                        .ok_or("peer missing provisioned store")
                })
                .collect::<Result<_, _>>()?,
        };
        let loaded = source.load()?;
        let reload = worker::Reload::new(
            worker::Paths {
                source,
                journal: config.startup.directory.join(RECORD),
                owner: PeerIdentity {
                    node: config.startup.node,
                    store: config.startup.store,
                },
            },
            loaded.generation,
            loaded.digest,
        )?;
        if reload
            .latest()
            .is_some_and(|r| r.request.replacement != loaded.generation)
        {
            return Err("peer credential generation differs from durable record".into());
        }
        config.startup.tls = loaded.material.tls;
        for (node, pin) in loaded.material.peers {
            config
                .startup
                .peers
                .get_mut(&node)
                .ok_or("peer identity changed")?
                .certificate = pin.certificate;
        }
        Ok(Self {
            reload,
            generation: loaded.generation,
        })
    }
    pub fn startup(&self, protocol: NativePeerProtocol) -> NativePeerRotationStartup {
        NativePeerRotationStartup {
            protocol,
            generation: self.generation,
            latest: self.reload.latest(),
        }
    }
    pub fn command(&mut self, words: &[&str]) -> Result<String, String> {
        match words {
            ["peer-credential-status", sequence] => Ok(self.reload.status(
                sequence.parse().map_err(|_| "invalid reload sequence")?,
                self.generation,
            )),
            ["reload-peers", fields @ ..] => self
                .reload
                .submit(worker::request(fields)?, self.generation),
            _ => Err(
                "expected peer-credential-status REQUEST or reload-peers REQUEST EXPECTED NEXT"
                    .into(),
            ),
        }
    }
    pub fn poll<A>(&mut self, service: &mut NativeNode<A, NativeServiceConnector>) -> bool
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.reload.poll(Some(self.generation), |p| {
            service
                .replace_peer_credentials(
                    p.record.request.expected,
                    p.record.request.replacement,
                    p.material,
                )
                .map_err(|(e, _)| format!("peer credential publication: {e:?}"))?;
            self.generation = p.record.request.replacement;
            Ok(())
        })
    }
    pub fn finish(&mut self) -> Result<(), Failure> {
        self.reload.finish_for_restart()
    }
}
