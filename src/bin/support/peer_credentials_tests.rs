// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{fs, sync::mpsc, thread};
use voteboat::credential_reload::*;
struct Fixture {
    root: PathBuf,
    manifest: PathBuf,
    config: NativeMemberStartup,
    peers: Peers,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-peer-worker-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let manifest = root.join("manifest");
        let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        fs::write(
            &manifest,
            format!("voteboat-peer-credentials-v1 1\ntls {}\n", tls.display()),
        )
        .unwrap();
        let mut config = super::super::service_setup::configuration(
            &root,
            1,
            19000,
            &tls,
            true,
            super::super::service_setup::PeerInput::Legacy(None),
        )
        .unwrap();
        let peers = Peers::load(&mut config, &manifest, 1).unwrap();
        Self {
            root,
            manifest,
            config,
            peers,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.peers.finish().unwrap();
        fs::remove_dir_all(&self.root).unwrap();
    }
}
#[test]
fn shutdown_joins_unpublished_preparation_and_startup_recovers_its_original_record() {
    let mut f = Fixture::new();
    let replacement =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls");
    fs::write(
        &f.manifest,
        format!(
            "voteboat-peer-credentials-v1 2\ntls {}\n",
            replacement.display()
        ),
    )
    .unwrap();
    let request = CredentialReloadRequest {
        sequence: 71,
        expected: f.peers.generation,
        replacement: CredentialGeneration::new(2).unwrap(),
    };
    let paths = f.peers.reload.paths.clone();
    let (release, held) = mpsc::channel();
    f.peers.reload.pending = Some(worker::Pending {
        request,
        handle: thread::spawn(move || {
            held.recv().unwrap();
            worker::prepare(paths, request)
        }),
    });
    assert!(worker::journal(&f.peers.reload.paths)
        .unwrap()
        .latest()
        .unwrap()
        .is_none());
    assert!(f
        .peers
        .command(&["reload-peers", "71", "1", "2"])
        .unwrap()
        .contains("pending=true"));
    release.send(()).unwrap();
    f.peers.finish().unwrap();
    assert_eq!(f.peers.generation.get(), 1); // no publication into a stopped node
    let record = worker::journal(&f.peers.reload.paths)
        .unwrap()
        .latest()
        .unwrap()
        .unwrap();
    assert_eq!(record.request, request);
    let mut recovered = Peers::load(&mut f.config, &f.manifest, 1).unwrap();
    assert_eq!(recovered.generation.get(), 2);
    assert_eq!(
        recovered.startup(NativePeerProtocol::TcpTls).latest,
        Some(record)
    );
    assert!(recovered
        .command(&["reload-peers", "71", "1", "2"])
        .unwrap()
        .contains("already_recorded=true"));
}
#[test]
fn malformed_or_unauthenticated_selection_cannot_create_a_peer_record() {
    let f = Fixture::new();
    assert!(Peers::validate_profile(&f.root, Some(&f.manifest), false).is_err());
    for text in [
        "",
        "voteboat-peer-credentials-v1 0\ntls none",
        "voteboat-peer-credentials-v1 1\ntls none trailing",
    ] {
        fs::write(&f.manifest, text).unwrap();
        assert!(f.peers.reload.paths.source.load().is_err());
        assert!(!f.root.join(RECORD).exists());
    }
}
