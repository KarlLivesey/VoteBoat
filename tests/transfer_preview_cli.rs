// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
#[path = "transfer_source/fixtures.rs"]
mod fixture;
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
use voteboat::{routing::ResponsibilityManifest, transfer::TransferIntent};
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-directory");
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Profile {
    path: PathBuf,
}
impl Profile {
    fn new(bytes: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "voteboat-preview-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, bytes).unwrap();
        Self { path }
    }
    fn run(&self) -> std::process::Output {
        Command::new(BIN)
            .arg("split-preview")
            .arg(&self.path)
            .output()
            .unwrap()
    }
}
impl Drop for Profile {
    fn drop(&mut self) {
        fs::remove_file(&self.path).unwrap();
    }
}
fn profile(intent: TransferIntent) -> String {
    let hex = intent
        .encode(32768)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    format!("voteboat-transfer-preview-v1 20000\nintent {hex}\nsource 20 1 16 2048 10000\ntarget 21 1 8 16 2048 5000 m:2 v:11 v:13\nreplica 21 1 11 501 9 1 voter\nreplica 21 1 13 502 9 2 voter\nreplica 21 1 14 503 9 1 learner\ntarget 22 1 9 16 2048 5000 w:2 3 v:11 2 v:13\nreplica 22 1 11 601 7 1 voter\nreplica 22 1 13 602 7 2 voter\n")
}
#[test]
fn native_offline_preview_preserves_exact_placement_and_declares_limits() {
    let text = profile(fixture::intent());
    let input = Profile::new(text.as_bytes());
    let out = input.run();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = String::from_utf8(out.stdout).unwrap();
    for expected in [
        "preview_only=true source_view=configured_bounds data_import_checked=false",
        "responsibility=10:1 epoch=1->2 generation=1 payload_upper_bound=9232",
        "move source=20:1 target=21:1 scope=0..128 payload_upper_bound=4616",
        "move source=20:1 target=22:1 scope=128..256 payload_upper_bound=4616",
        "target=21:1 configuration=8 payload_upper_bound=4616",
        "node=14 store=503:9 domain=1 role=learner",
        "duration_upper_bound=unknown",
        "wal_bytes=unknown",
        "cross_group_atomicity=false",
    ] {
        assert!(out.contains(expected), "missing {expected}: {out}");
    }
    assert_eq!(fs::read(&input.path).unwrap(), text.as_bytes());
}
#[test]
fn malformed_profiles_refuse_without_printing_success() {
    let valid = profile(fixture::intent());
    for text in [
        valid.replace("source 20 1 16 2048 10000", "source 20 1 16 2048 1"),
        valid.replace(
            "replica 21 1 13 502 9 2 voter",
            "replica 21 1 13 501 9 2 voter",
        ),
        valid.replace(
            "replica 21 1 13 502 9 2 voter",
            "replica 21 1 11 502 9 2 voter",
        ),
        valid.replace("m:2 v:11 v:13", "m:2 v:11 v:15"),
        valid.replace("source 20 1", "source 20 2"),
        valid.replace("voteboat-transfer-preview-v1", "unknown"),
        format!("{valid}source 20 1 16 2048 10000\n"),
        "voteboat-transfer-preview-v1 20000\nintent ff\n".to_owned(),
    ] {
        let input = Profile::new(text.as_bytes());
        let out = input.run();
        assert!(!out.status.success(), "accepted invalid profile");
        assert!(out.stdout.is_empty());
        assert_eq!(fs::read(&input.path).unwrap(), text.as_bytes());
    }
}
#[test]
fn unsupported_application_and_oversized_profile_are_rejected() {
    let original = fixture::intent();
    let mut before = original.before().clone().into_input();
    let mut after = original.after().clone().into_input();
    before.application.version = 2;
    after.application.version = 2;
    let other = TransferIntent::new(
        ResponsibilityManifest::new(before).unwrap(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    for bytes in [profile(other).into_bytes(), vec![b'x'; 512 * 1024 + 1]] {
        let input = Profile::new(&bytes);
        let out = input.run();
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
    }
}
