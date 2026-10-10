// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
use std::process::Command;

#[test]
fn invalid_service_profile_is_refused_before_files_or_listener_ownership() {
    let root =
        std::env::temp_dir().join(format!("voteboat-profile-refusal-{}", std::process::id()));
    assert!(!root.exists());
    for (binary, suffix) in [
        (env!("CARGO_BIN_EXE_voteboat-counter"), vec![]),
        (
            env!("CARGO_BIN_EXE_voteboat-directory"),
            vec!["missing-plan", "missing-access"],
        ),
        (
            env!("CARGO_BIN_EXE_voteboat-transfer"),
            vec!["missing-profile", "source", "missing-access", "tcp"],
        ),
    ] {
        for (options, expected) in [
            (vec!["--timing-profile", "auto"], "timing profile"),
            (
                vec!["--timing-profile", "edge", "--timing-profile", "edge"],
                "duplicate",
            ),
        ] {
            let output = Command::new(binary)
                .args(["serve", "create"])
                .arg(&root)
                .args(["1", "19000", "missing-tls"])
                .args(&suffix)
                .args(options)
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(output.stdout.is_empty(), "{output:?}");
            assert!(String::from_utf8(output.stderr).unwrap().contains(expected));
            assert!(!root.exists());
        }
    }
}
