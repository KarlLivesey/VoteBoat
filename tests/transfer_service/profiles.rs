// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn invalid_profiles_fail_before_opening_replica_files() {
    let root =
        std::env::temp_dir().join(format!("voteboat-transfer-profiles-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let hex = source_fixture::intent()
        .encode(32768)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let valid=format!("voteboat-transfer-profile-v1 200 201\nintent {hex}\nmetadata 1 1 1000 1001\nsource 20 1 100 0\ntarget 21 1 200 0\ntarget 22 1 200 0\n");
    let cases = [
        (valid.replace("200 201", "200 200"), "transfer plan"),
        (valid.replace("target 22 1 200 0\n", ""), "every group"),
        (
            valid.replace("target 22 1", "target 22 2"),
            "wrong group binding",
        ),
        (
            valid.replace("source 20 1 100 0", "source 20 1 200 0"),
            "bootstrap/grant",
        ),
        (valid.replace("1000 1001", "1000 200"), "bootstrap/grant"),
        (format!("{valid}target 22 1 200 0\n"), "duplicate group"),
    ];
    for (text, expected) in cases {
        let profile = root.join("profile");
        fs::write(&profile, text).unwrap();
        let replica = root.join("must-not-exist");
        let out = run(Command::new(BIN)
            .args(["serve", "create"])
            .arg(&replica)
            .args(["1", "14000", "absent-tls"])
            .arg(&profile)
            .args(["1", "absent-access", "tcp"]));
        assert!(!out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!replica.exists());
    }
    fs::remove_dir_all(root).unwrap();
}
