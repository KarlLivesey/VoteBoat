// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const LEADERSHIP_CHANGED: &str = "UNKNOWN Unknown(LeadershipChanged)";
type FailedAttempt = (String, String);

fn receipt(mut attempt: impl FnMut() -> Result<String, FailedAttempt>) -> Result<String, String> {
    for _ in 0..4 {
        match attempt() {
            Ok(text) if text.starts_with("OK receipt=") => return Ok(text),
            Ok(text) => return Err(format!("missing applied initialization receipt: {text}")),
            Err((text, _)) if text.trim_end() == LEADERSHIP_CHANGED => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err((text, error)) => return Err(format!("{text} {error}")),
        }
    }
    Err("initialization remains unknown after four attempts; preserve the original profile".into())
}

pub(super) fn command(rig: &Cluster, group: u128, verb: &str) -> String {
    assert!(matches!(verb, "initialize" | "grant"));
    let result = receipt(|| {
        let out = rig.request(0, 3, group, &[verb]);
        let text = String::from_utf8(out.stdout).unwrap();
        if out.status.success() {
            Ok(text)
        } else {
            Err((text, String::from_utf8(out.stderr).unwrap()))
        }
    });
    result.unwrap_or_else(|error| panic!("{group} {verb}: {error}"))
}

#[test]
fn setup_repeats_only_explicit_leadership_loss_and_requires_applied_receipt() {
    let mut attempts = 0;
    let result = receipt(|| {
        attempts += 1;
        match attempts {
            1 | 2 => Err((
                format!("{LEADERSHIP_CHANGED}\n"),
                "original operation remains unknown".into(),
            )),
            3 => Ok("OK receipt=original-operation\n".into()),
            _ => panic!("unexpected extra request"),
        }
    });
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(attempts, 3);
}

#[test]
fn setup_attempts_are_bounded_and_other_failures_remain_terminal() {
    let mut attempts = 0;
    assert!(receipt(|| {
        attempts += 1;
        Err((LEADERSHIP_CHANGED.into(), "unknown".into()))
    })
    .is_err());
    assert_eq!(attempts, 4);
    for failed in [
        "UNKNOWN storage failure",
        "ERR AUTHORIZATION",
        "UNKNOWN reply deadline expired",
        "UNKNOWN Unknown(LeadershipChanged) extra",
    ] {
        let mut attempts = 0;
        assert!(receipt(|| {
            attempts += 1;
            Err((failed.into(), "refused".into()))
        })
        .is_err());
        assert_eq!(attempts, 1);
    }
    assert!(receipt(|| Ok("OK next=RecordIntent".into())).is_err());
}

fn history(quic: bool) {
    let mut rig = Cluster::new(quic);
    let profile = fs::read(rig.root.join("profile")).unwrap();
    for (group, verb) in [(1, "initialize"), (1, "grant"), (20, "initialize")] {
        cuts::support::interrupt_command(&mut rig, group, &[verb.into()]);
        command(&rig, group, verb);
    }
    initialize(&rig);
    if quic {
        cuts::support::checkpoint(&rig);
    }
    rig.crash();
    rig.start("recover");
    // Replaying fixed bootstrap/grant IDs must retain the already-written data.
    for (group, verb) in [(1, "initialize"), (1, "grant"), (20, "initialize")] {
        command(&rig, group, verb);
    }
    assert_eq!(fs::read(rig.root.join("profile")).unwrap(), profile);
    assert!(rig.ok(20, &["read", "1"]).contains("value=7"));
    assert!(rig.ok(20, &["read", "200"]).contains("value=11"));
    assert!(rig
        .ok(20, &["add", "1", "1", "7"])
        .contains("duplicate: true"));
    assert!(rig.operate("start").contains("OK complete"));
    finish(rig);
}

#[test]
fn tcp_initialization_recovers_lost_bootstrap_and_grant_receipts() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn quic_initialization_recovers_lost_receipts_and_checkpointed_data() {
    history(true);
}
