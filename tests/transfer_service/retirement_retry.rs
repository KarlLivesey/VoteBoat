// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const UNKNOWN: &str = "UNKNOWN Unknown(LeadershipChanged)";
const REPEAT: &str =
    "Error: \"retirement outcome unknown; repeat the same profile, source and release\"";
type FailedAttempt = (String, String);

fn confirm(
    source: &str,
    release: &str,
    mut attempt: impl FnMut(&str, &str) -> Result<String, FailedAttempt>,
) -> Result<String, String> {
    for _ in 0..4 {
        match attempt(source, release) {
            Ok(text) => return record(&text, source, release),
            Err((text, error)) if text.trim_end() == UNKNOWN && error.trim_end() == REPEAT => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err((text, error)) => return Err(format!("{text} {error}")),
        }
    }
    Err(format!(
        "retirement {source}/{release} remains unknown after four attempts"
    ))
}

fn scalar(text: &str, name: &str) -> Result<u64, String> {
    text.split_whitespace()
        .find_map(|field| field.strip_prefix(name))
        .ok_or_else(|| format!("missing {name}: {text}"))?
        .parse()
        .map_err(|_| format!("invalid {name}: {text}"))
}

fn record(text: &str, source: &str, release: &str) -> Result<String, String> {
    let line = text
        .lines()
        .find(|line| line.starts_with("OK retirement=retired "))
        .ok_or_else(|| format!("missing confirmed retirement: {text}"))?;
    for expected in [
        format!("source={source}"),
        "incarnation=1".into(),
        "operation=200".into(),
        format!("release={release}"),
        "evidence=quorum".into(),
    ] {
        if !line.split_whitespace().any(|field| field == expected) {
            return Err(format!("missing original {expected}: {text}"));
        }
    }
    let fence = scalar(line, "fence=")?;
    let index = scalar(line, "index=")?;
    let read = scalar(line, "read_index=")?;
    if fence == 0 || index <= fence || read < index {
        return Err(format!("invalid retirement prefix: {text}"));
    }
    Ok(line
        .split_whitespace()
        .filter(|field| !field.starts_with("read_index="))
        .collect::<Vec<_>>()
        .join(" "))
}

pub(super) fn retired(rig: &Cluster, source: &str, release: &str) -> String {
    confirm(source, release, |source, release| {
        let out = run(rig.client(0, 3, "client").args(["retire", source, release]));
        let text = String::from_utf8(out.stdout).unwrap();
        if out.status.success() {
            Ok(text)
        } else {
            Err((text, String::from_utf8(out.stderr).unwrap()))
        }
    })
    .unwrap_or_else(|error| panic!("retire {source}/{release}: {error}"))
}

fn status() -> String {
    "OK retirement=retired source=20 incarnation=1 operation=200 fence=4 release=700 index=8 read_index=10 evidence=quorum\n".into()
}

#[test]
fn retirement_confirmation_repeats_original_identity_after_explicit_leadership_loss() {
    let mut arguments = Vec::new();
    let result = confirm("20", "700", |source, release| {
        arguments.push((source.to_owned(), release.to_owned()));
        match arguments.len() {
            1 => Err((format!("{UNKNOWN}\n"), format!("{REPEAT}\n"))),
            2 => Ok(status()),
            _ => panic!("unexpected further retirement"),
        }
    });
    assert_eq!(result.unwrap(), record(&status(), "20", "700").unwrap());
    assert_eq!(arguments, vec![("20".into(), "700".into()); 2]);
}

#[test]
fn retirement_confirmation_bounds_attempts_and_preserves_terminal_failures() {
    let mut attempts = 0;
    assert!(confirm("20", "700", |source, release| {
        attempts += 1;
        assert_eq!((source, release), ("20", "700"));
        Err((UNKNOWN.into(), REPEAT.into()))
    })
    .is_err());
    assert_eq!(attempts, 4);
    for (reply, error) in [
        ("UNKNOWN storage failure", REPEAT),
        ("ERR AUTHORIZATION", REPEAT),
        ("UNKNOWN reply deadline expired", REPEAT),
        ("UNKNOWN Unknown(LeadershipChanged) extra", REPEAT),
        (UNKNOWN, "Error: conflicting retirement identity or release"),
        (UNKNOWN, "Error: storage failure"),
    ] {
        let mut attempts = 0;
        let result = confirm("20", "700", |_, _| {
            attempts += 1;
            Err((reply.into(), error.into()))
        });
        assert!(result.is_err(), "{reply} {error}");
        assert_eq!(attempts, 1);
    }
}

#[test]
fn retirement_confirmation_requires_original_quorum_status_and_stable_record() {
    let original = status();
    let normalized = record(&original, "20", "700").unwrap();
    assert_eq!(
        record(
            &original.replace("read_index=10", "read_index=11"),
            "20",
            "700"
        )
        .unwrap(),
        normalized
    );
    for (before, after) in [
        ("source=20", "source=200"),
        ("incarnation=1", "incarnation=2"),
        ("operation=200", "operation=201"),
        ("release=700", "release=701"),
        ("evidence=quorum", "evidence=local"),
        ("retirement=retired", "retirement=live"),
        ("fence=4", "fence=0"),
        ("index=8 read_index=10", "index=4 read_index=10"),
        ("read_index=10", "read_index=7"),
        ("index=8 read_index=10", "index=invalid read_index=10"),
        ("read_index=10", ""),
    ] {
        let changed = original.replace(before, after);
        let mut attempts = 0;
        assert!(confirm("20", "700", |_, _| {
            attempts += 1;
            Ok(changed.clone())
        })
        .is_err());
        assert_eq!(attempts, 1);
    }
    assert_ne!(
        record(&original.replace("index=8 ", "index=9 "), "20", "700").unwrap(),
        normalized
    );
    assert!(record("OK receipt=Applied", "20", "700").is_err());
}
