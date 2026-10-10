// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const ABSENT: &str = r#"Error: "UNKNOWN initial drain admission: source drain observation unsuccessful: \"ERR no matching durable local drain; retry original drain-node if outcome was unknown\\n\"; rerun the same sequence and operation""#;
type FailedAttempt = (String, String);

fn confirm(
    sequence: u64,
    operation: u128,
    mut attempt: impl FnMut(u64, u128) -> Result<String, FailedAttempt>,
) -> Result<String, String> {
    for _ in 0..4 {
        match attempt(sequence, operation) {
            Ok(text) => return completion(&text, sequence, operation),
            Err((text, error)) if text.is_empty() && error.trim_end() == ABSENT => {
                std::thread::park_timeout(Duration::from_millis(10));
            }
            Err((text, error)) => return Err(format!("{text} {error}")),
        }
    }
    Err(format!(
        "drain {sequence}/{operation} remains unknown after bounded attempts"
    ))
}

fn completion(text: &str, sequence: u64, operation: u128) -> Result<String, String> {
    if text.lines().count() != 1 || !text.starts_with("OK ") {
        return Err(format!("invalid drain completion: {text}"));
    }
    for (key, value) in [
        ("sequence", sequence.to_string()),
        ("operation", operation.to_string()),
        ("shutdown_requested", "true".into()),
        ("retained_replica", "true".into()),
        ("evidence", "source_ready_and_stop_accepted".into()),
    ] {
        let prefix = format!("{key}=");
        let values = text
            .split_whitespace()
            .filter_map(|field| field.strip_prefix(&prefix))
            .collect::<Vec<_>>();
        if values != [value] {
            return Err(format!("invalid original {key}: {text}"));
        }
    }
    Ok(text.to_owned())
}

pub(crate) fn confirmed(c: &Cluster, source: usize, operation: u128) -> String {
    // Keep the exact source, arguments, endpoint file and principal across runs.
    let mut invocation = command(c, source, &operation.to_string(), 3);
    confirm(1, operation, |_, _| {
        let output = run(&mut invocation);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() {
            Ok(text)
        } else {
            Err((text, String::from_utf8(output.stderr).unwrap()))
        }
    })
    .unwrap_or_else(|error| {
        panic!("drain source={source} sequence=1 operation={operation}: {error}")
    })
}

fn completed() -> String {
    "OK sequence=1 operation=19701 shutdown_requested=true evidence=source_ready_and_stop_accepted retained_replica=true\n".into()
}

#[test]
fn caller_repeats_original_identity_only_after_explicit_absent_record() {
    let mut identities = Vec::new();
    let result = confirm(1, 19701, |sequence, operation| {
        identities.push((sequence, operation));
        match identities.len() {
            1 => Err((String::new(), format!("{ABSENT}\n"))),
            2 => Ok(completed()),
            _ => panic!("unexpected further invocation"),
        }
    });
    assert_eq!(result.unwrap(), completed());
    assert_eq!(identities, [(1, 19701); 2]);
}

#[test]
fn caller_bounds_unknown_invocations_and_preserves_terminal_refusals() {
    let mut calls = 0;
    assert!(confirm(1, 19701, |sequence, operation| {
        calls += 1;
        assert_eq!((sequence, operation), (1, 19701));
        Err((String::new(), ABSENT.into()))
    })
    .is_err());
    assert_eq!(calls, 4);
    for (text, error) in [
        ("", "Error: source replied with a different drain identity"),
        ("", "Error: invalid source plan"),
        ("", "Error: authentication failed"),
        (
            "",
            "Error: UNKNOWN source unavailable; preserve original drain identity",
        ),
        ("UNKNOWN original drain outcome\n", ABSENT),
        ("OK partial\n", ABSENT),
    ] {
        let mut calls = 0;
        assert!(confirm(1, 19701, |_, _| {
            calls += 1;
            Err((text.into(), error.into()))
        })
        .is_err());
        assert_eq!(calls, 1, "{text} {error}");
    }
}

#[test]
fn caller_refuses_wrong_duplicate_and_unconfirmed_success_fields() {
    for text in [
        completed().replace("sequence=1", "sequence=2"),
        completed().replace("operation=19701", "operation=19702"),
        completed().replace("shutdown_requested=true", "shutdown_requested=false"),
        completed().replace("retained_replica=true", "retained_replica=false"),
        completed().replace(
            "evidence=source_ready_and_stop_accepted",
            "evidence=local_volatile",
        ),
        completed().replace("sequence=1", "sequence=1 sequence=2"),
        format!("{}ERR later refusal\n", completed()),
    ] {
        let mut calls = 0;
        assert!(confirm(1, 19701, |_, _| {
            calls += 1;
            Ok(text.clone())
        })
        .is_err());
        assert_eq!(calls, 1);
    }
}
