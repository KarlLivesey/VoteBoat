// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[path = "initialization/recovery.rs"]
mod recovery;

type Attempt = Result<(usize, String), (String, String)>;

fn applied(
    operation: &str,
    outcome: &str,
    mut request: impl FnMut() -> Attempt,
) -> Result<(usize, String), String> {
    let prefix = format!("OK operation={operation} outcome={outcome} duplicate=");
    for _ in 0..4 {
        match request() {
            Ok((leader, text))
                if text
                    .trim_end()
                    .strip_prefix(&prefix)
                    .is_some_and(|flag| matches!(flag, "true" | "false")) =>
            {
                return Ok((leader, text))
            }
            Ok((_, text)) => return Err(format!("missing original applied receipt: {text}")),
            Err((text, _))
                if matches!(
                    text.as_str(),
                    "UNKNOWN Unknown(LeadershipChanged); retry original plan operation\n"
                        | "UNKNOWN NotProposed(NotLeader); retry original plan operation\n"
                        | "ERR Consensus(NotLeader)\n"
                ) =>
            {
                std::thread::park_timeout(Duration::from_millis(10))
            }
            Err((text, error)) => return Err(format!("{text} {error}")),
        }
    }
    Err("original metadata operation unresolved after four attempts".into())
}

pub(super) fn command(c: &mut Cluster, args: &[&str]) -> (usize, String) {
    let (operation, outcome) = match args {
        ["initialize"] => ("100", "Initialized"),
        ["publish", "101"] => ("101", "Published(RouteGeneration(1))"),
        _ => panic!("not an original fixture operation: {args:?}"),
    };
    applied(operation, outcome, || {
        let leader = c.leader();
        let output = c.request(leader, 3, args);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() {
            Ok((leader, text))
        } else {
            Err((text, String::from_utf8(output.stderr).unwrap()))
        }
    })
    .unwrap_or_else(|error| panic!("{args:?}: {error}"))
}

#[test]
fn setup_retries_exact_leadership_loss_and_requires_original_applied_receipt() {
    let mut calls = 0;
    let result = applied("100", "Initialized", || {
        calls += 1;
        match calls {
            1 => Err((
                "UNKNOWN Unknown(LeadershipChanged); retry original plan operation\n".into(),
                "unknown".into(),
            )),
            2 => Err(("ERR Consensus(NotLeader)\n".into(), "not leader".into())),
            3 => Ok((
                2,
                "OK operation=100 outcome=Initialized duplicate=true\n".into(),
            )),
            _ => panic!("unexpected replay"),
        }
    })
    .unwrap();
    assert_eq!(calls, 3);
    assert_eq!(result.0, 2);
    assert!(result.1.contains("duplicate=true"));
}

#[test]
fn setup_is_bounded_and_rejects_unrelated_errors_or_changed_receipts() {
    let mut calls = 0;
    assert!(applied("100", "Initialized", || {
        calls += 1;
        Err((
            "UNKNOWN Unknown(LeadershipChanged); retry original plan operation\n".into(),
            "unknown".into(),
        ))
    })
    .is_err());
    assert_eq!(calls, 4);
    for error in [
        "ERR AUTHORIZATION\n",
        "UNKNOWN storage failure\n",
        "UNKNOWN reply deadline expired\n",
        "UNKNOWN Unknown(LeadershipChanged); retry original plan operation extra\n",
    ] {
        let mut calls = 0;
        assert!(applied("100", "Initialized", || {
            calls += 1;
            Err((error.into(), "refused".into()))
        })
        .is_err());
        assert_eq!(calls, 1);
    }
    for text in [
        "OK next=Ready\n",
        "OK operation=101 outcome=Initialized duplicate=false\n",
        "OK operation=100 outcome=Published(RouteGeneration(1)) duplicate=false\n",
        "OK operation=100 outcome=Initialized duplicate=unknown\n",
    ] {
        assert!(applied("100", "Initialized", || Ok((1, text.into()))).is_err());
    }
}
