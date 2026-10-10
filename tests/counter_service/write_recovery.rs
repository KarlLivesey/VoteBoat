// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const LEADERSHIP: &str = "UNKNOWN LeadershipChanged; retry the same operation ID and delta\n";
const READ_FAILED: &str =
    "UNKNOWN authenticated read failed; retry the same operation ID and delta\n";
const AUTHENTICATION_DEADLINE: &str =
    "UNKNOWN authentication deadline expired; retry the same operation ID and delta\n";
const DEADLINE: &str = "UNKNOWN reply deadline expired; retry the same operation ID and delta\n";
const BUSY: &str = "ERR not_proposed=Busy\n";
const INTERRUPTED: &str =
    "Error: \"request interrupted after connection; automatic routing stopped\"";
const UNSUCCESSFUL: &str =
    "Error: \"request unsuccessful; preserve the operation ID and payload when retrying a write\"";
type FailedAttempt = (String, String);
#[path = "write_recovery/authentication.rs"]
mod authentication;

fn repeatable(text: &str, error: &str) -> bool {
    match text {
        LEADERSHIP | BUSY => error.trim_end() == UNSUCCESSFUL,
        READ_FAILED | DEADLINE | AUTHENTICATION_DEADLINE => error.trim_end() == INTERRUPTED,
        _ => false,
    }
}

pub(super) fn invoke(c: &Cluster, args: &[&str], budget: Duration) -> String {
    retry(
        args,
        Instant::now() + budget,
        |words| {
            let output = c.target("auto", words);
            let text = String::from_utf8(output.stdout).unwrap();
            if output.status.success() {
                Ok(text)
            } else {
                Err((text, String::from_utf8(output.stderr).unwrap()))
            }
        },
        Instant::now,
        || std::thread::park_timeout(Duration::from_millis(10)),
    )
    .unwrap_or_else(|error| panic!("{error}\n{}", failure_diagnostics::snapshot(c, args)))
}

fn retry(
    args: &[&str],
    deadline: Instant,
    mut attempt: impl FnMut(&[&str]) -> Result<String, FailedAttempt>,
    mut now: impl FnMut() -> Instant,
    mut pause: impl FnMut(),
) -> Result<String, String> {
    loop {
        if now() >= deadline {
            return Err(format!("original write caller deadline expired: {args:?}"));
        }
        match attempt(args) {
            Ok(text) => return Ok(text),
            Err((text, error)) if repeatable(&text, &error) => pause(),
            Err((text, error)) => {
                return Err(format!("original write {args:?}: {text:?} {error}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    const ORIGINAL: &[&str] = &["group", "7", "3", "add", "42", "5"];
    const RECEIPT: &str = "OK outcome=Value(5) duplicate=true\n";

    fn sequence(first: &str, error: &str) {
        let start = Instant::now();
        let mut requests = Vec::new();
        let found = retry(
            ORIGINAL,
            start + Duration::from_secs(15),
            |args| {
                requests.push(args.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>());
                match requests.len() {
                    1 => Err((first.into(), error.into())),
                    2 => Ok(RECEIPT.into()),
                    _ => panic!("no invocation after a positive receipt"),
                }
            },
            || start,
            || (),
        );
        assert_eq!(found.unwrap(), RECEIPT);
        assert_eq!(requests, [ORIGINAL, ORIGINAL]);
    }

    #[test]
    fn explicit_busy_repeats_only_the_original_write_until_positive_receipt() {
        sequence(BUSY, UNSUCCESSFUL);
    }

    #[test]
    fn interrupted_reply_repeats_only_the_original_write_until_positive_receipt() {
        sequence(DEADLINE, INTERRUPTED);
    }

    #[test]
    fn existing_leadership_and_read_interruptions_retain_original_identity() {
        sequence(LEADERSHIP, UNSUCCESSFUL);
        sequence(READ_FAILED, INTERRUPTED);
    }

    #[test]
    fn authentication_timeout_repeats_only_the_original_scoped_write() {
        sequence(AUTHENTICATION_DEADLINE, INTERRUPTED);
    }

    #[test]
    fn unrelated_or_modified_failures_remain_terminal() {
        let start = Instant::now();
        for (text, error) in [
            ("ERR AUTHORIZATION\n", UNSUCCESSFUL),
            ("ERR not_proposed=Busy extra=unknown\n", UNSUCCESSFUL),
            ("UNKNOWN unspecified outcome\n", INTERRUPTED),
            (
                "UNKNOWN LeadershipChanged; changed identity\n",
                UNSUCCESSFUL,
            ),
            (BUSY, INTERRUPTED),
            (DEADLINE, UNSUCCESSFUL),
            (AUTHENTICATION_DEADLINE, UNSUCCESSFUL),
            (AUTHENTICATION_DEADLINE, "Error: invalid credentials"),
            ("UNKNOWN authentication failed; retry the same operation ID and delta\n", INTERRUPTED),
            ("UNKNOWN authentication deadline expired; retry the same operation ID and delta\nextra\n", INTERRUPTED),
            (LEADERSHIP, "Error: invalid credentials"),
            (READ_FAILED, "Error: invalid credentials"),
            ("OK partial\n", INTERRUPTED),
            ("", INTERRUPTED),
        ] {
            let mut calls = 0;
            let failed = retry(
                ORIGINAL,
                start + Duration::from_secs(15),
                |args| {
                    assert_eq!(args, ORIGINAL);
                    calls += 1;
                    Err((text.into(), error.into()))
                },
                || start,
                || panic!("terminal error must not wait/repeat"),
            );
            assert!(failed.is_err());
            assert_eq!(calls, 1);
        }
    }

    #[test]
    fn caller_deadline_never_renews_or_becomes_a_success() {
        let start = Instant::now();
        for reason in [DEADLINE, AUTHENTICATION_DEADLINE] {
            let now = Cell::new(start);
            let mut calls = 0;
            let result = retry(
                ORIGINAL,
                start + Duration::from_millis(30),
                |args| {
                    assert_eq!(args, ORIGINAL);
                    calls += 1;
                    Err((reason.into(), INTERRUPTED.into()))
                },
                || now.get(),
                || now.set(now.get() + Duration::from_millis(10)),
            );
            assert!(result.unwrap_err().contains("caller deadline expired"));
            assert_eq!(calls, 3);
        }
        let expired = retry(
            ORIGINAL,
            start,
            |_| panic!("expired caller must not start another invocation"),
            || start,
            || panic!("expired caller must not pause"),
        );
        assert!(expired.is_err());
    }
}
