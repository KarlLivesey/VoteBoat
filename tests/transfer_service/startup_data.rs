// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{bucket_counter::*, identity::OperationId, retirement::*, routed::*};

pub(super) fn add(rig: &Cluster, operation: &str, key: &str, delta: &str, value: i64) -> String {
    invoke(
        &["add", operation, key, delta],
        value,
        rig.retirement,
        |words| initialization::response(rig.request(0, rig.admin, 20, words)),
    )
    .unwrap_or_else(|error| panic!("original source add {operation}/{key}/{delta}: {error}"))
}

pub(super) fn invoke(
    words: &[&str],
    value: i64,
    wrapped: bool,
    mut attempt: impl FnMut(&[&str]) -> Result<String, (String, String)>,
) -> Result<String, String> {
    let ["add", operation, key, delta] = words else {
        return Err("original source add required".into());
    };
    let operation = operation
        .parse()
        .ok()
        .and_then(OperationId::new)
        .ok_or("invalid operation")?;
    key.parse::<u8>().map_err(|_| "invalid key")?;
    delta.parse::<i64>().map_err(|_| "invalid delta")?;
    let text = initialization::receipt(|| attempt(words))?;
    record(&text, operation, value, wrapped)
}

fn record(text: &str, operation: OperationId, value: i64, wrapped: bool) -> Result<String, String> {
    let prefix = if wrapped {
        "OK receipt=RetirementReceipt { index: "
    } else {
        "OK receipt=RoutedReceipt { index: "
    };
    let index: u64 = text
        .strip_prefix(prefix)
        .and_then(|s| s.split_once(',').map(|(n, _)| n))
        .ok_or("missing original applied source receipt")?
        .parse()
        .map_err(|_| "invalid applied index")?;
    if index == 0 {
        return Err("zero applied index".into());
    }
    for duplicate in [false, true] {
        let receipt = RoutedReceipt {
            index,
            operation,
            outcome: RoutedOutcome::Applied(BucketReceipt {
                index,
                operation,
                outcome: BucketOutcome::Value(value),
                duplicate,
            }),
        };
        let expected = if wrapped {
            format!(
                "OK receipt={:?}",
                RetirementReceipt {
                    index,
                    operation,
                    outcome: RetirementOutcome::Owner(receipt),
                }
            )
        } else {
            format!("OK receipt={receipt:?}")
        };
        if text.trim_end() == expected {
            return Ok(text.into());
        }
    }
    Err(format!("wrong original applied source receipt: {text}"))
}

#[test]
fn original_source_write_repeats_exact_record_after_leadership_loss() {
    let words = ["add", "2", "200", "11"];
    let mut seen = Vec::new();
    let result = invoke(&words, 11, false, |original| {
        seen.push(original.iter().map(|w| (*w).to_owned()).collect::<Vec<_>>());
        if seen.len() == 1 {
            Err((
                "UNKNOWN Unknown(LeadershipChanged)\n".into(),
                "Error: \"outcome unknown; inspect and retry original operation\"\n".into(),
            ))
        } else {
            Ok("OK receipt=RoutedReceipt { index: 8, operation: OperationId(2), outcome: Applied(BucketReceipt { index: 8, operation: OperationId(2), outcome: Value(11), duplicate: true }) }\n".into())
        }
    });
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(seen, [words, words]);
}

#[test]
fn original_source_write_bounds_retries_and_preserves_other_failures() {
    let unknown = "UNKNOWN Unknown(LeadershipChanged)\n";
    let error = "Error: \"outcome unknown; inspect and retry original operation\"\n";
    for (reply, reason, expected) in [
        (unknown, error, 4),
        ("UNKNOWN Unknown(LeadershipChanged) extra\n", error, 1),
        ("UNKNOWN storage failure\n", error, 1),
        ("UNKNOWN reply deadline expired\n", error, 1),
        ("ERR AUTHORIZATION\n", error, 1),
        (unknown, "Error: storage failure\n", 1),
        (unknown, "Error: partial reply\n", 1),
    ] {
        let mut calls = 0;
        let words = ["add", "2", "200", "11"];
        let result = invoke(&words, 11, false, |original| {
            calls += 1;
            assert_eq!(original, words);
            Err((reply.into(), reason.into()))
        });
        assert!(result.is_err());
        assert_eq!(calls, expected, "{reply} {reason}");
    }
}

#[test]
fn original_source_write_requires_exact_applied_value_identity_index_and_wrapper() {
    let receipt = "OK receipt=RoutedReceipt { index: 8, operation: OperationId(2), outcome: Applied(BucketReceipt { index: 8, operation: OperationId(2), outcome: Value(11), duplicate: true }) }\n";
    let wrapped = format!("OK receipt=RetirementReceipt {{ index: 8, operation: OperationId(2), outcome: Owner({}) }}\n", receipt.trim_end().strip_prefix("OK receipt=").unwrap());
    for (text, wrap) in [(receipt.to_owned(), false), (wrapped.clone(), true)] {
        assert!(invoke(&["add", "2", "200", "11"], 11, wrap, |_| Ok(text.clone())).is_ok());
    }
    for (text, wrap) in [
        (receipt.replace("Value(11)", "Value(12)"), false),
        (receipt.replace("OperationId(2)", "OperationId(3)"), false),
        (receipt.replacen("index: 8", "index: 9", 1), false),
        (receipt.replace("index: 8", "index: 0"), false),
        (receipt.replace("Value(11)", "OperationConflict"), false),
        (receipt.trim_end_matches(" }\n").to_owned(), false),
        (wrapped, false),
        (receipt.into(), true),
        ("OK next=RecordIntent\n".into(), false),
    ] {
        let mut calls = 0;
        assert!(
            invoke(&["add", "2", "200", "11"], 11, wrap, |_| {
                calls += 1;
                Ok(text.clone())
            })
            .is_err(),
            "{text}"
        );
        assert_eq!(
            calls, 1,
            "positive malformed/conflicting result never triggers a retry"
        );
    }
}
