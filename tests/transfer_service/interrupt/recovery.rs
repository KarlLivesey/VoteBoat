// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[cfg(unix)]
fn history(quic: bool) {
    let mut rig = Cluster::new(quic);
    initialize(&rig);
    let profile = fs::read(rig.root.join("profile")).unwrap();
    let sampled = startup_discovery::leader(&rig, 1, &[1, 2, 3]);
    let pid = rig
        .children
        .iter()
        .find(|(g, n, _)| (*g, *n) == (1, sampled))
        .unwrap()
        .2
        .id();
    let mut paused = Some(startup_recovery::Paused::new(pid));
    let others = (1..=3).filter(|n| *n != sampled).collect::<Vec<_>>();
    startup_discovery::leader(&rig, 1, &others);
    let mut calls = 0;
    let result = prepared(&rig, || {
        calls += 1;
        if calls == 1 {
            Ok(Some(sampled))
        } else {
            if let Some(pause) = paused.take() {
                pause.resume();
            }
            startup_discovery::probe(&rig, 1, &others)
        }
    });
    if let Some(pause) = paused.take() {
        pause.resume();
    }
    assert!(result.is_ok(), "{result:?}");
    assert!(calls >= 2);
    let wrong =
        initialization::response(rig.request(0, rig.admin, 1, &["transfer-read", "publication"]));
    assert!(observation::ready(result.unwrap(), wrong)
        .unwrap_err()
        .contains("wrong metadata readiness query"));
    assert_eq!(fs::read(rig.root.join("profile")).unwrap(), profile);
    read(&mut rig);
    assert!(rig.operate("start").contains("OK complete"));
    finish(rig);
}

#[cfg(unix)]
#[test]
fn metadata_readiness_reselects_after_sampled_authority_stalls_tcp() {
    history(false);
}
#[cfg(all(unix, feature = "quic"))]
#[test]
fn metadata_readiness_reselects_after_sampled_authority_stalls_quic() {
    history(true);
}

#[test]
fn disconnected_wait_observes_usage_until_actual_release() {
    let start = Instant::now();
    let mut calls = 0;
    let result = released(
        || {
            calls += 1;
            Ok(format!(
                "OK pending_reads={} evidence=local\n",
                if calls == 1 { 1 } else { 0 }
            ))
        },
        start + Duration::from_secs(5),
        || start,
        || (),
    );
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(calls, 2);
}

#[test]
fn readiness_retries_only_exact_unavailability_and_rejects_invalid_observations() {
    let unavailable = "Error: \"group unavailable; original operation remains unresolved\"";
    assert_eq!(
        observation::ready(2, Err((String::new(), unavailable.into()))).unwrap(),
        None
    );
    for (text, error) in [
        ("ERR AUTHORIZATION", unavailable),
        ("", "Error: invalid credentials"),
        ("", "Error: group unavailable"),
        ("UNKNOWN unspecified", unavailable),
        (
            "",
            "Error: \"group unavailable; original operation remains unresolved\" extra",
        ),
    ] {
        assert!(observation::ready(2, Err((text.into(), error.into()))).is_err());
    }
    for text in [
        "OK receipt=partial",
        "OK observation 0",
        "OK observation zz",
        "OK observation ",
        "OK observation 00\nextra",
    ] {
        assert!(observation::ready(2, Ok(text.into())).is_err());
    }
}

#[test]
fn cleanup_deadline_and_invalid_status_cannot_become_release() {
    let start = Instant::now();
    let now = std::cell::Cell::new(start);
    let mut calls = 0;
    let result = released(
        || {
            calls += 1;
            Ok("OK pending_reads=1 evidence=local\n".into())
        },
        start + Duration::from_secs(5),
        || now.get(),
        || now.set(start + Duration::from_secs(5)),
    );
    assert!(result.unwrap_err().contains("deadline"));
    assert_eq!(calls, 1);
    for text in [
        "ERR AUTHORIZATION",
        "OK pending_reads=zero",
        "OK pending_reads=1 pending_reads=0",
        "OK pending_reads=0\nextra",
        "OK role=Leader",
    ] {
        let mut calls = 0;
        assert!(released(
            || {
                calls += 1;
                Ok(text.into())
            },
            start + Duration::from_secs(5),
            || start,
            || panic!("terminal reply must not repeat")
        )
        .is_err());
        assert_eq!(calls, 1);
    }
    assert!(released(
        || Err((String::new(), "original authentication failure".into())),
        start + Duration::from_secs(5),
        || start,
        || panic!("failed status must not repeat")
    )
    .is_err());
}
