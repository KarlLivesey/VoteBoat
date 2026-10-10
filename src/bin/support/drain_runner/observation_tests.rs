// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{recovery_tests::*, *};
use std::sync::mpsc;

#[test]
fn observation_retry_refuses_authentication_failure_and_invalid_transport_data() {
    for reason in [
        "authentication deadline expired",
        "request deadline expired",
        "reply deadline expired",
        "connection closed during request",
        "connection closed without a complete reply",
    ] {
        assert!(repeat_observation(reason));
    }
    for reason in [
        "authentication failed",
        "authentication setup failed",
        "authenticated read failed",
        "invalid reply encoding",
        "multiple reply lines",
        "reply exceeds limit",
        "connection closed with an incomplete reply",
    ] {
        assert!(!repeat_observation(reason));
    }
}

fn observation_history(offset: Option<usize>, stall_authentication: bool, exhausted: bool) {
    let (mut runner, listener, active, root) = fixture();
    if exhausted {
        runner.remaining = 1;
    }
    let remaining = runner.remaining;
    let deadline = runner.deadline;
    let command = match offset {
        Some(offset) => format!("drain-group 1 2 {offset}\n"),
        None => "drain-status 1 2\n".into(),
    };
    let expected = command.clone();
    let (stop, stopped) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let start = Instant::now();
        let mut first = accept(&listener, &stopped).unwrap();
        if !stall_authentication {
            assert_eq!(request(&mut first, &active, start), expected);
        }
        // The first connection stays open with either a pending authentication
        // or an accepted observation. Neither produces an application reply.
        let second = accept(&listener, &stopped);
        if let Some(mut second) = second {
            assert_eq!(request(&mut second, &active, start), expected);
            respond(
                &mut second,
                &active,
                start,
                "OK sequence=1 operation=2 observed=true\n",
            );
            2
        } else {
            1
        }
    });
    let result = runner.source_observation(offset);
    let _ = stop.send(());
    let attempts = server.join().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    if exhausted {
        assert_eq!(
            result.unwrap_err().to_string(),
            "UNKNOWN drain runner budget expired; rerun the same sequence and operation"
        );
        assert_eq!(attempts, 1);
        assert_eq!(runner.remaining, 0);
    } else {
        assert_eq!(result.unwrap(), "OK sequence=1 operation=2 observed=true\n");
        assert_eq!(attempts, 2);
        assert_eq!(runner.remaining, remaining - 2);
    }
    assert_eq!(runner.deadline, deadline);
    assert!(Instant::now() < deadline);
}

#[test]
fn lost_source_status_reply_repeats_only_original_observation() {
    observation_history(None, false, false);
}

#[test]
fn lost_assignment_page_reply_repeats_the_same_offset() {
    observation_history(Some(7), false, false);
}

#[test]
fn source_authentication_timeout_preserves_observation_budget() {
    observation_history(None, true, false);
}

#[test]
fn lost_source_observation_cannot_replenish_exhausted_requests() {
    observation_history(None, false, true);
}

#[test]
fn completed_observation_still_rejects_invalid_identity_or_authorization() {
    for reply in [
        "ERR Unauthorized\n",
        "OK sequence=9 operation=2 phase=Active\n",
        "OK sequence=1 operation=2\n",
    ] {
        let (mut runner, listener, active, root) = fixture();
        let remaining = runner.remaining;
        let (stop, stopped) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let start = Instant::now();
            let mut channel = accept(&listener, &stopped).unwrap();
            assert_eq!(request(&mut channel, &active, start), "drain-status 1 2\n");
            respond(&mut channel, &active, start, reply);
        });
        let result = runner.status();
        let _ = stop.send(());
        server.join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
        assert!(result.is_err(), "{reply}");
        assert_eq!(runner.remaining, remaining - 1);
    }
}

#[test]
fn stalled_observation_cannot_renew_the_absolute_deadline() {
    let (mut runner, listener, _active, root) = fixture();
    let remaining = runner.remaining;
    let (stop, stopped) = mpsc::channel();
    let server = std::thread::spawn(move || {
        // Hold an unauthenticated channel until the caller is finished. If its
        // deadline expires before accept, cancellation still owns cleanup.
        let _channel = accept(&listener, &stopped);
        let _ = stopped.recv_timeout(Duration::from_secs(2));
    });
    runner.deadline = Instant::now() + Duration::from_millis(300);
    let deadline = runner.deadline;
    let result = runner.source_observation(None);
    let _ = stop.send(());
    server.join().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert_eq!(
        result.unwrap_err().to_string(),
        "UNKNOWN drain runner budget expired; rerun the same sequence and operation"
    );
    assert_eq!(runner.deadline, deadline);
    assert_eq!(runner.remaining, remaining - 1);
}
