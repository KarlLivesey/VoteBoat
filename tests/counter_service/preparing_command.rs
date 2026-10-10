// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Admission observation for the native unread-preparation recovery cut.
use super::*;
use voteboat::{runtime::MonoTime, secure::*};

pub(super) struct Preparing {
    pub endpoint: usize,
    pub channel: UnobservedCommand,
    pub refusals: usize,
}

// This channel remains unread on the preparing path. Only an actual complete
// pre-admission follower refusal authorizes another send of the original record.
pub(super) fn send(
    c: &mut Cluster,
    mut endpoint: usize,
    record: &str,
    event: &str,
) -> Result<Preparing, String> {
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut refusals = 0;
    loop {
        if Instant::now() >= deadline {
            return Err(format!(
                "preparation deadline; endpoint={endpoint} refusals={refusals}"
            ));
        }
        let offset = c.service_log(endpoint).len();
        let mut channel = UnobservedCommand::send(c, endpoint, record);
        let mut bytes = Vec::new();
        loop {
            if Instant::now() >= deadline {
                return Err(format!(
                    "preparation deadline; endpoint={endpoint} refusals={refusals} reply={:?}; {}",
                    String::from_utf8_lossy(&bytes),
                    c.service_log(endpoint)
                ));
            }
            let log = c.service_log(endpoint);
            if log.get(offset..).is_some_and(|fresh| fresh.contains(event)) {
                return Ok(Preparing {
                    endpoint,
                    channel,
                    refusals,
                });
            }
            match follower_refusal(&mut channel, &mut bytes) {
                Ok(true) => {
                    channel.disconnect();
                    refusals += 1;
                    eprintln!("preparing_follower_refusal endpoint={endpoint} refusals={refusals} record={record:?}");
                    // The shared deadline is not renewed by discovery. Existing
                    // child discovery/send limits can delay observing expiry.
                    endpoint = c.leader();
                    break;
                }
                Ok(false) => (),
                Err(error) => {
                    return Err(format!(
                    "preparation unresolved endpoint={endpoint} refusals={refusals}: {error}; {}",
                    c.service_log(endpoint)
                ))
                }
            }
            std::thread::park_timeout(Duration::from_millis(5));
        }
    }
}

fn follower_refusal(channel: &mut UnobservedCommand, bytes: &mut Vec<u8>) -> Result<bool, String> {
    channel
        .session
        .poll(
            MonoTime(channel.start.elapsed().as_millis() as u64),
            SessionPollBudget::default(),
        )
        .map_err(|e| format!("interrupted channel {e:?}"))?;
    let mut buffer = [0; 128];
    loop {
        match channel.session.read_plaintext(&mut buffer) {
            Ok(0) if !bytes.contains(&b'\n') => {
                return Err("closed without a complete reply".into())
            }
            Ok(0) | Err(SessionError::WouldBlock | SessionError::NotReady) => break,
            Ok(n) => {
                if bytes.len() + n > 1024 {
                    return Err("reply exceeds1024bytes".into());
                }
                bytes.extend_from_slice(&buffer[..n]);
            }
            Err(error) => return Err(format!("interrupted reply {error:?}")),
        }
    }
    if !bytes.contains(&b'\n') {
        return Ok(false);
    }
    if bytes == b"ERR NOT_LEADER\n" {
        Ok(true)
    } else {
        Err(format!(
            "terminal reply {:?}",
            String::from_utf8_lossy(bytes)
        ))
    }
}

#[test]
fn unread_preparation_reroutes_only_explicit_follower_refusal_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn unread_preparation_reroutes_only_explicit_follower_refusal_quic() {
    history(true);
}
fn history(quic: bool) {
    let mut c = configuration_pending::setup(quic);
    retry_configuration_record(&mut c, "joint 17910 1 2 3 3 m:2 v:1 v:2");
    retry_configuration_record(&mut c, "final 17910 2 3");
    drain::kill(&mut c, 3);
    let leader = c.leader();
    let follower = 3 - leader;
    assert!(c.ok(follower, &["status"]).contains("role=Follower"));
    let record = "configure-record joint 17911 3 4 5 - m:3 v:1 v:2 v:3";
    let preparing = send(
        &mut c,
        follower,
        record,
        "administration operation=17911 preparing_learner=3",
    )
    .unwrap();
    assert!(preparing.refusals > 0);
    assert_ne!(preparing.endpoint, follower);
    let endpoint = preparing.endpoint;
    // The command connection remains owned until cancellation. Querying its
    // server here would wait behind that connection and reach its deadline.
    let mut channel = preparing.channel;
    channel.close_notify();
    wait_administration_event(
        &c,
        endpoint,
        "administration observation_cancelled operation=17911 phase=preparing reason=channel",
    );
    channel.disconnect();
    c.start(3, "recover-member");
    assert!(c
        .ok(endpoint, &["configuration-status", "17911"])
        .contains("inconclusive_local_absence"));
    assert!(leader_request(
        &mut c,
        &[
            "configure-record",
            record.trim_start_matches("configure-record ")
        ]
    )
    .contains("operation=17911"));
    assert!(
        leader_request(&mut c, &["configure-record", "final 17911 4 5"])
            .contains("committed_index=")
    );
    for id in 1..=3 {
        c.ok(id, &["checkpoint"]);
        drain::wait_manual_checkpoint(&c, id);
    }
    c.stop();
    for id in 1..=3 {
        c.start(id, "recover-member");
    }
    assert!(authenticated_write(&c, &["add", "21100", "42"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=42\n");
    let leader = c.leader();
    assert!(c
        .ok(leader, &["configuration-status", "17911"])
        .contains("action=completed"));
    c.stop();
}

#[test]
fn unknown_configuration_reply_keeps_original_durable_record_without_rerouting() {
    let mut c = configuration_pending::setup(false);
    let leader = c.leader();
    for id in (1..=3).filter(|id| *id != leader) {
        drain::kill(&mut c, id);
    }
    let record = "learners 17920 1 2 - m:3 v:1 v:2 v:3";
    let error = match send(
        &mut c,
        leader,
        &format!("configure-record {record}"),
        "administration operation=17920 preparing_learner=3",
    ) {
        Ok(_) => panic!("a quorum-lost learner record must not authorize a preparing cut"),
        Err(error) => error,
    };
    assert!(error.contains("refusals=0"), "{error}");
    assert!(error.contains("interrupted channel Truncated"), "{error}");
    let status = c.ok(leader, &["configuration-status", "17920"]);
    assert!(status.contains("accepted=Learners"), "{status}");
    assert!(status.contains("action=wait_for_commit"), "{status}");
    // This is an explicit caller retry after inspecting durable status. The
    // helper itself did not resend an interrupted or accepted request.
    let error = match send(
        &mut c,
        leader,
        &format!("configure-record {record}"),
        "administration operation=17920 preparing_learner=3",
    ) {
        Ok(_) => panic!("a retained uncommitted record must not authorize a preparing cut"),
        Err(error) => error,
    };
    assert!(error.contains("refusals=0"), "{error}");
    assert!(
        error.contains("UNKNOWN exact record locally durable but not committed"),
        "{error}"
    );
    assert_eq!(c.ok(leader, &["configuration-status", "17920"]), status);
    let error = match send(
        &mut c,
        leader,
        "configure-record learners 17920 1 99 - m:3 v:1 v:2 v:3",
        "administration operation=17920 preparing_learner=3",
    ) {
        Ok(_) => panic!("a changed original record must not authorize a preparing cut"),
        Err(error) => error,
    };
    assert!(error.contains("refusals=0"), "{error}");
    assert!(
        error.contains("configuration operation conflicts with retained record"),
        "{error}"
    );
    assert_eq!(c.ok(leader, &["configuration-status", "17920"]), status);
    for id in (1..=3).filter(|id| *id != leader) {
        c.start(id, "recover-member");
    }
    assert!(leader_request(&mut c, &["configure-record", record]).contains("operation=17920"));
    assert!(authenticated_write(&c, &["add", "21100", "42"]).contains("duplicate=true"));
    c.stop();
}

#[test]
fn committed_reply_is_not_preparing_evidence_and_does_not_reroute() {
    let mut c = configuration_pending::setup(false);
    let leader = c.leader();
    let record = "learners 17930 1 2 - m:3 v:1 v:2 v:3";
    let error = match send(
        &mut c,
        leader,
        &format!("configure-record {record}"),
        "administration operation=17930 preparing_learner=3",
    ) {
        Ok(_) => panic!("a completed record must not be mistaken for an unread preparing cut"),
        Err(error) => error,
    };
    assert!(error.contains("refusals=0"), "{error}");
    assert!(
        error.contains("terminal reply") && error.contains("OK operation=17930 committed_index="),
        "{error}"
    );
    let receipt = leader_request(&mut c, &["configure-record", record]);
    assert!(receipt.contains("duplicate=true"), "{receipt}");
    assert!(authenticated_write(&c, &["add", "21100", "42"]).contains("duplicate=true"));
    c.stop();
}
