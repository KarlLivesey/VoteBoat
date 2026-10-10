// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
fn sample(cluster: &Cluster, id: usize) -> String {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let output = cluster.request(id, &["timings"]);
        if output.status.success() {
            let reply = String::from_utf8(output.stdout).unwrap();
            assert!(
                reply.starts_with("OK evidence=local_volatile unit=ns "),
                "{reply}"
            );
            assert!(reply.contains("percentile=bucket_upper_bound"));
            assert!(reply.len() < 4096);
            return reply;
        }
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(Instant::now() < until, "timing listener unavailable");
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn count(reply: &str, field: &str) -> u64 {
    let value = reply
        .split_whitespace()
        .filter_map(|s| s.split_once('='))
        .find(|(key, _)| *key == field)
        .unwrap()
        .1;
    value.split(',').next().unwrap().parse().unwrap()
}
fn history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("timing-access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n2 reader 2 2\n3 admin 1 1\n",
    )
    .unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    let before = sample(&cluster, leader);
    assert!(count(&before, "PollCompleted") > 0);
    assert!(authenticated_write(&cluster, &["add", "17601", "7"]).contains("Value(7)"));
    let stream =
        std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, cluster.base + 100 + leader as u16))
            .unwrap();
    drop(stream); // No TLS selector or command: still a measured interrupted connection.
    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        let after = sample(&cluster, leader);
        if count(&after, "ConnectionInterrupted") > count(&before, "ConnectionInterrupted") {
            break after;
        }
        assert!(
            Instant::now() < deadline,
            "interruption was omitted: {after}"
        );
    };
    assert!(count(&after, "ConnectionCompleted") > count(&before, "ConnectionCompleted"));
    assert_eq!(count(&after, "PollFailed"), 0);
    assert_eq!(count(&after, "rejected"), 0);
    cluster.command_principal = Some(2);
    let denied = cluster.request(leader, &["timings"]);
    assert!(!denied.status.success());
    assert!(String::from_utf8(denied.stdout)
        .unwrap()
        .contains("AUTHORIZATION"));
    cluster.command_principal = Some(3);
    cluster.stop();
    cluster.start(leader, "recover");
    let fresh = sample(&cluster, leader);
    assert!(count(&fresh, "store_session") > count(&after, "store_session"));
    assert_eq!(count(&fresh, "ConnectionCompleted"), 0);
    assert!(fresh.contains("ConnectionCompleted=0,sum:0,min:NA,max:NA,p99_upper:NA"));
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn service_timings_include_interruptions_and_reset_on_recovery_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn service_timings_include_interruptions_and_reset_on_recovery_quic() {
    history(true);
}
