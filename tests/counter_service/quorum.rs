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
fn page(cluster: &Cluster, voters: &str, offset: &str, count: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = cluster.request(1, &["explain-quorum", voters, offset, count]);
        if output.status.success() {
            return String::from_utf8(output.stdout).unwrap();
        }
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(Instant::now() < deadline, "inspection listener unavailable");
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("quorum-access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n2 reader 2 2\n3 admin 1 1\n",
    )
    .unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    cluster.start(1, "create"); // Other voters never start; inspection needs no quorum.
    let first = page(&cluster, "1,2", "0", "2");
    assert!(first.contains("evidence=hypothetical_nodes stable=1 next=- satisfied=true unknown=0 rows=4 offset=0 next_offset=2"), "{first}");
    assert!(first.contains("0:-:stable:majority:1:2/2/3:true"));
    assert!(!cluster.ok(1, &["status"]).contains("role=Leader"));
    let tail = page(&cluster, "1,2", "2", "2");
    assert!(tail.contains("2:0:stable:voter2:1:1/1/1:true"));
    assert!(tail.contains("3:0:stable:voter3:1:0/1/1:false"));
    assert!(page(&cluster, "1,99", "0", "16").contains("satisfied=false unknown=1"));
    assert!(page(&cluster, "-", "0", "16").contains("satisfied=false unknown=0"));
    for args in [
        ["1,1", "0", "1"],
        ["0", "0", "1"],
        ["1", "5", "1"],
        ["1", "0", "17"],
    ] {
        assert!(!cluster
            .request(1, &["explain-quorum", args[0], args[1], args[2]])
            .status
            .success());
    }
    cluster.command_principal = Some(2);
    let denied = cluster.request(1, &["explain-quorum", "1,2", "0", "16"]);
    assert!(!denied.status.success());
    assert!(String::from_utf8(denied.stdout)
        .unwrap()
        .contains("AUTHORIZATION"));
    cluster.command_principal = Some(3);
    cluster.stop();
    cluster.start(1, "recover");
    assert!(page(&cluster, "1,2", "0", "2").contains("stable=1 next=- satisfied=true"));
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn quorum_explanation_is_hypothetical_bounded_and_authorized_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quorum_explanation_is_hypothetical_bounded_and_authorized_quic() {
    history(true);
}
