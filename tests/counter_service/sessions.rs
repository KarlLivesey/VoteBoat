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
//! The counter service's bounded write protocol: registration, numbered writes
//! with acknowledgements, typed refusals and the per-session window.
use super::*;

fn reply(cluster: &Cluster, args: &[&str]) -> String {
    String::from_utf8(cluster.target("auto", args).stdout).unwrap()
}

#[test]
fn sessions_bound_retries_and_answer_late_copies_with_expired() {
    let mut cluster = Cluster::new();
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    cluster.leader();
    // A retried registration nonce returns the same session.
    assert_eq!(
        cluster.routed(&["register", "77"]),
        "OK outcome=Registered(2) duplicate=false\n"
    );
    assert_eq!(
        cluster.routed(&["register", "77"]),
        "OK outcome=Registered(2) duplicate=true\n"
    );
    assert_eq!(
        cluster.routed(&["add", "2", "1", "0", "5"]),
        "OK outcome=Value(5) duplicate=false\n"
    );
    assert_eq!(
        cluster.routed(&["add", "2", "1", "0", "5"]),
        "OK outcome=Value(5) duplicate=true\n"
    );
    // Sequence 2 acknowledges sequence 1, whose late copy is then refused and
    // never executed again.
    assert_eq!(
        cluster.routed(&["add", "2", "2", "1", "3"]),
        "OK outcome=Value(8) duplicate=false\n"
    );
    assert_eq!(
        cluster.routed(&["add", "2", "1", "0", "5"]),
        "OK outcome=Refused(Expired) duplicate=false\n"
    );
    assert_eq!(
        cluster.routed(&["add", "9", "1", "0", "1"]),
        "OK outcome=Refused(UnknownSession) duplicate=false\n"
    );
    // The built-in session keeps at most 32 unacknowledged outcomes: the 33rd
    // is refused at admission until the client acknowledges.
    for sequence in 1..=32 {
        let sequence = sequence.to_string();
        assert!(cluster
            .routed(&["add", "1", &sequence, "0", "0"])
            .contains("Value(8)"));
    }
    assert_eq!(
        reply(&cluster, &["add", "1", "33", "0", "1"]),
        "ERR Application(SessionWindow)\n"
    );
    assert_eq!(
        cluster.routed(&["add", "1", "33", "1", "1"]),
        "OK outcome=Value(9) duplicate=false\n"
    );
    // Malformed requests are refused before proposal.
    for invalid in [
        &["register", "0"][..],
        &["add", "2", "3", "3", "1"],
        &["add", "0", "1", "0", "1"],
        &["add", "2", "0", "0", "1"],
        &["add", "2", "3", "1"],
    ] {
        assert!(
            !cluster.target("auto", invalid).status.success(),
            "{invalid:?}"
        );
    }
    assert_eq!(cluster.routed(&["read"]), "OK value=9\n");
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
