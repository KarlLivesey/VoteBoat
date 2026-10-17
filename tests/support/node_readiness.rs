// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Advisory host readiness: queued work and deadlines a waiting host must see.
use super::*;

#[cfg(unix)]
#[test]
fn queued_proposal_is_announced_as_immediate_until_the_node_settles() {
    let mut n = elected();
    let mut fds = Vec::new();
    n.readiness_fds(&mut fds);
    assert!(
        fds.iter().all(|f| !f.immediate),
        "a settled node announces no queued work: {fds:?}"
    );
    n.propose(request(1)).unwrap();
    fds.clear();
    n.readiness_fds(&mut fds);
    assert!(
        fds.iter().any(|f| f.immediate),
        "an admitted proposal is queued work: {fds:?}"
    );
    settle(&mut n);
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    fds.clear();
    n.readiness_fds(&mut fds);
    assert!(fds.iter().all(|f| !f.immediate), "{fds:?}");
    shutdown(&mut n);
}
