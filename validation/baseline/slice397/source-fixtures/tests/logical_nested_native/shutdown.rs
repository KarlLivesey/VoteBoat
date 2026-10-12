// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Typed original shutdown observations are local metadata, never quorum truth.
use std::collections::BTreeMap;
use voteboat::{
    identity::{GroupIdentity, NodeId, StoreIdentity},
    logical_voter::*,
    outbound::{LocalSendCompletion, OutboundBinding},
    runtime::ReplicaProgress,
    secure::SessionError,
};
pub fn record(
    result: Result<LogicalNodeShutdownProgress, LogicalNodeShutdownRejected>,
    group: GroupIdentity,
    binding: OutboundBinding,
    peers: &BTreeMap<NodeId, StoreIdentity>,
) -> ReplicaProgress {
    let progress = match result {
        Ok(progress) => progress,
        Err(rejected) => {
            validate(&rejected.progress, group, binding, peers);
            panic!("original nested shutdown rejected group={group:?},binding={binding:?},reason={:?},progress={:?}",rejected.reason,rejected.progress);
        }
    };
    validate(&progress, group, binding, peers);
    progress.replica
}
fn validate(
    progress: &LogicalNodeShutdownProgress,
    group: GroupIdentity,
    binding: OutboundBinding,
    peers: &BTreeMap<NodeId, StoreIdentity>,
) {
    for completion in &progress.completions {
        let LocalSendCompletion { ticket, result } = *completion;
        assert_eq!(ticket.binding, binding);
        assert!(ticket.sequence > 0);
        assert!(ticket.peer != binding.node && peers.contains_key(&ticket.peer));
        eprintln!("nested original shutdown LOCAL_ONLY group={group:?},binding={binding:?},ticket={ticket:?},result={result:?}");
    }
    for closure in &progress.peer_closures {
        assert!(closure.peer != binding.node && peers.contains_key(&closure.peer));
        assert!(matches!(
            closure.reason,
            LogicalNetworkError::Closed | LogicalNetworkError::Session(SessionError::Closed)
        ));
        eprintln!("nested original shutdown LOCAL_ONLY group={group:?},binding={binding:?},closure={closure:?}");
    }
}
