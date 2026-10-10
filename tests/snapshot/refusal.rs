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
use voteboat::raft::*;

#[test]
fn delayed_snapshot_refusal_persists_higher_term_without_replication_credit() {
    let mut cluster = SnapshotCluster::new(bootstrap(1, 3), |id| {
        (HostLogStore::new(id as u128), host_snapshots(id))
    });
    capture_buffered_appends(&mut cluster);
    cluster.compact(1);
    cluster.pump();
    cluster.blocked.clear();
    cluster.act(1, Event::Heartbeat);
    let snapshot = cluster
        .messages
        .iter()
        .find(|m| m.to == node(3) && matches!(m.rpc, Rpc::Snapshot { .. }))
        .expect("compacted leader sends snapshot to lagging follower")
        .clone();
    cluster.messages.clear();
    cluster.act(3, Event::Campaign);
    cluster.messages.clear();
    let follower = cluster.replicas.get_mut(&3).unwrap();
    let follower_state = follower.core.state().clone();
    assert!(follower_state.hard_state.term > snapshot.term);
    let effects = follower.core.step(Event::Receive(snapshot)).unwrap();
    let [Effect::Send(refusal)] = effects.as_slice() else {
        panic!("refusal must not stage or acknowledge snapshot data: {effects:?}")
    };
    assert_eq!(refusal.rpc, Rpc::SnapshotAck { index: 0 });
    assert_eq!(refusal.term, follower_state.hard_state.term);
    assert_eq!(follower.core.state(), &follower_state);
    assert_eq!(cluster.installs, 0);
    let refusal = wire_roundtrip(refusal.clone());
    let leader = cluster.replicas.get_mut(&1).unwrap();
    let before = leader.core.state().clone();
    let mut same_term = refusal.clone();
    same_term.term = before.hard_state.term;
    assert_eq!(
        leader.core.step(Event::Receive(same_term)),
        Err(RaftError::InvalidMessage)
    );
    assert_eq!(leader.core.state(), &before);
    let effects = leader.core.step(Event::Receive(refusal.clone())).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("higher term requires persistence: {effects:?}")
    };
    assert_eq!(update.hard_state.term, refusal.term);
    assert_eq!(update.commit_index, before.commit_index);
    assert!(update.suffix.is_none());
    assert_eq!(leader.core.state(), &before);
    assert_eq!(leader.core.role(), Role::Follower);
    let after = persist_effect(&mut leader.core, &mut leader.log, update.clone()).unwrap();
    assert!(after.is_empty());
    assert_eq!(leader.core.state().hard_state.term, refusal.term);
    assert_eq!(leader.core.state().commit_index, before.commit_index);
    assert_eq!(leader.core.state().entries, before.entries);
    let mut recovered = Counter::new(100).unwrap();
    let (core, _) = recover_replica(
        node(1),
        group(1),
        &leader.log,
        &mut leader.snapshots,
        &mut recovered,
    )
    .unwrap();
    assert_eq!(core.state(), leader.core.state());
    assert_eq!(recovered.read_applied(before.commit_index), Ok(7));
}

fn wire_roundtrip(message: Message) -> Message {
    #[cfg(feature = "native")]
    {
        use voteboat::{native::wire::NativeWireCodec, wire::*};
        let limits = WireLimits::default();
        let scope = WireScope {
            from: message.from,
            sender: message.sender,
            to: message.to,
        };
        for codec in [
            NativeWireCodec::new(limits),
            NativeWireCodec::with_membership(limits),
            NativeWireCodec::with_authority(limits),
            NativeWireCodec::with_readiness(limits),
            NativeWireCodec::with_learner_repair(limits),
            NativeWireCodec::with_snapshot_repair(limits),
            NativeWireCodec::with_committed_snapshot_repair(limits),
        ] {
            let codec = codec.unwrap();
            let encoded = codec
                .encode_batch(scope, std::slice::from_ref(&message))
                .unwrap();
            assert_eq!(
                codec
                    .encoded_length(scope, std::slice::from_ref(&message))
                    .unwrap(),
                encoded.len()
            );
            let mut provisioned = vec![0; encoded.len()];
            codec
                .encode_into(scope, std::slice::from_ref(&message), &mut provisioned)
                .unwrap();
            assert_eq!(provisioned, encoded);
            assert_eq!(
                codec.decode_batch(scope, &encoded).unwrap(),
                std::slice::from_ref(&message)
            );
        }
    }
    message
}
