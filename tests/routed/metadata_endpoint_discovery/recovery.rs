// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn unknown(nodes: &mut [Node<Owner>], clock: &Instant, bytes: &[u8]) -> bool {
    let operation = OperationId::new(3).unwrap();
    let ticket = nodes[0]
        .propose(ClientRequest {
            group: group(20),
            operation,
            bytes: bytes.to_vec(),
        })
        .unwrap();
    // Poll only the proposer: the real command reaches local WAL before any
    // other core can provide its replication acknowledgement.
    drive(&mut nodes[..1], clock, |ns| {
        let core = ns[0].local().owner.core(group(20)).unwrap();
        !core.has_pending_dependency()
            && core.state().entries.iter().any(|entry| {
                matches!(&entry.payload, EntryPayload::Command { operation: id, bytes: payload }
                if *id == operation && payload.as_slice() == bytes && entry.index > core.state().commit_index)
            })
    });
    nodes[0].control(group(20), NodeControl::Campaign).unwrap();
    let mut completion = None;
    drive(&mut nodes[..1], clock, |ns| {
        if completion.is_none() {
            completion = ns[0].poll_client();
        }
        completion.is_some()
    });
    let completion = completion.unwrap();
    assert_eq!(completion.ticket(), ticket);
    let outcome = nodes[0]
        .complete_client(completion)
        .unwrap_or_else(|_| panic!("owned unknown"));
    let uncertain = matches!(
        outcome,
        ClientOutcome::Unknown(ClientUnknown::LeadershipChanged)
    );
    assert!(uncertain, "actual outcome: {outcome:?}");
    drive(nodes, clock, |ns| {
        ns[0].local().clients.usage().requests == 0
    });
    uncertain
}
fn fresh_receipt(nodes: &mut [Node<Owner>], clock: &Instant, cache: &NativeManifestCache) {
    let replica = nodes[0].local().owner.core(group(20)).unwrap().local_node();
    let mut before = nodes[0].local().applications[&group(20)].clone();
    let bytes = encode_routed(
        hint(cache),
        &[1],
        &encode_add(&[1], 7, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    let (receipt, uncertain) = propose_recovering_observed(nodes, clock, 20, 4, bytes);
    assert_write(&receipt, 4, 12, false, uncertain);
    drive(nodes, clock, |ns| {
        ns.iter().all(|n| {
            n.local().applications[&group(20)]
                .routed()
                .application()
                .outbox()
                .count()
                == 2
        })
    });
    // Campaigns can reorder nodes. Replay the captured replica's actual applied
    // prefix to obtain its first application receipt, even after client retries.
    let node = nodes
        .iter()
        .find(|n| n.local().owner.core(group(20)).unwrap().local_node() == replica)
        .unwrap();
    let app = &node.local().applications[&group(20)];
    let log = node.local().owner.core(group(20)).unwrap().state();
    assert!(log.base_index() <= before.applied_index());
    assert!(app.applied_index() <= log.commit_index);
    let tail: Vec<_> = log
        .entries
        .iter()
        .filter(|e| e.index > before.applied_index() && e.index <= app.applied_index())
        .cloned()
        .collect();
    let receipts = before.apply_batch(&tail).unwrap();
    assert_eq!(
        before.checkpoint(4_000_000).unwrap(),
        app.checkpoint(4_000_000).unwrap()
    );
    let first = receipts
        .iter()
        .find(|r| r.operation == OperationId::new(4).unwrap())
        .unwrap();
    assert_write(first, 4, 12, false, false);
    assert!(std::panic::catch_unwind(|| assert_write(first, 4, 12, true, true)).is_err());
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-endpoint-unknown-{}-{protocol:?}-{checkpoint}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Instant::now();
    let env = Environment {
        root: &root,
        clock: &clock,
        protocol,
        checkpoint,
    };
    let cache = initialize_cache();
    let mut nodes = initialize_owner(&env);
    let bytes = encode_routed(
        hint(&cache),
        &[1],
        &encode_add(&[1], 5, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    let uncertain = unknown(&mut nodes, &clock, &bytes);
    campaign(&mut nodes, &clock, 20);
    assert_eq!(
        value(&mut nodes, &clock, &cache),
        5,
        "original unknown command is committed"
    );
    let (receipt, _) = propose_recovering_observed(&mut nodes, &clock, 20, 3, bytes.clone());
    assert_write(&receipt, 3, 5, false, uncertain);
    assert_write(&receipt, 3, 5, true, false);
    for node in &nodes {
        assert_eq!(
            node.local().applications[&group(20)]
                .routed()
                .application()
                .outbox()
                .count(),
            1
        );
    }
    // Detect an overbroad rule using the actual original duplicate receipt.
    assert!(std::panic::catch_unwind(|| assert_write(&receipt, 3, 5, false, false)).is_err());
    fresh_receipt(&mut nodes, &clock, &cache);
    if checkpoint {
        compact(&mut nodes, &clock, 20);
    }
    close(nodes, &clock, 20, || {});
    let mut nodes = open(
        configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        owner,
    );
    campaign(&mut nodes, &clock, 20);
    let receipt = propose_recovering(&mut nodes, &clock, 20, 3, bytes);
    assert_write(&receipt, 3, 5, true, false);
    assert_eq!(value(&mut nodes, &clock, &cache), 12);
    for node in &nodes {
        assert_eq!(
            node.local().applications[&group(20)]
                .routed()
                .application()
                .outbox()
                .count(),
            2
        );
    }
    close(nodes, &clock, 20, || {});
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_known_unknown_preserves_original_duplicate_and_cold_retry() {
    for checkpoint in [false, true] {
        history(NativePeerProtocol::TcpTls, checkpoint);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_known_unknown_preserves_original_duplicate_and_cold_retry() {
    for checkpoint in [false, true] {
        history(NativePeerProtocol::Quic, checkpoint);
    }
}
