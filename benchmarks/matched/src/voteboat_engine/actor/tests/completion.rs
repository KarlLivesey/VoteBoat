// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn commit_update<const TRACE: bool>(
    actor: &mut Actor<TRACE>,
    ids: &[OperationId],
    bytes: &[u8],
) -> (VolatilePersist, Vec<oneshot::Receiver<OperationId>>) {
    let mut originals = Vec::new();
    let mut waits = Vec::new();
    for &operation in ids {
        let (reply, wait) = oneshot::channel();
        originals.push(Original {
            operation,
            bytes: bytes.to_vec(),
            reply: Reply::Owned(reply),
        });
        waits.push(wait);
    }
    let mut batch = staging(actor);
    actor
        .include(Request::Owned { originals }, &mut batch)
        .unwrap();
    let mut initial = actor.parts.core.propose_entries(batch).unwrap();
    assert_eq!(initial.len(), 1);
    let Effect::PersistVolatile(append) = initial.remove(0) else {
        panic!("proposal must first persist its entries");
    };
    let mut following = persist_volatile(&mut actor.parts.core, &actor.parts.store.log, append)
        .unwrap()
        .into_owned();
    assert_eq!(following.len(), 1);
    let Effect::PersistVolatile(commit) = following.remove(0) else {
        panic!("single voter must persist commitment before application");
    };
    assert_eq!(actor.parts.application.applied, 1);
    (commit, waits)
}

#[test]
fn empty_fifo_applies_borrowed_commit_and_delivers_every_original() {
    check_immediate::<false>();
    check_immediate::<true>();
}

fn check_immediate<const TRACE: bool>() {
    let (mut actor, _sender) = ready_actor_clients::<TRACE>(crate::Envelopes::Common64, 1);
    let ids = [
        OperationId::new(u128::MAX).unwrap(),
        OperationId::new(1 << 127).unwrap(),
    ];
    let (update, mut waits) = commit_update(&mut actor, &ids, &[]);
    for wait in &mut waits {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }
    actor
        .effects(vec![Effect::PersistVolatile(update)])
        .unwrap();
    assert_eq!(actor.parts.application.applied, 3);
    assert_eq!(actor.parts.core.state().commit_index, 3);
    assert!(actor.pending.is_empty());
    for (wait, id) in waits.iter_mut().zip(ids) {
        assert_eq!(wait.try_recv().unwrap(), id);
    }
}

#[test]
fn existing_fifo_application_error_precedes_new_commit_application() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::Common64, 1);
    let id = OperationId::new(u128::MAX).unwrap();
    let (update, mut waits) = commit_update(&mut actor, &[id], &[]);
    let invalid = LogEntry {
        index: 99,
        term: 1,
        payload: EntryPayload::Noop,
    };
    let error = actor
        .effects(vec![
            Effect::PersistVolatile(update),
            Effect::Committed(vec![invalid]),
        ])
        .unwrap_err();
    assert!(error.to_string().contains("IndexGap"));
    assert_eq!(actor.parts.core.state().commit_index, 2);
    assert_eq!(actor.parts.application.applied, 1);
    assert_eq!(actor.pending.len(), 1);
    assert_eq!(
        waits[0].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    drop(actor);
    assert_eq!(
        waits[0].try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    );
}

#[test]
fn existing_fifo_send_error_precedes_new_commit_receipt() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::Common64, 1);
    let id = OperationId::new(1 << 127).unwrap();
    let (update, mut waits) = commit_update(&mut actor, &[id], &[]);
    let message = Message {
        group: actor.parts.application.group,
        configuration: ConfigurationId::new(1).unwrap(),
        from: NodeId::new(1).unwrap(),
        sender: actor.parts.store.log.binding(),
        to: NodeId::new(2).unwrap(),
        term: 1,
        context: RequestContext {
            origin: actor.parts.store.log.binding(),
            sequence: 1,
        },
        rpc: Rpc::ReadProbe,
    };
    assert_eq!(
        actor
            .effects(vec![Effect::PersistVolatile(update), Effect::Send(message)])
            .unwrap_err()
            .to_string(),
        "unknown recipient"
    );
    assert_eq!(actor.parts.core.state().commit_index, 2);
    assert_eq!(actor.parts.application.applied, 1);
    assert_eq!(
        waits[0].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
}

#[test]
fn borrowed_application_failure_preserves_frontier_and_all_original_waits() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::Common64, 1);
    let id = OperationId::new(u128::MAX).unwrap();
    let (update, mut waits) = commit_update(&mut actor, &[id], &[7, 255]);
    assert!(actor
        .effects(vec![Effect::PersistVolatile(update)])
        .unwrap_err()
        .to_string()
        .contains("InvalidCommand"));
    assert_eq!(actor.parts.core.state().commit_index, 2);
    assert_eq!(actor.parts.application.applied, 1);
    assert_eq!(actor.pending.len(), 1);
    assert_eq!(
        waits[0].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
}

#[test]
fn borrowed_and_fifo_owned_delivery_stop_after_same_dropped_wait() {
    for queued in [false, true] {
        let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::Common64, 1);
        let ids = [
            OperationId::new(1 << 127).unwrap(),
            OperationId::new(u128::MAX).unwrap(),
            OperationId::new(17).unwrap(),
        ];
        let (update, mut waits) = commit_update(&mut actor, &ids, &[]);
        drop(waits.remove(1));
        let mut effects = vec![Effect::PersistVolatile(update)];
        if queued {
            effects.push(Effect::Committed(Vec::new()));
        }
        assert_eq!(
            actor.effects(effects).unwrap_err().to_string(),
            "original client dropped result wait"
        );
        assert_eq!(actor.parts.application.applied, 4);
        assert_eq!(actor.parts.core.state().commit_index, 4);
        assert_eq!(actor.pending.len(), 1);
        assert_eq!(waits[0].try_recv().unwrap(), ids[0]);
        assert_eq!(
            waits[1].try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        drop(actor);
        assert_eq!(
            waits[1].try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
    }
}
