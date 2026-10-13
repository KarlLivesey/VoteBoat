// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
mod fixture;
use fixture::*;

#[test]
fn follower_can_persist_before_local_barrier_and_held_send_survives_durable() {
    let mut nodes = cluster();
    let written = stage(&mut nodes[0]);
    let before = nodes[0].owner.usage().reserved_bytes;
    nodes[0].owner.deliver_worker(written, MonoTime(0)).unwrap();
    assert!(nodes[0].owner.usage().reserved_bytes > before);
    let held = nodes[0].owner.take_effect().unwrap().unwrap();
    let Effect::Send(message) = &held.effect else {
        panic!("early append")
    };
    let target = message.to.get() as usize - 1;
    let Rpc::Append { entries, .. } = &message.rpc else {
        panic!()
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index, 2);
    nodes[target]
        .owner
        .admit(group(1), Event::Receive(message.clone()))
        .unwrap();
    drive_one(&mut nodes, target);
    assert_eq!(
        nodes[target]
            .owner
            .core(group(1))
            .unwrap()
            .state()
            .last_index(),
        2
    );
    assert_eq!(
        nodes[0].owner.core(group(1)).unwrap().state().last_index(),
        1
    );
    assert_eq!(nodes[0].app.applied_index(), 1);
    assert_eq!(nodes[0].owner.advance(MonoTime(0), 4).unwrap().len(), 0);
    let durable = nodes[0].worker.poll(1).pop().unwrap();
    nodes[0].owner.deliver_worker(durable, MonoTime(0)).unwrap();
    assert_eq!(nodes[0].owner.usage().leased, 1);
    assert!(nodes[0].owner.take_effect().unwrap().is_none());
    assert_eq!(
        nodes[0].owner.core(group(1)).unwrap().state().commit_index,
        1
    );
    nodes[0]
        .owner
        .release_transferred_send(held.ticket)
        .unwrap();
    drive(&mut nodes);
    assert_eq!(nodes[0].app.read_applied(2), Ok(7));
    assert_eq!(nodes[0].owner.usage().reserved_bytes, 0);
}

#[test]
fn written_and_durable_in_one_poll_preserve_queued_sends_and_original_commit() {
    let mut nodes = cluster();
    let written = stage(&mut nodes[0]);
    let durable = nodes[0].worker.poll(1).pop().unwrap();
    nodes[0].owner.deliver_worker(written, MonoTime(0)).unwrap();
    nodes[0].owner.deliver_worker(durable, MonoTime(0)).unwrap();
    assert_eq!(nodes[0].app.applied_index(), 1);
    drive(&mut nodes);
    assert_eq!(nodes[0].app.read_applied(2), Ok(7));
    assert!(nodes
        .iter()
        .all(|node| node.owner.usage().reserved_bytes == 0));
}

#[test]
fn local_failure_after_early_send_fences_and_retains_external_charge() {
    let mut nodes = cluster();
    let written = stage(&mut nodes[0]);
    nodes[0].owner.deliver_worker(written, MonoTime(0)).unwrap();
    let held = nodes[0].owner.take_effect().unwrap().unwrap();
    assert!(matches!(held.effect, Effect::Send(_)));
    nodes[0].worker.fail = true;
    let failed = nodes[0].worker.poll(1).pop().unwrap();
    assert!(nodes[0].owner.deliver_worker(failed, MonoTime(0)).is_err());
    assert!(nodes[0].owner.is_failed());
    assert_eq!(nodes[0].owner.usage().leased, 1);
    assert!(nodes[0].owner.usage().reserved_bytes > 0);
    assert_eq!(nodes[0].app.applied_index(), 1);
    assert_eq!(
        nodes[0].owner.core(group(1)).unwrap().state().commit_index,
        1
    );
    nodes[0].owner.discard_failed(held).unwrap();
    assert_eq!(nodes[0].owner.usage().reserved_bytes, 0);
}

#[test]
fn insufficient_extra_space_keeps_original_serialized_progress() {
    let mut nodes = cluster();
    let written = stage_with_remaining_space(&mut nodes[0], 16);
    let original = nodes[0].owner.usage().reserved_bytes;
    let extra = nodes[0]
        .owner
        .core(group(1))
        .unwrap()
        .admitted_replication_reservation()
        .unwrap();
    let limits = nodes[0].owner.limits();
    assert!(extra > limits.reserved_bytes - limits.control_bytes - original);
    nodes[0].owner.deliver_worker(written, MonoTime(0)).unwrap();
    assert_eq!(nodes[0].owner.usage().reserved_bytes, original);
    assert!(nodes[0].owner.take_effect().unwrap().is_none());
    drive(&mut nodes);
    assert_eq!(nodes[0].app.read_applied(2), Ok(7));
    assert!(!nodes[0].owner.is_failed());
}
