// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual native queue allocations and codec, through original effect transfer.
use super::*;
use crate::{
    native::{outbound::NativeOutbound, relay::*, runtime::*, wire::NativeWireCodec},
    outbound::*,
    runtime::*,
    wire::*,
    worker::WorkerBinding,
};

type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
fn fixture() -> (Owner, NativeOutbound, Message, *const Message) {
    let core = core();
    let group = core.state().bootstrap.group;
    let id = RuntimeOwner {
        store: core.binding,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let binding = OutboundBinding {
        node: core.node,
        store: core.binding,
        generation: OutboundGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        id,
        ShardLimits::default(),
        FairScheduler::new(4096).unwrap(),
    )
    .unwrap();
    shard.register(core).unwrap();
    let timed = TimedShard::new(
        shard,
        DeadlineQueue::new(id, 8192).unwrap(),
        JitterEntropy::new(7),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    let mut owner = EffectOwner::new(
        timed,
        WorkerBinding {
            store: id.store,
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap();
    let mut outbound = NativeOutbound::new(binding, OutboundLimits::default()).unwrap();
    owner.admit(group, Event::Heartbeat).unwrap();
    owner.advance(MonoTime(0), 1).unwrap();
    let mut first = None;
    while let Some(lease) = owner.take_effect().unwrap() {
        let EffectLease {
            ticket,
            effect: Effect::Send(message),
        } = lease
        else {
            panic!("unexpected effect");
        };
        let messages = vec![message];
        if first.is_none() {
            first = Some((messages[0].clone(), messages.as_ptr()));
        }
        outbound.submit(messages).unwrap();
        owner.release_transferred_send(ticket).unwrap();
    }
    let (original, pointer) = first.unwrap();
    (owner, outbound, original, pointer)
}
fn capture(
    owner: &Owner,
    outbound: &mut NativeOutbound,
    limits: RelayOutputLimits,
) -> NativeRelayBatch {
    match NativeRelayBatch::capture(owner, outbound, limits) {
        Ok(Some(batch)) => batch,
        Ok(None) => panic!("missing original"),
        Err(rejected) => panic!("{:?}", rejected.reason),
    }
}
fn cleanup(outbound: &mut NativeOutbound) {
    for batch in outbound.poll(8) {
        outbound
            .complete(batch, LocalSendResult::Cancelled)
            .unwrap();
    }
    assert!(outbound.is_drained());
}

#[test]
fn relay_native_original_batch_encodes_without_rebinding_and_local_sent_keeps_scope() {
    let (owner, mut outbound, original, pointer) = fixture();
    let held = capture(&owner, &mut outbound, RelayOutputLimits::default());
    assert_eq!(held.original().messages.as_ptr(), pointer);
    assert_eq!(held.original().messages, vec![original.clone()]);
    let ticket = held.ticket();
    let codec = NativeWireCodec::new(WireLimits::default()).unwrap();
    let frame = held.encode(&owner, &codec).unwrap();
    let decoded = codec
        .decode_batch(
            WireScope {
                from: original.from,
                sender: original.sender,
                to: original.to,
            },
            &frame,
        )
        .unwrap();
    assert_eq!(decoded, vec![original]);
    let completed = held
        .complete(&mut outbound, LocalSendResult::Sent)
        .unwrap_or_else(|r| panic!("{:?}", r.reason));
    assert_eq!(
        completed.local,
        LocalSendCompletion {
            ticket,
            result: LocalSendResult::Sent
        }
    );
    assert_eq!(completed.requests.ticket(), ticket);
    assert_eq!(
        completed.requests.current_requests(&owner),
        1,
        "local Sent is not protocol retirement"
    );
    cleanup(&mut outbound);
}

#[test]
fn relay_native_frame_and_completion_refusal_preserve_original_pointer_and_ticket() {
    let (owner, mut outbound, _, pointer) = fixture();
    let held = capture(
        &owner,
        &mut outbound,
        RelayOutputLimits {
            frame_bytes: 1,
            ..RelayOutputLimits::default()
        },
    );
    let ticket = held.ticket();
    let codec = NativeWireCodec::new(WireLimits::default()).unwrap();
    assert_eq!(held.encode(&owner, &codec), Err(RelayOutputError::TooLarge));
    let mut wrong = NativeOutbound::new(
        OutboundBinding {
            generation: OutboundGeneration::new(2).unwrap(),
            ..outbound.binding()
        },
        OutboundLimits::default(),
    )
    .unwrap();
    let rejected = match held.complete(&mut wrong, LocalSendResult::Sent) {
        Ok(_) => panic!("wrong queue accepted"),
        Err(rejected) => rejected,
    };
    assert_eq!(rejected.reason, RelayOutputError::WrongBinding);
    let returned = rejected.output.unwrap();
    assert_eq!(returned.original().messages.as_ptr(), pointer);
    assert_eq!(returned.ticket(), ticket);
    let completed = returned
        .complete(&mut outbound, LocalSendResult::Cancelled)
        .unwrap_or_else(|r| panic!("{:?}", r.reason));
    assert_eq!(completed.requests.current_requests(&owner), 1);
    cleanup(&mut outbound);
}

#[test]
fn relay_native_late_original_cannot_encode_after_its_real_recipient_reply() {
    let (mut owner, mut outbound, original, pointer) = fixture();
    let held = capture(&owner, &mut outbound, RelayOutputLimits::default());
    owner
        .admit(
            original.group,
            Event::Receive(response(
                &original,
                Rpc::Appended {
                    success: true,
                    matching_index: 1,
                },
            )),
        )
        .unwrap();
    owner.advance(MonoTime(0), 1).unwrap();
    assert_eq!(
        held.encode(
            &owner,
            &NativeWireCodec::new(WireLimits::default()).unwrap()
        ),
        Err(RelayOutputError::RetiredRequest)
    );
    assert_eq!(held.original().messages.as_ptr(), pointer);
    let completed = held
        .complete(&mut outbound, LocalSendResult::Cancelled)
        .unwrap_or_else(|r| panic!("{:?}", r.reason));
    assert_eq!(completed.requests.current_requests(&owner), 0);
    assert!(owner
        .core(original.group)
        .unwrap()
        .requests
        .contains_key(&node(3)));
    cleanup(&mut outbound);
}

#[test]
fn relay_native_stale_capture_returns_original_allocation_for_explicit_local_cancel() {
    let (mut owner, mut outbound, original, pointer) = fixture();
    owner
        .admit(
            original.group,
            Event::Receive(response(
                &original,
                Rpc::Appended {
                    success: true,
                    matching_index: 1,
                },
            )),
        )
        .unwrap();
    owner.advance(MonoTime(0), 1).unwrap();
    let rejected =
        match NativeRelayBatch::capture(&owner, &mut outbound, RelayOutputLimits::default()) {
            Ok(_) => panic!("stale request accepted"),
            Err(rejected) => rejected,
        };
    assert_eq!(rejected.reason, RelayOutputError::RetiredRequest);
    assert_eq!(rejected.batches.len(), 1);
    assert_eq!(rejected.batches[0].messages.as_ptr(), pointer);
    for batch in rejected.batches {
        outbound
            .complete(batch, LocalSendResult::Cancelled)
            .unwrap();
    }
    cleanup(&mut outbound);
}
