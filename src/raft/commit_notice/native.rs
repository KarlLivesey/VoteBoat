// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    native::{outbound::NativeOutbound, relay::*, runtime::*, wire::NativeWireCodec},
    outbound::*,
    runtime::*,
    wire::*,
    worker::WorkerBinding,
};
type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
fn owner(core: Raft) -> Owner {
    let id = RuntimeOwner {
        store: core.binding,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
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
    EffectOwner::new(
        timed,
        WorkerBinding {
            store: id.store,
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap()
}
#[test]
fn genuine_notice_frame_is_one_way_and_local_sent_failed_cancelled_have_no_ack_drain() {
    for result in [
        LocalSendResult::Sent,
        LocalSendResult::Failed,
        LocalSendResult::Cancelled,
    ] {
        let mut c = Cluster::new(true, false);
        c.propose(1);
        let notice = c.notices().remove(0);
        let core = c.cores.remove(0);
        let binding = OutboundBinding {
            node: core.node,
            store: core.binding,
            generation: OutboundGeneration::new(1).unwrap(),
        };
        let owner = owner(core);
        let mut outbound = NativeOutbound::new(binding, OutboundLimits::default()).unwrap();
        outbound.submit(vec![notice.clone()]).unwrap();
        let batch =
            match NativeRelayBatch::capture(&owner, &mut outbound, RelayOutputLimits::default()) {
                Ok(Some(batch)) => batch,
                _ => panic!("genuine original capture"),
            };
        let codec = NativeWireCodec::with_commit_notices(WireLimits::default()).unwrap();
        let frame = batch.frame(&owner, &codec).unwrap();
        assert_eq!(
            frame.record_kinds(),
            Some([RelayRecordKind::OneWay].as_slice())
        );
        assert_eq!(
            codec
                .decode_batch(
                    WireScope {
                        from: notice.from,
                        sender: notice.sender,
                        to: notice.to
                    },
                    frame.wire_bytes().unwrap()
                )
                .unwrap(),
            std::slice::from_ref(&notice)
        );
        let done = batch
            .complete(&mut outbound, result)
            .unwrap_or_else(|r| panic!("{:?}", r.reason));
        assert_eq!(done.requests.current_requests(&owner), 0);
        assert!(outbound.is_drained());
        let raft = owner.core(notice.group).unwrap();
        assert!(raft.requests.is_empty());
        assert!(raft.admitted_ranges.is_empty());
        assert!(raft.empty_append_aliases.is_empty());
    }
}

#[test]
fn notice_uses_bounded_control_reserve_and_rejected_output_returns_ownership() {
    let mut c = Cluster::new(true, false);
    c.propose(1);
    let notice = c.notices().remove(0);
    let data = c
        .sent
        .iter()
        .find(|m| {
            m.to == notice.to && matches!(&m.rpc,Rpc::Append {entries,..} if !entries.is_empty())
        })
        .unwrap()
        .clone();
    assert_eq!(
        message_cost(&notice, usize::MAX).unwrap().0,
        MessageClass::Control
    );
    assert!(message_cost(&notice, std::mem::size_of::<Message>() - 1).is_err());
    let binding = OutboundBinding {
        node: notice.from,
        store: notice.sender,
        generation: OutboundGeneration::new(1).unwrap(),
    };
    let limits = OutboundLimits::default();
    let mut outbound = NativeOutbound::new(binding, limits).unwrap();
    let mut refused = false;
    for _ in 0..=limits.node.max.batches {
        if let Err(rejected) = outbound.submit(vec![data.clone()]) {
            assert_eq!(rejected.reason, OutboundError::Overloaded);
            assert_eq!(rejected.messages, std::slice::from_ref(&data));
            refused = true;
            break;
        }
    }
    assert!(refused);
    outbound.submit(vec![notice.clone()]).unwrap();
    let mut refused = false;
    for _ in 0..=limits.node.max.batches {
        if let Err(rejected) = outbound.submit(vec![notice.clone()]) {
            assert_eq!(rejected.reason, OutboundError::Overloaded);
            assert_eq!(rejected.messages, std::slice::from_ref(&notice));
            refused = true;
            break;
        }
    }
    assert!(refused);
    for batch in outbound.poll(limits.node.max.batches) {
        outbound
            .complete(batch, LocalSendResult::Cancelled)
            .unwrap();
    }
    assert!(outbound.is_drained());
}
