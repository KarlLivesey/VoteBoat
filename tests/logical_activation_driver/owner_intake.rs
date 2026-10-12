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
//! Real verified-image startup refusal retains the supplied original runtime.
use super::super::*;
use voteboat::native::runtime::{DeadlineQueue, FairScheduler, JitterEntropy};
pub(super) fn short_input_reserves_preserve_original_runtime(image: &VerifiedLogicalImage) {
    for field in 0..4 {
        let mut limits = ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        };
        let window = image
            .activation_control_profile()
            .unwrap()
            .limits()
            .command_bytes
            + std::mem::size_of::<voteboat::raft::Event>()
            + std::mem::size_of::<AdmissionTicket>();
        match field {
            0 => limits.control_items = 1,
            1 => limits.group_control_items = 1,
            2 => limits.control_bytes = limits.control_items * window - 1,
            _ => limits.group_control_bytes = limits.group_control_items * window - 1,
        }
        let runtime = runtime(image, limits);
        let original = runtime
            .core(image.mapping().parent)
            .unwrap()
            .state()
            .clone();
        let authorization = runtime.authorization();
        let rejected = image
            .into_owner(
                runtime,
                image.worker_binding(StorageWorkerGeneration::new(1).unwrap()),
                EffectOwnerLimits::default(),
            )
            .err()
            .expect("short selected original owner input windows accepted");
        assert_eq!(
            rejected.reason,
            EffectOwnerError::Runtime(RuntimeError::InvalidLimits)
        );
        assert_eq!(rejected.runtime.authorization(), authorization);
        assert!(!rejected.runtime.authorization().is_logical());
        assert_eq!(
            rejected
                .runtime
                .core(image.mapping().parent)
                .unwrap()
                .state(),
            &original
        );
        assert!(rejected.runtime.is_drained());
    }
    let owner = image
        .into_owner(
            runtime(
                image,
                ShardLimits {
                    max_groups: 1,
                    ..ShardLimits::default()
                },
            ),
            image.worker_binding(StorageWorkerGeneration::new(1).unwrap()),
            EffectOwnerLimits::default(),
        )
        .unwrap_or_else(|e| panic!("exact selected owner {:?}", e.reason));
    assert!(owner.authorization().is_logical());
    assert!(owner.is_drained());
}
fn runtime(
    image: &VerifiedLogicalImage,
    limits: ShardLimits,
) -> TimedShard<FairScheduler, DeadlineQueue, JitterEntropy> {
    let identity = RuntimeOwner {
        store: image.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let (core, _) = image.recover_parent(&mut fixture::app()).unwrap();
    let mut shard = Shard::new(identity, limits, FairScheduler::new(1).unwrap()).unwrap();
    shard.register(core).unwrap();
    TimedShard::new(
        shard,
        DeadlineQueue::new(identity, 2).unwrap(),
        JitterEntropy::new(19),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap()
}
