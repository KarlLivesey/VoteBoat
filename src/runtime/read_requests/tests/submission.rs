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
use crate::worker::WorkerBinding;
// Empty public-provider host owner; these tests admit no core work or authority.
struct Empty;
impl ReadyScheduler for Empty {
    fn capacity(&self) -> usize {
        1
    }
    fn enqueue(&mut self, _: GroupIdentity) -> Result<(), RuntimeError> {
        Err(RuntimeError::UnknownGroup)
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        None
    }
    fn cancel(&mut self, _: GroupIdentity) {}
    fn len(&self) -> usize {
        0
    }
}
impl TimerService for Empty {
    fn owner(&self) -> RuntimeOwner {
        auth().owner()
    }
    fn capacity(&self) -> usize {
        1
    }
    fn register(
        &mut self,
        _: GroupIdentity,
        _: TimerKind,
        _: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        Err(RuntimeError::SchedulerContract)
    }
    fn cancel(&mut self, _: TimerToken) -> Result<(), RuntimeError> {
        Err(RuntimeError::StaleTicket)
    }
    fn poll(&mut self, _: MonoTime, _: usize) -> Result<Vec<Expiration>, RuntimeError> {
        Ok(vec![])
    }
    fn len(&self) -> usize {
        0
    }
}
impl ElectionEntropy for Empty {
    fn sample(&mut self) -> u64 {
        0
    }
}
fn owner() -> EffectOwner<Empty, Empty, Empty> {
    let shard = Shard::new(
        auth().owner(),
        ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        },
        Empty,
    )
    .unwrap();
    let runtime =
        TimedShard::new(shard, Empty, Empty, TimerConfig::default(), MonoTime(0)).unwrap();
    EffectOwner::new(
        runtime,
        WorkerBinding {
            store: auth().owner().store,
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap()
}
fn unchanged(r: &Requests, owner: &EffectOwner<Empty, Empty, Empty>) {
    assert_eq!(r.sequence, 0);
    assert_eq!(r.usage(), ReadInvocationUsage::default());
    assert_eq!(r.bulk, ReadInvocationUsage::default());
    assert!(r.pending.is_empty() && r.admissions.is_empty() && r.active.is_empty());
    assert!(r.groups.is_empty() && r.bulk_groups.is_empty() && r.is_drained());
    assert!(owner.is_drained() && !owner.is_failed());
}
#[test]
fn typed_original_encoding_runs_once_and_refusal_preserves_query_and_credits() {
    let mut owner = owner();
    let mut r = selected();
    let a = app(group(1));
    r.close();
    let mut original = Vec::with_capacity(64);
    original.extend_from_slice(&[2, 7, 9]);
    let pointer = original.as_ptr();
    let capacity = original.capacity();
    let rejected = r
        .submit_logical(&mut owner, &a, group(1), Query::Business(original))
        .unwrap_err();
    assert_eq!(rejected.reason, ReadInvocationError::Closed);
    let Query::Business(original) = rejected.query else {
        panic!("unchanged original")
    };
    assert_eq!(original.as_ptr(), pointer);
    assert_eq!(original.capacity(), capacity);
    assert_eq!(original, [2, 7, 9]);
    assert_eq!(a.inner().query_encodes(), 1);
    unchanged(&r, &owner);
    let rejected = r
        .submit(&mut owner, &a, group(1), Query::Business(original))
        .unwrap_err();
    assert_eq!(rejected.reason, ReadInvocationError::Closed);
    assert_eq!(
        a.inner().query_encodes(),
        1,
        "generic submit does not encode or select control"
    );
    unchanged(&r, &owner);
}
#[test]
fn wrong_typed_profile_preserves_original_without_encoding_or_spending_admission() {
    let mut owner = owner();
    let mut r = selected();
    let a = app(group(2));
    let mut original = Vec::with_capacity(64);
    original.extend_from_slice(&[2]);
    let pointer = original.as_ptr();
    let rejected = r
        .submit_logical(&mut owner, &a, group(1), Query::Business(original))
        .unwrap_err();
    assert_eq!(rejected.reason, ReadInvocationError::WrongBinding);
    let Query::Business(original) = rejected.query else {
        panic!("unchanged original")
    };
    assert_eq!(original.as_ptr(), pointer);
    assert_eq!(original.capacity(), 64);
    assert_eq!(original, [2]);
    assert_eq!(a.inner().query_encodes(), 0);
    unchanged(&r, &owner);
}
