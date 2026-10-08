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
#![allow(dead_code)] // Shared by source conformance and native transport histories.
use voteboat::{
    application::*, bucket_counter::*, identity::*, log::*, placement::*, routed::*, routing::*,
    transfer::*, transfer_source::*,
};
#[derive(Clone, Debug)]
pub struct Policy;
impl PartitionPolicy for Policy {
    fn scheme(&self) -> PartitionScheme {
        PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        }
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        match key {
            [b] => Ok((*b).into()),
            _ => Err(RoutingError::InvalidKey),
        }
    }
}
pub type Source = TransferSource<BucketCounter<Policy>, Policy>;
pub fn group(n: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(n).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub fn range(a: u16, b: u16) -> BucketRange {
    BucketRange::new(a, b).unwrap()
}
pub fn op(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
pub fn grant() -> ResponsibilityManifest {
    ResponsibilityManifest::new(ManifestInput {
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(10).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        parent: None,
        authority: group(1),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: Policy.scheme(),
        scope: range(0, 256),
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 1,
            survive_any_single_domain_loss: false,
        },
        state: ResponsibilityState::Active,
        execution: ExecutionMode::Single(group(20)),
    })
    .unwrap()
}
pub fn intent() -> TransferIntent {
    let before = grant();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(21)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(22)),
        },
    ]);
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
pub fn bucket_limits() -> BucketCounterLimits {
    BucketCounterLimits {
        operations: 32,
        semantic_bytes: 8192,
    }
}
pub fn routed() -> RoutedApplication<BucketCounter<Policy>, Policy> {
    RoutedApplication::new(
        group(20),
        grant(),
        BucketCounter::new(range(0, 256), Policy, bucket_limits()).unwrap(),
        Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|r| panic!("{:?}", r.error))
}
pub fn fresh() -> Source {
    TransferSource::new(routed(), 65536).unwrap_or_else(|r| panic!("{:?}", r.0))
}
pub fn hint(key: u8) -> RouteHint {
    let input = grant().into_input();
    RouteHint {
        responsibility: input.responsibility,
        group: group(20),
        application: input.application,
        scheme: input.scheme,
        scope: input.scope,
        bucket: key.into(),
        epoch: input.epoch,
        generation: input.generation,
    }
}
pub fn data(key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
pub fn entry(index: u64, id: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: op(id),
            bytes,
        },
    }
}
pub fn noop(index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Noop,
    }
}
pub fn freeze() -> Vec<u8> {
    Source::freeze_command(&intent(), 32776).unwrap()
}
pub fn ready() -> Source {
    let mut s = fresh();
    s.apply_batch(&[entry(1, 100, s.bootstrap_command(100000).unwrap())])
        .unwrap();
    s
}
