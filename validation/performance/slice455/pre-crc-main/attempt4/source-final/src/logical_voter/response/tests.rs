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
use crate::{
    quorum::{Limits as PolicyLimits, Policy, Tree},
    secure::PeerIdentity,
    wire::WireLimits,
};
mod decode;
mod limits;
mod providers;
mod reads;
mod receipts;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn bootstrap(group: GroupIdentity, node: u64, store: StoreIdentity) -> Bootstrap {
    let node = NodeId::new(node).unwrap();
    Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), PolicyLimits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, store)]),
    }
}
fn app() -> LogicalApplication {
    configured(
        LogicalLimits::default(),
        NativeLogCodec,
        NativeSnapshotCodec,
    )
    .unwrap()
}
fn configured<C: LogCodec + Clone, S: SnapshotCodec + Clone>(
    limits: LogicalLimits,
    c: C,
    s: S,
) -> Result<LogicalApplication<C, S>, ApplicationError> {
    LogicalApplication::new(
        mapping(),
        bootstrap(group(2), 2, store(2)),
        bootstrap(group(1), 1, store(1)),
        limits,
        c,
        s,
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
    )
}
fn mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2)).unwrap()
}
fn representative() -> RepresentativeBinding {
    RepresentativeBinding {
        epoch: RepresentativeEpoch::new(1).unwrap(),
        physical: PeerIdentity {
            node: NodeId::new(2).unwrap(),
            store: store(2),
        },
        physical_session: StoreSession::new(2).unwrap(),
        logical_session: StoreSession::new(1).unwrap(),
    }
}
fn reference() -> SnapshotRef {
    SnapshotRef {
        store: store(1),
        group: group(1),
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: ConfigurationId::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 123,
        checksum: 12345,
    }
}
fn receipt() -> LogicalReceipt {
    LogicalReceipt {
        index: 4,
        operation: OperationId::new(30).unwrap(),
        first_index: 2,
        outcome: LogicalOutcome::SnapshotPublished(reference()),
        parent_revision: LogRevision::new(2),
        parent_generation: LogGeneration::new(3),
        epoch: RepresentativeEpoch::new(4),
        duplicate: true,
    }
}
fn claim(a: &mut LogicalApplication) -> LogicalReceipt {
    let command = Command {
        operation: OperationId::new(1).unwrap(),
        origin_configuration: ConfigurationId::new(1).unwrap(),
        representative: representative(),
        body: CommandBody::Claim {
            expected_epoch: None,
        },
    };
    let bytes = a.encode_command(&command, usize::MAX).unwrap();
    a.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: command.operation,
            bytes: bytes.into(),
        },
    }])
    .unwrap()
    .remove(0)
}
fn encoded(a: &LogicalApplication, value: &LogicalRead) -> Vec<u8> {
    let mut out = vec![
        0;
        a.limits
            .max_parent_checkpoint_bytes
            .max(a.limits.max_command_bytes + OUTCOME_OVERHEAD)
    ];
    let n = a.encode_read(value, &mut out).unwrap();
    out.truncate(n);
    out
}
fn image(a: &LogicalApplication) -> LogicalImage {
    let LogicalRead::Image(i) = a.read_at(a.applied_index(), LogicalQuery::Image).unwrap() else {
        panic!("claimed image")
    };
    *i
}
fn assert_read(a: &LogicalApplication, value: LogicalRead) {
    let bytes = encoded(a, &value);
    assert_eq!(decode::read(a, &bytes), value);
}
