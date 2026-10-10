// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded trusted file input. It binds intent, never grants placement authority.
use super::*;
use std::{collections::BTreeMap, io::Read};
use voteboat::placement::PlannedVoterChange;

fn line<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    kind: &str,
) -> Result<Vec<&'a str>, Failure> {
    let mut words = lines
        .next()
        .ok_or("truncated drain plan")?
        .split_whitespace();
    if words.next() != Some(kind) {
        return Err(format!("expected drain plan {kind}").into());
    }
    Ok(words.collect())
}
fn peer(words: &[&str], stores: &BTreeMap<NodeId, StoreIdentity>) -> Result<PeerIdentity, Failure> {
    let [node, store, incarnation] = words else {
        return Err("expected NODE STORE INCARNATION".into());
    };
    let peer = PeerIdentity {
        node: crate::administration::node(node)?,
        store: StoreIdentity {
            id: StoreId::new(store.parse()?).ok_or("invalid drain store")?,
            incarnation: StoreIncarnation::new(incarnation.parse()?)
                .ok_or("invalid drain incarnation")?,
        },
    };
    if stores.get(&peer.node) != Some(&peer.store) {
        return Err("drain store differs from deployment".into());
    }
    Ok(peer)
}
pub fn load(
    path: &Path,
    stores: &BTreeMap<NodeId, StoreIdentity>,
    administration: &crate::administration::Administration,
) -> Result<MembershipDrainPlan, Failure> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(65537)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err("drain plan exceeds64KiB".into());
    }
    let mut lines = std::str::from_utf8(&bytes)?.lines();
    if lines.next() != Some("voteboat-counter-drain-v1") {
        return Err("expected voteboat-counter-drain-v1".into());
    }
    let operation = line(&mut lines, "operation")?;
    let [operation] = operation.as_slice() else {
        return Err("expected operation ID".into());
    };
    let operation = OperationId::new(operation.parse()?).ok_or("invalid drain operation")?;
    let owner = peer(&line(&mut lines, "source")?, stores)?;
    let handoff = peer(&line(&mut lines, "handoff")?, stores)?;
    // The existing bounded configuration parser supplies the same tree and
    // exact store semantics as the provisioned membership executor.
    let original = line(&mut lines, "original")?;
    let mut fields = original.into_iter();
    let configuration =
        crate::administration::cid(fields.next().ok_or("missing original configuration")?)?;
    let original = crate::administration::parse_configuration(configuration, fields, stores)?;
    let joint = crate::administration::parse_record(
        "joint",
        line(&mut lines, "joint")?.into_iter(),
        stores,
    )?;
    let finalize = crate::administration::parse_record(
        "final",
        line(&mut lines, "final")?.into_iter(),
        stores,
    )?;
    if lines.next().is_some()
        || !administration.contains(&joint)
        || !administration.contains(&finalize)
    {
        return Err("drain records must exactly match the provisioned administration plan".into());
    }
    checked(MembershipDrainPlan::new(
        owner,
        operation,
        vec![DrainMembershipGroup {
            group: group(),
            original,
            handoff,
            change: PlannedVoterChange { joint, finalize },
        }],
    ))
}
pub(super) fn intent(
    plan: &MembershipDrainPlan,
    words: &[&str],
    sequence: u64,
    operation: OperationId,
) -> Result<(DrainRecord, LeadershipIntent), String> {
    if words.len() != 3 {
        return Err("planned drain requires drain-node SEQUENCE OP".into());
    }
    let record = plan.record(sequence).map_err(|e| format!("{e:?}"))?;
    if operation != record.request.operation {
        return Err("operation differs from original drain plan".into());
    }
    let entry = &plan.groups()[0];
    let intent = LeadershipIntent {
        source: record.owner,
        request: LeadershipTransferRequest {
            operation,
            configuration: entry.original.id(),
            target: entry.handoff,
        },
    };
    Ok((record, intent))
}
pub(super) fn restore(
    plan: Option<&MembershipDrainPlan>,
    latest: Option<&DrainRecord>,
    owner: PeerIdentity,
) -> Result<(), Failure> {
    if let Some(plan) = plan {
        let mut expected = checked(plan.record(latest.map_or(1, |r| r.sequence)))?;
        if expected.owner != owner {
            return Err("drain plan belongs to another local owner".into());
        }
        if let Some(latest) = latest {
            expected.phase = latest.phase;
            if &expected != latest {
                return Err("drain plan differs from original durable intent".into());
            }
        }
    } else if latest.is_some_and(|record| record.plan.is_some()) {
        return Err("membership drain journal requires its original host plan; retained-replica profile cannot resume it".into());
    }
    Ok(())
}
