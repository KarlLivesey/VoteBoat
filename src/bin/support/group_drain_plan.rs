// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    administration,
    administration_set::Administrations,
    drain_commands,
    setup::{checked, Failure},
};
use std::{collections::BTreeMap, path::Path};
use voteboat::{drain::*, identity::*, secure::PeerIdentity};

fn source(text: &str, stores: &BTreeMap<NodeId, StoreIdentity>) -> Result<PeerIdentity, Failure> {
    let words = text.split_whitespace().collect::<Vec<_>>();
    let ["source", node, store, incarnation] = words.as_slice() else {
        return Err("expected source NODE STORE INCARNATION".into());
    };
    let peer = PeerIdentity {
        node: administration::node(node)?,
        store: StoreIdentity {
            id: StoreId::new(store.parse()?).ok_or("invalid source store")?,
            incarnation: StoreIncarnation::new(incarnation.parse()?)
                .ok_or("invalid source incarnation")?,
        },
    };
    if stores.get(&peer.node) != Some(&peer.store) {
        return Err("source differs from deployment".into());
    }
    Ok(peer)
}
pub fn load(
    path: &Path,
    stores: &BTreeMap<NodeId, StoreIdentity>,
    admins: Option<&Administrations>,
) -> Result<MembershipDrainPlan, Failure> {
    let data = super::service_setup::material(path, 65536)?;
    let mut total = data.len();
    let mut lines = std::str::from_utf8(&data)?.lines();
    if lines.next() != Some("voteboat-counter-group-drain-v1") {
        return Err("expected voteboat-counter-group-drain-v1".into());
    }
    let operation = lines
        .next()
        .and_then(|l| l.strip_prefix("operation "))
        .ok_or("missing drain operation")?;
    let operation = OperationId::new(operation.parse()?).ok_or("invalid drain operation")?;
    let owner = source(lines.next().ok_or("missing drain source")?, stores)?;
    let mut voters = Vec::new();
    let mut retained = Vec::new();
    let mut previous = None;
    for line in lines {
        if voters.len() + retained.len() == 256 {
            return Err("group drain exceeds 256 assignments".into());
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        let [kind, id, incarnation, rest @ ..] = words.as_slice() else {
            return Err("expected voter or retained group row".into());
        };
        let group = GroupIdentity {
            id: GroupId::new(id.parse()?).ok_or("invalid group")?,
            incarnation: GroupIncarnation::new(incarnation.parse()?)
                .ok_or("invalid group incarnation")?,
        };
        if previous.is_some_and(|old| old >= group) {
            return Err("group drain rows must be sorted and unique".into());
        }
        previous = Some(group);
        match *kind {
            "voter" => {
                let [file] = rest else {
                    return Err("expected voter GROUP INCARNATION PLAN_FILE".into());
                };
                let file = path.parent().unwrap_or(Path::new(".")).join(file);
                // Charge actual input before delegating to the existing bounded parser.
                let bytes = super::service_setup::material(&file, 65536)?;
                total = total
                    .checked_add(bytes.len())
                    .filter(|n| *n <= 1024 * 1024)
                    .ok_or("group drain aggregate input limit")?;
                let plan = drain_commands::parse_plan_for(
                    &bytes,
                    stores,
                    admins
                        .ok_or("voter drain requires group administration")?
                        .get(group)?,
                    group,
                )?;
                let record = checked(plan.record(1))?;
                if record.owner != owner || record.request.operation != operation {
                    return Err("group drain source or operation differs".into());
                }
                voters.push(plan.groups()[0].clone());
            }
            "retained" => {
                let (config, fields) =
                    rest.split_first().ok_or("missing retained configuration")?;
                let original = administration::parse_configuration(
                    administration::cid(config)?,
                    fields.iter().copied(),
                    stores,
                )?;
                retained.push(DrainRetainedGroup { group, original });
            }
            _ => return Err("expected voter or retained group row".into()),
        }
    }
    voters.shrink_to_fit();
    retained.shrink_to_fit();
    checked(MembershipDrainPlan::with_retained_learners(
        owner, operation, voters, retained,
    ))
}
