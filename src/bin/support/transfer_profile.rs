// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    setup::{checked, Failure},
    wire,
};
use std::{collections::BTreeMap, path::Path};
use voteboat::{identity::*, routing::*, transfer::*};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Metadata,
    Source,
    Target,
}
#[derive(Clone, Copy)]
pub struct Binding {
    pub group: GroupIdentity,
    pub role: Role,
    pub bootstrap: OperationId,
    pub grant: Option<OperationId>,
}
pub struct Profile {
    pub operation: TransferOperation,
    pub groups: BTreeMap<u128, Binding>,
    pub retirement: bool,
    pub digest: ContentDigest,
}
impl Profile {
    pub fn load(path: &Path) -> Result<Self, Failure> {
        let text = wire::file(path, 128 * 1024)?;
        Self::parse(&text)
    }
    fn parse(text: &str) -> Result<Self, Failure> {
        let mut lines = text.lines();
        let head = lines
            .next()
            .ok_or("missing transfer profile")?
            .split_whitespace()
            .collect::<Vec<_>>();
        let [version, operation, publication] = head.as_slice() else {
            return Err("invalid transfer profile".into());
        };
        let retirement = match *version {
            "voteboat-transfer-profile-v1" => false,
            "voteboat-transfer-profile-v2-retirement" => true,
            "voteboat-transfer-profile-v3-merge-retirement" => true,
            _ => return Err("invalid transfer profile".into()),
        };
        let first = lines
            .next()
            .ok_or("missing intent")?
            .split_whitespace()
            .collect::<Vec<_>>();
        let ["intent", hex] = first.as_slice() else {
            return Err("expected intent HEX".into());
        };
        let intent = checked(TransferIntent::decode(&wire::unhex(
            hex,
            MAX_TRANSFER_INTENT_BYTES,
        )?))?;
        let merge = *version == "voteboat-transfer-profile-v3-merge-retirement";
        let compatible = if merge {
            matches!(
                intent.before().input().execution,
                ExecutionMode::Partitioned(_)
            ) && matches!(intent.after().input().execution, ExecutionMode::Single(_))
        } else {
            matches!(intent.before().input().execution, ExecutionMode::Single(_))
                && matches!(
                    intent.after().input().execution,
                    ExecutionMode::Partitioned(_)
                )
        };
        if !compatible {
            return Err("transfer profile version does not match split/merge shape".into());
        }
        let operation = TransferOperation::new(intent, op(operation)?, op(publication)?)
            .map_err(|e| format!("transfer plan: {:?}", e.0))?;
        let mut groups = BTreeMap::new();
        for line in lines {
            if groups.len() >= 18 {
                return Err("at most18 transfer groups".into());
            }
            let words = line.split_whitespace().collect::<Vec<_>>();
            let [role, group, incarnation, bootstrap, grant] = words.as_slice() else {
                return Err("expected ROLE GROUP INC BOOTSTRAP GRANT_OR_ZERO".into());
            };
            let role = match *role {
                "metadata" => Role::Metadata,
                "source" => Role::Source,
                "target" => Role::Target,
                _ => return Err("invalid role".into()),
            };
            let group = GroupIdentity {
                id: GroupId::new(group.parse()?).ok_or("invalid group")?,
                incarnation: GroupIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid incarnation")?,
            };
            let binding = Binding {
                group,
                role,
                bootstrap: op(bootstrap)?,
                grant: if *grant == "0" {
                    None
                } else {
                    Some(op(grant)?)
                },
            };
            if groups.insert(group.id.get(), binding).is_some() {
                return Err("duplicate group".into());
            }
        }
        let profile = Self {
            operation,
            groups,
            retirement,
            digest: ContentDigest::sha256(text.as_bytes()),
        };
        profile.validate()?;
        Ok(profile)
    }
    fn validate(&self) -> Result<(), Failure> {
        let p = &self.operation;
        let native = p.intent().before().input();
        if native.application.id.get() != 1
            || native.application.version != 1
            || native.scheme.id.get() != 1
            || native.scheme.version != 1
        {
            return Err(
                "profile requires native byte-bucket adapter1 version1 and scheme1 version1".into(),
            );
        }
        let mut expected = vec![(p.intent().before().input().authority, Role::Metadata)];
        expected.extend(
            p.intent()
                .sources()
                .iter()
                .map(|r| (route_group(r), Role::Source)),
        );
        expected.extend(
            p.intent()
                .targets()
                .iter()
                .map(|r| (route_group(r), Role::Target)),
        );
        if self.groups.len() != expected.len() {
            return Err("profile must name every group exactly once".into());
        }
        for (g, role) in expected {
            let b = self.groups.get(&g.id.get()).ok_or("missing group")?;
            if b.group != g || b.role != role {
                return Err("wrong group binding".into());
            }
            match role {
                Role::Metadata
                    if b.grant.is_some_and(|grant| {
                        grant != b.bootstrap
                            && grant != p.operation()
                            && grant != p.publication_operation()
                    }) && b.bootstrap != p.operation()
                        && b.bootstrap != p.publication_operation() => {}
                Role::Source
                    if b.grant.is_none()
                        && b.bootstrap != p.operation()
                        && b.bootstrap != p.publication_operation() => {}
                Role::Target if b.grant.is_none() && b.bootstrap == p.operation() => (),
                _ => return Err("invalid bootstrap/grant operation binding".into()),
            }
        }
        Ok(())
    }
    pub fn binding(&self, id: &str) -> Result<Binding, Failure> {
        self.groups
            .get(&id.parse::<u128>()?)
            .copied()
            .ok_or_else(|| "unknown profile group".into())
    }
}
pub fn plan(args: &[String], merge: bool) -> Result<String, Failure> {
    use voteboat::placement::PlacementRequirements;
    let [authority, source, left, right, responsibility, split, lifecycle, publication, rest @ ..] =
        args
    else {
        return Err(super::HELP.into());
    };
    let version = match (merge, rest) {
        (true, []) => "voteboat-transfer-profile-v3-merge-retirement",
        (false, []) => "voteboat-transfer-profile-v1",
        (false, [flag]) if flag == "--retirement" => "voteboat-transfer-profile-v2-retirement",
        _ => return Err("expected optional --retirement".into()),
    };
    let group = |s: &str| -> Result<GroupIdentity, Failure> {
        Ok(GroupIdentity {
            id: GroupId::new(s.parse()?).ok_or("invalid group")?,
            incarnation: GroupIncarnation::new(1).unwrap(),
        })
    };
    let authority = group(authority)?;
    let source = group(source)?;
    let left = group(left)?;
    let right = group(right)?;
    let split = split.parse::<u16>()?;
    let routes = |a, b| -> Result<ExecutionMode, Failure> {
        Ok(ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: checked(BucketRange::new(0, split))?,
                target: RouteTarget::Group(a),
            },
            RouteEntry {
                scope: checked(BucketRange::new(split, 256))?,
                target: RouteTarget::Group(b),
            },
        ]))
    };
    let before = checked(ResponsibilityManifest::new(ManifestInput {
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(responsibility.parse()?).ok_or("invalid responsibility")?,
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        parent: None,
        authority,
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        },
        scope: checked(BucketRange::new(0, 256))?,
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 1,
            survive_any_single_domain_loss: false,
        },
        state: ResponsibilityState::Active,
        execution: if merge {
            routes(source, left)?
        } else {
            ExecutionMode::Single(source)
        },
    }))?;
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = if merge {
        ExecutionMode::Single(right)
    } else {
        routes(left, right)?
    };
    let intent = TransferIntent::new(before, checked(ResponsibilityManifest::new(after))?)
        .map_err(|e| format!("invalid transfer: {:?}", e.0))?;
    let mut text = format!(
        "{version} {lifecycle} {publication}\nintent {}\nmetadata {} 1 1000 1001\n",
        wire::hex(&checked(intent.encode(MAX_TRANSFER_INTENT_BYTES))?),
        authority.id.get()
    );
    for route in intent.sources() {
        text.push_str(&format!(
            "source {} 1 100 0\n",
            route_group(&route).id.get()
        ));
    }
    for route in intent.targets() {
        text.push_str(&format!(
            "target {} 1 {lifecycle} 0\n",
            route_group(&route).id.get()
        ));
    }
    Profile::parse(&text)?;
    Ok(text)
}
pub fn op(s: &str) -> Result<OperationId, Failure> {
    OperationId::new(s.parse()?).ok_or_else(|| "invalid operation".into())
}
fn route_group(route: &RouteEntry) -> GroupIdentity {
    let RouteTarget::Group(g) = route.target else {
        unreachable!()
    };
    g
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments() -> Vec<String> {
        ["1", "20", "21", "22", "10", "128", "200", "201"]
            .map(str::to_owned)
            .to_vec()
    }

    #[test]
    fn merge_profile_rejects_mislabeled_missing_and_conflicting_bindings() {
        let text = plan(&arguments(), true).unwrap();
        let profile = Profile::parse(&text).unwrap();
        assert!(profile.retirement);
        assert_eq!(profile.operation.intent().sources().len(), 2);
        assert_eq!(profile.operation.intent().targets().len(), 1);
        for bad in [
            text.replace(
                "voteboat-transfer-profile-v3-merge-retirement",
                "voteboat-transfer-profile-v1",
            ),
            text.replace(
                "voteboat-transfer-profile-v3-merge-retirement",
                "voteboat-transfer-profile-v2-retirement",
            ),
            text.replace("source 21 1 100 0\n", ""),
            text.replace("source 21 1 100 0", "target 21 1 200 0"),
            text.replace("source 21 1 100 0", "source 21 1 200 0"),
            text.replace("target 22 1 200 0", "target 22 2 200 0"),
        ] {
            assert!(Profile::parse(&bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn merge_plan_rejects_overlapping_identities_and_invalid_boundaries() {
        for (at, value) in [
            (2, "20"),
            (3, "20"),
            (5, "0"),
            (5, "256"),
            (5, "257"),
            (7, "200"),
        ] {
            let mut args = arguments();
            args[at] = value.to_owned();
            assert!(plan(&args, true).is_err(), "accepted {args:?}");
        }
        let split = plan(&arguments(), false).unwrap();
        assert!(Profile::parse(&split.replace(
            "voteboat-transfer-profile-v1",
            "voteboat-transfer-profile-v3-merge-retirement"
        ))
        .is_err());
    }
}
