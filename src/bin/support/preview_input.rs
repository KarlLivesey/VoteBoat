// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    policy_input,
    setup::{checked, Failure},
};
use std::{collections::BTreeMap, io::Read, path::Path};
use voteboat::{
    bucket_counter::{BucketCounter, BucketCounterLimits},
    identity::*,
    membership::Configuration,
    native::routing::NativeBytePartition,
    placement::ReplicaPlacement,
    quorum::{Limits, Policy, Tree},
    routing::*,
    transfer::*,
};
const FILE_LIMIT: u64 = 512 * 1024;
pub struct Application {
    pub group: GroupIdentity,
    pub app: BucketCounter<NativeBytePartition>,
    pub budget: usize,
}
pub struct Target {
    pub application: Application,
    pub configuration: Configuration,
    pub replicas: BTreeMap<NodeId, ReplicaPlacement>,
}
pub struct Input {
    pub budget: usize,
    pub intent: TransferIntent,
    pub sources: Vec<Application>,
    pub targets: Vec<Target>,
}
struct TargetInput {
    application: Application,
    id: ConfigurationId,
    tree: Tree,
    replicas: BTreeMap<NodeId, ReplicaPlacement>,
    voters: BTreeMap<NodeId, StoreIdentity>,
    learners: BTreeMap<NodeId, StoreIdentity>,
}
fn group(id: &str, incarnation: &str) -> Result<GroupIdentity, Failure> {
    Ok(GroupIdentity {
        id: GroupId::new(id.parse()?).ok_or("invalid group")?,
        incarnation: GroupIncarnation::new(incarnation.parse()?).ok_or("invalid incarnation")?,
    })
}
fn application(words: &[&str], routes: &[RouteEntry]) -> Result<Application, Failure> {
    let [id, incarnation, operations, semantic, budget] = words else {
        return Err("expected GROUP INCARNATION OPERATIONS SEMANTIC_BYTES PAYLOAD_BUDGET".into());
    };
    let group = group(id, incarnation)?;
    let route = routes
        .iter()
        .find(|r| r.target == RouteTarget::Group(group))
        .ok_or("unassigned group")?;
    Ok(Application {
        group,
        budget: budget.parse()?,
        app: BucketCounter::new(
            route.scope,
            NativeBytePartition,
            BucketCounterLimits {
                operations: operations.parse()?,
                semantic_bytes: semantic.parse()?,
            },
        )
        .map_err(|e| format!("application: {:?}", e.error))?,
    })
}
fn target(words: &[&str], routes: &[RouteEntry]) -> Result<TargetInput, Failure> {
    let [id, incarnation, config, operations, semantic, budget, policy @ ..] = words else {
        return Err(
            "expected target GROUP INC CONFIG OPERATIONS SEMANTIC_BYTES PAYLOAD_BUDGET POLICY"
                .into(),
        );
    };
    let application = application(&[id, incarnation, operations, semantic, budget], routes)?;
    let mut tokens = policy.iter().copied();
    let tree = policy_input::tree(&mut tokens, 0, &mut 16384, u64::MAX)?;
    if tokens.next().is_some() {
        return Err("trailing policy tokens".into());
    }
    Ok(TargetInput {
        application,
        id: ConfigurationId::new(config.parse()?).ok_or("invalid config")?,
        tree,
        replicas: BTreeMap::new(),
        voters: BTreeMap::new(),
        learners: BTreeMap::new(),
    })
}
fn replica(words: &[&str], targets: &mut [TargetInput]) -> Result<(), Failure> {
    let [id, inc, node, store, store_inc, domain, role] = words else {
        return Err("expected replica GROUP INC NODE STORE STORE_INC DOMAIN voter|learner".into());
    };
    let group = group(id, inc)?;
    let target = targets
        .iter_mut()
        .find(|t| t.application.group == group)
        .ok_or("declare target first")?;
    let node = NodeId::new(node.parse()?).ok_or("invalid node")?;
    let placement = ReplicaPlacement {
        store: StoreIdentity {
            id: StoreId::new(store.parse()?).ok_or("invalid store")?,
            incarnation: StoreIncarnation::new(store_inc.parse()?)
                .ok_or("invalid store incarnation")?,
        },
        domain: FailureDomainId::new(domain.parse()?).ok_or("invalid domain")?,
    };
    if target.replicas.len() == MAX_PREVIEW_REPLICAS
        || target.replicas.insert(node, placement).is_some()
    {
        return Err("duplicate or excess replicas".into());
    }
    match *role {
        "voter" => {
            target.voters.insert(node, placement.store);
        }
        "learner" => {
            target.learners.insert(node, placement.store);
        }
        _ => return Err("invalid replica role".into()),
    }
    Ok(())
}
fn intent(line: &str) -> Result<TransferIntent, Failure> {
    let words = line.split_whitespace().collect::<Vec<_>>();
    let ["intent", hex] = words.as_slice() else {
        return Err("expected intent HEX".into());
    };
    if hex.len() > 2 * MAX_TRANSFER_INTENT_BYTES || !hex.len().is_multiple_of(2) {
        return Err("invalid intent size".into());
    }
    let bytes = hex
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| {
            let digit = |c: u8| (c as char).to_digit(16).ok_or("invalid hex");
            Ok(((digit(p[0])? << 4) | digit(p[1])?) as u8)
        })
        .collect::<Result<Vec<_>, &str>>()?;
    let intent = checked(TransferIntent::decode(&bytes))?;
    let input = intent.before().input();
    if input.application.id.get() != 1
        || input.application.version != 1
        || input.scheme != NativeBytePartition.scheme()
    {
        return Err(
            "profile requires native bucket adapter1/version1 and byte partition1/version1".into(),
        );
    }
    Ok(intent)
}
pub fn load(path: &Path) -> Result<Input, Failure> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(FILE_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > FILE_LIMIT {
        return Err("preview profile exceeds512KiB".into());
    }
    let mut lines = std::str::from_utf8(&bytes)?.lines();
    let header = lines
        .next()
        .ok_or("missing header")?
        .split_whitespace()
        .collect::<Vec<_>>();
    let ["voteboat-transfer-preview-v1", budget] = header.as_slice() else {
        return Err("invalid preview header".into());
    };
    let budget = budget.parse()?;
    let intent = intent(lines.next().ok_or("missing intent")?)?;
    let before = intent.before().input();
    let source_routes = match &before.execution {
        ExecutionMode::Single(group) => vec![RouteEntry {
            scope: before.scope,
            target: RouteTarget::Group(*group),
        }],
        ExecutionMode::Partitioned(routes) | ExecutionMode::Delegated(routes) => routes.clone(),
    };
    let mut sources = Vec::new();
    let mut targets = Vec::new();
    for (count, line) in lines.enumerate() {
        if count >= 2 * MAX_MANIFEST_ROUTES + MAX_PREVIEW_REPLICAS {
            return Err("excess preview rows".into());
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        match words.as_slice() {
            ["source", rest @ ..] => {
                if sources.len() == MAX_MANIFEST_ROUTES {
                    return Err("excess sources".into());
                }
                sources.push(application(rest, &source_routes)?);
            }
            ["target", rest @ ..] => {
                if targets.len() == MAX_MANIFEST_ROUTES {
                    return Err("excess targets".into());
                }
                targets.push(target(rest, &intent.targets())?);
            }
            ["replica", rest @ ..] => replica(rest, &mut targets)?,
            _ => return Err("unknown preview row".into()),
        }
    }
    let targets = targets
        .into_iter()
        .map(|t| {
            let configuration = checked(Configuration::new(
                t.id,
                checked(Policy::new(t.tree, Limits::default()))?,
                t.voters,
                t.learners,
            ))?;
            Ok(Target {
                application: t.application,
                configuration,
                replicas: t.replicas,
            })
        })
        .collect::<Result<_, Failure>>()?;
    Ok(Input {
        budget,
        intent,
        sources,
        targets,
    })
}
