// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Read-only preflight; no result is an authorization or durability receipt.
use super::{ContentDigest, TransferIntent};
use crate::{
    application::ApplicationError,
    identity::*,
    membership::Configuration,
    placement::{validate_configuration_placement, PlacementError, ReplicaPlacement},
    routing::*,
    scope::{ScopeStateMachine, MAX_SCOPE_IMAGE_BYTES},
};
use std::collections::{BTreeMap, BTreeSet};

pub const TRANSFER_PREVIEW_CONTRACT_VERSION: u32 = 1;
pub const MAX_PREVIEW_REPLICAS: usize = 4096;

/// Caller-owned application view, serialized with its owner. The adapter binding
/// is a host assertion; this function cannot discover an application's identity.
pub struct PreviewSource<'a, A> {
    pub group: GroupIdentity,
    pub adapter: ApplicationAdapter,
    pub application: &'a A,
    pub export_bytes: usize,
}
pub struct PreviewTarget<'a, A> {
    pub group: GroupIdentity,
    pub adapter: ApplicationAdapter,
    pub application: &'a A,
    pub configuration: &'a Configuration,
    pub replicas: &'a BTreeMap<NodeId, ReplicaPlacement>,
    pub import_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreviewError {
    Assignments,
    Budget,
    IncompatibleApplication(GroupIdentity),
    Application(GroupIdentity, ApplicationError),
    Placement(GroupIdentity, PlacementError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreviewExport {
    pub source: GroupIdentity,
    pub target: GroupIdentity,
    pub scope: BucketRange,
    /// Lifetime payload capacity upper bound, not current bytes or WAL retention.
    pub payload_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreviewSourceSummary {
    pub group: GroupIdentity,
    /// Local observation only, never a committed fence or a target log position.
    pub observed_applied: u64,
    pub payload_bytes: usize,
    pub retained_scopes: Vec<BucketRange>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreviewReplica {
    pub node: NodeId,
    pub placement: ReplicaPlacement,
    pub voter: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreviewTargetSummary {
    pub group: GroupIdentity,
    pub scope: BucketRange,
    pub configuration: ConfigurationId,
    pub payload_bytes: usize,
    pub replicas: Vec<PreviewReplica>,
}
/// An immutable planning report. All exported scopes pause from durable source
/// fencing until valid target activation. No finite duration is guaranteed.
/// Ordering after activation is per target group; cross-group atomicity is not
/// supplied. Execution still needs current authorization, original operation
/// IDs, durable fences/imports/publication, retry/outbox preservation and explicit
/// retention release. No resources are reserved and no commands are submitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferPreview {
    pub intent_digest: ContentDigest,
    pub responsibility: ResponsibilityIdentity,
    pub expected_epoch: OwnershipEpoch,
    pub expected_generation: RouteGeneration,
    pub next_epoch: OwnershipEpoch,
    pub sources: Vec<PreviewSourceSummary>,
    pub targets: Vec<PreviewTargetSummary>,
    pub exports: Vec<PreviewExport>,
    pub payload_bytes: usize,
}

/// Bounded, synchronous, nonmutating preflight for checked split/merge intents,
/// including retained/delegated shapes accepted by TransferIntent. Calls only
/// pure provider validation and export bounds: it does not copy/import data.
/// max_payload_bytes bounds the aggregate declared payload (at most64MiB).
/// Input applications may be live local observations or configured templates;
/// callers must label which they supply. Rebuild after state/config changes.
pub fn preview_transfer<A: ScopeStateMachine>(
    intent: &TransferIntent,
    sources: &[PreviewSource<'_, A>],
    targets: &[PreviewTarget<'_, A>],
    max_payload_bytes: usize,
) -> Result<TransferPreview, PreviewError> {
    if max_payload_bytes == 0 || max_payload_bytes > MAX_SCOPE_IMAGE_BYTES {
        return Err(PreviewError::Budget);
    }
    let source_routes = intent.sources();
    let target_routes = intent.targets();
    check_assignments(&source_routes, sources.iter().map(|s| s.group))?;
    check_assignments(&target_routes, targets.iter().map(|t| t.group))?;
    let mut result = TransferPreview {
        intent_digest: ContentDigest::sha256(
            &intent
                .encode(super::MAX_TRANSFER_INTENT_BYTES)
                .map_err(|e| PreviewError::Application(intent.before().input().authority, e))?,
        ),
        responsibility: intent.before().input().responsibility,
        expected_epoch: intent.before().input().epoch,
        expected_generation: intent.before().input().generation,
        next_epoch: intent.after().input().epoch,
        sources: Vec::new(),
        targets: Vec::new(),
        exports: Vec::new(),
        payload_bytes: 0,
    };
    for route in &source_routes {
        let source = sources
            .iter()
            .find(|s| route.target == RouteTarget::Group(s.group))
            .ok_or(PreviewError::Assignments)?;
        inspect_source(
            intent,
            source,
            route.scope,
            &target_routes,
            max_payload_bytes,
            &mut result,
        )?;
    }
    let mut replica_count = 0usize;
    for route in &target_routes {
        let target = targets
            .iter()
            .find(|t| route.target == RouteTarget::Group(t.group))
            .ok_or(PreviewError::Assignments)?;
        replica_count = replica_count
            .checked_add(target.replicas.len())
            .ok_or(PreviewError::Budget)?;
        if replica_count > MAX_PREVIEW_REPLICAS {
            return Err(PreviewError::Budget);
        }
        result.targets.push(inspect_target(
            intent,
            target,
            route.scope,
            &result.exports,
            sources,
        )?);
    }
    Ok(result)
}

fn check_assignments(
    routes: &[RouteEntry],
    groups: impl Iterator<Item = GroupIdentity>,
) -> Result<(), PreviewError> {
    let mut seen = BTreeSet::new();
    for group in groups.take(MAX_MANIFEST_ROUTES + 1) {
        if !seen.insert(group) || !routes.iter().any(|r| r.target == RouteTarget::Group(group)) {
            return Err(PreviewError::Assignments);
        }
    }
    if seen.len() != routes.len() {
        return Err(PreviewError::Assignments);
    }
    Ok(())
}
fn check_application<A: ScopeStateMachine>(
    intent: &TransferIntent,
    group: GroupIdentity,
    adapter: ApplicationAdapter,
    app: &A,
    scope: BucketRange,
) -> Result<(), PreviewError> {
    if adapter != intent.before().input().application
        || app.scheme() != intent.before().input().scheme
        || app.schema_version() == 0
        || app.scope().start() > scope.start()
        || app.scope().end() < scope.end()
    {
        return Err(PreviewError::IncompatibleApplication(group));
    }
    app.validate_group(group)
        .map_err(|e| PreviewError::Application(group, e))
}
fn inspect_source<A: ScopeStateMachine>(
    intent: &TransferIntent,
    source: &PreviewSource<'_, A>,
    scope: BucketRange,
    targets: &[RouteEntry],
    budget: usize,
    result: &mut TransferPreview,
) -> Result<(), PreviewError> {
    check_application(
        intent,
        source.group,
        source.adapter,
        source.application,
        scope,
    )?;
    if source.export_bytes == 0 || source.export_bytes > MAX_SCOPE_IMAGE_BYTES {
        return Err(PreviewError::Budget);
    }
    let mut summary = PreviewSourceSummary {
        group: source.group,
        observed_applied: source.application.applied_index(),
        payload_bytes: 0,
        retained_scopes: Vec::new(),
    };
    for target in targets {
        let start = scope.start().max(target.scope.start());
        let end = scope.end().min(target.scope.end());
        if start >= end {
            continue;
        }
        let range = BucketRange::new(start, end).unwrap();
        let bytes = source
            .application
            .export_scope_bound(range)
            .map_err(|e| PreviewError::Application(source.group, e))?;
        if bytes == 0 || bytes > MAX_SCOPE_IMAGE_BYTES {
            return Err(PreviewError::Budget);
        }
        summary.payload_bytes = summary
            .payload_bytes
            .checked_add(bytes)
            .ok_or(PreviewError::Budget)?;
        result.payload_bytes = result
            .payload_bytes
            .checked_add(bytes)
            .ok_or(PreviewError::Budget)?;
        if summary.payload_bytes > source.export_bytes || result.payload_bytes > budget {
            return Err(PreviewError::Budget);
        }
        let RouteTarget::Group(group) = target.target else {
            return Err(PreviewError::Assignments);
        };
        result.exports.push(PreviewExport {
            source: source.group,
            target: group,
            scope: range,
            payload_bytes: bytes,
        });
    }
    summary.retained_scopes = retained_scopes(intent, source, &result.exports)?;
    result.sources.push(summary);
    Ok(())
}
fn retained_scopes<A: ScopeStateMachine>(
    intent: &TransferIntent,
    source: &PreviewSource<'_, A>,
    exports: &[PreviewExport],
) -> Result<Vec<BucketRange>, PreviewError> {
    let before = intent.before().input();
    let ranges = match &before.execution {
        ExecutionMode::Single(group) if *group == source.group => vec![before.scope],
        ExecutionMode::Partitioned(routes) | ExecutionMode::Delegated(routes) => routes
            .iter()
            .filter(|r| r.target == RouteTarget::Group(source.group))
            .map(|r| r.scope)
            .collect(),
        _ => return Err(PreviewError::Assignments),
    };
    let mut retained = Vec::new();
    for owned in ranges {
        check_application(
            intent,
            source.group,
            source.adapter,
            source.application,
            owned,
        )?;
        let mut cursor = owned.start();
        for export in exports.iter().filter(|e| e.source == source.group) {
            let start = owned.start().max(export.scope.start());
            let end = owned.end().min(export.scope.end());
            if start >= end {
                continue;
            }
            if cursor < start {
                retained.push(BucketRange::new(cursor, start).unwrap());
            }
            cursor = end;
        }
        if cursor < owned.end() {
            retained.push(BucketRange::new(cursor, owned.end()).unwrap());
        }
    }
    Ok(retained)
}
fn inspect_target<A: ScopeStateMachine>(
    intent: &TransferIntent,
    target: &PreviewTarget<'_, A>,
    scope: BucketRange,
    exports: &[PreviewExport],
    sources: &[PreviewSource<'_, A>],
) -> Result<PreviewTargetSummary, PreviewError> {
    check_application(
        intent,
        target.group,
        target.adapter,
        target.application,
        scope,
    )?;
    if target.application.scope() != scope {
        return Err(PreviewError::IncompatibleApplication(target.group));
    }
    if target.replicas.len()
        != target.configuration.voter_stores().len() + target.configuration.learners().len()
        || target
            .replicas
            .values()
            .map(|p| (p.store.id, p.store.incarnation))
            .collect::<BTreeSet<_>>()
            .len()
            != target.replicas.len()
    {
        return Err(PreviewError::Assignments);
    }
    validate_configuration_placement(
        target.configuration,
        target.replicas,
        intent
            .target_manifest(target.group)
            .ok_or(PreviewError::Assignments)?
            .input()
            .placement,
    )
    .map_err(|e| PreviewError::Placement(target.group, e))?;
    let mut payload_bytes = 0usize;
    let mut cursor = scope.start();
    for export in exports.iter().filter(|e| e.target == target.group) {
        let source = sources
            .iter()
            .find(|s| s.group == export.source)
            .ok_or(PreviewError::Assignments)?;
        if source.application.schema_version() != target.application.schema_version()
            || cursor != export.scope.start()
        {
            return Err(PreviewError::IncompatibleApplication(target.group));
        }
        cursor = export.scope.end();
        payload_bytes = payload_bytes
            .checked_add(export.payload_bytes)
            .ok_or(PreviewError::Budget)?;
    }
    if cursor != scope.end() {
        return Err(PreviewError::Assignments);
    }
    if target.import_bytes == 0
        || target.import_bytes > MAX_SCOPE_IMAGE_BYTES
        || payload_bytes > target.import_bytes
    {
        return Err(PreviewError::Budget);
    }
    Ok(PreviewTargetSummary {
        group: target.group,
        scope,
        configuration: target.configuration.id(),
        payload_bytes,
        replicas: target
            .replicas
            .iter()
            .map(|(&node, &placement)| PreviewReplica {
                node,
                placement,
                voter: target.configuration.voter_stores().contains_key(&node),
            })
            .collect(),
    })
}
