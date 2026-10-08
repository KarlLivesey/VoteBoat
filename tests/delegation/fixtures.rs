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
#![allow(dead_code)] // Shared deterministic/native public-contract assembly.
pub use crate::source_fixture as base;
use base::{entry, group, op};
use std::collections::BTreeMap;
use voteboat::{
    application::*, bucket_counter::*, delegation::*, directory::*, identity::*, routed::*,
    routing::*, transfer::*, transfer_publication::*, transfer_source::*, transfer_target::*,
};
pub fn id(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
pub fn before() -> ResponsibilityManifest {
    let mut input = base::grant().into_input();
    input.parent = Some(ParentAuthority {
        responsibility: id(500),
        group: group(100),
    });
    ResponsibilityManifest::new(input).unwrap()
}
pub fn after() -> ResponsibilityManifest {
    let mut input = base::intent().after().clone().into_input();
    input.parent = before().input().parent;
    ResponsibilityManifest::new(input).unwrap()
}
pub fn parent() -> ResponsibilityManifest {
    let mut input = base::grant().into_input();
    input.responsibility = id(500);
    input.authority = group(100);
    // A non-root parent proves the update need not change its own delegation.
    input.parent = Some(ParentAuthority {
        responsibility: id(600),
        group: group(200),
    });
    input.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: input.scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: before().input().responsibility,
            group: group(1),
            epoch: before().input().epoch,
        }),
    }]);
    ResponsibilityManifest::new(input).unwrap()
}
pub fn grandparent() -> ResponsibilityManifest {
    let mut input = parent().into_input();
    input.responsibility = id(600);
    input.authority = group(200);
    input.parent = None;
    input.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: input.scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: id(500),
            group: group(100),
            epoch: parent().input().epoch,
        }),
    }]);
    ResponsibilityManifest::new(input).unwrap()
}
pub fn plan() -> DelegationPlan {
    DelegationPlan::new(parent(), before(), after(), op(200)).unwrap()
}
pub fn fresh_directory(p: bool, operations: usize) -> LifecycleDirectory {
    let manifest = if p { parent() } else { before() };
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(manifest.input().authority, vec![manifest]).unwrap(),
            DirectoryLimits {
                operations,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    )
}
pub fn command<A: StateMachine>(app: &mut A, operation: u128, bytes: Vec<u8>) -> A::Receipt {
    app.apply_batch(&[entry(app.applied_index() + 1, operation, bytes)])
        .unwrap()
        .remove(0)
}
pub fn ready_directory(p: bool, operations: usize) -> LifecycleDirectory {
    let mut d = fresh_directory(p, operations);
    let bootstrap = d.directory().bootstrap_command(65536).unwrap();
    command(&mut d, 1000, bootstrap);
    command(
        &mut d,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: if p { parent() } else { before() },
        }
        .encode(65536)
        .unwrap(),
    );
    d
}
pub fn reservation(parent: &LifecycleDirectory) -> DelegationReservationStatus {
    let DirectoryRead::DelegationReservation(Some(s)) = parent
        .read_at(
            parent.applied_index(),
            DirectoryQuery::DelegationReservation(op(400)),
        )
        .unwrap()
    else {
        panic!("reservation")
    };
    s
}
pub fn fresh_source() -> base::Source {
    fresh_source_for(before())
}
pub fn fresh_source_for(grant: ResponsibilityManifest) -> base::Source {
    TransferSource::new(
        RoutedApplication::new(
            group(20),
            grant,
            BucketCounter::new(base::range(0, 256), base::Policy, base::bucket_limits()).unwrap(),
            base::Policy,
            RoutedLimits {
                operations: 32,
                semantic_bytes: 8192,
                payload_bytes: 1024,
                inner_checkpoint_bytes: base::bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|e| panic!("{:?}", e.error)),
        65536,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn fresh_target(
    g: u128,
    intent: &TransferIntent,
) -> TransferTarget<BucketCounter<base::Policy>, base::Policy> {
    TransferTarget::new(
        group(g),
        op(200),
        intent.clone(),
        BucketCounter::new(
            if g == 21 {
                base::range(0, 128)
            } else {
                base::range(128, 256)
            },
            base::Policy,
            base::bucket_limits(),
        )
        .unwrap(),
        base::Policy,
        TargetLimits {
            import_bytes: 65536,
            application_checkpoint_bytes: base::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub struct Cache(pub BTreeMap<ResponsibilityIdentity, ResponsibilityManifest>);
impl ManifestCache for Cache {
    fn get(&self, r: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.0.get(&r)
    }
    fn admit(
        &mut self,
        m: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        self.0.insert(m.input().responsibility, m);
        Ok(())
    }
    fn invalidate(&mut self, r: ResponsibilityIdentity, _: RouteGeneration) -> bool {
        self.0.remove(&r).is_some()
    }
    fn limits(&self) -> ManifestCacheLimits {
        ManifestCacheLimits {
            manifests: 4,
            bytes: 65536,
        }
    }
    fn usage(&self) -> ManifestCacheUsage {
        ManifestCacheUsage {
            manifests: self.0.len(),
            bytes: self.0.values().map(|m| m.retained_bytes()).sum(),
        }
    }
}
pub fn recover_directory(d: &LifecycleDirectory, p: bool, operations: usize) -> LifecycleDirectory {
    let cp = d.checkpoint(1000000).unwrap();
    let mut recovered = fresh_directory(p, operations);
    recovered
        .restore_checkpoint(d.schema_version(), d.applied_index(), &cp)
        .unwrap();
    assert_eq!(cp, recovered.checkpoint(1000000).unwrap());
    recovered
}
pub fn completed_child(
    intent: &TransferIntent,
    child: &mut LifecycleDirectory,
) -> (
    base::Source,
    Vec<TransferTarget<BucketCounter<base::Policy>, base::Policy>>,
    TransferPublicationStatus,
) {
    assert_eq!(
        command(child, 200, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let mut source = fresh_source_for(intent.before().clone());
    let boot = source.bootstrap_command(65536).unwrap();
    command(&mut source, 100, boot);
    command(&mut source, 1, base::data(1, 7));
    command(&mut source, 2, base::data(200, 11));
    let mut targets = [21, 22]
        .into_iter()
        .map(|g| fresh_target(g, intent))
        .collect::<Vec<_>>();
    for target in &mut targets {
        let boot = target.bootstrap_command(65536).unwrap();
        command(target, 200, boot);
    }
    command(
        &mut source,
        200,
        base::Source::freeze_command(intent, 65536).unwrap(),
    );
    let SourceRead::Freeze(Some(frozen)) = source
        .read_at(source.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("freeze")
    };
    let cfg = ConfigurationId::new(1).unwrap();
    let mut ready = Vec::new();
    for (g, target) in [21, 22].into_iter().zip(&mut targets) {
        let digest = frozen
            .exports
            .iter()
            .find(|e| e.target == group(g))
            .unwrap()
            .digest;
        let import = TargetImport::new(
            op(200),
            intent.clone(),
            group(g),
            vec![SourceImport {
                fence: frozen.fence,
                configuration: cfg,
                image: source.export_target(group(g), 65536).unwrap(),
                digest,
            }],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let bytes = target.import_command(&import, 65536).unwrap();
        command(target, 200, bytes);
        ready.push(
            TargetReadyEvidence::from_status(cfg, target.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        );
    }
    let publication = TransferPublication::new(
        op(200),
        intent.clone(),
        vec![SourceFenceEvidence::from_status(cfg, frozen).unwrap_or_else(|e| panic!("{:?}", e.0))],
        ready,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert_eq!(
        command(child, 201, publication.encode(65536).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(2).unwrap())
    );
    let decision = child
        .directory()
        .transfer_publication_at(child.applied_index(), op(200))
        .unwrap()
        .unwrap();
    (source, targets, decision)
}
