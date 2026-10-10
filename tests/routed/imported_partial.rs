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
use super::super::{creation, creation_source, insertion};
use super::*;
use voteboat::{
    bucket_counter::{encode_add, BucketCounter, BucketOutcome},
    group_creation::*,
    native::group_creation::*,
    retirement::*,
    scoped_source::*,
};
type Target = target_fixture::Target;
type Guard = RetirementGuard<Target>;
type Source = ScopedTransferSource<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
            DirectoryLimits {
                operations: 48,
                history_bytes: 300000,
            },
        )
        .unwrap()
        .with_remaining_transfer()
        .unwrap_or_else(|_| panic!("schema14")),
    )
}
fn source() -> Source {
    super::retained::source(false)
}
fn target(i: &TransferIntent, g: u128, operation: u128) -> Target {
    let scope = i
        .targets()
        .into_iter()
        .find(|r| r.target == RouteTarget::Group(group(g)))
        .unwrap()
        .scope;
    TransferTarget::new(
        group(g),
        source_fixture::op(operation),
        i.clone(),
        BucketCounter::new(
            scope,
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|_| panic!("target"))
}
fn guard(i: &TransferIntent) -> Guard {
    let t = target(i, 21, 200)
        .with_parent_adoption(4)
        .unwrap_or_else(|_| panic!("parent"))
        .with_partial_delegation(2, 65536)
        .unwrap_or_else(|_| panic!("partial"));
    RetirementGuard::new(t).unwrap_or_else(|_| panic!("guard"))
}
#[derive(Clone)]
struct Run {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
}
impl Run {
    fn open<A>(&self, g: u128, mode: NativeOpenMode, factory: impl Fn() -> A) -> Vec<Node<A>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        open(
            configuration(&self.root, g, &[1, 2, 3], mode),
            &self.clock,
            self.protocol,
            factory,
        )
    }
    fn restart<A>(&self, nodes: &mut Vec<Node<A>>, g: u128, factory: impl Fn() -> A)
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        if self.checkpoint {
            compact(nodes, &self.clock, g);
        }
        let before: Vec<_> = nodes
            .iter()
            .map(|n| {
                n.local().applications[&group(g)]
                    .checkpoint(4_000_000)
                    .unwrap()
            })
            .collect();
        creation::abandon(std::mem::take(nodes), g);
        *nodes = self.open(g, NativeOpenMode::Recover, factory);
        for (n, expected) in nodes.iter().zip(before) {
            assert_eq!(
                n.local().applications[&group(g)]
                    .checkpoint(4_000_000)
                    .unwrap(),
                expected
            );
        }
    }
    fn phase<A>(
        &self,
        nodes: &mut Vec<Node<A>>,
        g: u128,
        operation: u128,
        bytes: Vec<u8>,
        factory: impl Fn() -> A,
    ) where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        eprintln!(
            "partial {:?} checkpoint={} group={g} operation={operation}",
            self.protocol, self.checkpoint
        );
        campaign(nodes, &self.clock, g);
        phase_write(true, nodes, &self.clock, g, operation, bytes.clone());
        self.restart(nodes, g, factory);
        campaign(nodes, &self.clock, g);
        propose_recovering(nodes, &self.clock, g, operation, bytes);
    }
}
struct Rig {
    run: Run,
    metadata: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<Source>>,
    owner: Vec<Node<Guard>>,
    first: TransferIntent,
    children: BTreeMap<u128, Vec<Node<Target>>>,
    bindings: BTreeMap<(u128, NodeId), Vec<u8>>,
}
fn hint(m: &ResponsibilityManifest, g: u128, key: u8) -> RouteHint {
    let m = m.input();
    let scope = match &m.execution {
        ExecutionMode::Single(_) => m.scope,
        ExecutionMode::Delegated(routes) => {
            routes
                .iter()
                .find(|r| r.scope.contains(key.into()))
                .unwrap()
                .scope
        }
        _ => panic!("serving grant"),
    };
    RouteHint {
        responsibility: m.responsibility,
        group: group(g),
        application: m.application,
        scheme: m.scheme,
        scope,
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    }
}
fn query(m: &ResponsibilityManifest, g: u128, key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: hint(m, g, key),
        key: vec![key],
        query: vec![key],
    })
}
fn data(m: &ResponsibilityManifest, g: u128, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(m, g, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
// The same committed metadata authorizes both the insertion intent and each
// replica's durable bootstrap. No local target is created from a route alone.
struct ChildRequest {
    group: u128,
    scope: BucketRange,
    operation: u128,
    reservation: u128,
}
fn insertion(
    run: &Run,
    nodes: &mut Vec<Node<LifecycleDirectory>>,
    before: ResponsibilityManifest,
    request: ChildRequest,
    bindings: &mut BTreeMap<(u128, NodeId), Vec<u8>>,
) -> (TransferIntent, Option<DelegationReservationStatus>) {
    let ChildRequest {
        group: g,
        scope,
        operation,
        reservation: reserve,
    } = request;
    let configs = configuration(&run.root, g, &[1, 2, 3], NativeOpenMode::Recover);
    let mut child = before.clone().into_input();
    child.responsibility = responsibility(g);
    child.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: group(1),
    });
    child.scope = scope;
    child.epoch = OwnershipEpoch::new(1).unwrap();
    child.generation = RouteGeneration::new(1).unwrap();
    child.execution = ExecutionMode::Single(group(g));
    let child = ResponsibilityManifest::new(child).unwrap();
    let creation = GroupCreationIntent {
        authority: group(1),
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: child.input().responsibility,
        bootstrap: configs[0].bootstrap.clone(),
        application: before.input().application,
        mode: GroupCreationMode::Staging,
    };
    run.phase(nodes, 1, g, creation.encode(200000).unwrap(), metadata);
    let _ = manifest(nodes, &run.clock, 1, before.input().responsibility);
    let boundary = nodes[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .state()
        .commit_index;
    let created = nodes[0].local().applications[&group(1)]
        .directory()
        .group_creation_at(boundary, group(g))
        .unwrap()
        .unwrap();
    for config in &configs {
        bindings.insert(
            (g, config.node),
            insertion::establish(nodes, config, &created),
        );
    }
    let after = insertion_manifest(&before, &child, if g == 21 { 20 } else { 21 }, scope);
    let child = InsertionChild::from_creation(child, &created).unwrap();
    if before.input().parent.is_none() {
        (
            TransferIntent::insert_retained_child(before, after, child).unwrap(),
            None,
        )
    } else {
        let parent = manifest(
            nodes,
            &run.clock,
            1,
            source_fixture::grant().input().responsibility,
        );
        let plan = DelegationPlan::retained_insertion(
            parent,
            before,
            after,
            child,
            source_fixture::op(operation),
        )
        .unwrap();
        run.phase(nodes, 1, reserve, plan.encode(200000).unwrap(), metadata);
        let DirectoryRead::DelegationReservation(Some(reservation)) = observe(
            nodes,
            &run.clock,
            1,
            DirectoryQuery::DelegationReservation(source_fixture::op(reserve)),
        ) else {
            panic!("reservation")
        };
        (
            reservation
                .child_intent(ConfigurationId::new(1).unwrap())
                .unwrap(),
            Some(reservation),
        )
    }
}
fn insertion_manifest(
    before: &ResponsibilityManifest,
    child: &ResponsibilityManifest,
    source: u128,
    scope: BucketRange,
) -> ResponsibilityManifest {
    let old = match &before.input().execution {
        ExecutionMode::Single(g) => vec![RouteEntry {
            scope: before.input().scope,
            target: RouteTarget::Group(*g),
        }],
        ExecutionMode::Delegated(routes) => routes.clone(),
        _ => panic!("grant"),
    };
    let mut routes = Vec::new();
    for r in old {
        if r.target == RouteTarget::Group(group(source))
            && r.scope.start() <= scope.start()
            && r.scope.end() >= scope.end()
        {
            if r.scope.start() < scope.start() {
                routes.push(RouteEntry {
                    scope: source_fixture::range(r.scope.start(), scope.start()),
                    target: r.target,
                });
            }
            routes.push(RouteEntry {
                scope,
                target: RouteTarget::Child(ChildAuthority {
                    responsibility: child.input().responsibility,
                    group: group(1),
                    epoch: child.input().epoch,
                }),
            });
            if scope.end() < r.scope.end() {
                routes.push(RouteEntry {
                    scope: source_fixture::range(scope.end(), r.scope.end()),
                    target: r.target,
                });
            }
        } else {
            routes.push(r);
        }
    }
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    after.execution = ExecutionMode::Delegated(routes);
    ResponsibilityManifest::new(after).unwrap()
}
fn initialize_source(run: &Run) -> Vec<Node<Source>> {
    let mut source_nodes = run.open(20, NativeOpenMode::Create, source);
    campaign(&mut source_nodes, &run.clock, 20);
    propose_recovering(
        &mut source_nodes,
        &run.clock,
        20,
        100,
        source().bootstrap_command(200000).unwrap(),
    );
    for (op, key, delta) in [(1, 1, 7), (2, 100, 9), (3, 200, 11), (4, 70, 5)] {
        propose_recovering(
            &mut source_nodes,
            &run.clock,
            20,
            op,
            data(&source_fixture::grant(), 20, key, delta),
        );
    }
    source_nodes
}
fn prepare_owner(
    run: &Run,
    source_nodes: &mut Vec<Node<Source>>,
    first: &TransferIntent,
) -> (Vec<Node<Guard>>, ScopedExportStatus) {
    let mut owner = run.open(21, NativeOpenMode::Recover, || guard(first));
    run.phase(
        &mut owner,
        21,
        200,
        guard(first)
            .owner()
            .unwrap()
            .bootstrap_command(200000)
            .unwrap(),
        || guard(first),
    );
    run.phase(source_nodes, 20, 200, first.encode(200000).unwrap(), source);
    let ScopedSourceRead::Frozen(Some(frozen)) = observe(
        source_nodes,
        &run.clock,
        20,
        ScopedSourceQuery::Frozen(source_fixture::op(200)),
    ) else {
        panic!("fence")
    };
    let image = source_nodes[0].local().applications[&group(20)]
        .export(source_fixture::op(200), 65536)
        .unwrap();
    let cfg = ConfigurationId::new(1).unwrap();
    let import = TargetImport::new(
        source_fixture::op(200),
        first.clone(),
        group(21),
        vec![SourceImport {
            fence: frozen.fence.fence,
            configuration: cfg,
            image,
            digest: frozen.digest,
        }],
    )
    .unwrap();
    run.phase(
        &mut owner,
        21,
        200,
        guard(first)
            .owner()
            .unwrap()
            .import_command(&import, 200000)
            .unwrap(),
        || guard(first),
    );
    assert_eq!(
        observe(
            &mut owner,
            &run.clock,
            21,
            RetirementQuery::Owner(query(first.target_manifest(group(21)).unwrap(), 21, 1))
        ),
        RetirementRead::Owner(TargetRead::NotActive)
    );
    (owner, frozen)
}
fn retry_retained_child(
    rig: &mut Rig,
    g: u128,
    key: u8,
    operation: u128,
    value: i64,
) -> Vec<Node<Target>> {
    let mut child = rig.children.remove(&g).unwrap();
    let grant = child[0].local().applications[&group(g)].grant().clone();
    assert_eq!(
        observe(&mut child, &rig.run.clock, g, query(&grant, g, key)),
        TargetRead::Data(value)
    );
    let r = propose_recovering(
        &mut child,
        &rig.run.clock,
        g,
        operation,
        data(&grant, g, key, value),
    );
    assert!(
        matches!(r.outcome,TargetOutcome::Applied(r) if r.duplicate && r.outcome==BucketOutcome::Value(value))
    );
    child
}
impl Rig {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let run = Run {
            root: std::env::temp_dir().join(format!(
                "voteboat-partial-native-{}-{protocol:?}-{checkpoint}",
                std::process::id()
            )),
            clock: Instant::now(),
            protocol,
            checkpoint,
        };
        std::fs::create_dir_all(&run.root).unwrap();
        let mut metadata_nodes = run.open(1, NativeOpenMode::Create, metadata);
        initialize(&mut metadata_nodes, &run.clock, 1, source_fixture::grant());
        let mut source_nodes = initialize_source(&run);
        let mut bindings = BTreeMap::new();
        let (first, _) = insertion(
            &run,
            &mut metadata_nodes,
            source_fixture::grant(),
            ChildRequest {
                group: 21,
                scope: source_fixture::range(0, 128),
                operation: 200,
                reservation: 0,
            },
            &mut bindings,
        );
        run.phase(
            &mut metadata_nodes,
            1,
            200,
            first.encode(200000).unwrap(),
            metadata,
        );
        let (mut owner, frozen) = prepare_owner(&run, &mut source_nodes, &first);
        let cfg = ConfigurationId::new(1).unwrap();
        let publication = TransferPublication::new(
            source_fixture::op(200),
            first.clone(),
            vec![SourceFenceEvidence::from_scoped_status(cfg, frozen, &first).unwrap()],
            vec![TargetReadyEvidence::from_status(
                cfg,
                owner[0].local().applications[&group(21)]
                    .owner()
                    .unwrap()
                    .status(),
            )
            .unwrap()],
        )
        .unwrap();
        run.phase(
            &mut metadata_nodes,
            1,
            201,
            publication.encode(200000).unwrap(),
            metadata,
        );
        let DirectoryRead::Publication(Some(decision)) = observe(
            &mut metadata_nodes,
            &run.clock,
            1,
            DirectoryQuery::Publication(source_fixture::op(200)),
        ) else {
            panic!("decision")
        };
        run.phase(
            &mut source_nodes,
            20,
            300,
            RetainedGrantAdoption {
                metadata_configuration: cfg,
                decision: decision.clone(),
            }
            .encode(200000)
            .unwrap(),
            source,
        );
        let b = owner[0].local().applications[&group(21)]
            .owner()
            .unwrap()
            .activation_command(
                &TargetActivation {
                    metadata_configuration: cfg,
                    decision,
                },
                200000,
            )
            .unwrap();
        run.phase(&mut owner, 21, 200, b, || guard(&first));
        let mut rig = Self {
            run,
            metadata: metadata_nodes,
            source: source_nodes,
            owner,
            first,
            children: BTreeMap::new(),
            bindings,
        };
        rig.owner_value(100, 9);
        rig
    }
    fn import_delegated_child(
        &mut self,
        child: &mut Vec<Node<Target>>,
        i: &TransferIntent,
        g: u128,
        operation: u128,
        cfg: ConfigurationId,
    ) -> ScopedExportStatus {
        let old = self.owner[0].local().applications[&group(21)]
            .owner()
            .unwrap();
        let frozen = old.scoped_freeze(source_fixture::op(operation)).unwrap();
        let image = old
            .export_scoped(source_fixture::op(operation), 65536)
            .unwrap();
        let import = TargetImport::new(
            source_fixture::op(operation),
            i.clone(),
            group(g),
            vec![SourceImport {
                fence: frozen.fence.fence,
                configuration: cfg,
                image,
                digest: frozen.digest,
            }],
        )
        .unwrap();
        let b = target(i, g, operation)
            .import_command(&import, 200000)
            .unwrap();
        self.run
            .phase(child, g, operation, b, || target(i, g, operation));
        frozen
    }
    fn reserve_remaining(&mut self) -> (DelegationReservationStatus, TransferIntent) {
        let before = self.grant();
        let mut after = before.clone().into_input();
        after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
        after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
        let ExecutionMode::Delegated(routes) = &mut after.execution else {
            panic!("delegated")
        };
        for r in routes {
            if r.target == RouteTarget::Group(group(21)) {
                r.target = RouteTarget::Group(group(40));
            }
        }
        let parent = manifest(
            &mut self.metadata,
            &self.run.clock,
            1,
            source_fixture::grant().input().responsibility,
        );
        let plan = DelegationPlan::move_remaining(
            parent,
            before,
            ResponsibilityManifest::new(after).unwrap(),
            source_fixture::op(3000),
        )
        .unwrap();
        self.run.phase(
            &mut self.metadata,
            1,
            6002,
            plan.encode(200000).unwrap(),
            metadata,
        );
        let DirectoryRead::DelegationReservation(Some(reservation)) = observe(
            &mut self.metadata,
            &self.run.clock,
            1,
            DirectoryQuery::DelegationReservation(source_fixture::op(6002)),
        ) else {
            panic!("reservation")
        };
        let cfg = ConfigurationId::new(1).unwrap();
        let i = reservation.child_intent(cfg).unwrap();
        (reservation, i)
    }
    fn import_remaining(
        &mut self,
        successor: &mut Vec<Node<Target>>,
        i: &TransferIntent,
        cfg: ConfigurationId,
    ) -> SourceFreezeStatus {
        let b = self.owner[0].local().applications[&group(21)]
            .owner()
            .unwrap()
            .freeze_command(i, 65536, 200000)
            .unwrap();
        self.run
            .phase(&mut self.owner, 21, 3000, b, || guard(&self.first));
        let old = &self.owner[0].local().applications[&group(21)];
        let frozen = old.freeze_status().unwrap().unwrap();
        let image = old.export_target(group(40), 65536).unwrap();
        assert_eq!(image.scope(), source_fixture::range(96, 128));
        assert!(matches!(
            observe(
                &mut self.owner,
                &self.run.clock,
                21,
                RetirementQuery::Owner(query(i.before(), 21, 100))
            ),
            RetirementRead::Owner(TargetRead::Rejected(_))
        ));
        let import = TargetImport::new(
            source_fixture::op(3000),
            i.clone(),
            group(40),
            vec![SourceImport {
                fence: frozen.fence,
                configuration: cfg,
                digest: ContentDigest::scope_image(&image),
                image,
            }],
        )
        .unwrap();
        self.run.phase(
            successor,
            40,
            3000,
            target(i, 40, 3000).import_command(&import, 200000).unwrap(),
            || target(i, 40, 3000),
        );
        assert_eq!(
            observe(successor, &self.run.clock, 40, query(i.after(), 40, 100)),
            TargetRead::NotActive
        );
        frozen
    }
    fn reclaim_retired(&mut self, retired: RetirementStatus, b: Vec<u8>, lineage: &[u8]) {
        compact(&mut self.owner, &self.run.clock, 21);
        let requests: Vec<_> = self
            .owner
            .iter_mut()
            .map(|n| n.reclaim(LogLimits::default().max_wal_bytes).unwrap())
            .collect();
        let mut done = [false; 3];
        drive(&mut self.owner, &self.run.clock, |nodes| {
            for (j, n) in nodes.iter_mut().enumerate() {
                if let Some(result) = n.poll_reclaim() {
                    assert_eq!(result.request, requests[j]);
                    let report = result.result.unwrap();
                    assert!(report.after_bytes < report.before_bytes);
                    done[j] = true;
                }
            }
            done.iter().all(|x| *x)
        });
        let logs = creation::abandon(std::mem::take(&mut self.owner), 21);
        for log in logs.values() {
            assert!(log.base_index() >= retired.index);
            assert!(log
                .entries
                .iter()
                .all(|e| !matches!(e.payload, EntryPayload::Command { .. })));
        }
        self.owner = self
            .run
            .open(21, NativeOpenMode::Recover, || guard(&self.first));
        assert_eq!(
            observe(
                &mut self.owner,
                &self.run.clock,
                21,
                RetirementQuery::Status
            ),
            RetirementRead::Status(Some(retired))
        );
        assert_eq!(
            propose_recovering(&mut self.owner, &self.run.clock, 21, 3000, b).outcome,
            RetirementOutcome::Retired(retired)
        );
        assert_eq!(
            self.owner[0].local().applications[&group(21)].retired_lineage(),
            Some(lineage)
        );
    }
    fn grant(&self) -> ResponsibilityManifest {
        self.owner[0].local().applications[&group(21)]
            .owner()
            .unwrap()
            .grant()
            .clone()
    }
    fn owner_value(&mut self, key: u8, value: i64) {
        let grant = self.grant();
        assert_eq!(
            observe(
                &mut self.owner,
                &self.run.clock,
                21,
                RetirementQuery::Owner(query(&grant, 21, key))
            ),
            RetirementRead::Owner(TargetRead::Data(value))
        );
    }
    fn decision(&mut self, operation: u128) -> TransferPublicationStatus {
        let DirectoryRead::Publication(Some(p)) = observe(
            &mut self.metadata,
            &self.run.clock,
            1,
            DirectoryQuery::Publication(source_fixture::op(operation)),
        ) else {
            panic!("decision")
        };
        p
    }
    fn finish_parent(
        &mut self,
        reservation: &DelegationReservationStatus,
        decision: TransferPublicationStatus,
    ) {
        let cfg = ConfigurationId::new(1).unwrap();
        let b = DelegationCompletion {
            reservation: reservation.operation,
            reservation_index: reservation.index,
            parent_configuration: cfg,
            child_configuration: cfg,
            decision,
        }
        .encode(200000)
        .unwrap();
        self.run.phase(
            &mut self.metadata,
            1,
            reservation.operation.get() + 1,
            b,
            metadata,
        );
    }
    fn delegate(&mut self, cycle: u128, scope: BucketRange) {
        let g = 30 + cycle;
        let operation = 2000 + cycle;
        let grant = self.grant();
        let (i, reservation) = insertion(
            &self.run,
            &mut self.metadata,
            grant,
            ChildRequest {
                group: g,
                scope,
                operation,
                reservation: 6000 + cycle * 10,
            },
            &mut self.bindings,
        );
        self.run.phase(
            &mut self.metadata,
            1,
            operation,
            i.encode(200000).unwrap(),
            metadata,
        );
        let mut child = self
            .run
            .open(g, NativeOpenMode::Recover, || target(&i, g, operation));
        self.run.phase(
            &mut child,
            g,
            operation,
            target(&i, g, operation).bootstrap_command(200000).unwrap(),
            || target(&i, g, operation),
        );
        self.run.phase(
            &mut self.owner,
            21,
            operation,
            i.encode(200000).unwrap(),
            || guard(&self.first),
        );
        let cfg = ConfigurationId::new(1).unwrap();
        let frozen = self.import_delegated_child(&mut child, &i, g, operation, cfg);
        self.owner_value(100, 9);
        let grant = self.grant();
        assert!(matches!(
            observe(
                &mut self.owner,
                &self.run.clock,
                21,
                RetirementQuery::Owner(query(&grant, 21, scope.start() as u8))
            ),
            RetirementRead::Owner(TargetRead::Rejected(_))
        ));
        let publication = TransferPublication::new(
            source_fixture::op(operation),
            i.clone(),
            vec![SourceFenceEvidence::from_scoped_status(cfg, frozen, &i).unwrap()],
            vec![TargetReadyEvidence::from_status(
                cfg,
                child[0].local().applications[&group(g)].status(),
            )
            .unwrap()],
        )
        .unwrap();
        self.run.phase(
            &mut self.metadata,
            1,
            operation + 100,
            publication.encode(200000).unwrap(),
            metadata,
        );
        let decision = self.decision(operation);
        self.finish_parent(&reservation.unwrap(), decision.clone());
        let b = child[0].local().applications[&group(g)]
            .activation_command(
                &TargetActivation {
                    metadata_configuration: cfg,
                    decision: decision.clone(),
                },
                200000,
            )
            .unwrap();
        self.run
            .phase(&mut child, g, operation, b, || target(&i, g, operation));
        self.run.phase(
            &mut self.owner,
            21,
            7000 + cycle,
            RetainedGrantAdoption {
                metadata_configuration: cfg,
                decision,
            }
            .encode(200000)
            .unwrap(),
            || guard(&self.first),
        );
        self.owner_value(100, 9);
        self.children.insert(g, child);
    }
}
impl Rig {
    fn remaining(&mut self) -> (TransferIntent, TransferPublicationStatus) {
        let (reservation, i) = self.reserve_remaining();
        let cfg = ConfigurationId::new(1).unwrap();
        self.run.phase(
            &mut self.metadata,
            1,
            3000,
            i.encode(200000).unwrap(),
            metadata,
        );
        let mut successor = self
            .run
            .open(40, NativeOpenMode::Create, || target(&i, 40, 3000));
        self.run.phase(
            &mut successor,
            40,
            3000,
            target(&i, 40, 3000).bootstrap_command(200000).unwrap(),
            || target(&i, 40, 3000),
        );
        let frozen = self.import_remaining(&mut successor, &i, cfg);
        let p = TransferPublication::new(
            source_fixture::op(3000),
            i.clone(),
            vec![SourceFenceEvidence::from_status(cfg, frozen).unwrap()],
            vec![TargetReadyEvidence::from_status(
                cfg,
                successor[0].local().applications[&group(40)].status(),
            )
            .unwrap()],
        )
        .unwrap();
        self.run.phase(
            &mut self.metadata,
            1,
            3001,
            p.encode(200000).unwrap(),
            metadata,
        );
        let decision = self.decision(3000);
        self.finish_parent(&reservation, decision.clone());
        let b = successor[0].local().applications[&group(40)]
            .activation_command(
                &TargetActivation {
                    metadata_configuration: cfg,
                    decision: decision.clone(),
                },
                200000,
            )
            .unwrap();
        self.run
            .phase(&mut successor, 40, 3000, b, || target(&i, 40, 3000));
        self.children.insert(40, successor);
        (i, decision)
    }
    fn retire(&mut self, decision: TransferPublicationStatus) {
        let cfg = ConfigurationId::new(1).unwrap();
        let old = &self.owner[0].local().applications[&group(21)];
        let frozen = old.freeze_status().unwrap().unwrap();
        let lineage = old.owner().unwrap().retirement_lineage().unwrap();
        let proof = RetirementProof {
            metadata_configuration: cfg,
            decision,
            targets: vec![TargetActivationEvidence::from_status(
                cfg,
                self.children[&40][0].local().applications[&group(40)].status(),
            )
            .unwrap()],
            release: RetentionRelease {
                source: group(21),
                operation: source_fixture::op(3000),
                fence_index: frozen.fence.index,
                release: source_fixture::op(9900),
            },
        };
        let mut incomplete = proof.clone();
        incomplete.targets.clear();
        assert!(old
            .retirement_command(&incomplete, MAX_RETIREMENT_COMMAND_BYTES)
            .is_err());
        let b = old
            .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
            .unwrap();
        // Preserve the live checkpoint and recover retirement from its WAL tail
        // before allowing a retired checkpoint to replace it.
        campaign(&mut self.owner, &self.run.clock, 21);
        phase_write(true, &mut self.owner, &self.run.clock, 21, 3000, b.clone());
        let retired = self.owner[0].local().applications[&group(21)]
            .status()
            .unwrap();
        let logs = creation::abandon(std::mem::take(&mut self.owner), 21);
        for log in logs.values() {
            assert!(log.base_index() < retired.index);
            assert!(log
                .entries
                .iter()
                .any(|e| e.index == retired.index
                    && matches!(e.payload, EntryPayload::Command { .. })));
        }
        self.owner = self
            .run
            .open(21, NativeOpenMode::Recover, || guard(&self.first));
        for n in &self.owner {
            let app = &n.local().applications[&group(21)];
            assert!(app.owner().is_none());
            assert_eq!(app.status(), Some(retired));
            assert_eq!(app.retired_lineage(), Some(lineage.as_slice()));
            assert_eq!(app.freeze_status().unwrap(), Some(frozen.clone()));
            assert!(app.export_target(group(40), 65536).is_err());
        }
        assert_eq!(
            observe(
                &mut self.owner,
                &self.run.clock,
                21,
                RetirementQuery::Owner(TargetQuery::Status)
            ),
            RetirementRead::Retired
        );
        let retry = propose_recovering(&mut self.owner, &self.run.clock, 21, 3000, b.clone());
        assert_eq!(retry.outcome, RetirementOutcome::Retired(retired));
        assert!(self.owner[0]
            .propose(ClientRequest {
                group: group(21),
                operation: source_fixture::op(9999),
                bytes: data(proof.decision.publication.intent().before(), 21, 100, 1)
            })
            .is_err());
        if self.run.checkpoint {
            self.reclaim_retired(retired, b, &lineage);
        }
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _lock = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Rig::new(protocol, checkpoint);
    rig.delegate(0, source_fixture::range(0, 64));
    rig.delegate(1, source_fixture::range(64, 96));
    let old_children: Vec<_> = [30, 31]
        .into_iter()
        .map(|g| manifest(&mut rig.metadata, &rig.run.clock, 1, responsibility(g)))
        .collect();
    let (remaining, decision) = rig.remaining();
    for old in old_children {
        assert_eq!(
            manifest(
                &mut rig.metadata,
                &rig.run.clock,
                1,
                old.input().responsibility
            ),
            old
        );
    }
    rig.retire(decision);
    for ((g, node), expected) in &rig.bindings {
        let mut binding =
            FileCreationBindings::open(rig.run.root.join(format!("{g}/{}", node.get()))).unwrap();
        assert_eq!(binding.load().unwrap().as_ref(), Some(expected));
    }
    creation::abandon(std::mem::take(&mut rig.owner), 21);
    creation::abandon(std::mem::take(&mut rig.metadata), 1);
    creation::abandon(std::mem::take(&mut rig.source), 20);
    let mut stopped = BTreeMap::new();
    for g in [1, 20, 21] {
        stopped.extend(creation_source::durable_files(
            &rig.run.root.join(g.to_string()),
        ));
    }
    let child30 = retry_retained_child(&mut rig, 30, 1, 1, 7);
    let mut successor = rig.children.remove(&40).unwrap();
    assert_eq!(
        observe(
            &mut successor,
            &rig.run.clock,
            40,
            query(remaining.after(), 40, 100)
        ),
        TargetRead::Data(9)
    );
    let r = propose_recovering(
        &mut successor,
        &rig.run.clock,
        40,
        2,
        data(remaining.after(), 40, 100, 9),
    );
    assert!(
        matches!(r.outcome,TargetOutcome::Applied(r) if r.duplicate && r.outcome==BucketOutcome::Value(9))
    );
    let r = propose_recovering(
        &mut successor,
        &rig.run.clock,
        40,
        99,
        data(remaining.after(), 40, 100, 3),
    );
    assert!(
        matches!(r.outcome,TargetOutcome::Applied(r) if !r.duplicate && r.outcome==BucketOutcome::Value(12))
    );
    rig.run
        .restart(&mut successor, 40, || target(&remaining, 40, 3000));
    assert_eq!(
        observe(
            &mut successor,
            &rig.run.clock,
            40,
            query(remaining.after(), 40, 100)
        ),
        TargetRead::Data(12)
    );
    for g in [1, 20, 21] {
        for (path, bytes) in creation_source::durable_files(&rig.run.root.join(g.to_string())) {
            assert_eq!(stopped[&path], bytes);
        }
    }
    let child31 = retry_retained_child(&mut rig, 31, 70, 4, 5);
    creation::abandon(child30, 30);
    creation::abandon(child31, 31);
    creation::abandon(successor, 40);
    std::fs::remove_dir_all(rig.run.root).unwrap();
}
#[test]
fn tcp_partial_import_wal() {
    history(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_partial_import_checkpoint() {
    history(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_partial_import_wal() {
    history(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_partial_import_checkpoint() {
    history(NativePeerProtocol::Quic, true)
}
