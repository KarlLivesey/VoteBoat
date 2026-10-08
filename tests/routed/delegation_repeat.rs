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
use voteboat::bucket_counter::BucketCounter;
type Target = target_fixture::Target;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MovePhase {
    Reserve,
    Intent,
    Stage(u128),
    Fence(u128),
    Import(u128),
    ChildPublication,
    ParentPublication,
    Activate(u128),
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct MoveObserved {
    initial: Observed,
    reservation: Option<DelegationReservationStatus>,
    intent: Option<TransferIntentStatus>,
    decision: Option<TransferPublicationStatus>,
    parent_publication: Option<DelegationPublicationStatus>,
    sources: Vec<(u128, TargetStatus, Option<SourceFreezeStatus>)>,
    targets: Vec<(u128, Option<TargetStatus>)>,
}
fn op(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn cfg() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn groups(operation: u128) -> (&'static [u128], &'static [u128]) {
    match operation {
        202 => (&[21, 22], &[23]),
        204 => (&[23], &[24, 25]),
        _ => panic!("known movement"),
    }
}
fn target_application(g: u128, intent: &TransferIntent, operation: u128) -> Target {
    let scope = intent
        .targets()
        .iter()
        .find(|r| r.target == RouteTarget::Group(group(g)))
        .unwrap()
        .scope;
    TransferTarget::new(
        group(g),
        op(operation),
        intent.clone(),
        BucketCounter::new(
            scope,
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        target_fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn hint(m: &ResponsibilityManifest, key: u8) -> RouteHint {
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 1,
        bytes: 65536,
    })
    .unwrap();
    cache.admit(m.clone()).unwrap();
    resolve(&cache, &source_fixture::Policy, fixture::id(10), &[key], 1).unwrap()
}
fn data(m: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(m, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn query(m: &ResponsibilityManifest, key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: hint(m, key),
        key: vec![key],
        query: vec![key],
    })
}
struct Moves {
    base: Delegated,
    later: BTreeMap<u128, Vec<Node<Target>>>,
}
impl Moves {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let mut base = Delegated::new(protocol, checkpoint);
        for phase in [
            Phase::Reserve,
            Phase::Intent,
            Phase::Stage(21),
            Phase::Stage(22),
            Phase::Fence,
            Phase::Import(21),
            Phase::Import(22),
            Phase::ChildPublication,
            Phase::ParentPublication,
            Phase::Activate(21),
            Phase::Activate(22),
        ] {
            assert_eq!(base.resume_one(), phase);
        }
        assert_eq!(base.resume_one(), Phase::Done);
        let mut rig = Self {
            base,
            later: BTreeMap::new(),
        };
        rig.write(21, &fixture::after(), 3, 1, 2, 9, false);
        rig
    }
    fn nodes(&mut self, g: u128) -> &mut Vec<Node<Target>> {
        if g == 21 || g == 22 {
            &mut self.base.targets[(g - 21) as usize]
        } else {
            self.later.get_mut(&g).unwrap()
        }
    }
    fn target_status(&mut self, g: u128) -> TargetStatus {
        let clock = self.base.clock;
        let TargetRead::Status(s) = observe(self.nodes(g), &clock, g, TargetQuery::Status) else {
            panic!("target status")
        };
        s
    }
    fn target_fence(&mut self, g: u128) -> Option<SourceFreezeStatus> {
        let clock = self.base.clock;
        let TargetRead::Freeze(s) = observe(self.nodes(g), &clock, g, TargetQuery::Freeze) else {
            panic!("target fence")
        };
        s
    }
    fn observed(&mut self, operation: u128) -> MoveObserved {
        let initial = self.base.observed();
        let DirectoryRead::DelegationReservation(reservation) = observe(
            &mut self.base.parent,
            &self.base.clock,
            100,
            DirectoryQuery::DelegationReservation(op(operation + 200)),
        ) else {
            panic!("reservation")
        };
        let DirectoryRead::DelegationPublication(parent_publication) = observe(
            &mut self.base.parent,
            &self.base.clock,
            100,
            DirectoryQuery::DelegationPublication(op(operation + 200)),
        ) else {
            panic!("parent publication")
        };
        let DirectoryRead::Transfer(intent) = observe(
            &mut self.base.child,
            &self.base.clock,
            1,
            DirectoryQuery::Transfer(op(operation)),
        ) else {
            panic!("intent")
        };
        let DirectoryRead::Publication(decision) = observe(
            &mut self.base.child,
            &self.base.clock,
            1,
            DirectoryQuery::Publication(op(operation)),
        ) else {
            panic!("decision")
        };
        let (sources, targets) = groups(operation);
        MoveObserved {
            initial,
            reservation,
            intent,
            decision,
            parent_publication,
            sources: sources
                .iter()
                .map(|&g| (g, self.target_status(g), self.target_fence(g)))
                .collect(),
            targets: targets
                .iter()
                .map(|&g| {
                    (
                        g,
                        if self.later.contains_key(&g) {
                            Some(self.target_status(g))
                        } else {
                            None
                        },
                    )
                })
                .collect(),
        }
    }
    fn commit(&mut self, g: u128, operation: u128, bytes: Vec<u8>) {
        let clock = self.base.clock;
        campaign(self.nodes(g), &clock, g);
        let _ = propose_recovering(self.nodes(g), &clock, g, operation, bytes);
    }
    fn resume_one(&mut self, operation: u128) -> MovePhase {
        let state = self.observed(operation);
        if state.reservation.is_none() {
            let before = state.initial.child;
            let mut after = before.clone().into_input();
            after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
            after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
            after.execution = if operation == 202 {
                ExecutionMode::Single(group(23))
            } else {
                ExecutionMode::Partitioned(vec![
                    RouteEntry {
                        scope: source_fixture::range(0, 128),
                        target: RouteTarget::Group(group(24)),
                    },
                    RouteEntry {
                        scope: source_fixture::range(128, 256),
                        target: RouteTarget::Group(group(25)),
                    },
                ])
            };
            let plan = DelegationPlan::new(
                state.initial.parent,
                before,
                ResponsibilityManifest::new(after).unwrap(),
                op(operation),
            )
            .unwrap();
            campaign(&mut self.base.parent, &self.base.clock, 100);
            let _ = propose_recovering(
                &mut self.base.parent,
                &self.base.clock,
                100,
                operation + 200,
                plan.encode(65536).unwrap(),
            );
            return MovePhase::Reserve;
        }
        let reservation = state.reservation.unwrap();
        let intent = reservation.child_intent(cfg()).unwrap();
        if state.intent.is_none() {
            campaign(&mut self.base.child, &self.base.clock, 1);
            let _ = propose_recovering(
                &mut self.base.child,
                &self.base.clock,
                1,
                operation,
                intent.encode(65536).unwrap(),
            );
            return MovePhase::Intent;
        }
        assert_eq!(state.intent.unwrap().intent, intent);
        for (g, status) in &state.targets {
            if status.as_ref().is_none_or(|s| s.staged_index.is_none()) {
                if !self.later.contains_key(g) {
                    let nodes = open(
                        configuration(&self.base.root, *g, &[1, 2, 3], NativeOpenMode::Create),
                        &self.base.clock,
                        self.base.protocol,
                        || target_application(*g, &intent, operation),
                    );
                    self.later.insert(*g, nodes);
                }
                let bytes = self.nodes(*g)[0].local().applications[&group(*g)]
                    .bootstrap_command(65536)
                    .unwrap();
                self.commit(*g, operation, bytes);
                return MovePhase::Stage(*g);
            }
        }
        for (g, _, fence) in &state.sources {
            if fence.is_none() {
                let bytes = self.nodes(*g)[0].local().applications[&group(*g)]
                    .freeze_command(&intent, 65536, 65536)
                    .unwrap();
                self.commit(*g, operation, bytes);
                return MovePhase::Fence(*g);
            }
        }
        for (g, status) in &state.targets {
            if status.as_ref().unwrap().imported.is_none() {
                let mut imports = Vec::new();
                for (source, _, fence) in &state.sources {
                    let f = fence.as_ref().unwrap();
                    if let Some(export) = f.exports.iter().find(|e| e.target == group(*g)) {
                        let clock = self.base.clock;
                        let TargetRead::Freeze(_) =
                            observe(self.nodes(*source), &clock, *source, TargetQuery::Freeze)
                        else {
                            panic!("source boundary")
                        };
                        let image = self.nodes(*source)[0].local().applications[&group(*source)]
                            .export_target(group(*g), 65536)
                            .unwrap();
                        imports.push(SourceImport {
                            fence: f.fence,
                            configuration: cfg(),
                            image,
                            digest: export.digest,
                        });
                    }
                }
                let import = TargetImport::new(op(operation), intent.clone(), group(*g), imports)
                    .unwrap_or_else(|e| panic!("{:?}", e.0));
                let bytes = self.nodes(*g)[0].local().applications[&group(*g)]
                    .import_command(&import, 65536)
                    .unwrap();
                self.commit(*g, operation, bytes);
                return MovePhase::Import(*g);
            }
        }
        if state.decision.is_none() {
            let publication = TransferPublication::new(
                op(operation),
                intent.clone(),
                state
                    .sources
                    .iter()
                    .map(|(_, _, f)| {
                        SourceFenceEvidence::from_status(cfg(), f.clone().unwrap())
                            .unwrap_or_else(|e| panic!("{:?}", e.0))
                    })
                    .collect(),
                state
                    .targets
                    .iter()
                    .map(|(_, s)| {
                        TargetReadyEvidence::from_status(cfg(), s.clone().unwrap())
                            .unwrap_or_else(|e| panic!("{:?}", e.0))
                    })
                    .collect(),
            )
            .unwrap_or_else(|e| panic!("{:?}", e.0));
            campaign(&mut self.base.child, &self.base.clock, 1);
            let _ = propose_recovering(
                &mut self.base.child,
                &self.base.clock,
                1,
                operation + 1,
                publication.encode(65536).unwrap(),
            );
            return MovePhase::ChildPublication;
        }
        let decision = state.decision.unwrap();
        if state.parent_publication.is_none() {
            let completion = DelegationCompletion {
                reservation: reservation.operation,
                reservation_index: reservation.index,
                parent_configuration: cfg(),
                child_configuration: cfg(),
                decision: decision.clone(),
            };
            campaign(&mut self.base.parent, &self.base.clock, 100);
            let _ = propose_recovering(
                &mut self.base.parent,
                &self.base.clock,
                100,
                operation + 201,
                completion.encode(100000).unwrap(),
            );
            return MovePhase::ParentPublication;
        }
        assert_eq!(
            state.parent_publication.unwrap().completion.decision,
            decision
        );
        for (g, status) in &state.targets {
            if status.as_ref().unwrap().activated.is_none() {
                let activation = TargetActivation {
                    metadata_configuration: cfg(),
                    decision: decision.clone(),
                };
                let bytes = self.nodes(*g)[0].local().applications[&group(*g)]
                    .activation_command(&activation, 65536)
                    .unwrap();
                self.commit(*g, operation, bytes);
                return MovePhase::Activate(*g);
            }
        }
        MovePhase::Done
    }
    fn drive_base(&mut self) {
        let clock = &self.base.clock;
        drive(&mut self.base.grandparent, clock, |_| true);
        drive(&mut self.base.parent, clock, |_| true);
        drive(&mut self.base.child, clock, |_| true);
        drive(&mut self.base.source, clock, |_| true);
        for t in &mut self.base.targets {
            drive(t, clock, |_| true);
        }
    }
    fn close_later(&mut self) {
        let keys: Vec<_> = self.later.keys().copied().collect();
        for g in keys {
            let mut nodes = self.later.remove(&g).unwrap();
            let clock = self.base.clock;
            if self.base.checkpoint {
                compact(&mut nodes, &clock, g);
            }
            close(nodes, &clock, g, || {
                self.drive_base();
                for t in self.later.values_mut() {
                    drive(t, &clock, |_| true);
                }
            });
        }
    }
    fn restart(&mut self) {
        self.close_later();
        self.base.restart();
        for g in [23, 24, 25] {
            if !self.base.root.join(format!("{g}/1")).exists() {
                continue;
            }
            let operation = if g == 23 { 202 } else { 204 };
            let DirectoryRead::Transfer(Some(original)) = observe(
                &mut self.base.child,
                &self.base.clock,
                1,
                DirectoryQuery::Transfer(op(operation)),
            ) else {
                panic!("target bootstrap")
            };
            let nodes = open(
                configuration(&self.base.root, g, &[1, 2, 3], NativeOpenMode::Recover),
                &self.base.clock,
                self.base.protocol,
                || target_application(g, &original.intent, operation),
            );
            self.later.insert(g, nodes);
        }
    }
    fn check(&mut self, operation: u128, state: &MoveObserved) {
        assert_eq!(state.initial.root, fixture::grandparent());
        assert_eq!(
            state.initial.parent.input().epoch,
            OwnershipEpoch::new(1).unwrap()
        );
        assert_eq!(
            state.initial.parent.input().parent,
            fixture::parent().input().parent
        );
        for m in [
            &state.initial.root,
            &state.initial.parent,
            &state.initial.child,
        ] {
            self.base.cache.admit(m.clone()).unwrap();
        }
        let cold = resolve(
            &self.base.cache,
            &source_fixture::Policy,
            fixture::id(600),
            &[1],
            3,
        );
        if state.decision.is_some() && state.parent_publication.is_none() {
            assert_eq!(cold, Err(RoutingError::WrongChild));
        } else {
            assert_eq!(cold.unwrap(), hint(&state.initial.child, 1));
        }
        let before = state
            .reservation
            .as_ref()
            .map(|s| s.plan.before())
            .unwrap_or(&state.initial.child);
        let clock = self.base.clock;
        for (g, _, fence) in &state.sources {
            let key = if *g == 22 { 200 } else { 1 };
            let value = observe(self.nodes(*g), &clock, *g, query(before, key));
            assert_eq!(
                value,
                if fence.is_some() {
                    TargetRead::Rejected(RoutingError::Fenced)
                } else {
                    TargetRead::Data(if key == 1 { 9 } else { 11 })
                }
            );
        }
        for (g, status) in &state.targets {
            if let Some(status) = status {
                let after = state.reservation.as_ref().unwrap().plan.after();
                let key = if *g == 25 { 200 } else { 1 };
                let value = observe(self.nodes(*g), &clock, *g, query(after, key));
                if status.activated.is_some() {
                    assert!(
                        state.sources.iter().all(|(_, _, f)| f.is_some())
                            && state.decision.is_some()
                            && state.parent_publication.is_some()
                    );
                    assert_eq!(value, TargetRead::Data(if key == 1 { 9 } else { 14 }));
                } else {
                    assert_eq!(value, TargetRead::NotActive);
                }
            }
        }
        assert_eq!(
            operation,
            if before.input().epoch.get() == 2 {
                202
            } else {
                204
            }
        );
    }
    fn parent_outage(&mut self, operation: u128, state: &MoveObserved) {
        self.base.close_parent();
        let clock = self.base.clock;
        for (g, _, _) in &state.sources {
            let key = if *g == 22 { 200 } else { 1 };
            assert_eq!(
                observe(
                    self.nodes(*g),
                    &clock,
                    *g,
                    query(state.reservation.as_ref().unwrap().plan.before(), key)
                ),
                TargetRead::Rejected(RoutingError::Fenced)
            );
        }
        for (g, _) in &state.targets {
            let key = if *g == 25 { 200 } else { 1 };
            assert_eq!(
                observe(
                    self.nodes(*g),
                    &clock,
                    *g,
                    query(state.reservation.as_ref().unwrap().plan.after(), key)
                ),
                TargetRead::NotActive
            );
        }
        self.base.parent = open(
            configuration(&self.base.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
            &clock,
            self.base.protocol,
            parent_metadata,
        );
        assert_eq!(self.observed(operation), *state);
    }
    #[allow(clippy::too_many_arguments)]
    fn write(
        &mut self,
        g: u128,
        m: &ResponsibilityManifest,
        operation: u128,
        key: u8,
        delta: i64,
        value: i64,
        duplicate: bool,
    ) {
        let clock = self.base.clock;
        campaign(self.nodes(g), &clock, g);
        let TargetOutcome::Applied(r) =
            propose_recovering(self.nodes(g), &clock, g, operation, data(m, key, delta)).outcome
        else {
            panic!("write")
        };
        assert_eq!(r.outcome, BucketOutcome::Value(value));
        assert_eq!(r.duplicate, duplicate);
    }
}
fn interrupted(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Moves::new(protocol, checkpoint);
    for operation in [202, 204] {
        let (sources, targets) = groups(operation);
        let phases = std::iter::once(MovePhase::Reserve)
            .chain(std::iter::once(MovePhase::Intent))
            .chain(targets.iter().copied().map(MovePhase::Stage))
            .chain(sources.iter().copied().map(MovePhase::Fence))
            .chain(targets.iter().copied().map(MovePhase::Import))
            .chain([MovePhase::ChildPublication, MovePhase::ParentPublication])
            .chain(targets.iter().copied().map(MovePhase::Activate));
        for phase in phases {
            eprintln!("delegated repeated {protocol:?} checkpoint={checkpoint} operation={operation} phase={phase:?}");
            assert_eq!(rig.resume_one(operation), phase);
            let original = rig.observed(operation);
            rig.check(operation, &original);
            if phase == MovePhase::ChildPublication {
                rig.parent_outage(operation, &original);
            }
            rig.restart();
            let recovered = rig.observed(operation);
            assert_eq!(recovered, original);
            rig.check(operation, &recovered);
        }
        assert_eq!(rig.resume_one(operation), MovePhase::Done);
        if operation == 202 {
            let m = manifest(&mut rig.base.child, &rig.base.clock, 1, fixture::id(10));
            rig.write(23, &m, 4, 200, 3, 14, false);
        }
    }
    let final_state = rig.observed(204);
    let m = final_state.initial.child.clone();
    rig.base.close_all();
    let nodes = rig.later.remove(&23).unwrap();
    let clock = rig.base.clock;
    close(nodes, &clock, 23, || {
        for t in rig.later.values_mut() {
            drive(t, &clock, |_| true);
        }
    });
    for (g, id, key, delta, value) in [
        (24, 1, 1, 7, 7),
        (24, 3, 1, 2, 9),
        (25, 2, 200, 11, 11),
        (25, 4, 200, 3, 14),
    ] {
        rig.write(g, &m, id, key, delta, value, true);
    }
    rig.write(24, &m, 5, 1, 1, 10, false);
    rig.write(25, &m, 6, 200, 2, 16, false);
    rig.restart();
    assert_eq!(rig.observed(204), final_state);
    for (g, key, value) in [(24, 1, 10), (25, 200, 16)] {
        assert_eq!(
            observe(rig.nodes(g), &clock, g, query(&m, key)),
            TargetRead::Data(value)
        );
    }
    for g in [21, 22, 23] {
        assert!(rig.target_fence(g).is_some());
    }
    rig.close_later();
    rig.base.close_all();
    std::fs::remove_dir_all(rig.base.root).unwrap();
}
#[test]
fn tcp_delegated_repeated_moves_recover_from_wal() {
    interrupted(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_delegated_repeated_moves_recover_from_checkpoints() {
    interrupted(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_delegated_repeated_moves_recover_from_wal() {
    interrupted(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_delegated_repeated_moves_recover_from_checkpoints() {
    interrupted(NativePeerProtocol::Quic, true);
}
