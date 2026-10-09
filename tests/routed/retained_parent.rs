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
//! Real transfer lineage followed by native cross-authority owner adoption.
use super::*;
use voteboat::{reparent_commit::*, reparent_guard::*};

#[derive(Clone, Copy, Debug)]
enum Family {
    Retained,
    Imported,
}
impl Family {
    fn group(self) -> u128 {
        match self {
            Self::Retained => 20,
            Self::Imported => 21,
        }
    }
    fn key(self) -> u8 {
        match self {
            Self::Retained => 200,
            Self::Imported => 1,
        }
    }
}
fn op(n: u128) -> OperationId {
    source_fixture::op(n)
}
fn destination(family: Family) -> ResponsibilityManifest {
    let mut m = source_fixture::grant().into_input();
    m.responsibility = fixture::id(700);
    m.authority = group(200);
    if matches!(family, Family::Imported) {
        m.scope = source_fixture::range(0, 128);
    }
    m.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: m.scope,
        target: RouteTarget::Vacant,
    }]);
    ResponsibilityManifest::new(m).unwrap()
}
fn destination_metadata(family: Family) -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(200), vec![destination(family)]).unwrap(),
            DirectoryLimits {
                operations: 32,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_cross_authority_reparenting()
        .unwrap_or_else(|_| panic!("schema13")),
    )
}
struct Moves {
    base: Retained,
    destination: Vec<Node<LifecycleDirectory>>,
    family: Family,
    plan: CrossReparentPlan,
    cache: NativeManifestCache,
    original: Facts,
    activation: Vec<u8>,
    image: voteboat::scope::ScopeImage,
}
impl Moves {
    fn new(protocol: NativePeerProtocol, checkpoint: bool, family: Family) -> Self {
        let mut base = Retained::profile(
            protocol,
            checkpoint,
            true,
            true,
            match family {
                Family::Retained => "retained-parent",
                Family::Imported => "imported-parent",
            },
        );
        let (original, activation, image) = activate(&mut base);
        let mut destination_nodes = open(
            configuration(&base.root, 200, &[1, 2, 3], NativeOpenMode::Create),
            &base.clock,
            protocol,
            || destination_metadata(family),
        );
        initialize(
            &mut destination_nodes,
            &base.clock,
            200,
            destination(family),
        );
        let parent = original.parent.as_ref().unwrap().manifest.clone();
        let child = original.child.clone().unwrap();
        let (old, id, manifests) = match family {
            Family::Retained => (
                fixture::id(500),
                original.manifest.input().responsibility,
                vec![
                    parent.clone(),
                    original.manifest.clone(),
                    child.clone(),
                    destination(family),
                ],
            ),
            Family::Imported => (
                original.manifest.input().responsibility,
                child.input().responsibility,
                vec![
                    original.manifest.clone(),
                    child.clone(),
                    destination(family),
                ],
            ),
        };
        let plan = CrossReparentPlan::new(old, fixture::id(700), id, manifests).unwrap();
        let mut cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 8,
            bytes: 200000,
        })
        .unwrap()
        .with_cross_authority_reparenting()
        .unwrap_or_else(|_| panic!("cache"));
        // Every imported value below was obtained by activate's quorum reads.
        for m in [parent, original.manifest.clone(), child] {
            cache.admit(m).unwrap();
        }
        let DirectoryRead::Manifest(Some(m)) = observe(
            &mut destination_nodes,
            &base.clock,
            200,
            DirectoryQuery::Manifest(fixture::id(700)),
        ) else {
            panic!("destination")
        };
        cache.admit(m).unwrap();
        Self {
            base,
            destination: destination_nodes,
            family,
            plan,
            cache,
            original,
            activation,
            image,
        }
    }
    fn nodes(&mut self, g: u128) -> &mut Vec<Node<LifecycleDirectory>> {
        match g {
            1 => &mut self.base.metadata,
            100 => &mut self.base.parent,
            200 => &mut self.destination,
            _ => unreachable!(),
        }
    }
    fn query(&mut self, g: u128, q: DirectoryQuery) -> DirectoryRead {
        let clock = self.base.clock;
        observe(self.nodes(g), &clock, g, q)
    }
    fn config(&mut self, g: u128) -> ConfigurationId {
        self.nodes(g)[0]
            .local()
            .owner
            .core(group(g))
            .unwrap()
            .membership()
            .id()
    }
    fn facts(&mut self, g: u128) -> Vec<DirectoryRead> {
        let mut queries = vec![
            DirectoryQuery::ReparentGuard(op(5000)),
            DirectoryQuery::ReparentDecision(op(5000)),
            DirectoryQuery::ReparentPublication(op(5000)),
            DirectoryQuery::ReparentCompletion(op(5000)),
        ];
        for m in self.plan.manifests() {
            if m.input().authority == group(g) {
                queries.push(DirectoryQuery::Manifest(m.input().responsibility));
            }
        }
        match g {
            1 => queries.extend([
                DirectoryQuery::Transfer(op(200)),
                DirectoryQuery::Publication(op(200)),
            ]),
            100 => queries.extend([
                DirectoryQuery::DelegationReservation(op(400)),
                DirectoryQuery::DelegationPublication(op(400)),
            ]),
            _ => {}
        }
        queries.into_iter().map(|q| self.query(g, q)).collect()
    }
    fn reopen_metadata(&mut self, g: u128) {
        let clock = self.base.clock;
        if !self.nodes(g).is_empty() {
            if self.base.checkpoint {
                compact(self.nodes(g), &clock, g);
            }
            creation::abandon(std::mem::take(self.nodes(g)), g);
        }
        let configs = configuration(&self.base.root, g, &[1, 2, 3], NativeOpenMode::Recover);
        let family = self.family;
        let nodes = open(configs, &clock, self.base.protocol, || match g {
            1 => metadata_profile(true, true),
            100 => retained_parent_profile(true),
            200 => destination_metadata(family),
            _ => unreachable!(),
        });
        *self.nodes(g) = nodes;
    }
    fn phase(&mut self, g: u128, id: u128, bytes: Vec<u8>, outcome: DirectoryOutcome) {
        let clock = self.base.clock;
        campaign(self.nodes(g), &clock, g);
        phase_write(true, self.nodes(g), &clock, g, id, bytes.clone());
        let original = self.facts(g);
        self.reopen_metadata(g);
        assert_eq!(self.facts(g), original);
        let r = propose_recovering(self.nodes(g), &clock, g, id, bytes);
        assert!(r.duplicate);
        assert_eq!(r.outcome, outcome);
        assert_eq!(self.facts(g), original);
    }
    fn refresh(&mut self, g: u128) {
        let ids = self
            .plan
            .manifests()
            .iter()
            .filter(|m| m.input().authority == group(g))
            .map(|m| m.input().responsibility)
            .collect::<Vec<_>>();
        for id in ids {
            let DirectoryRead::Manifest(Some(m)) = self.query(g, DirectoryQuery::Manifest(id))
            else {
                panic!("manifest")
            };
            self.cache.admit(m).unwrap();
        }
    }
    fn hint(&self, root: ResponsibilityIdentity) -> Result<RouteHint, RoutingError> {
        resolve(
            &self.cache,
            &source_fixture::Policy,
            root,
            &[self.family.key()],
            4,
        )
    }
    fn value(&mut self, h: RouteHint) -> i64 {
        let q = RoutedQuery {
            hint: h,
            key: vec![self.family.key()],
            query: vec![self.family.key()],
        };
        match self.family {
            Family::Retained => {
                let ScopedSourceRead::Data(RoutedRead::Served(v)) = observe(
                    &mut self.base.source,
                    &self.base.clock,
                    20,
                    ScopedSourceQuery::Data(q),
                ) else {
                    panic!("retained read")
                };
                v
            }
            Family::Imported => {
                let TargetRead::Data(v) = observe(
                    &mut self.base.target,
                    &self.base.clock,
                    21,
                    TargetQuery::Data(q),
                ) else {
                    panic!("imported read")
                };
                v
            }
        }
    }
    fn write(&mut self, h: RouteHint, id: u128, delta: i64, duplicate: bool) {
        let bytes = data(h, self.family.key(), delta);
        let clock = self.base.clock;
        let receipt = match self.family {
            Family::Retained => {
                campaign(&mut self.base.source, &clock, 20);
                let RoutedOutcome::Applied(r) =
                    propose_recovering(&mut self.base.source, &clock, 20, id, bytes).outcome
                else {
                    panic!("retained write")
                };
                r
            }
            Family::Imported => {
                campaign(&mut self.base.target, &clock, 21);
                let TargetOutcome::Applied(r) =
                    propose_recovering(&mut self.base.target, &clock, 21, id, bytes).outcome
                else {
                    panic!("imported write")
                };
                r
            }
        };
        assert_eq!(receipt.duplicate, duplicate);
    }
    fn adoption(&mut self) -> Option<ParentGrantStatus> {
        match self.family {
            Family::Retained => {
                let ScopedSourceRead::ParentAdoption(s) = observe(
                    &mut self.base.source,
                    &self.base.clock,
                    20,
                    ScopedSourceQuery::ParentAdoption(op(6000)),
                ) else {
                    panic!("retained status")
                };
                s
            }
            Family::Imported => {
                let TargetRead::ParentAdoption(s) = observe(
                    &mut self.base.target,
                    &self.base.clock,
                    21,
                    TargetQuery::ParentAdoption(op(6000)),
                ) else {
                    panic!("imported status")
                };
                s
            }
        }
    }
    fn reopen_owner(&mut self) {
        let b = &mut self.base;
        match self.family {
            Family::Retained => {
                if b.checkpoint {
                    compact(&mut b.source, &b.clock, 20);
                }
                creation::abandon(std::mem::take(&mut b.source), 20);
                b.source = open(
                    configuration(&b.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
                    &b.clock,
                    b.protocol,
                    || source_profile(true, true),
                );
            }
            Family::Imported => {
                if b.checkpoint {
                    compact(&mut b.target, &b.clock, 21);
                }
                creation::abandon(std::mem::take(&mut b.target), 21);
                b.target = open(
                    configuration(&b.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
                    &b.clock,
                    b.protocol,
                    || target_profile(&b.intent, true),
                );
            }
        }
    }
    fn adopt(&mut self, bytes: Vec<u8>, unread: bool) -> Option<ParentGrantStatus> {
        let b = &mut self.base;
        match self.family {
            Family::Retained => {
                campaign(&mut b.source, &b.clock, 20);
                if unread {
                    phase_write(true, &mut b.source, &b.clock, 20, 6000, bytes);
                    None
                } else {
                    let RoutedOutcome::ParentAdopted(s) =
                        propose_recovering(&mut b.source, &b.clock, 20, 6000, bytes).outcome
                    else {
                        panic!("retained adoption")
                    };
                    Some(s)
                }
            }
            Family::Imported => {
                campaign(&mut b.target, &b.clock, 21);
                if unread {
                    phase_write(true, &mut b.target, &b.clock, 21, 6000, bytes);
                    None
                } else {
                    let TargetOutcome::ParentAdopted(s) =
                        propose_recovering(&mut b.target, &b.clock, 21, 6000, bytes).outcome
                    else {
                        panic!("imported adoption")
                    };
                    Some(s)
                }
            }
        }
    }
    fn preserved(&mut self) {
        let b = &mut self.base;
        let TargetRead::Status(s) = observe(&mut b.target, &b.clock, 21, TargetQuery::Status)
        else {
            panic!("import status")
        };
        assert_eq!(s, self.original.target);
        let ScopedSourceRead::Frozen(s) = observe(
            &mut b.source,
            &b.clock,
            20,
            ScopedSourceQuery::Frozen(op(200)),
        ) else {
            panic!("source fence")
        };
        assert_eq!(s, self.original.frozen);
        let ScopedSourceRead::Grant(s) = observe(
            &mut b.source,
            &b.clock,
            20,
            ScopedSourceQuery::Grant(op(300)),
        ) else {
            panic!("retained publication")
        };
        assert_eq!(s, self.original.adoption);
        assert_eq!(
            b.source[0].local().applications[&group(20)]
                .export(op(200), 65536)
                .unwrap(),
            self.image
        );
        for (node, expected) in &b.bindings {
            let mut store =
                FileCreationBindings::open(b.root.join(format!("21/{}", node.get()))).unwrap();
            assert_eq!(store.load().unwrap().as_ref(), Some(expected));
        }
        campaign(&mut b.target, &b.clock, 21);
        assert!(
            matches!(propose_recovering(&mut b.target,&b.clock,21,200,self.activation.clone()).outcome,TargetOutcome::Activated(s) if Some(s)==self.original.target.activated)
        );
    }
}
fn run(protocol: NativePeerProtocol, checkpoint: bool, family: Family) {
    let _guard = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut r = Moves::new(protocol, checkpoint, family);
    let original_hint = r.hint(fixture::id(500)).unwrap();
    let original_value = if matches!(family, Family::Retained) {
        11
    } else {
        7
    };
    assert_eq!(r.value(original_hint), original_value);
    let authorities = r
        .plan
        .authorities()
        .iter()
        .map(|g| g.id.get())
        .collect::<Vec<_>>();
    r.phase(
        1,
        5000,
        PrepareReparent {
            plan: r.plan.clone(),
            coordinator: None,
        }
        .encode(MAX_REPARENT_PREPARE_BYTES)
        .unwrap(),
        DirectoryOutcome::ReparentGuarded,
    );
    let DirectoryRead::ReparentGuard(Some(guard)) =
        r.query(1, DirectoryQuery::ReparentGuard(op(5000)))
    else {
        panic!("guard")
    };
    let proof = ReparentGuardEvidence::from_status(r.config(1), &guard).unwrap();
    for &g in &authorities[1..] {
        r.phase(
            g,
            5000,
            PrepareReparent {
                plan: r.plan.clone(),
                coordinator: Some(proof),
            }
            .encode(MAX_REPARENT_PREPARE_BYTES)
            .unwrap(),
            DirectoryOutcome::ReparentGuarded,
        );
    }
    let mut guards = Vec::new();
    for &g in &authorities {
        let DirectoryRead::ReparentGuard(Some(s)) =
            r.query(g, DirectoryQuery::ReparentGuard(op(5000)))
        else {
            panic!("guard")
        };
        guards.push(ReparentGuardEvidence::from_status(r.config(g), &s).unwrap());
    }
    r.write(original_hint, 6100, 1, false);
    r.phase(
        1,
        5001,
        CommitReparent::new(op(5000), guards)
            .unwrap()
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap(),
        DirectoryOutcome::ReparentCommitted,
    );
    let DirectoryRead::ReparentDecision(Some(decision)) =
        r.query(1, DirectoryQuery::ReparentDecision(op(5000)))
    else {
        panic!("decision")
    };
    let publish = PublishReparent {
        configuration: r.config(1),
        decision,
    };
    r.refresh(1);
    assert!(r.hint(fixture::id(500)).is_err());
    assert_eq!(r.hint(fixture::id(700)), Err(RoutingError::Vacant));
    for &g in &authorities[1..] {
        r.phase(
            g,
            5002,
            publish.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap(),
            DirectoryOutcome::ReparentPublished,
        );
    }
    let mut publications = Vec::new();
    let mut child_publication = None;
    for &g in &authorities {
        let DirectoryRead::ReparentPublication(Some(s)) =
            r.query(g, DirectoryQuery::ReparentPublication(op(5000)))
        else {
            panic!("publication")
        };
        if g == 1 {
            child_publication = Some(s);
        }
        publications.push(ReparentPublicationEvidence::from_status(r.config(g), s).unwrap());
    }
    r.phase(
        1,
        5003,
        FinishReparent::new(op(5000), publications)
            .unwrap()
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap(),
        DirectoryOutcome::ReparentCompleted,
    );
    let DirectoryRead::ReparentCompletion(Some(completion)) =
        r.query(1, DirectoryQuery::ReparentCompletion(op(5000)))
    else {
        panic!("completion")
    };
    let release = ReleaseCommittedReparent {
        configuration: r.config(1),
        completion,
    };
    for &g in &authorities[1..] {
        r.phase(
            g,
            5004,
            release.encode().unwrap(),
            DirectoryOutcome::ReparentReleased,
        );
    }
    let command = CrossOwnerParentAdoption {
        plan: r.plan.clone(),
        decision: publish,
        child_configuration: r.config(1),
        child_publication: child_publication.unwrap(),
        completion: release,
    };
    let bytes = command.encode(MAX_CROSS_PARENT_ADOPTION_BYTES).unwrap();
    r.adopt(bytes.clone(), true);
    let adopted = r.adoption().unwrap();
    r.reopen_owner();
    assert_eq!(r.adoption(), Some(adopted));
    assert_eq!(r.adopt(bytes.clone(), false), Some(adopted));
    for &g in &authorities {
        r.refresh(g);
    }
    let hint = r.hint(fixture::id(700)).unwrap();
    assert_eq!(hint.group, group(family.group()));
    assert_eq!(r.value(hint), original_value + 1);
    assert!(r.hint(fixture::id(500)).is_err());
    if matches!(family, Family::Imported) {
        // Moving the imported child must leave the old parent's retained
        // data path usable through refreshed routing (same epoch and scope).
        let h = resolve(
            &r.cache,
            &source_fixture::Policy,
            fixture::id(500),
            &[200],
            4,
        )
        .unwrap();
        assert_eq!(
            source_read(&mut r.base.source, &r.base.clock, h, 200),
            ScopedSourceRead::Data(RoutedRead::Served(11))
        );
        campaign(&mut r.base.source, &r.base.clock, 20);
        assert!(
            matches!(propose_recovering(&mut r.base.source,&r.base.clock,20,6200,data(h,200,2)).outcome,
            RoutedOutcome::Applied(receipt) if receipt.outcome == BucketOutcome::Value(13))
        );
        // Its next transfer still needs an explicit metadata-only grant
        // refresh; ordinary data admission does not compare route generation.
        assert_ne!(
            r.base.source[0].local().applications[&group(20)].grant(),
            r.cache
                .get(r.original.manifest.input().responsibility)
                .unwrap()
        );
    }
    r.preserved();
    let facts = [1, 100, 200].map(|g| r.facts(g));
    let logs = [1, 100, 200].map(|g| creation::abandon(std::mem::take(r.nodes(g)), g));
    let files = [1, 100, 200].map(|g| durable_files(&r.base.root.join(g.to_string())));
    r.write(hint, 6101, 3, false);
    r.reopen_owner();
    assert_eq!(r.adoption(), Some(adopted));
    assert_eq!(r.value(hint), original_value + 4);
    r.write(
        hint,
        if matches!(family, Family::Retained) {
            2
        } else {
            1
        },
        original_value,
        true,
    );
    r.write(hint, 6100, 1, true);
    r.write(hint, 6101, 3, true);
    assert_eq!(r.value(hint), original_value + 4);
    assert_eq!(r.adopt(bytes, false), Some(adopted));
    r.preserved();
    match family {
        Family::Retained => {
            for node in &r.base.source {
                let app = &node.local().applications[&group(20)];
                assert_eq!(app.grant(), &command.after());
                assert_eq!(app.routed().application().outbox().count(), 4);
            }
        }
        Family::Imported => {
            for node in &r.base.target {
                let app = &node.local().applications[&group(21)];
                assert_eq!(app.grant(), &command.after());
                assert_eq!(app.application().outbox().count(), 3);
            }
        }
    }
    for (i, g) in [1, 100, 200].into_iter().enumerate() {
        assert_eq!(durable_files(&r.base.root.join(g.to_string())), files[i]);
        for config in configuration(&r.base.root, g, &[1, 2, 3], NativeOpenMode::Recover) {
            let log = NativeLogStore::recover(
                FileLogIo::open(&config.directory).unwrap(),
                config.store,
                LogLimits::default(),
            )
            .unwrap();
            assert_eq!(log.state(group(g)).unwrap(), logs[i][&config.node]);
        }
        r.reopen_metadata(g);
        assert_eq!(r.facts(g), facts[i]);
    }
    for g in [1, 100, 200] {
        creation::abandon(std::mem::take(r.nodes(g)), g);
    }
    creation::abandon(std::mem::take(&mut r.base.source), 20);
    creation::abandon(std::mem::take(&mut r.base.target), 21);
    std::fs::remove_dir_all(&r.base.root).unwrap();
}
#[test]
fn tcp_retained_parent_move_wal() {
    run(NativePeerProtocol::TcpTls, false, Family::Retained)
}
#[test]
fn tcp_retained_parent_move_checkpoint() {
    run(NativePeerProtocol::TcpTls, true, Family::Retained)
}
#[test]
fn tcp_imported_parent_move_wal() {
    run(NativePeerProtocol::TcpTls, false, Family::Imported)
}
#[test]
fn tcp_imported_parent_move_checkpoint() {
    run(NativePeerProtocol::TcpTls, true, Family::Imported)
}
#[cfg(feature = "quic")]
#[test]
fn quic_retained_parent_move_wal() {
    run(NativePeerProtocol::Quic, false, Family::Retained)
}
#[cfg(feature = "quic")]
#[test]
fn quic_retained_parent_move_checkpoint() {
    run(NativePeerProtocol::Quic, true, Family::Retained)
}
#[cfg(feature = "quic")]
#[test]
fn quic_imported_parent_move_wal() {
    run(NativePeerProtocol::Quic, false, Family::Imported)
}
#[cfg(feature = "quic")]
#[test]
fn quic_imported_parent_move_checkpoint() {
    run(NativePeerProtocol::Quic, true, Family::Imported)
}
