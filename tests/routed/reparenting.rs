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
use super::super::{creation, creation_source::durable_files};
use super::*;
use voteboat::{bucket_counter::BucketCounter, reparent_commit::*, reparent_guard::*};
type Owner = TransferSource<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;
fn plan() -> CrossReparentPlan {
    let mut child = source_fixture::grant().into_input();
    child.responsibility = fixture::id(11);
    child.authority = group(3);
    child.scope = source_fixture::range(0, 128);
    child.parent = Some(ParentAuthority {
        responsibility: fixture::id(10),
        group: group(1),
    });
    child.execution = ExecutionMode::Single(group(21));
    let child = ResponsibilityManifest::new(child).unwrap();
    let mut old = source_fixture::grant().into_input();
    old.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: child.input().scope,
            target: RouteTarget::Child(ChildAuthority {
                responsibility: fixture::id(11),
                group: group(3),
                epoch: child.input().epoch,
            }),
        },
        RouteEntry {
            scope: source_fixture::range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    let old = ResponsibilityManifest::new(old).unwrap();
    let mut new = source_fixture::grant().into_input();
    new.responsibility = fixture::id(30);
    new.authority = group(2);
    new.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: child.input().scope,
            target: RouteTarget::Vacant,
        },
        RouteEntry {
            scope: source_fixture::range(128, 256),
            target: RouteTarget::Group(group(40)),
        },
    ]);
    CrossReparentPlan::new(
        fixture::id(10),
        fixture::id(30),
        fixture::id(11),
        vec![old, ResponsibilityManifest::new(new).unwrap(), child],
    )
    .unwrap()
}
fn metadata(g: u128) -> LifecycleDirectory {
    let ms = plan()
        .manifests()
        .iter()
        .filter(|m| m.input().authority == group(g))
        .cloned()
        .collect();
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(g), ms).unwrap(),
            DirectoryLimits {
                operations: 8,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_cross_authority_reparenting()
        .unwrap_or_else(|_| panic!("schema13")),
    )
}
fn owner() -> Owner {
    let p = plan();
    let r = RoutedApplication::new(
        group(21),
        p.child().clone(),
        BucketCounter::new(
            source_fixture::range(0, 128),
            source_fixture::Policy,
            source_fixture::bucket_limits(),
        )
        .unwrap(),
        source_fixture::Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: source_fixture::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("owner"));
    TransferSource::new(
        r.with_cross_authority_parent_adoption(2)
            .unwrap_or_else(|_| panic!("schema4")),
        65536,
    )
    .unwrap_or_else(|_| panic!("source"))
}
struct Rig {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    metadata: Vec<Vec<Node<LifecycleDirectory>>>,
    owner: Vec<Node<Owner>>,
    cache: NativeManifestCache,
}
impl Rig {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-reparent-native-{}-{protocol:?}-{checkpoint}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let p = plan();
        let metadata = (1..=3)
            .map(|g| {
                let mut nodes = open(
                    configuration(&root, g, &[1, 2, 3], NativeOpenMode::Create),
                    &clock,
                    protocol,
                    || metadata(g),
                );
                initialize(
                    &mut nodes,
                    &clock,
                    g,
                    p.manifests()
                        .iter()
                        .find(|m| m.input().authority == group(g))
                        .unwrap()
                        .clone(),
                );
                nodes
            })
            .collect();
        let mut nodes = open(
            configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            owner,
        );
        campaign(&mut nodes, &clock, 21);
        propose_recovering(
            &mut nodes,
            &clock,
            21,
            100,
            owner().bootstrap_command(200000).unwrap(),
        );
        let cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 8,
            bytes: 200000,
        })
        .unwrap()
        .with_cross_authority_reparenting()
        .unwrap_or_else(|_| panic!("cache"));
        let mut r = Self {
            root,
            clock,
            protocol,
            checkpoint,
            metadata,
            owner: nodes,
            cache,
        };
        for m in p.manifests() {
            r.cache.admit(m.clone()).unwrap();
        }
        let h = resolve(&r.cache, &source_fixture::Policy, fixture::id(10), &[1], 3).unwrap();
        r.write(1, 7, h);
        assert_eq!(r.value(h), 7);
        r
    }
    fn config(&self, g: u128) -> ConfigurationId {
        self.metadata[g as usize - 1][0]
            .local()
            .owner
            .core(group(g))
            .unwrap()
            .membership()
            .id()
    }
    fn query(&mut self, g: u128, q: DirectoryQuery) -> DirectoryRead {
        observe(&mut self.metadata[g as usize - 1], &self.clock, g, q)
    }
    fn facts(&mut self, g: u128) -> Vec<DirectoryRead> {
        let p = plan();
        let id = p
            .manifests()
            .iter()
            .find(|m| m.input().authority == group(g))
            .unwrap()
            .input()
            .responsibility;
        [
            DirectoryQuery::Manifest(id),
            DirectoryQuery::ReparentGuard(source_fixture::op(200)),
            DirectoryQuery::ReparentDecision(source_fixture::op(200)),
            DirectoryQuery::ReparentPublication(source_fixture::op(200)),
            DirectoryQuery::ReparentCompletion(source_fixture::op(200)),
        ]
        .into_iter()
        .map(|q| self.query(g, q))
        .collect()
    }
    fn phase(&mut self, g: u128, op: u128, bytes: Vec<u8>) {
        let i = g as usize - 1;
        campaign(&mut self.metadata[i], &self.clock, g);
        phase_write(
            true,
            &mut self.metadata[i],
            &self.clock,
            g,
            op,
            bytes.clone(),
        );
        let facts = self.facts(g);
        if self.checkpoint {
            compact(&mut self.metadata[i], &self.clock, g);
        }
        creation::abandon(std::mem::take(&mut self.metadata[i]), g);
        self.metadata[i] = open(
            configuration(&self.root, g, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || metadata(g),
        );
        assert_eq!(self.facts(g), facts);
        let retry = propose_recovering(&mut self.metadata[i], &self.clock, g, op, bytes);
        assert!(retry.duplicate);
        assert_eq!(
            retry.outcome,
            match op {
                200 => DirectoryOutcome::ReparentGuarded,
                400 => DirectoryOutcome::ReparentCommitted,
                401 => DirectoryOutcome::ReparentPublished,
                500 => DirectoryOutcome::ReparentCompleted,
                501 => DirectoryOutcome::ReparentReleased,
                _ => unreachable!(),
            }
        );
        assert_eq!(self.facts(g), facts);
    }
    fn write(&mut self, op: u128, delta: i64, h: RouteHint) {
        let b = encode_routed(
            h,
            &[1],
            &encode_add(&[1], delta, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        campaign(&mut self.owner, &self.clock, 21);
        assert!(matches!(
            propose_recovering(&mut self.owner, &self.clock, 21, op, b).outcome,
            RoutedOutcome::Applied(_)
        ));
    }
    fn value(&mut self, h: RouteHint) -> i64 {
        let SourceRead::Data(RoutedRead::Served(v)) = observe(
            &mut self.owner,
            &self.clock,
            21,
            SourceQuery::Data(RoutedQuery {
                hint: h,
                key: vec![1],
                query: vec![1],
            }),
        ) else {
            panic!("owner read")
        };
        v
    }
    fn adoption(&mut self) -> Option<ParentGrantStatus> {
        let SourceRead::ParentAdoption(a) = observe(
            &mut self.owner,
            &self.clock,
            21,
            SourceQuery::ParentAdoption(source_fixture::op(300)),
        ) else {
            panic!("adoption")
        };
        a
    }
    fn reopen_owner(&mut self) {
        if self.checkpoint {
            compact(&mut self.owner, &self.clock, 21);
        }
        creation::abandon(std::mem::take(&mut self.owner), 21);
        self.owner = open(
            configuration(&self.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            owner,
        );
    }
}
fn prepare_reparent(r: &mut Rig, p: &CrossReparentPlan) -> Vec<ReparentGuardEvidence> {
    r.phase(
        1,
        200,
        PrepareReparent {
            plan: p.clone(),
            coordinator: None,
        }
        .encode(MAX_REPARENT_PREPARE_BYTES)
        .unwrap(),
    );
    let DirectoryRead::ReparentGuard(Some(guard)) =
        r.query(1, DirectoryQuery::ReparentGuard(source_fixture::op(200)))
    else {
        panic!("guard")
    };
    let proof = ReparentGuardEvidence::from_status(r.config(1), &guard).unwrap();
    for g in [2, 3] {
        r.phase(
            g,
            200,
            PrepareReparent {
                plan: p.clone(),
                coordinator: Some(proof),
            }
            .encode(MAX_REPARENT_PREPARE_BYTES)
            .unwrap(),
        );
    }
    let guards = (1..=3)
        .map(|g| {
            let DirectoryRead::ReparentGuard(Some(s)) =
                r.query(g, DirectoryQuery::ReparentGuard(source_fixture::op(200)))
            else {
                panic!("guard")
            };
            ReparentGuardEvidence::from_status(r.config(g), &s).unwrap()
        })
        .collect();
    guards
}
fn commit_reparent(
    r: &mut Rig,
    p: &CrossReparentPlan,
    guards: Vec<ReparentGuardEvidence>,
) -> CrossOwnerParentAdoption {
    r.phase(
        1,
        400,
        CommitReparent::new(source_fixture::op(200), guards)
            .unwrap()
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap(),
    );
    let DirectoryRead::ReparentDecision(Some(decision)) =
        r.query(1, DirectoryQuery::ReparentDecision(source_fixture::op(200)))
    else {
        panic!("decision")
    };
    let publication = PublishReparent {
        configuration: r.config(1),
        decision,
    };
    r.phase(
        3,
        401,
        publication.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap(),
    );
    let DirectoryRead::Manifest(Some(published_child)) =
        r.query(3, DirectoryQuery::Manifest(fixture::id(11)))
    else {
        panic!("published child")
    };
    assert_eq!(published_child, p.updated_manifests()[2]);
    r.cache.admit(published_child).unwrap();
    assert!(resolve(&r.cache, &source_fixture::Policy, fixture::id(10), &[1], 3).is_err());
    assert_eq!(
        resolve(&r.cache, &source_fixture::Policy, fixture::id(30), &[1], 3),
        Err(RoutingError::Vacant)
    );
    r.phase(
        2,
        401,
        publication.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap(),
    );
    let mut publications = Vec::new();
    let mut child_publication = None;
    for g in 1..=3 {
        let DirectoryRead::ReparentPublication(Some(s)) = r.query(
            g,
            DirectoryQuery::ReparentPublication(source_fixture::op(200)),
        ) else {
            panic!("publication")
        };
        if g == 3 {
            child_publication = Some(s);
        }
        publications.push(ReparentPublicationEvidence::from_status(r.config(g), s).unwrap());
    }
    r.phase(
        1,
        500,
        FinishReparent::new(source_fixture::op(200), publications)
            .unwrap()
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap(),
    );
    let DirectoryRead::ReparentCompletion(Some(completion)) = r.query(
        1,
        DirectoryQuery::ReparentCompletion(source_fixture::op(200)),
    ) else {
        panic!("completion")
    };
    let completed = ReleaseCommittedReparent {
        configuration: r.config(1),
        completion,
    };
    for g in [2, 3] {
        r.phase(g, 501, completed.encode().unwrap());
    }
    CrossOwnerParentAdoption {
        plan: p.clone(),
        decision: publication,
        child_configuration: r.config(3),
        child_publication: child_publication.unwrap(),
        completion: completed,
    }
}
fn run(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut r = Rig::new(protocol, checkpoint);
    let p = plan();
    let original_hint =
        resolve(&r.cache, &source_fixture::Policy, fixture::id(10), &[1], 3).unwrap();
    let guards = prepare_reparent(&mut r, &p);
    r.write(2, 1, original_hint);
    assert_eq!(r.value(original_hint), 8);
    let adoption = commit_reparent(&mut r, &p, guards);
    let bytes = adoption.encode(MAX_CROSS_PARENT_ADOPTION_BYTES).unwrap();
    campaign(&mut r.owner, &r.clock, 21);
    phase_write(true, &mut r.owner, &r.clock, 21, 300, bytes.clone());
    let status = r.adoption().unwrap();
    r.reopen_owner();
    assert_eq!(r.adoption(), Some(status));
    assert_eq!(
        propose_recovering(&mut r.owner, &r.clock, 21, 300, bytes.clone()).outcome,
        RoutedOutcome::ParentAdopted(status)
    );
    for g in 1..=3 {
        let id = if g == 1 {
            fixture::id(10)
        } else if g == 2 {
            fixture::id(30)
        } else {
            fixture::id(11)
        };
        let DirectoryRead::Manifest(Some(m)) = r.query(g, DirectoryQuery::Manifest(id)) else {
            panic!("manifest")
        };
        r.cache.admit(m).unwrap();
    }
    let hint = resolve(&r.cache, &source_fixture::Policy, fixture::id(30), &[1], 3).unwrap();
    assert_eq!(hint.group, group(21));
    assert_eq!(r.value(hint), 8);
    assert_eq!(
        resolve(&r.cache, &source_fixture::Policy, fixture::id(10), &[1], 3),
        Err(RoutingError::Vacant)
    );
    let facts = (1..=3).map(|g| r.facts(g)).collect::<Vec<_>>();
    let logs = (1..=3)
        .map(|g| creation::abandon(std::mem::take(&mut r.metadata[g as usize - 1]), g))
        .collect::<Vec<_>>();
    let files = (1..=3)
        .map(|g| durable_files(&r.root.join(g.to_string())))
        .collect::<Vec<_>>();
    r.write(3, 5, hint);
    assert_eq!(r.value(hint), 13);
    r.reopen_owner();
    assert_eq!(r.adoption(), Some(status));
    assert_eq!(r.value(hint), 13);
    r.write(1, 7, hint);
    r.write(2, 1, hint);
    r.write(3, 5, hint);
    assert_eq!(r.value(hint), 13);
    r.write(4, 2, hint);
    assert_eq!(r.value(hint), 15);
    assert_eq!(
        propose_recovering(&mut r.owner, &r.clock, 21, 300, bytes).outcome,
        RoutedOutcome::ParentAdopted(status)
    );
    for n in &r.owner {
        let app = n.local().applications[&group(21)].routed();
        assert_eq!(app.grant(), &adoption.after());
        assert_eq!(app.application().outbox().count(), 4);
    }
    for g in 1..=3 {
        assert_eq!(
            durable_files(&r.root.join(g.to_string())),
            files[g as usize - 1]
        );
        for c in configuration(&r.root, g, &[1, 2, 3], NativeOpenMode::Recover) {
            let log = NativeLogStore::recover(
                FileLogIo::open(&c.directory).unwrap(),
                c.store,
                LogLimits::default(),
            )
            .unwrap();
            assert_eq!(log.state(group(g)).unwrap(), logs[g as usize - 1][&c.node]);
        }
        r.metadata[g as usize - 1] = open(
            configuration(&r.root, g, &[1, 2, 3], NativeOpenMode::Recover),
            &r.clock,
            protocol,
            || metadata(g),
        );
        assert_eq!(r.facts(g), facts[g as usize - 1]);
    }
    for g in 1..=3 {
        creation::abandon(std::mem::take(&mut r.metadata[g as usize - 1]), g);
    }
    creation::abandon(std::mem::take(&mut r.owner), 21);
    std::fs::remove_dir_all(&r.root).unwrap();
}
#[test]
fn tcp_cross_authority_reparent_resumes_wal_and_serves_without_metadata() {
    run(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_cross_authority_reparent_resumes_checkpoints_and_original_observations() {
    run(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_cross_authority_reparent_resumes_wal_and_serves_without_metadata() {
    run(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_cross_authority_reparent_resumes_checkpoints_and_original_observations() {
    run(NativePeerProtocol::Quic, true)
}
