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
use voteboat::reparent_commit::*;
fn fresh13(g: u128, p: &CrossReparentPlan, ops: usize) -> Directory {
    let old = fresh(g, p, ops);
    Directory::new(old.plan().clone(), old.limits())
        .unwrap()
        .with_cross_authority_reparenting()
        .unwrap_or_else(|_| panic!("schema13"))
}
fn initial13(g: u128, p: &CrossReparentPlan, ops: usize) -> Directory {
    let mut d = fresh13(g, p, ops);
    let b = d.bootstrap_command(200000).unwrap();
    commit(&mut d, 1000, b);
    let ms = d.plan().manifests().cloned().collect::<Vec<_>>();
    for (i, m) in ms.into_iter().enumerate() {
        commit(
            &mut d,
            1001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest: m,
            }
            .encode(200000)
            .unwrap(),
        );
    }
    d
}
fn reopen13(d: &Directory, p: &CrossReparentPlan) -> Directory {
    let mut n = fresh13(d.plan().authority().id.get(), p, d.limits().operations);
    n.restore_checkpoint(13, d.applied_index(), &d.checkpoint(2000000).unwrap())
        .unwrap();
    assert_eq!(
        d.checkpoint(2000000).unwrap(),
        n.checkpoint(2000000).unwrap()
    );
    n
}
fn prepared_with(ops: usize) -> (CrossReparentPlan, Vec<Directory>, CommitReparent) {
    let p = fixture();
    let mut nodes = vec![
        initial13(1, &p, ops),
        initial13(2, &p, ops - 1),
        initial13(3, &p, ops - 1),
    ];
    let coordinator = start(&mut nodes[0], &p);
    for d in &mut nodes[1..] {
        assert_eq!(
            commit(d, 200, prepare(&p, Some(coordinator))).outcome,
            DirectoryOutcome::ReparentGuarded
        );
    }
    let guards = nodes
        .iter()
        .map(|d| {
            ReparentGuardEvidence::from_status(
                ConfigurationId::new(3).unwrap(),
                &d.reparent_guard_at(d.applied_index(), op(200))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    let c = CommitReparent::new(op(200), guards).unwrap();
    (p, nodes, c)
}
fn prepared() -> (CrossReparentPlan, Vec<Directory>, CommitReparent) {
    prepared_with(4)
}
fn decide(nodes: &mut [Directory], c: &CommitReparent) -> ReparentDecisionStatus {
    let bytes = c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap();
    nodes[0]
        .validate_proposal(op(400), &bytes, std::iter::empty())
        .unwrap();
    assert_eq!(
        commit(&mut nodes[0], 400, bytes).outcome,
        DirectoryOutcome::ReparentCommitted
    );
    nodes[0]
        .reparent_decision_at(nodes[0].applied_index(), op(200))
        .unwrap()
        .unwrap()
}
fn publish(nodes: &mut [Directory], decision: &ReparentDecisionStatus) {
    let bytes = PublishReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        decision: decision.clone(),
    }
    .encode(MAX_REPARENT_COMPLETION_BYTES)
    .unwrap();
    for d in &mut nodes[1..] {
        d.validate_proposal(op(401), &bytes, std::iter::empty())
            .unwrap();
        assert_eq!(
            commit(d, 401, bytes.clone()).outcome,
            DirectoryOutcome::ReparentPublished
        );
    }
}
fn completion(nodes: &mut [Directory]) -> ReparentCompletionStatus {
    let pubs = nodes
        .iter()
        .map(|d| {
            ReparentPublicationEvidence::from_status(
                ConfigurationId::new(3).unwrap(),
                d.reparent_publication_at(d.applied_index(), op(200))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    let finish = FinishReparent::new(op(200), pubs)
        .unwrap()
        .encode(MAX_REPARENT_COMPLETION_BYTES)
        .unwrap();
    nodes[0]
        .validate_proposal(op(500), &finish, std::iter::empty())
        .unwrap();
    assert_eq!(
        commit(&mut nodes[0], 500, finish).outcome,
        DirectoryOutcome::ReparentCompleted
    );
    nodes[0]
        .reparent_completion_at(nodes[0].applied_index(), op(200))
        .unwrap()
        .unwrap()
}
struct View(Vec<ResponsibilityManifest>);
impl ManifestCache for View {
    fn get(&self, r: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.0.iter().find(|m| m.input().responsibility == r)
    }
    fn admit(
        &mut self,
        m: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        self.0
            .retain(|n| n.input().responsibility != m.input().responsibility);
        self.0.push(m);
        Ok(())
    }
    fn invalidate(&mut self, _: ResponsibilityIdentity, _: RouteGeneration) -> bool {
        false
    }
    fn limits(&self) -> ManifestCacheLimits {
        ManifestCacheLimits {
            manifests: 16,
            bytes: MAX_CACHE_BYTES,
        }
    }
    fn usage(&self) -> ManifestCacheUsage {
        ManifestCacheUsage {
            manifests: self.0.len(),
            bytes: self
                .0
                .iter()
                .map(ResponsibilityManifest::retained_bytes)
                .sum(),
        }
    }
}
#[test]
fn commit_publish_finish_and_release_move_real_routes_with_full_ordinary_history() {
    let (p, mut nodes, c) = prepared();
    for d in &nodes {
        assert_eq!(d.remaining_operations(), 0);
    }
    let mut owner = RoutedApplication::new(
        group(21),
        p.child().clone(),
        BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap(),
        Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("owner"));
    let b = owner.bootstrap_command(200000).unwrap();
    commit(&mut owner, 100, b);
    let original_hint = resolve(&View(vec![p.child().clone()]), &Policy, id(11), &[1], 1).unwrap();
    let data = encode_routed(
        original_hint,
        &[1],
        &encode_add(&[1], 7, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(&mut owner, 1, data).outcome,
        RoutedOutcome::Applied(_)
    ));
    let decision = decide(&mut nodes, &c);
    let expected = p.updated_manifests();
    assert_eq!(nodes[0].manifest(id(10)), Some(&expected[0]));
    assert_eq!(nodes[1].manifest(id(30)), Some(p.new_parent()));
    assert_eq!(nodes[2].manifest(id(11)), Some(p.child()));
    for d in &mut nodes {
        *d = reopen13(d, &p);
        assert!(d.reparent_guard_active(op(200)));
    }
    let bytes = PublishReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        decision: decision.clone(),
    }
    .encode(MAX_REPARENT_COMPLETION_BYTES)
    .unwrap();
    assert_eq!(
        commit(&mut nodes[1], 401, bytes.clone()).outcome,
        DirectoryOutcome::ReparentPublished
    );
    assert_eq!(
        resolve(
            &View(vec![expected[1].clone(), p.child().clone()]),
            &Policy,
            id(30),
            &[1],
            3
        ),
        Err(RoutingError::WrongParent)
    );
    nodes[1] = reopen13(&nodes[1], &p);
    assert_eq!(
        commit(&mut nodes[2], 401, bytes).outcome,
        DirectoryOutcome::ReparentPublished
    );
    nodes[2] = reopen13(&nodes[2], &p);
    for d in &nodes {
        assert!(d.reparent_guard_active(op(200)));
        assert_eq!(
            d.reserved_publication_bytes(),
            MAX_REPARENT_COMPLETION_BYTES
        );
        assert_eq!(
            d.reparent_decision_at(d.applied_index(), op(200)).unwrap(),
            Some(decision.clone())
        );
    }
    nodes = release_completed_nodes(nodes, &p);
    check_moved_data(&expected, &nodes, owner);
    let result = commit(
        &mut nodes[0],
        400,
        c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap(),
    );
    assert!(result.duplicate);
    assert_eq!(result.outcome, DirectoryOutcome::ReparentCommitted);
    check_completed_reads(LifecycleDirectory::new(nodes.remove(0)));
}

#[test]
fn missing_stale_or_changed_facts_and_early_release_are_rejected_without_publishing() {
    let (p, mut nodes, c) = prepared_with(32);
    let mut missing = c.clone();
    missing.guards.pop();
    assert_eq!(
        commit(
            &mut nodes[0],
            410,
            missing.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let mut wrong = c.clone();
    wrong.guards[0].index += 1;
    assert_eq!(
        commit(
            &mut nodes[0],
            411,
            wrong.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(nodes[0].manifest(id(10)), Some(p.old_parent()));
    let d = decide(&mut nodes, &c);
    let bytes = c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap();
    assert_eq!(
        commit(&mut nodes[0], 412, bytes).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(
        commit(
            &mut nodes[0],
            300,
            CancelReparent { guard: op(200) }.encode()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    check_early_publication(&mut nodes, &d, &p);
    publish(&mut nodes, &d);
    for node in &mut nodes[1..] {
        assert_eq!(
            commit(
                node,
                417,
                PublishReparent {
                    configuration: ConfigurationId::new(3).unwrap(),
                    decision: d.clone()
                }
                .encode(MAX_REPARENT_COMPLETION_BYTES)
                .unwrap()
            )
            .outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
    }
    check_changed_publication(&mut nodes);
    let done = completion(&mut nodes);
    let mut stale = done;
    stale.index = d.index;
    assert_eq!(
        commit(
            &mut nodes[1],
            419,
            ReleaseCommittedReparent {
                configuration: ConfigurationId::new(3).unwrap(),
                completion: stale
            }
            .encode()
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    for node in &mut nodes[1..] {
        assert_eq!(
            commit(
                node,
                501,
                ReleaseCommittedReparent {
                    configuration: ConfigurationId::new(3).unwrap(),
                    completion: done
                }
                .encode()
                .unwrap()
            )
            .outcome,
            DirectoryOutcome::ReparentReleased
        );
    }
    for node in &mut nodes {
        *node = reopen13(node, &p);
        assert!(!node.reparent_guard_active(op(200)));
    }
}
#[test]
fn cancellation_and_commit_are_mutually_exclusive_and_success_releases_allow_next_move() {
    let (p, mut cancelled, c) = prepared_with(32);
    let cancellation = cancel(&mut cancelled[0]);
    assert_eq!(
        commit(
            &mut cancelled[0],
            400,
            c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let release = ReleaseReparentGuard {
        configuration: ConfigurationId::new(3).unwrap(),
        decision: cancellation,
    };
    for d in &mut cancelled[1..] {
        commit(d, 301, release.encode().unwrap());
    }
    assert!(cancelled.iter().all(|d| !d.reparent_guard_active(op(200))));
    let (_, mut nodes, c) = prepared_with(32);
    let d = decide(&mut nodes, &c);
    publish(&mut nodes, &d);
    let done = completion(&mut nodes);
    let release = ReleaseCommittedReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        completion: done,
    };
    for n in &mut nodes[1..] {
        commit(n, 501, release.encode().unwrap());
    }
    let ms = p
        .manifests()
        .iter()
        .map(|m| {
            nodes
                .iter()
                .find_map(|d| d.manifest(m.input().responsibility))
                .unwrap()
                .clone()
        })
        .collect();
    let back = CrossReparentPlan::new(id(30), id(10), id(11), ms).unwrap();
    assert_eq!(
        commit(&mut nodes[0], 600, prepare(&back, None)).outcome,
        DirectoryOutcome::ReparentGuarded
    );
    let evidence = ReparentGuardEvidence::from_status(
        ConfigurationId::new(3).unwrap(),
        &nodes[0]
            .reparent_guard_at(nodes[0].applied_index(), op(600))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    for n in &mut nodes[1..] {
        assert_eq!(
            commit(n, 600, prepare(&back, Some(evidence))).outcome,
            DirectoryOutcome::ReparentGuarded
        );
    }
    let again = ReleaseCommittedReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        completion: done,
    };
    for n in &mut nodes[1..] {
        assert_eq!(
            commit(n, 501, again.encode().unwrap()).outcome,
            DirectoryOutcome::ReparentReleased
        );
        assert!(n.reparent_guard_active(op(600)));
    }
    let guards = nodes
        .iter()
        .map(|n| {
            ReparentGuardEvidence::from_status(
                ConfigurationId::new(3).unwrap(),
                &n.reparent_guard_at(n.applied_index(), op(600))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    let c = CommitReparent::new(op(600), guards).unwrap();
    assert_eq!(
        commit(
            &mut nodes[0],
            601,
            c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentCommitted
    );
    assert_eq!(
        nodes[0].manifest(id(10)),
        Some(&back.updated_manifests()[1])
    );
}

#[test]
fn codecs_old_profiles_and_checkpoint_replay_preserve_completed_decisions() {
    let (p, mut nodes, c) = prepared_with(32);
    let commit_bytes = c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap();
    let old = initial(1, &p, 32);
    assert!(old
        .validate_proposal(op(400), &commit_bytes, std::iter::empty())
        .is_err());
    let index = nodes[0].applied_index();
    let before = nodes[0].checkpoint(2000000).unwrap();
    assert!(nodes[0]
        .apply_batch(&[
            entry(index + 1, 400, commit_bytes.clone()),
            entry(index + 2, 900, vec![0])
        ])
        .is_err());
    assert_eq!(nodes[0].checkpoint(2000000).unwrap(), before);
    let d = decide(&mut nodes, &c);
    let pb = PublishReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        decision: d.clone(),
    }
    .encode(MAX_REPARENT_COMPLETION_BYTES)
    .unwrap();
    publish(&mut nodes, &d);
    let pubs = nodes
        .iter()
        .map(|n| {
            ReparentPublicationEvidence::from_status(
                ConfigurationId::new(3).unwrap(),
                n.reparent_publication_at(n.applied_index(), op(200))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    let fb = FinishReparent::new(op(200), pubs)
        .unwrap()
        .encode(MAX_REPARENT_COMPLETION_BYTES)
        .unwrap();
    let done = completion(&mut nodes);
    let rb = ReleaseCommittedReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        completion: done,
    }
    .encode()
    .unwrap();
    for end in 0..commit_bytes.len() {
        assert!(CommitReparent::decode(&commit_bytes[..end]).is_err());
    }
    for end in 0..pb.len() {
        assert!(PublishReparent::decode(&pb[..end]).is_err());
    }
    for end in 0..fb.len() {
        assert!(FinishReparent::decode(&fb[..end]).is_err());
    }
    for end in 0..rb.len() {
        assert!(ReleaseCommittedReparent::decode(&rb[..end]).is_err());
    }
    for n in &mut nodes[1..] {
        commit(n, 501, rb.clone());
    }
    for n in &nodes {
        let cp = n.checkpoint(2000000).unwrap();
        let mut fresh = fresh13(n.plan().authority().id.get(), &p, n.limits().operations);
        let empty = fresh.checkpoint(2000000).unwrap();
        for end in 0..cp.len() {
            assert!(fresh
                .restore_checkpoint(13, n.applied_index(), &cp[..end])
                .is_err());
            assert_eq!(fresh.checkpoint(2000000).unwrap(), empty);
        }
        fresh
            .restore_checkpoint(13, n.applied_index(), &cp)
            .unwrap();
        fresh
            .restore_checkpoint(13, n.applied_index(), &cp)
            .unwrap();
        assert_eq!(
            fresh
                .reparent_completion_at(fresh.applied_index(), op(200))
                .unwrap(),
            Some(done)
        );
        let mut old = super::fresh(n.plan().authority().id.get(), &p, n.limits().operations);
        assert!(old.restore_checkpoint(12, n.applied_index(), &cp).is_err());
    }
}
#[test]
fn either_participant_publication_order_recovers_and_preserves_one_physical_owner() {
    for order in [[1usize, 2usize], [2, 1]] {
        let (p, mut nodes, c) = prepared();
        let d = decide(&mut nodes, &c);
        let pb = PublishReparent {
            configuration: ConfigurationId::new(3).unwrap(),
            decision: d,
        }
        .encode(MAX_REPARENT_COMPLETION_BYTES)
        .unwrap();
        for (step, i) in order.into_iter().enumerate() {
            commit(&mut nodes[i], 401, pb.clone());
            nodes[i] = reopen13(&nodes[i], &p);
            let current = View(vec![
                nodes[1].manifest(id(30)).unwrap().clone(),
                nodes[2].manifest(id(11)).unwrap().clone(),
            ]);
            let route = resolve(&current, &Policy, id(30), &[1], 3);
            if step == 0 {
                assert!(matches!(
                    route,
                    Err(RoutingError::Vacant) | Err(RoutingError::WrongParent)
                ));
            } else {
                assert_eq!(route.unwrap().group, group(21));
            }
            assert_eq!(
                nodes[2].manifest(id(11)).unwrap().input().epoch,
                p.child().input().epoch
            );
        }
        completion(&mut nodes);
    }
}
#[cfg(feature = "native")]
#[test]
fn native_decision_publication_completion_and_release_cuts_keep_exact_phase_state() {
    use support::Fault;
    use voteboat::{log::*, native::log_store::*};
    let (p, mut nodes, c) = prepared();
    let d = decide(&mut nodes, &c);
    publish(&mut nodes, &d);
    let pubs = nodes
        .iter()
        .map(|n| {
            ReparentPublicationEvidence::from_status(
                ConfigurationId::new(3).unwrap(),
                n.reparent_publication_at(n.applied_index(), op(200))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    let finish = FinishReparent::new(op(200), pubs)
        .unwrap()
        .encode(MAX_REPARENT_COMPLETION_BYTES)
        .unwrap();
    let done = completion(&mut nodes);
    let coor = ReparentGuardEvidence::from_status(
        ConfigurationId::new(3).unwrap(),
        &nodes[0]
            .reparent_guard_at(nodes[0].applied_index(), op(200))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let limits = LogLimits::default();
    for g in [1, 3] {
        let ordinary = if g == 1 { 4 } else { 3 };
        let (entries, first) = commit_journal(&p, g, coor, &c, &d, &finish, done);
        for boundary in [first, first + 1] {
            let seed = || seed_reparent_log(g, &entries, boundary, limits);
            let (_, log) = seed();
            let mutation = support::update(
                &log.state(group(g)).unwrap(),
                1,
                boundary,
                Some(Suffix {
                    from: boundary,
                    entries: vec![entries[boundary as usize - 1].clone()],
                }),
            );
            let frame = NativeLogCodec
                .encode_batch(3, std::slice::from_ref(&mutation), limits)
                .unwrap();
            let (mut old, mut complete) = (false, false);
            for fault in (0..=frame.len()).map(Fault::Append).chain([
                Fault::Sync,
                Fault::PublishBefore,
                Fault::PublishAfter,
            ]) {
                let (io, mut log) = seed();
                io.0.borrow_mut().fault = fault;
                if let Ok(tickets) = log.append_batch(vec![mutation.clone()]) {
                    assert!(log.barrier(&tickets).is_err());
                }
                drop(log);
                io.0.borrow_mut().power_loss();
                let log = NativeLogStore::recover(io, support::identity(g), limits).unwrap();
                let state = log.state(group(g)).unwrap();
                let mut app = fresh13(g, &p, ordinary);
                app.apply_batch(
                    &state
                        .entries
                        .into_iter()
                        .filter(|e| e.index <= state.commit_index)
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                if state.commit_index == boundary {
                    complete = true
                } else {
                    old = true;
                    assert_eq!(state.commit_index, boundary - 1);
                }
                check_recovered_publication(app, &p, g, first, state.commit_index, &entries);
            }
            assert!(old && complete);
        }
    }
}

#[path = "commit_owner.rs"]
mod owner;

fn check_moved_data(
    expected: &[ResponsibilityManifest],
    nodes: &[Directory],
    mut owner: RoutedApplication<BucketCounter<Policy>, Policy>,
) {
    let view = View(expected.to_vec());
    let hint = resolve(&view, &Policy, id(30), &[1], 3).unwrap();
    assert_eq!(hint.group, group(21));
    assert_eq!(
        resolve(&view, &Policy, id(10), &[1], 3),
        Err(RoutingError::Vacant)
    );
    let unchanged = nodes
        .iter()
        .map(|d| d.checkpoint(2000000).unwrap())
        .collect::<Vec<_>>();
    let retry = encode_routed(
        hint,
        &[1],
        &encode_add(&[1], 7, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    let RoutedOutcome::Applied(receipt) = commit(&mut owner, 1, retry).outcome else {
        panic!("original data retry")
    };
    assert!(receipt.duplicate);
    assert_eq!(owner.application().outbox().count(), 1);
    let next = encode_routed(
        hint,
        &[1],
        &encode_add(&[1], 3, b"next", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(&mut owner, 2, next).outcome,
        RoutedOutcome::Applied(_)
    ));
    assert_eq!(owner.application().outbox().count(), 2);
    assert_eq!(
        nodes
            .iter()
            .map(|d| d.checkpoint(2000000).unwrap())
            .collect::<Vec<_>>(),
        unchanged
    );
}

fn check_completed_reads(read: LifecycleDirectory) {
    let q = DirectoryQuery::ReparentDecision(op(200));
    let r = read.read_at(read.applied_index(), q).unwrap();
    assert!(read.read_result_bound(&q).unwrap() > std::mem::size_of::<DirectoryRead>());
    assert!(read.read_result_bytes(&r, 0).is_err());
    for q in [
        DirectoryQuery::ReparentPublication(op(200)),
        DirectoryQuery::ReparentCompletion(op(200)),
    ] {
        assert_eq!(
            read.read_result_bound(&q).unwrap(),
            std::mem::size_of::<DirectoryRead>()
        );
        assert_eq!(
            read.read_result_bytes(&read.read_at(read.applied_index(), q).unwrap(), 0)
                .unwrap(),
            0
        );
        assert_eq!(
            read.read_at(read.applied_index() + 1, q),
            Err(ApplicationError::NotApplied)
        );
    }
}

fn check_early_publication(
    nodes: &mut [Directory],
    d: &ReparentDecisionStatus,
    p: &CrossReparentPlan,
) {
    let early = ReleaseCommittedReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        completion: ReparentCompletionStatus {
            coordinator: group(1),
            operation: op(500),
            index: 99,
            guard: op(200),
            decision_digest: d.digest().unwrap(),
        },
    };
    assert_eq!(
        commit(&mut nodes[1], 413, early.encode().unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let mut wrong = d.clone();
    wrong.commit.guards[1].index += 1;
    assert_eq!(
        commit(
            &mut nodes[1],
            414,
            PublishReparent {
                configuration: ConfigurationId::new(3).unwrap(),
                decision: wrong
            }
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let mut wrong = d.clone();
    wrong.commit.guards[0].configuration = ConfigurationId::new(4).unwrap();
    assert_eq!(
        commit(
            &mut nodes[1],
            415,
            PublishReparent {
                configuration: ConfigurationId::new(3).unwrap(),
                decision: wrong
            }
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(nodes[1].manifest(id(30)), Some(p.new_parent()));
    let local = nodes[0]
        .reparent_publication_at(nodes[0].applied_index(), op(200))
        .unwrap()
        .unwrap();
    let missing = FinishReparent::new(
        op(200),
        vec![
            ReparentPublicationEvidence::from_status(ConfigurationId::new(3).unwrap(), local)
                .unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        commit(
            &mut nodes[0],
            416,
            missing.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
}

fn check_changed_publication(nodes: &mut [Directory]) {
    let mut pubs = nodes
        .iter()
        .map(|n| {
            ReparentPublicationEvidence::from_status(
                ConfigurationId::new(3).unwrap(),
                n.reparent_publication_at(n.applied_index(), op(200))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    pubs[0].index += 1;
    assert_eq!(
        commit(
            &mut nodes[0],
            418,
            FinishReparent::new(op(200), pubs)
                .unwrap()
                .encode(MAX_REPARENT_COMPLETION_BYTES)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
}

#[cfg(feature = "native")]
fn commit_journal(
    p: &CrossReparentPlan,
    g: u128,
    coor: ReparentGuardEvidence,
    c: &CommitReparent,
    d: &ReparentDecisionStatus,
    finish: &[u8],
    done: ReparentCompletionStatus,
) -> (Vec<voteboat::log::LogEntry>, u64) {
    let ordinary = if g == 1 { 4 } else { 3 };
    let prototype = fresh13(g, p, ordinary);
    let mut entries = vec![entry(1, 1000, prototype.bootstrap_command(200000).unwrap())];
    for (i, m) in prototype.plan().manifests().enumerate() {
        entries.push(entry(
            2 + i as u64,
            1001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest: m.clone(),
            }
            .encode(200000)
            .unwrap(),
        ));
    }
    let guard_index = entries.len() as u64 + 1;
    entries.push(entry(
        guard_index,
        200,
        prepare(p, if g == 1 { None } else { Some(coor) }),
    ));
    let first = guard_index + 1;
    entries.push(entry(
        first,
        if g == 1 { 400 } else { 401 },
        if g == 1 {
            c.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        } else {
            PublishReparent {
                configuration: ConfigurationId::new(3).unwrap(),
                decision: d.clone(),
            }
            .encode(MAX_REPARENT_COMPLETION_BYTES)
            .unwrap()
        },
    ));
    entries.push(entry(
        first + 1,
        if g == 1 { 500 } else { 501 },
        if g == 1 {
            finish.to_vec()
        } else {
            ReleaseCommittedReparent {
                configuration: ConfigurationId::new(3).unwrap(),
                completion: done,
            }
            .encode()
            .unwrap()
        },
    ));

    (entries, first)
}

#[cfg(feature = "native")]
fn check_recovered_publication(
    mut app: Directory,
    p: &CrossReparentPlan,
    g: u128,
    first: u64,
    committed: u64,
    entries: &[voteboat::log::LogEntry],
) {
    let published = committed >= first;
    let released = committed > first;
    assert_eq!(
        app.reparent_publication_at(app.applied_index(), op(200))
            .unwrap()
            .is_some(),
        published
    );
    assert_eq!(app.reparent_guard_active(op(200)), !released);
    let id = if g == 1 { id(10) } else { id(11) };
    let expected = if published {
        p.updated_manifests()
            .into_iter()
            .find(|m| m.input().responsibility == id)
            .unwrap()
    } else {
        p.manifests()
            .iter()
            .find(|m| m.input().responsibility == id)
            .unwrap()
            .clone()
    };
    assert_eq!(app.manifest(id), Some(&expected));
    app = reopen13(&app, p);
    let voteboat::log::EntryPayload::Command { bytes, .. } = &entries[first as usize - 1].payload
    else {
        unreachable!()
    };
    let receipt = commit(&mut app, if g == 1 { 400 } else { 401 }, bytes.clone());
    assert_eq!(receipt.duplicate, published);
}

fn release_completed_nodes(mut nodes: Vec<Directory>, p: &CrossReparentPlan) -> Vec<Directory> {
    let done = completion(&mut nodes);
    assert!(!nodes[0].reparent_guard_active(op(200)));
    assert!(nodes[1].reparent_guard_active(op(200)));
    let release = ReleaseCommittedReparent {
        configuration: ConfigurationId::new(3).unwrap(),
        completion: done,
    }
    .encode()
    .unwrap();
    for d in &mut nodes[1..] {
        d.validate_proposal(op(501), &release, std::iter::empty())
            .unwrap();
        assert_eq!(
            commit(d, 501, release.clone()).outcome,
            DirectoryOutcome::ReparentReleased
        );
    }
    for d in &mut nodes {
        *d = reopen13(d, p);
        assert_eq!(d.remaining_operations(), 0);
        assert_eq!(d.reserved_publication_bytes(), 0);
        assert!(!d.reparent_guard_active(op(200)));
        assert_eq!(
            d.reparent_completion_at(d.applied_index(), op(200))
                .unwrap(),
            Some(done)
        );
    }

    nodes
}
