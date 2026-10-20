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
//! DR1 round 6 (Sol, round 5): construction inputs that decide committed
//! entries are part of the readiness identity, so a learner built with other
//! inputs is refused before it receives authority. Each test first shows the
//! divergence the inputs cause on the same admitted entry, then that the real
//! learner verifier refuses each replica for the other's declaration while its
//! own declaration still gets a receipt.
use super::*;
use std::collections::BTreeMap;
use voteboat::{
    logical_voter::*,
    membership::Configuration,
    quorum::{Limits as PolicyLimits, Policy, Tree},
    transaction::*,
};

fn command(index: u64, operation: OperationId, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation,
            bytes: bytes.into(),
        },
    }
}
/// The committed configuration/Noop prefix every `readiness_cluster` learner
/// applies before verification.
fn caught_up<A: CheckpointStateMachine>(mut app: A) -> A {
    let mut log = HostLogStore::new(4);
    let (_, learner, _, _) = readiness_cluster(&mut log);
    app.apply_batch(learner.replay_committed()).unwrap();
    assert_eq!(app.applied_index(), 2);
    app
}

fn config(nodes: [u64; 3], id: u64) -> Configuration {
    let voters = nodes.map(node);
    Configuration::new(
        cid(id),
        Policy::new(
            Tree::Majority(voters.map(Tree::Voter).to_vec()),
            PolicyLimits::default(),
        )
        .unwrap(),
        voters
            .into_iter()
            .map(|n| (n, identity(n.get() as u128)))
            .collect(),
        BTreeMap::new(),
    )
    .unwrap()
}
fn mapping_to(child: u128) -> LogicalStoreMapping {
    LogicalStoreMapping::new(group(1), node(1), identity(1), group(child)).unwrap()
}
fn activation(child: u128) -> impl Fn() -> LogicalActivationApplication<Counter> {
    move || {
        LogicalActivationApplication::new(
            group(1),
            vec![mapping_to(child)],
            LogicalActivationLimits::default(),
            Counter::new(4).unwrap(),
        )
        .unwrap()
    }
}

#[test]
fn adversarial_activation_mapping() {
    // Sol, round 5: parents mapping (g1, n1, s1) to child group 2 and to child
    // group 3 presented equal requirements; the first learner got a receipt
    // for the second's request, then failed the second's admitted `Begin`.
    let (a, b) = (activation(2), activation(3));
    let (da, db) = (
        a().deployment_requirements().unwrap(),
        b().deployment_requirements().unwrap(),
    );
    assert_eq!(
        (
            da.application_schema,
            da.identity,
            da.inner_profile.layers()
        ),
        (
            db.application_schema,
            db.identity,
            db.inner_profile.layers()
        )
    );
    assert_ne!(da.inner_profile.plan(), db.inner_profile.plan());
    // Parent (3 words), then the mapping: voter, store (3), child (3).
    let plan = da.inner_profile.plan();
    assert_eq!(plan.segments().count(), 1);
    assert_eq!(plan.segment(0).unwrap().len(), 3 + 7);
    let mut mapping = vec![1];
    mapping.extend(DeploymentPlan::store_words(identity(1)));
    mapping.extend(DeploymentPlan::group_words(group(2)));
    assert_eq!(plan.segment(0).unwrap()[3..], mapping[..]);

    // The divergence: b's admitted Begin at index 3.
    let intent = ChildActivationIntent::new(
        mapping_to(3),
        ChildActivationOperations::reserved(1).unwrap(),
        &config([1, 2, 3], 1),
        cid(2),
        cid(3),
        &config([1, 2, 4], 4),
    )
    .unwrap();
    let begin = ChildActivationOperations::reserved(1).unwrap().begin;
    let (mut on_a, mut on_b) = (caught_up(a()), caught_up(b()));
    let entry = command(3, begin, on_b.begin(&intent).unwrap());
    match on_b
        .apply_batch(std::slice::from_ref(&entry))
        .unwrap()
        .as_slice()
    {
        [ActivationReceipt::Control(r)] => assert_eq!(r.outcome, ChildActivationOutcome::Begun),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        on_a.apply_batch(std::slice::from_ref(&entry)).map(|_| ()),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!((on_a.applied_index(), on_b.applied_index()), (2, 3));

    // Readiness now refuses each for the other's declaration, directly and
    // through the real learner verifier.
    assert_eq!(
        a().validate_deployment_requirements(db),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(
        b().validate_deployment_requirements(da),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_learners_refuse_each_other(a, b, |r| r);
}

#[test]
fn activation_plan_names_every_mapping_and_its_order_is_canonical() {
    // Same mappings in another order: the constructor sorts by voter, so the
    // plan is equal; any one changed identity makes it differ.
    let mapping = |voter, store, child| {
        LogicalStoreMapping::new(group(1), node(voter), identity(store), group(child)).unwrap()
    };
    let build = |mappings| {
        LogicalActivationApplication::new(
            group(1),
            mappings,
            LogicalActivationLimits::default(),
            Counter::new(4).unwrap(),
        )
        .unwrap()
        .deployment_requirements()
        .unwrap()
    };
    let base = build(vec![mapping(1, 1, 2), mapping(2, 2, 3)]);
    assert_eq!(base, build(vec![mapping(2, 2, 3), mapping(1, 1, 2)]));
    for other in [
        vec![mapping(1, 1, 2), mapping(3, 2, 3)],
        vec![mapping(1, 1, 2), mapping(2, 4, 3)],
        vec![mapping(1, 1, 2), mapping(2, 2, 5)],
        vec![mapping(1, 1, 2)],
    ] {
        assert_ne!(base.inner_profile, build(other).inner_profile);
    }
    // At the 16-mapping limit the plan still fits: 1 + 3 + 16 × 7 words.
    let full = build(
        (1..=16)
            .map(|n| mapping(n, n as u128, 100 + n as u128))
            .collect(),
    );
    assert_eq!(full.inner_profile.plan().words().len(), 116);
    assert!(full.inner_profile.layers().is_some());
}

fn transfer_profile() -> TransferProfile {
    TransferProfile::new(group(1), [group(2), group(3)]).unwrap()
}
fn coordinator(transactions: usize) -> impl Fn() -> TransferApplication {
    move || {
        TransferApplication::coordinator(
            group(1),
            transfer_profile(),
            TransferLimits::new(transactions, 2).unwrap(),
        )
        .unwrap()
    }
}
fn transfer(id: u128) -> TransferSpec {
    let phases = PhaseIds::new(
        std::array::from_fn(|i| op(id * 100 + i as u128 + 1)),
        std::array::from_fn(|_| std::array::from_fn(|i| op(id * 100 + i as u128 + 1))),
    )
    .unwrap();
    TransferSpec::new(
        TransactionId::new(id).unwrap(),
        transfer_profile(),
        AccountRef::new(group(2), AccountId::new(1).unwrap()),
        AccountRef::new(group(3), AccountId::new(2).unwrap()),
        5,
        phases,
    )
    .unwrap()
}
fn op(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn start(index: u64, id: u128) -> LogEntry {
    let spec = transfer(id);
    command(
        index,
        spec.phases().coordinator()[0],
        TransferApplication::start(spec).unwrap(),
    )
}
/// A covering envelope both replicas accept.
fn covering(
    a: ReadinessRequirements,
    b: ReadinessRequirements,
) -> impl Fn(ReadinessRequirements) -> ReadinessRequirements {
    move |r| ReadinessRequirements {
        command_bytes: a.command_bytes.max(b.command_bytes),
        snapshot_bytes: a.snapshot_bytes.max(b.snapshot_bytes),
        ..r
    }
}

#[test]
fn adversarial_transaction_leaf_capacity() {
    // Sol, round 5: coordinators with transaction capacity 1 and 2 declared
    // the family alone; the capacity-1 learner got a receipt, then refused the
    // second admitted Start with Err(DedupCapacity).
    let (small, large) = (coordinator(1), coordinator(2));
    let (ds, dl) = (
        small().deployment_requirements().unwrap(),
        large().deployment_requirements().unwrap(),
    );
    assert_eq!(ds.identity.configuration, [1, 2, 0, 0]);
    assert_eq!(dl.identity.configuration, [2, 2, 0, 0]);
    // The group and profile are this layer's plan: 4 groups of 3 words.
    assert_eq!(ds.inner_profile.plan().segment(0).unwrap().len(), 12);
    assert_eq!(ds.inner_profile.plan(), dl.inner_profile.plan());

    let (mut on_small, mut on_large) = (caught_up(small()), caught_up(large()));
    for app in [&mut on_small, &mut on_large] {
        let receipts = app.apply_batch(&[start(3, 1)]).unwrap();
        assert_eq!(receipts[0].outcome, TransferOutcome::Started);
    }
    let receipts = on_large.apply_batch(&[start(4, 2)]).unwrap();
    assert_eq!(receipts[0].outcome, TransferOutcome::Started);
    assert_eq!(
        on_small.apply_batch(&[start(4, 2)]).map(|_| ()),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!((on_small.applied_index(), on_large.applied_index()), (3, 4));

    let common = covering(ds, dl);
    assert_eq!(
        small().validate_deployment_requirements(common(dl)),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_learners_refuse_each_other(small, large, common);
}

#[test]
fn transaction_profiles_and_groups_are_compared_exactly() {
    // Same limits, another participant group or another role: refused.
    let with = |profile: TransferProfile, group_: GroupIdentity| {
        if profile.coordinator() == group_ {
            TransferApplication::coordinator(group_, profile, TransferLimits::new(2, 2).unwrap())
        } else {
            TransferApplication::participant(
                group_,
                profile,
                &[(AccountId::new(1).unwrap(), 10)],
                TransferLimits::new(2, 2).unwrap(),
            )
        }
        .unwrap()
        .deployment_requirements()
        .unwrap()
    };
    let base = with(transfer_profile(), group(1));
    let other = TransferProfile::new(group(1), [group(2), group(4)]).unwrap();
    assert_ne!(base.inner_profile, with(other, group(1)).inner_profile);
    let participant = with(transfer_profile(), group(2));
    assert_ne!(base.inner_profile, participant.inner_profile);
    assert_ne!(
        participant.inner_profile,
        with(transfer_profile(), group(3)).inner_profile
    );
}

#[test]
fn adversarial_routed_transaction_leaf_capacity() {
    use transfer_fixtures::{grant_for, Policy};
    use voteboat::routed::*;
    // Sol, round 5: the same pair inside RoutedApplication had equal full
    // requirements and a learner receipt. Routed propagates the leaf's
    // configuration and plan through its inner profile.
    let routed = |transactions| {
        move || {
            // One inner checkpoint limit for both, so the Routed layer's own
            // configuration is equal and only the leaf differs.
            RoutedApplication::new(
                group(1),
                grant_for(group(1)),
                coordinator(transactions)(),
                Policy,
                RoutedLimits {
                    operations: 32,
                    semantic_bytes: 8192,
                    payload_bytes: 1024,
                    inner_checkpoint_bytes: 8192,
                },
            )
            .unwrap_or_else(|r| panic!("{:?}", r.error))
        }
    };
    let (small, large) = (routed(1), routed(2));
    let (ds, dl) = (
        small().deployment_requirements().unwrap(),
        large().deployment_requirements().unwrap(),
    );
    assert_eq!(
        (ds.application_schema, ds.identity),
        (dl.application_schema, dl.identity)
    );
    let layers = ds.inner_profile.layers().unwrap();
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].identity.configuration, [1, 2, 0, 0]);
    assert_eq!(
        dl.inner_profile.layers().unwrap()[0].identity.configuration,
        [2, 2, 0, 0]
    );
    // The leaf's plan, one layer deeper.
    assert_eq!(ds.inner_profile.plan().segment(0), None);
    assert_eq!(ds.inner_profile.plan().segment(1).unwrap().len(), 12);
    assert_learners_refuse_each_other(small, large, covering(ds, dl));
}

#[test]
fn deployment_plans_are_canonical_bounded_and_refused_when_they_overflow() {
    // Canonical: header `depth << 32 | count`, count ≥ 1, depths increasing
    // and at most the layer count.
    let ok = DeploymentPlan::from_words(&[2, 7, 8, 1 << 32 | 1, 9], 1).unwrap();
    assert_eq!(
        ok.segments().collect::<Vec<_>>(),
        vec![(0, &[7, 8][..]), (1, &[9][..])]
    );
    for bad in [
        &[0][..],                            // empty segment
        &[3, 1, 2][..],                      // count beyond the words
        &[1 << 32 | 1, 9, 1][..],            // depth not increasing
        &[2 << 32 | 1, 9][..],               // depth beyond one layer
        &[1, 7, 1 << 32 | 1][..],            // truncated last segment
        &[(1 << 32) | (1 << 31) | 1, 9][..], // count field out of range
    ] {
        assert_eq!(DeploymentPlan::from_words(bad, 1), None, "{bad:?}");
    }
    assert!(DeploymentPlan::from_words(&[0; MAX_DEPLOYMENT_PLAN_WORDS + 1], 7).is_none());
    // An own segment that does not fit overflows the profile, which readiness
    // refuses rather than truncating.
    let profile = InnerProfile::LEAF.planned(&[1; MAX_DEPLOYMENT_PLAN_WORDS]);
    assert_eq!(profile.layers(), None);
    let fits = InnerProfile::LEAF.planned(&[1; MAX_DEPLOYMENT_PLAN_WORDS - 1]);
    assert_eq!(fits.plan().words().len(), MAX_DEPLOYMENT_PLAN_WORDS);
    // A second own segment is refused, too.
    assert_eq!(
        InnerProfile::LEAF.planned(&[1]).planned(&[2]).layers(),
        None
    );
    assert_eq!(InnerProfile::LEAF.planned(&[]), InnerProfile::LEAF);
}

#[test]
fn bucket_counter_scope_and_scheme_are_compared_exactly() {
    use transfer_fixtures::{bucket_limits, range, Policy};
    use voteboat::bucket_counter::*;
    // DR1 round 6 audit: the scope refuses keys outside it, and was not in
    // the identity of a bare BucketCounter.
    let counter = |end| move || BucketCounter::new(range(0, end), Policy, bucket_limits()).unwrap();
    let (wide, narrow) = (counter(256), counter(128));
    let add = command(
        3,
        op(1),
        encode_add(&[200], 5, &[], MAX_BUCKET_COMMAND_BYTES).unwrap(),
    );
    let (mut on_wide, mut on_narrow) = (caught_up(wide()), caught_up(narrow()));
    assert_eq!(
        on_wide.apply_batch(std::slice::from_ref(&add)).unwrap()[0].outcome,
        BucketOutcome::Value(5)
    );
    assert_eq!(
        on_narrow
            .apply_batch(std::slice::from_ref(&add))
            .map(|_| ()),
        Err(ApplicationError::InvalidCommand)
    );
    let (dw, dn) = (
        wide().deployment_requirements().unwrap(),
        narrow().deployment_requirements().unwrap(),
    );
    assert_eq!(dw.identity, dn.identity);
    // Plan: scope, then scheme id (high, low) and version.
    assert_eq!(
        dw.inner_profile.plan().segment(0).unwrap(),
        &[LayerIdentity::pair(0, 256), 0, 1, 1]
    );
    // The comparator the learner verifier calls (a bare BucketCounter's
    // command bound exceeds this fixture's log limit, so it is not run
    // through the cluster; inside Routed the grant binds the scope).
    assert_eq!(wide().validate_deployment_requirements(dw), Ok(()));
    assert_eq!(
        wide().validate_deployment_requirements(dn),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(
        narrow().validate_deployment_requirements(dw),
        Err(ApplicationError::UnsupportedSchema)
    );
}
