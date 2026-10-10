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
//! Automatic lookup consumes current metadata reads before/after actual moves.
use super::*;
use voteboat::native::lookup_discovery::*;
fn now(clock: &Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
fn request(g: u128) -> ManifestLookup {
    ManifestLookup {
        locator: AuthorityLocator {
            responsibility: root_manifest().input().responsibility,
            authority: group(g),
        },
        minimum_epoch: None,
        minimum_generation: None,
    }
}
type Lookup<A, M> = NativeManifestLookup<MappedManifestReadSource<Node<A>, M>>;
fn tick<A, M>(lookup: &mut Lookup<A, M>, others: &mut [Node<A>], clock: &Instant)
where
    A: ProposalAdmission
        + BoundedReadableStateMachine<Query = M::Query, ReadResult = M::ReadResult>
        + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    M: ManifestReadMapping,
{
    lookup
        .source_mut()
        .node_mut()
        .poll(now(clock), NodePollBudget::default())
        .unwrap();
    for node in others {
        node.poll(now(clock), NodePollBudget::default()).unwrap();
    }
}
fn lookup<A, M>(
    nodes: &mut Vec<Node<A>>,
    clock: &Instant,
    g: u128,
) -> Result<ResponsibilityManifest, ManifestDiscoveryError>
where
    A: ProposalAdmission
        + BoundedReadableStateMachine<Query = M::Query, ReadResult = M::ReadResult>
        + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    M: ManifestReadMapping,
    M::ReadResult: Debug,
{
    campaign(nodes, clock, g);
    let source = MappedManifestReadSource::<_, M>::new(nodes.remove(0));
    let mut lookup = NativeManifestLookup::new(
        source,
        ManifestCacheLimits {
            manifests: 2,
            bytes: 65536,
        },
        60000,
        10000,
        1,
        now(clock),
    )
    .unwrap_or_else(|_| panic!("lookup construction"));
    let q = request(g);
    assert_eq!(
        lookup.lookup(q, now(clock)).unwrap_err(),
        ManifestDiscoveryError::Unavailable
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut unavailable = false;
    while !lookup.is_drained() {
        tick(&mut lookup, nodes, clock);
        match lookup.poll(now(clock)) {
            Ok(_) => (),
            Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Unavailable)) => {
                unavailable = true;
            }
            e => panic!("lookup failed: {e:?}"),
        }
        assert!(Instant::now() < deadline, "metadata lookup stalled");
    }
    let result = if unavailable {
        Err(ManifestDiscoveryError::Unavailable)
    } else {
        lookup.lookup(q, now(clock)).map(|o| o.manifest)
    };
    if let Ok(manifest) = &result {
        verify_lookup(&mut lookup, nodes, clock, q, manifest);
    }
    lookup.close();
    nodes.insert(
        0,
        lookup
            .into_source()
            .unwrap_or_else(|_| panic!("pending lookup"))
            .into_node(),
    );
    result
}
fn verify_lookup<A, M>(
    lookup: &mut Lookup<A, M>,
    nodes: &mut [Node<A>],
    clock: &Instant,
    q: ManifestLookup,
    manifest: &ResponsibilityManifest,
) where
    A: ProposalAdmission
        + BoundedReadableStateMachine<Query = M::Query, ReadResult = M::ReadResult>
        + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    M: ManifestReadMapping,
    M::ReadResult: Debug,
{
    let deadline = Instant::now() + Duration::from_secs(15);

    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 2,
        bytes: 65536,
    })
    .unwrap();
    let route = resolve_discovered(
        &mut cache,
        &source_fixture::Policy,
        lookup,
        DiscoverRouteRequest {
            start: q,
            key: &[1],
            max_hops: 1,
            lookups: 1,
            now: now(clock),
        },
    )
    .unwrap();
    assert_eq!(route.group, group(20));
    assert_eq!(route.generation, manifest.input().generation);
    let observed = lookup.lookup(q, now(clock)).unwrap().observation;
    assert!(lookup.invalidate(q.locator, observed));
    assert_eq!(
        lookup.lookup(q, now(clock)).unwrap_err(),
        ManifestDiscoveryError::Unavailable
    );
    let ticket = lookup.pending().unwrap().ticket;
    lookup.cancel_pending().unwrap();
    assert_eq!(lookup.pending().unwrap().ticket, ticket);
    while !lookup.is_drained() {
        tick(lookup, nodes, clock);
        match lookup.poll(now(clock)) {
            Ok(_) | Err(ManifestLookupPollError::Discovery(ManifestDiscoveryError::Cancelled)) => {}
            e => panic!("cancel failed: {e:?}"),
        }
        assert!(Instant::now() < deadline, "cancellation stalled");
    }
    while lookup.source_mut().pending_reads() != 0 {
        tick(lookup, nodes, clock);
        assert!(
            Instant::now() < deadline,
            "retained cancellation did not drain"
        );
    }
    assert_eq!(lookup.source_mut().pending_reads(), 0);
    rejected_completion(lookup, nodes, clock, q);
}
fn rejected_completion<A, M>(
    lookup: &mut Lookup<A, M>,
    nodes: &mut [Node<A>],
    clock: &Instant,
    q: ManifestLookup,
) where
    A: ProposalAdmission
        + BoundedReadableStateMachine<Query = M::Query, ReadResult = M::ReadResult>
        + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    M: ManifestReadMapping,
    M::ReadResult: Debug,
{
    let ticket = lookup.source_mut().submit(q).unwrap();
    let mut wrong = ticket;
    wrong.sequence += 1;
    let deadline = Instant::now() + Duration::from_secs(15);
    let rejected = loop {
        tick(lookup, nodes, clock);
        match lookup.source_mut().poll_result(wrong) {
            Ok(None) => (),
            Err(rejected) => break rejected,
            Ok(Some(_)) => panic!("wrong ticket accepted"),
        }
        assert!(Instant::now() < deadline, "read did not complete");
    };
    assert_eq!(rejected.reason, ReadInvocationError::WrongBinding);
    assert_eq!(rejected.completion.ticket(), ticket);
    assert_eq!(lookup.source_mut().pending_reads(), 1);
    let outcome = lookup
        .source_mut()
        .node_mut()
        .complete_read(*rejected.completion)
        .unwrap();
    let ReadOutcome::Read { barrier, result } = outcome else {
        panic!("original read was lost")
    };
    assert_eq!(barrier.request(), ticket.request);
    q.check(&M::manifest(result.unwrap()).unwrap().unwrap())
        .unwrap();
    assert_eq!(lookup.source_mut().pending_reads(), 0);
}
fn run_lookup(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-metadata-lookup-{}-{protocol:?}-{checkpoint}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Instant::now();
    let env = Environment {
        root: &root,
        clock: &clock,
        protocol,
        checkpoint,
    };
    let (mut a, _) = initialize_source(&env);
    assert_eq!(
        lookup::<_, MetadataPublishingManifestRead>(&mut a, &clock, 1).unwrap(),
        root_manifest()
    );
    let (plan, image) = freeze_initial(&env, &mut a);
    assert_eq!(
        lookup::<_, MetadataPublishingManifestRead>(&mut a, &clock, 1),
        Err(ManifestDiscoveryError::Unavailable)
    );
    let (mut b, template) = import_first(&env, &a, &plan, &image);
    assert_eq!(
        lookup::<_, MetadataPublishingManifestRead>(&mut b, &clock, 9),
        Err(ManifestDiscoveryError::Unavailable)
    );
    let activation = activate_first(&env, &mut a, &mut b, &template);
    let observed = lookup::<_, MetadataPublishingManifestRead>(&mut b, &clock, 9).unwrap();
    assert_eq!(observed.input().authority, group(9));
    assert!(observed.input().generation > root_manifest().input().generation);
    let mut first = FirstMove {
        nodes: b,
        template,
        plan,
        activation,
    };
    let mut second = second_move(&env, &mut first);
    assert_eq!(
        lookup::<_, MetadataPublishingManifestRead>(&mut first.nodes, &clock, 9),
        Err(ManifestDiscoveryError::Unavailable)
    );
    let moved = lookup::<_, MetadataServingManifestRead>(&mut second.nodes, &clock, 11).unwrap();
    assert_eq!(moved.input().authority, group(11));
    assert!(moved.input().generation > observed.input().generation);
    // Each phase helper already aborts/reopens the actual native files; lookup
    // starts from the recovered Node and preserves all original control APIs.
    creation::abandon(a, 1);
    creation::abandon(first.nodes, 9);
    creation::abandon(second.nodes, 11);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn automatic_metadata_reads_tcp_after_moves_and_reopen() {
    for checkpoint in [false, true] {
        run_lookup(NativePeerProtocol::TcpTls, checkpoint);
    }
}
#[cfg(feature = "quic")]
#[test]
fn automatic_metadata_reads_quic_after_moves_and_reopen() {
    for checkpoint in [false, true] {
        run_lookup(NativePeerProtocol::Quic, checkpoint);
    }
}
