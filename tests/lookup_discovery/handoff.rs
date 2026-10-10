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
use voteboat::{
    log::{LogMutation, LogStore},
    native::lookup_discovery::*,
    placement::PlacementRequirements,
    raft::{persist_effect, Effect, Event, Raft, ReadBarrier},
};
fn source(generation: u64) -> HostReads {
    let mut source = HostReads::new();
    source.binding.generation = ReadInvocationGeneration::new(generation).unwrap();
    source
}
fn driver() -> NativeManifestLookup<HostReads> {
    NativeManifestLookup::new(
        source(1),
        ManifestCacheLimits {
            manifests: 1,
            bytes: MAX_MANIFEST_BYTES,
        },
        100,
        10,
        5,
        MonoTime(0),
    )
    .unwrap()
}
fn refused(
    driver: &mut NativeManifestLookup<HostReads>,
    replacement: HostReads,
    now: MonoTime,
    error: ManifestDiscoveryError,
) -> HostReads {
    let binding = driver.source_mut().binding;
    let pending = driver.pending();
    let started = driver.source_mut().started;
    let replacement_binding = replacement.binding;
    let replacement_pending = replacement.pending;
    let (actual, replacement) = driver.replace_source(replacement, now).unwrap_err();
    assert_eq!(actual, error);
    assert_eq!(replacement.binding, replacement_binding);
    assert_eq!(replacement.pending, replacement_pending);
    assert_eq!(driver.source_mut().binding, binding);
    assert_eq!(driver.source_mut().started, started);
    assert_eq!(
        driver.pending().map(|p| p.ticket),
        pending.map(|p| p.ticket)
    );
    replacement
}
#[test]
fn accepted_cancelled_reads_block_handoff_until_original_completion() {
    let mut driver = driver();
    driver.lookup(query(), MonoTime(0)).unwrap_err();
    let original = driver.pending().unwrap().ticket;
    let replacement = refused(
        &mut driver,
        source(2),
        MonoTime(0),
        ManifestDiscoveryError::Unavailable,
    );
    driver.cancel_pending().unwrap();
    let replacement = refused(
        &mut driver,
        replacement,
        MonoTime(1),
        ManifestDiscoveryError::Unavailable,
    );
    assert_eq!(driver.source_mut().pending, Some(original));
    assert_eq!(driver.source_mut().cancelled, 1);
    driver.source_mut().ready = true;
    assert!(matches!(
        driver.poll(MonoTime(2)),
        Err(ManifestLookupPollError::Discovery(
            ManifestDiscoveryError::Cancelled
        ))
    ));
    let old = driver.replace_source(replacement, MonoTime(2)).unwrap();
    assert_eq!(old.binding, original.binding);
    assert_eq!(old.pending, None);
    assert_eq!(old.cancelled, 1);
    assert_eq!(old.started, 1);
    assert_eq!(driver.source_mut().started, 0);
    // Replacement clears the old negative retry slot, but starts nothing itself.
    assert_eq!(
        driver.lookup(query(), MonoTime(2)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    assert_eq!(driver.pending().unwrap().ticket.binding, source(2).binding);
    assert_eq!(driver.next_deadline(), Some(MonoTime(12)));
}
#[test]
fn untracked_old_and_replacement_reads_remain_owned_after_refusal() {
    let mut driver = driver();
    let mut replacement = source(2);
    let ticket = replacement.submit(query()).unwrap();
    let mut replacement = refused(
        &mut driver,
        replacement,
        MonoTime(0),
        ManifestDiscoveryError::Unavailable,
    );
    replacement.ready = true;
    assert!(replacement.poll_result(ticket).unwrap().is_some());
    let original = driver.source_mut().submit(query()).unwrap();
    let replacement = refused(
        &mut driver,
        replacement,
        MonoTime(0),
        ManifestDiscoveryError::Unavailable,
    );
    assert!(driver.is_drained()); // A source-owned read still prohibits replacement.
    assert_eq!(driver.source_mut().pending, Some(original));
    driver.source_mut().ready = true;
    assert!(driver.source_mut().poll_result(original).unwrap().is_some());
    let old = driver.replace_source(replacement, MonoTime(1)).unwrap();
    assert_eq!(old.started, 1);
    assert_eq!(driver.source_mut().started, 1);
}
#[test]
fn binding_clock_and_closed_refusals_leave_driver_and_replacement_intact() {
    let mut driver = driver();
    refused(
        &mut driver,
        source(1),
        MonoTime(0),
        ManifestDiscoveryError::WrongAuthority,
    );
    assert!(!driver.poll(MonoTime(5)).unwrap());
    refused(
        &mut driver,
        source(2),
        MonoTime(4),
        ManifestDiscoveryError::TimeWentBack,
    );
    // Failed handoff did not advance the driver's clock.
    assert!(!driver.poll(MonoTime(5)).unwrap());
    driver.source_mut().binding = source(3).binding;
    refused(
        &mut driver,
        source(2),
        MonoTime(5),
        ManifestDiscoveryError::WrongAuthority,
    );
    driver.source_mut().binding = source(1).binding;
    driver.close();
    refused(
        &mut driver,
        source(2),
        MonoTime(5),
        ManifestDiscoveryError::Closed,
    );
    assert_eq!(
        driver.lookup(query(), MonoTime(5)).err(),
        Some(ManifestDiscoveryError::Closed)
    );
}
fn barrier(rounds: usize, request: u64) -> ReadBarrier {
    let mut store = support::HostLogStore::new(1);
    support::append(
        &mut store,
        vec![LogMutation::Create(support::bootstrap(1, 1))],
    );
    let mut core = Raft::recover(
        support::node(1),
        store.binding(),
        store.state(support::group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    for _ in 0..rounds {
        let mut effects = std::collections::VecDeque::from(core.step(Event::Campaign).unwrap());
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Persist(update) => {
                    effects.extend(persist_effect(&mut core, &mut store, update).unwrap())
                }
                Effect::Committed(_) => (),
                other => panic!("single-voter campaign: {other:?}"),
            }
        }
    }
    let effects = core
        .step(Event::Read {
            request: ReadRequestId::new(request).unwrap(),
        })
        .unwrap();
    let [Effect::ReadReady(barrier)] = effects.as_slice() else {
        panic!("single-voter read: {effects:?}")
    };
    core.finish_read(barrier, barrier.index()).unwrap();
    *barrier
}
fn manifest(generation: u64) -> ResponsibilityManifest {
    ResponsibilityManifest::new(ManifestInput {
        responsibility: query().locator.responsibility,
        parent: None,
        authority: query().locator.authority,
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        },
        scope: BucketRange::new(0, 256).unwrap(),
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(generation).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 1,
            survive_any_single_domain_loss: false,
        },
        state: ResponsibilityState::Active,
        execution: ExecutionMode::Single(support::group(1)),
    })
    .unwrap()
}
fn complete(
    driver: &mut NativeManifestLookup<HostReads>,
    barrier: ReadBarrier,
    manifest: ResponsibilityManifest,
    now: MonoTime,
) -> Result<bool, ManifestLookupPollError> {
    driver.lookup(query(), now).unwrap_err();
    assert_eq!(driver.pending().unwrap().ticket.request, barrier.request());
    driver.source_mut().ready = true;
    driver.source_mut().result = Some(ReadOutcome::Read {
        barrier,
        result: Ok(Some(manifest)),
    });
    driver.poll(now)
}
#[test]
fn source_sequence_reset_does_not_reuse_observation_ids_or_renew_old_hints() {
    let mut driver = driver();
    let barrier = barrier(1, 1);
    complete(&mut driver, barrier, manifest(2), MonoTime(0)).unwrap();
    let first = driver.lookup(query(), MonoTime(0)).unwrap();
    let old = driver.replace_source(source(2), MonoTime(1)).unwrap();
    assert_eq!(old.started, 1);
    assert!(!driver.invalidate(first.locator, first.observation));
    assert_eq!(
        driver.lookup(query(), MonoTime(1)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    assert_eq!(driver.source_mut().started, 1); // New source has its own ticket namespace.
    driver.source_mut().ready = true;
    driver.source_mut().result = Some(ReadOutcome::Read {
        barrier,
        result: Ok(Some(manifest(2))),
    });
    driver.poll(MonoTime(1)).unwrap();
    let second = driver.lookup(query(), MonoTime(1)).unwrap();
    assert_ne!(first.observation, second.observation);
    assert_eq!(second.expires_at, MonoTime(101));
    assert!(!driver.invalidate(first.locator, first.observation));
    assert_eq!(
        driver.lookup(query(), MonoTime(1)).unwrap().observation,
        second.observation
    );
    assert!(driver.invalidate(second.locator, second.observation));
}
#[test]
fn source_handoff_retains_manifest_and_quorum_barrier_floors() {
    let mut driver = driver();
    complete(&mut driver, barrier(2, 1), manifest(2), MonoTime(0)).unwrap();
    let first = driver.lookup(query(), MonoTime(0)).unwrap();
    driver.replace_source(source(2), MonoTime(1)).unwrap();
    assert!(matches!(
        complete(&mut driver, barrier(1, 1), manifest(2), MonoTime(1)),
        Err(ManifestLookupPollError::Discovery(
            ManifestDiscoveryError::StaleObservation
        ))
    ));
    assert!(!driver.invalidate(first.locator, first.observation));
    driver.replace_source(source(3), MonoTime(2)).unwrap();
    assert!(matches!(
        complete(&mut driver, barrier(3, 1), manifest(1), MonoTime(2)),
        Err(ManifestLookupPollError::Discovery(
            ManifestDiscoveryError::Routing(RoutingError::StaleGeneration)
        ))
    ));
    driver.replace_source(source(4), MonoTime(3)).unwrap();
    complete(&mut driver, barrier(3, 1), manifest(2), MonoTime(3)).unwrap();
    let second = driver.lookup(query(), MonoTime(3)).unwrap();
    assert_ne!(first.observation, second.observation);
    assert_eq!(second.manifest, manifest(2));
}

#[test]
fn missing_read_advances_retained_floor_before_source_handoff() {
    let mut driver = driver();
    complete(&mut driver, barrier(1, 1), manifest(2), MonoTime(0)).unwrap();
    let first = driver.lookup(query(), MonoTime(0)).unwrap();
    assert!(driver.invalidate(first.locator, first.observation));
    driver.lookup(query(), MonoTime(1)).unwrap_err();
    driver.source_mut().result = Some(ReadOutcome::Read {
        barrier: barrier(3, 2),
        result: Ok(None),
    });
    assert!(matches!(
        driver.poll(MonoTime(1)),
        Err(ManifestLookupPollError::Discovery(
            ManifestDiscoveryError::Missing
        ))
    ));
    driver.replace_source(source(2), MonoTime(2)).unwrap();
    assert!(matches!(
        complete(&mut driver, barrier(2, 1), manifest(2), MonoTime(2)),
        Err(ManifestLookupPollError::Discovery(
            ManifestDiscoveryError::StaleObservation
        ))
    ));
    driver.replace_source(source(3), MonoTime(3)).unwrap();
    complete(&mut driver, barrier(3, 1), manifest(2), MonoTime(3)).unwrap();
    let second = driver.lookup(query(), MonoTime(3)).unwrap();
    assert_ne!(first.observation, second.observation);
}
