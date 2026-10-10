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
#![cfg(feature = "native")]
#[path = "remote_manifest/session.rs"]
mod session;
mod support;
#[path = "remote_manifest/wire.rs"]
mod wire;
use session::*;
use std::collections::BTreeMap;
use voteboat::{
    identity::*,
    native::{remote_manifest::*, routing::NativeManifestCache},
    placement::PlacementRequirements,
    routing::*,
    runtime::MonoTime,
    secure::*,
};
fn group(id: u128) -> GroupIdentity {
    support::group(id)
}
fn rid(id: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(id).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn query(id: u128) -> ManifestLookup {
    ManifestLookup {
        locator: AuthorityLocator {
            responsibility: rid(id),
            authority: group(1),
        },
        minimum_epoch: None,
        minimum_generation: None,
    }
}
fn manifest(id: u128, generation: u64) -> ResponsibilityManifest {
    ResponsibilityManifest::new(ManifestInput {
        responsibility: rid(id),
        parent: None,
        authority: group(1),
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
        execution: ExecutionMode::Single(group(20)),
    })
    .unwrap()
}
struct Source {
    entries: BTreeMap<u128, ResponsibilityManifest>,
    calls: usize,
    withhold: bool,
    closed: bool,
}
impl Source {
    fn new() -> Self {
        Self {
            entries: [(1, manifest(1, 1)), (2, manifest(2, 1))].into(),
            calls: 0,
            withhold: false,
            closed: false,
        }
    }
}
impl ManifestDiscovery for Source {
    fn lookup(
        &mut self,
        r: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        self.calls += 1;
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        if self.withhold {
            return Err(ManifestDiscoveryError::Unavailable);
        }
        let m = self
            .entries
            .get(&r.locator.responsibility.id.get())
            .ok_or(ManifestDiscoveryError::Missing)?
            .clone();
        r.check(&m)?;
        Ok(ManifestObservation {
            locator: r.locator,
            observation: ManifestObservationId::new(self.calls as u64).unwrap(),
            manifest: m,
            expires_at: MonoTime(now.0 + 50_000),
        })
    }
    fn invalidate(&mut self, _: AuthorityLocator, _: ManifestObservationId) -> bool {
        false
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
fn cache() -> NativeManifestCache {
    NativeManifestCache::new(ManifestCacheLimits {
        manifests: 4,
        bytes: 4 * MAX_MANIFEST_BYTES,
    })
    .unwrap()
}
fn client<S: SecureSession>(s: S) -> NativeRemoteManifestDiscovery<S, NativeManifestCache> {
    NativeRemoteManifestDiscovery::new(
        s,
        peer(2),
        group(1),
        cache(),
        RemoteManifestConfig::default(),
        MonoTime(0),
    )
    .ok()
    .unwrap()
}
fn server<S: SecureSession>(s: S) -> NativeManifestResponder<S, Source> {
    NativeManifestResponder::new(
        s,
        peer(1),
        group(1),
        Source::new(),
        RemoteManifestConfig::default(),
        MonoTime(0),
    )
    .ok()
    .unwrap()
}
fn drive<S: SecureSession, T: SecureSession>(
    c: &mut NativeRemoteManifestDiscovery<S, NativeManifestCache>,
    s: &mut NativeManifestResponder<T, Source>,
    start: MonoTime,
) -> (ManifestRefreshCompletion, MonoTime) {
    for step in 0..2000 {
        let now = MonoTime(start.0 + step);
        s.poll(now, SessionPollBudget::default()).unwrap();
        if let Some(result) = c.poll(now, SessionPollBudget::default()).unwrap() {
            return (result, now);
        }
    }
    panic!("remote manifest did not complete")
}
#[test]
fn short_io_lookup_refresh_cancel_and_cache_during_outage() {
    let (a, b) = pair(1);
    let mut c = client(a);
    let mut s = server(b);
    assert_eq!(
        c.lookup(query(1), MonoTime(0)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let request = c.pending().unwrap();
    assert_eq!(
        c.lookup(query(1), MonoTime(0)).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    assert_eq!(c.pending(), Some(request));
    assert_eq!(
        c.lookup(query(2), MonoTime(0)).err(),
        Some(ManifestDiscoveryError::Overloaded)
    );
    let (done, now) = drive(&mut c, &mut s, MonoTime(0));
    assert_eq!(done.request, request);
    assert!(done.result.is_ok());
    let first = c.lookup(query(1), now).unwrap();
    assert_eq!(first.manifest, manifest(1, 1));
    assert_eq!(
        c.lookup(query(2), now).err(),
        Some(ManifestDiscoveryError::Unavailable)
    );
    let (_, now) = drive(&mut c, &mut s, now);
    assert!(c.lookup(query(2), now).is_ok());
    assert!(c.invalidate(query(1).locator, first.observation));
    s.source_mut().entries.insert(1, manifest(1, 2));
    assert!(c.lookup(query(1), now).is_err());
    let pending = c.pending().unwrap();
    assert!(c.cancel(pending));
    let (done, now) = drive(&mut c, &mut s, now);
    assert_eq!(done.result, Err(RemoteManifestError::Cancelled));
    let later = MonoTime(now.0 + 100);
    assert!(c.lookup(query(1), later).is_err());
    let (_, now) = drive(&mut c, &mut s, later);
    let fresh = c.lookup(query(1), now).unwrap();
    assert_eq!(fresh.manifest, manifest(1, 2));
    assert!(!c.invalidate(query(1).locator, first.observation));
    assert_eq!(c.usage().manifests, 2);
    s.source_mut().withhold = true;
    assert!(c.lookup(query(3), now).is_err());
    let pending = c.pending().unwrap();
    assert!(c.lookup(query(2), now).is_ok());
    let deadline = pending.deadline;
    let result = c
        .poll(deadline, SessionPollBudget::default())
        .unwrap()
        .unwrap();
    assert_eq!(result.result, Err(RemoteManifestError::Timeout));
    assert!(c.source_failed());
    assert!(c.lookup(query(2), deadline).is_ok());
    c.close();
    assert!(c.into_parts().is_ok());
    s.close();
    let (_, source) = s.into_parts();
    assert!(source.closed);
}
#[test]
fn wrong_source_authority_stale_generation_and_reconnect_are_checked() {
    let (mut a, b) = pair(1);
    a.binding.peer.node = support::node(99);
    assert!(NativeRemoteManifestDiscovery::new(
        a,
        peer(2),
        group(1),
        cache(),
        RemoteManifestConfig::default(),
        MonoTime(0)
    )
    .is_err());
    let (a, _) = pair(1);
    let mut c = client(a);
    let mut wrong = query(1);
    wrong.locator.authority = group(2);
    assert_eq!(
        c.lookup(wrong, MonoTime(0)).err(),
        Some(ManifestDiscoveryError::WrongAuthority)
    );
    assert!(c.pending().is_none());
    let mut s = server(b);
    assert!(c.lookup(query(1), MonoTime(0)).is_err());
    // This client is paired with a different server: timeout, never cached data.
    let deadline = c.pending().unwrap().deadline;
    c.poll(deadline, SessionPollBudget::default()).unwrap();
    let (old, _) = pair(1);
    assert!(c.replace_session(old).is_err());
    let (a, b) = pair(2);
    c.replace_session(a).ok().unwrap();
    s.close();
    s = server(b);
    let later = MonoTime(deadline.0 + 100);
    assert!(c.lookup(query(1), later).is_err());
    let (_, now) = drive(&mut c, &mut s, later);
    let first = c.lookup(query(1), now).unwrap();
    assert!(c.invalidate(query(1).locator, first.observation));
    s.source_mut().entries.insert(1, manifest(1, 2));
    assert!(c.lookup(query(1), now).is_err());
    let (_, now) = drive(&mut c, &mut s, now);
    let second = c.lookup(query(1), now).unwrap();
    assert_eq!(second.manifest.input().generation.get(), 2);
    assert!(c.invalidate(query(1).locator, second.observation));
    s.source_mut().entries.insert(1, manifest(1, 1));
    assert!(c.lookup(query(1), now).is_err());
    let (done, _) = drive(&mut c, &mut s, now);
    assert_eq!(
        done.result,
        Err(RemoteManifestError::Discovery(
            ManifestDiscoveryError::Routing(RoutingError::StaleGeneration)
        ))
    );
}
