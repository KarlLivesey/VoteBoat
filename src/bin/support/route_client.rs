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
//! Cold recursive discovery using the public resolver and authenticated sources.
use super::{
    authority_endpoints::Authorities,
    route_discovery::Discovery,
    service_access::ClientAccess,
    setup::{checked, Failure},
};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{identity::*, native::routing::NativeManifestCache, routing::*};
struct Options {
    hops: usize,
    epoch: Option<OwnershipEpoch>,
    generation: Option<RouteGeneration>,
}
impl Options {
    fn parse(args: &[String]) -> Result<Self, Failure> {
        let mut result = Self {
            hops: MAX_ROUTE_HOPS,
            epoch: None,
            generation: None,
        };
        let mut hops = false;
        if !args.len().is_multiple_of(2) {
            return Err("expected option/value pairs".into());
        }
        for pair in args.as_chunks::<2>().0 {
            match pair[0].as_str() {
                "--max-hops" if !hops => {
                    hops = true;
                    result.hops = pair[1].parse()?;
                }
                "--min-epoch" if result.epoch.is_none() => {
                    result.epoch =
                        Some(OwnershipEpoch::new(pair[1].parse()?).ok_or("invalid minimum epoch")?)
                }
                "--min-generation" if result.generation.is_none() => {
                    result.generation = Some(
                        RouteGeneration::new(pair[1].parse()?)
                            .ok_or("invalid minimum generation")?,
                    )
                }
                _ => return Err("unknown or duplicate route option".into()),
            }
        }
        if !(1..=MAX_ROUTE_HOPS).contains(&result.hops) {
            return Err("max hops must be1..32".into());
        }
        Ok(result)
    }
}
struct BytePartition;
impl PartitionPolicy for BytePartition {
    fn scheme(&self) -> PartitionScheme {
        PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        }
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        match key {
            [byte] => Ok(u16::from(*byte)),
            _ => Err(RoutingError::InvalidKey),
        }
    }
}
pub fn route(args: &[String]) -> Result<(), Failure> {
    let [tls, principal, group, incarnation, responsibility, responsibility_incarnation, key, map, rest @ ..] =
        args
    else {
        return Err("expected TLS PRINCIPAL AUTHORITY INCARNATION RESPONSIBILITY INCARNATION KEY_BYTE AUTHORITIES_FILE".into());
    };
    let options = Options::parse(rest)?;
    let start = ManifestLookup {
        locator: AuthorityLocator {
            authority: GroupIdentity {
                id: GroupId::new(group.parse()?).ok_or("invalid authority")?,
                incarnation: GroupIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid authority incarnation")?,
            },
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(responsibility.parse()?)
                    .ok_or("invalid responsibility")?,
                incarnation: ResponsibilityIncarnation::new(responsibility_incarnation.parse()?)
                    .ok_or("invalid responsibility incarnation")?,
            },
        },
        minimum_epoch: options.epoch,
        minimum_generation: options.generation,
    };
    let key = [key.parse::<u8>()?];
    let authorities = Authorities::load(Path::new(map))?;
    if !authorities.groups.contains_key(&start.locator.authority) {
        return Err("starting authority not provisioned".into());
    }
    let access = ClientAccess::load(Path::new(tls), principal.parse()?, &authorities.pins)?;
    let mut discovery = Discovery::new(authorities, access, Instant::now());
    let mut cache = checked(NativeManifestCache::new(ManifestCacheLimits {
        manifests: 64,
        bytes: 256 * 1024,
    }))?;
    loop {
        let request = DiscoverRouteRequest {
            start,
            key: &key,
            max_hops: options.hops,
            lookups: options.hops,
            now: discovery.now(),
        };
        match resolve_discovered(&mut cache, &BytePartition, &mut discovery, request) {
            Ok(hint) => {
                print_hint(hint);
                discovery.close();
                return Ok(());
            }
            Err(ManifestDiscoveryError::Unavailable) => (),
            Err(e) => return Err(format!("recursive lookup: {e:?}").into()),
        }
        discovery
            .poll()
            .map_err(|error| format!("recursive lookup failed: {error}"))?;
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn print_hint(h: RouteHint) {
    println!("OK responsibility={} incarnation={} execution_group={} execution_incarnation={} scope={}..{} bucket={} epoch={} generation={} hint_only=true",h.responsibility.id.get(),h.responsibility.incarnation.get(),h.group.id.get(),h.group.incarnation.get(),h.scope.start(),h.scope.end(),h.bucket,h.epoch.get(),h.generation.get());
}
