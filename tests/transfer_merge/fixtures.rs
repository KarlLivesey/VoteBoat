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
#![allow(dead_code)] // Shared deterministic and native merge conformance.
pub use crate::source_fixture as base;
use voteboat::{
    application::*, bucket_counter::*, identity::*, routed::*, routing::*, transfer::*,
    transfer_source::*, transfer_target::*,
};
pub type Source = TransferSource<BucketCounter<base::Policy>, base::Policy>;
pub type Target = TransferTarget<BucketCounter<base::Policy>, base::Policy>;
pub fn before() -> ResponsibilityManifest {
    let mut input = base::grant().into_input();
    input.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: base::range(0, 128),
            target: RouteTarget::Group(base::group(21)),
        },
        RouteEntry {
            scope: base::range(128, 256),
            target: RouteTarget::Group(base::group(22)),
        },
    ]);
    ResponsibilityManifest::new(input).unwrap()
}
pub fn intent() -> TransferIntent {
    let before = before();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Single(base::group(23));
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
pub fn source(g: u128) -> Source {
    let scope = if g == 21 {
        base::range(0, 128)
    } else {
        assert_eq!(g, 22);
        base::range(128, 256)
    };
    let routed = RoutedApplication::new(
        base::group(g),
        before(),
        BucketCounter::new(scope, base::Policy, base::bucket_limits()).unwrap(),
        base::Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: base::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    TransferSource::new(routed, 65536).unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn target() -> Target {
    TransferTarget::new(
        base::group(23),
        base::op(200),
        intent(),
        BucketCounter::new(base::range(0, 256), base::Policy, base::bucket_limits()).unwrap(),
        base::Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: base::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn hint(key: u8, new_owner: bool) -> RouteHint {
    let mut h = base::hint(key);
    if new_owner {
        h.group = base::group(23);
        h.epoch = OwnershipEpoch::new(2).unwrap();
        h.generation = RouteGeneration::new(2).unwrap();
    } else {
        h.group = base::group(if key < 128 { 21 } else { 22 });
        h.scope = if key < 128 {
            base::range(0, 128)
        } else {
            base::range(128, 256)
        };
    }
    h
}
pub fn data(key: u8, delta: i64, new_owner: bool) -> Vec<u8> {
    encode_routed(
        hint(key, new_owner),
        &[key],
        &encode_add(&[key], delta, b"merge-effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
pub fn freeze() -> Vec<u8> {
    Source::freeze_command(&intent(), 32776).unwrap()
}
pub fn image(
    source: &Source,
    status: &SourceFreezeStatus,
    configuration: ConfigurationId,
) -> SourceImport {
    let export = status
        .exports
        .iter()
        .find(|e| e.target == base::group(23))
        .unwrap();
    SourceImport {
        fence: status.fence,
        configuration,
        image: source.export_target(base::group(23), 65536).unwrap(),
        digest: export.digest,
    }
}
pub fn source_ready(g: u128, id: u128) -> Source {
    let mut s = source(g);
    let (key, value) = if g == 21 { (1, 7) } else { (200, 11) };
    s.apply_batch(&[
        base::entry(1, 100, s.bootstrap_command(65536).unwrap()),
        base::entry(2, id, data(key, value, false)),
    ])
    .unwrap();
    s
}
