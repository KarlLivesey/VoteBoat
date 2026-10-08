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
#![allow(dead_code)] // Shared native and deterministic conformance fixture.
pub use crate::source_fixture as source;
use voteboat::{
    application::*, bucket_counter::*, identity::*, transfer_source::*, transfer_target::*,
};
pub type Target = TransferTarget<BucketCounter<source::Policy>, source::Policy>;
pub fn limits() -> TargetLimits {
    TargetLimits {
        import_bytes: 32768,
        application_checkpoint_bytes: source::bucket_limits().checkpoint_bound().unwrap(),
    }
}
pub fn fresh() -> Target {
    fresh_for(21)
}
pub fn fresh_for(g: u128) -> Target {
    let scope = if g == 21 {
        source::range(0, 128)
    } else {
        source::range(128, 256)
    };
    TransferTarget::new(
        source::group(g),
        source::op(200),
        source::intent(),
        BucketCounter::new(scope, source::Policy, source::bucket_limits()).unwrap(),
        source::Policy,
        limits(),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.0))
}
pub fn frozen() -> source::Source {
    let mut s = source::ready();
    s.apply_batch(&[
        source::entry(2, 1, source::data(1, 7)),
        source::entry(3, 2, source::data(200, 11)),
        source::entry(4, 200, source::freeze()),
    ])
    .unwrap();
    s
}
pub fn from_source(
    s: &source::Source,
    target: u128,
    configuration: ConfigurationId,
) -> TargetImport {
    let SourceRead::Freeze(Some(status)) =
        s.read_at(s.applied_index(), SourceQuery::Freeze).unwrap()
    else {
        panic!("source status")
    };
    let digest = status
        .exports
        .iter()
        .find(|e| e.target == source::group(target))
        .unwrap()
        .digest;
    TargetImport::new(
        source::op(200),
        source::intent(),
        source::group(target),
        vec![SourceImport {
            fence: s.fence().unwrap(),
            configuration,
            image: s.export_target(source::group(target), 65536).unwrap(),
            digest,
        }],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn import() -> TargetImport {
    from_source(&frozen(), 21, ConfigurationId::new(1).unwrap())
}
pub fn load() -> Vec<u8> {
    fresh().import_command(&import(), 65536).unwrap()
}
pub fn ready() -> Target {
    let mut t = fresh();
    t.apply_batch(&[source::entry(1, 200, t.bootstrap_command(65536).unwrap())])
        .unwrap();
    t
}
