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
#![allow(dead_code)] // Shared deterministic/native retirement composition.
pub use crate::source_fixture as base;
#[path = "../transfer_target/fixtures.rs"]
pub mod target;
use voteboat::{
    application::*, bucket_counter::*, identity::*, retirement::*, routing::*, transfer::*,
    transfer_publication::*, transfer_source::*, transfer_target::*,
};
pub type Source = RetirementGuard<base::Source>;
pub type Target = RetirementGuard<target::Target>;
pub fn cfg() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
pub fn fresh_source() -> Source {
    RetirementGuard::new(base::fresh()).unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn fresh_target(g: u128) -> Target {
    RetirementGuard::new(target::fresh_for(g)).unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn source_log() -> Vec<voteboat::log::LogEntry> {
    vec![
        base::entry(1, 100, base::fresh().bootstrap_command(65536).unwrap()),
        base::entry(2, 1, base::data(1, 7)),
        base::entry(3, 2, base::data(200, 11)),
        base::entry(4, 200, base::freeze()),
    ]
}
pub fn frozen_source() -> Source {
    let mut s = fresh_source();
    s.apply_batch(&source_log()).unwrap();
    s
}
pub fn imported_targets(source: &Source) -> Vec<Target> {
    [21, 22]
        .into_iter()
        .map(|g| {
            let mut t = fresh_target(g);
            let import = target::from_source(source.owner().unwrap(), g, cfg());
            t.apply_batch(&[
                base::entry(1, 200, t.owner().unwrap().bootstrap_command(65536).unwrap()),
                base::entry(
                    2,
                    200,
                    t.owner().unwrap().import_command(&import, 65536).unwrap(),
                ),
            ])
            .unwrap();
            t
        })
        .collect()
}
pub fn split_decision(source: &Source, targets: &[Target]) -> TransferPublicationStatus {
    let sources =
        vec![
            SourceFenceEvidence::from_status(cfg(), source.freeze_status().unwrap().unwrap())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        ];
    let targets = targets
        .iter()
        .map(|t| {
            TargetReadyEvidence::from_status(cfg(), t.owner().unwrap().status())
                .unwrap_or_else(|e| panic!("{:?}", e.0))
        })
        .collect();
    let publication = TransferPublication::new(base::op(200), base::intent(), sources, targets)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    TransferPublicationStatus {
        publication_operation: base::op(201),
        index: 4,
        publication,
    }
}
pub fn activate(t: &mut Target, decision: &TransferPublicationStatus) {
    let a = TargetActivation {
        metadata_configuration: cfg(),
        decision: decision.clone(),
    };
    let operation = decision.publication.operation().get();
    let bytes = t.owner().unwrap().activation_command(&a, 65536).unwrap();
    t.apply_batch(&[base::entry(t.applied_index() + 1, operation, bytes)])
        .unwrap();
}
pub fn active_split() -> (Source, Vec<Target>, TransferPublicationStatus) {
    let source = frozen_source();
    let mut targets = imported_targets(&source);
    let decision = split_decision(&source, &targets);
    for t in &mut targets {
        activate(t, &decision);
    }
    (source, targets, decision)
}
pub fn proof(
    source: &SourceFreezeStatus,
    targets: impl IntoIterator<Item = TargetStatus>,
    decision: &TransferPublicationStatus,
    release: u128,
) -> RetirementProof {
    RetirementProof {
        metadata_configuration: cfg(),
        decision: decision.clone(),
        targets: targets
            .into_iter()
            .map(|s| {
                TargetActivationEvidence::from_status(cfg(), s)
                    .unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect(),
        release: RetentionRelease {
            source: source.fence.group,
            operation: source.fence.operation,
            fence_index: source.fence.index,
            release: base::op(release),
        },
    }
}
pub fn split_proof(
    source: &Source,
    targets: &[Target],
    decision: &TransferPublicationStatus,
) -> RetirementProof {
    proof(
        &source.freeze_status().unwrap().unwrap(),
        targets.iter().map(|t| t.owner().unwrap().status()),
        decision,
        900,
    )
}
pub fn later_intent() -> TransferIntent {
    let before = base::intent().after().clone();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(3).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
    after.execution = ExecutionMode::Single(base::group(23));
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
pub fn fresh_merged() -> Target {
    let inner = TransferTarget::new(
        base::group(23),
        base::op(300),
        later_intent(),
        BucketCounter::new(base::range(0, 256), base::Policy, base::bucket_limits()).unwrap(),
        base::Policy,
        target::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    RetirementGuard::new(inner).unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub fn merge(targets: &mut [Target]) -> (Target, TransferPublicationStatus) {
    for t in targets.iter_mut() {
        let bytes = t
            .owner()
            .unwrap()
            .freeze_command(&later_intent(), 65536, 65536)
            .unwrap();
        t.apply_batch(&[base::entry(t.applied_index() + 1, 300, bytes)])
            .unwrap();
    }
    let statuses: Vec<_> = targets
        .iter()
        .map(|t| t.freeze_status().unwrap().unwrap())
        .collect();
    let mut merged = fresh_merged();
    let images = targets
        .iter()
        .zip(&statuses)
        .map(|(t, s)| SourceImport {
            fence: s.fence,
            configuration: cfg(),
            image: t.export_target(base::group(23), 65536).unwrap(),
            digest: s.exports[0].digest,
        })
        .collect();
    let import = TargetImport::new(base::op(300), later_intent(), base::group(23), images)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    merged
        .apply_batch(&[
            base::entry(
                1,
                300,
                merged.owner().unwrap().bootstrap_command(65536).unwrap(),
            ),
            base::entry(
                2,
                300,
                merged
                    .owner()
                    .unwrap()
                    .import_command(&import, 65536)
                    .unwrap(),
            ),
        ])
        .unwrap();
    let publication = TransferPublication::new(
        base::op(300),
        later_intent(),
        statuses
            .into_iter()
            .map(|s| {
                SourceFenceEvidence::from_status(cfg(), s).unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect(),
        vec![
            TargetReadyEvidence::from_status(cfg(), merged.owner().unwrap().status())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        ],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let decision = TransferPublicationStatus {
        publication_operation: base::op(301),
        index: 7,
        publication,
    };
    activate(&mut merged, &decision);
    (merged, decision)
}
pub fn merged_data(key: u8, delta: i64) -> Vec<u8> {
    let mut hint = base::hint(key);
    hint.group = base::group(23);
    hint.epoch = OwnershipEpoch::new(3).unwrap();
    hint.generation = RouteGeneration::new(3).unwrap();
    voteboat::routed::encode_routed(
        hint,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
