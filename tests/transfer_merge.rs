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
use voteboat::{
    application::*, bucket_counter::*, identity::*, routed::*, transfer_publication::*,
    transfer_source::*, transfer_target::*,
};
#[path = "transfer_merge/fixtures.rs"]
mod fixture;
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
use source_fixture::{entry, op};
fn status(source: &fixture::Source) -> SourceFreezeStatus {
    let SourceRead::Freeze(Some(status)) = source
        .read_at(source.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("fence")
    };
    status
}
fn frozen(collision: bool) -> [fixture::Source; 2] {
    let mut sources = [
        fixture::source_ready(21, 1),
        fixture::source_ready(22, if collision { 1 } else { 2 }),
    ];
    for source in &mut sources {
        source
            .apply_batch(&[entry(3, 200, fixture::freeze())])
            .unwrap();
    }
    sources
}
fn import(sources: &[fixture::Source; 2]) -> TargetImport {
    TargetImport::new(
        op(200),
        fixture::intent(),
        source_fixture::group(23),
        sources
            .iter()
            .map(|s| fixture::image(s, &status(s), ConfigurationId::new(1).unwrap()))
            .collect(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
#[test]
fn compatible_merge_combines_actual_data_original_results_and_outbox_through_recovery() {
    let sources = frozen(false);
    let mut target = fixture::target();
    let command = target.import_command(&import(&sources), 65536).unwrap();
    target
        .apply_batch(&[
            entry(1, 200, target.bootstrap_command(65536).unwrap()),
            entry(2, 200, command),
        ])
        .unwrap();
    assert_eq!(target.status().imported.as_ref().unwrap().sources.len(), 2);
    assert!(target.status().activated.is_none());
    let evidence = sources
        .iter()
        .map(|s| {
            SourceFenceEvidence::from_status(ConfigurationId::new(1).unwrap(), status(s))
                .unwrap_or_else(|e| panic!("{:?}", e.0))
        })
        .collect();
    let ready = TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), target.status())
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let publication = TransferPublication::new(op(200), fixture::intent(), evidence, vec![ready])
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let activation = TargetActivation {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: TransferPublicationStatus {
            publication_operation: op(201),
            index: 4,
            publication,
        },
    };
    target
        .apply_batch(&[entry(
            3,
            200,
            target.activation_command(&activation, 65536).unwrap(),
        )])
        .unwrap();
    for (index, id, key, value) in [(4, 1, 1, 7), (5, 2, 200, 11)] {
        let TargetOutcome::Applied(r) = target
            .apply_batch(&[entry(index, id, fixture::data(key, value, true))])
            .unwrap()
            .remove(0)
            .outcome
        else {
            panic!("imported retry")
        };
        assert!(r.duplicate);
        assert_eq!(r.outcome, BucketOutcome::Value(value));
    }
    assert_eq!(target.application().outbox().count(), 2);
    target
        .apply_batch(&[
            entry(6, 3, fixture::data(1, 2, true)),
            entry(7, 4, fixture::data(200, 2, true)),
        ])
        .unwrap();
    let bytes = target.checkpoint(200000).unwrap();
    let mut restored = fixture::target();
    restored.restore_checkpoint(1, 7, &bytes).unwrap();
    assert_eq!(restored.status(), target.status());
    assert_eq!(restored.application().value(&[1]), Ok(9));
    assert_eq!(restored.application().value(&[200]), Ok(13));
    assert_eq!(restored.application().outbox().count(), 4);
    for source in sources {
        assert!(source.fence().is_some());
    }
}
#[test]
fn duplicate_source_operation_ids_refuse_merge_without_activating_or_mutating_target() {
    let sources = frozen(true);
    let mut target = fixture::target();
    target
        .apply_batch(&[entry(1, 200, target.bootstrap_command(65536).unwrap())])
        .unwrap();
    let before = target.checkpoint(200000).unwrap();
    let command = target.import_command(&import(&sources), 65536).unwrap();
    assert!(target
        .validate_proposal(op(200), &command, std::iter::empty())
        .is_err());
    assert!(target.apply_batch(&[entry(2, 200, command)]).is_err());
    assert_eq!(target.checkpoint(200000).unwrap(), before);
    assert!(target.status().imported.is_none() && target.status().activated.is_none());
    assert!(
        TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), target.status())
            .is_err()
    );
    for source in sources {
        assert!(source.fence().is_some());
        let key = if source.routed().local() == source_fixture::group(21) {
            1
        } else {
            200
        };
        assert_eq!(
            source
                .read_at(
                    3,
                    SourceQuery::Data(RoutedQuery {
                        hint: fixture::hint(key, false),
                        key: vec![key],
                        query: vec![key]
                    })
                )
                .unwrap(),
            SourceRead::Data(RoutedRead::Rejected(
                voteboat::routing::RoutingError::Fenced
            ))
        );
    }
}
#[test]
fn partial_source_coverage_cannot_import_publish_or_activate() {
    let sources = frozen(false);
    let only_left = vec![fixture::image(
        &sources[0],
        &status(&sources[0]),
        ConfigurationId::new(1).unwrap(),
    )];
    assert!(TargetImport::new(
        op(200),
        fixture::intent(),
        source_fixture::group(23),
        only_left
    )
    .is_err());
    let left =
        SourceFenceEvidence::from_status(ConfigurationId::new(1).unwrap(), status(&sources[0]))
            .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert!(TransferPublication::new(op(200), fixture::intent(), vec![left], vec![]).is_err());
}
