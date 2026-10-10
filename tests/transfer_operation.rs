// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
mod support;
#[path = "transfer_target/fixtures.rs"]
mod target_fixture;
#[path = "transfer_operation/wire.rs"]
mod wire;
use source_fixture::{group, op};
use voteboat::{
    application::*, identity::*, log::*, raft::*, runtime::*, transfer::*, transfer_publication::*,
    transfer_source::*, transfer_target::*,
};

fn plan() -> TransferOperation {
    TransferOperation::new(source_fixture::intent(), op(200), op(201)).unwrap()
}

// A real single-voter core supplies the opaque barrier. The values below are
// deliberately synthetic adversarial inputs, not claims of native read proof.
// Native split histories separately exercise Node::complete_read end to end.
fn outcome<R>(g: u128, configuration: u64, result: R) -> ReadOutcome<R> {
    let mut store = support::HostLogStore::new(1);
    let mut boot = support::bootstrap(g, 1);
    boot.configuration = ConfigurationId::new(configuration).unwrap();
    support::append(&mut store, vec![LogMutation::Create(boot)]);
    let state = store.state(group(g)).unwrap();
    support::append(
        &mut store,
        vec![support::update(
            &state,
            1,
            100,
            Some(Suffix {
                from: 1,
                entries: (1..=100).map(source_fixture::noop).collect(),
            }),
        )],
    );
    let mut core = Raft::recover(
        support::node(1),
        store.binding(),
        store.state(group(g)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let mut effects = std::collections::VecDeque::from(core.step(Event::Campaign).unwrap());
    while let Some(effect) = effects.pop_front() {
        match effect {
            Effect::Persist(update) => {
                effects.extend(persist_effect(&mut core, &mut store, update).unwrap())
            }
            Effect::Committed(_) => {}
            other => panic!("unexpected campaign effect: {other:?}"),
        }
    }
    let [Effect::ReadReady(barrier)]: [Effect; 1] = core
        .step(Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        })
        .unwrap()
        .try_into()
        .unwrap()
    else {
        panic!("read barrier")
    };
    core.finish_read(&barrier, 101).unwrap();
    ReadOutcome::Read {
        barrier,
        result: Ok(result),
    }
}

#[derive(Clone)]
struct Views {
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    source: Option<SourceFreezeStatus>,
    targets: [TargetStatus; 2],
    source_configuration: u64,
}
impl Views {
    fn imported() -> Self {
        let source = target_fixture::frozen();
        let SourceRead::Freeze(status) = source
            .read_at(source.applied_index(), SourceQuery::Freeze)
            .unwrap()
        else {
            panic!("source")
        };
        let publication = target_fixture::publication();
        Self {
            intent: Some(TransferIntentStatus {
                operation: op(200),
                index: 3,
                intent: source_fixture::intent(),
            }),
            publication: None,
            source: status,
            targets: std::array::from_fn(|i| {
                let ready = &publication.targets()[i];
                TargetStatus {
                    group: ready.group,
                    operation: ready.operation,
                    staged_index: Some(ready.staged_index),
                    imported: Some(ready.imported.clone()),
                    activated: None,
                }
            }),
            source_configuration: 1,
        }
    }
    fn reads(&self) -> Vec<TransferObservation> {
        let mut reads = vec![
            TransferObservation::intent_read(&outcome(
                1,
                1,
                DirectoryRead::Transfer(self.intent.clone()),
            ))
            .unwrap(),
            TransferObservation::publication_read(&outcome(
                1,
                1,
                DirectoryRead::Publication(self.publication.clone()),
            ))
            .unwrap(),
            TransferObservation::source_read(&outcome(
                20,
                self.source_configuration,
                SourceRead::<()>::Freeze(self.source.clone()),
            ))
            .unwrap(),
        ];
        reads.extend(self.targets.iter().map(|t| {
            TransferObservation::target_read(&outcome(
                t.group.id.get(),
                1,
                TargetRead::<()>::Status(t.clone()),
            ))
            .unwrap()
        }));
        reads
    }
    fn next(&self) -> Result<TransferAction, TransferOperationError> {
        plan().next(&self.reads(), &[])
    }
    fn publish(&mut self) {
        let TransferAction::Publish(publication) = self.next().unwrap() else {
            panic!("publication")
        };
        self.publication = Some(TransferPublicationStatus {
            publication_operation: op(201),
            index: 4,
            publication,
        });
    }
}

#[test]
fn observations_reject_failed_wrong_type_wrong_group_and_out_of_prefix_values() {
    assert!(TransferOperation::new(source_fixture::intent(), op(200), op(200)).is_err());
    assert_eq!(
        TransferObservation::intent_read(&outcome(1, 1, DirectoryRead::Publication(None)))
            .unwrap_err(),
        TransferOperationError::WrongReadType
    );
    let mut v = Views::imported();
    v.intent.as_mut().unwrap().index = 102;
    assert_eq!(
        TransferObservation::intent_read(&outcome(1, 1, DirectoryRead::Transfer(v.intent)))
            .unwrap_err(),
        TransferOperationError::Inconsistent
    );
    assert_eq!(
        TransferObservation::target_read(&outcome(
            99,
            1,
            TargetRead::<()>::Status(v.targets[0].clone())
        ))
        .unwrap_err(),
        TransferOperationError::Inconsistent
    );
    v.targets[0].staged_index = Some(0);
    assert_eq!(
        TransferObservation::target_read(&outcome(
            21,
            1,
            TargetRead::<()>::Status(v.targets[0].clone())
        ))
        .unwrap_err(),
        TransferOperationError::Inconsistent
    );
    let mut reads = Views::imported().reads();
    reads[0] =
        TransferObservation::intent_read(&outcome(99, 1, DirectoryRead::Transfer(None))).unwrap();
    assert_eq!(
        plan().next(&reads, &[]),
        Err(TransferOperationError::UnexpectedRead)
    );
}

#[test]
fn missing_prerequisites_and_changed_operation_or_configurations_fail_closed() {
    let original = Views::imported();
    assert!(matches!(original.next(), Ok(TransferAction::Publish(_))));
    let mut cases = Vec::new();
    let mut v = original.clone();
    v.intent = None;
    cases.push(v);
    let mut v = original.clone();
    v.source = None;
    cases.push(v);
    let mut v = original.clone();
    v.targets[1].staged_index = None;
    cases.push(v);
    let mut v = original.clone();
    v.targets[0].operation = op(299);
    cases.push(v);
    let mut v = original.clone();
    v.intent.as_mut().unwrap().operation = op(299);
    cases.push(v);
    let mut v = original.clone();
    v.source_configuration = 2;
    cases.push(v);
    let mut v = original.clone();
    v.source.as_mut().unwrap().fence.operation = op(299);
    cases.push(v);
    let mut v = original.clone();
    v.targets[0].imported.as_mut().unwrap().sources[0].scope = source_fixture::range(1, 128);
    cases.push(v);
    let mut v = original;
    v.targets[0].imported.as_mut().unwrap().index = 1;
    cases.push(v);
    for (i, v) in cases.into_iter().enumerate() {
        assert_eq!(
            v.next(),
            Err(TransferOperationError::Inconsistent),
            "case {i}"
        );
    }
}

#[test]
fn published_provenance_is_preserved_when_current_source_configuration_changes() {
    let mut v = Views::imported();
    v.publish();
    let expected = v.next().unwrap();
    v.source_configuration = 7;
    assert_eq!(v.next().unwrap(), expected);
    let TransferAction::Activate { activation, .. } = expected else {
        panic!("activate")
    };
    assert_eq!(
        activation.decision.publication.sources()[0].configuration,
        ConfigurationId::new(1).unwrap()
    );
    v.publication.as_mut().unwrap().publication_operation = op(202);
    assert_eq!(v.next(), Err(TransferOperationError::Inconsistent));
}

#[test]
fn export_requires_exact_image_and_duplicate_images_are_rejected() {
    let mut v = Views::imported();
    v.targets[0].imported = None;
    let TransferAction::Export(export) = v.next().unwrap() else {
        panic!("export")
    };
    let source = target_fixture::frozen();
    let image = source.export_target(export.target, 65536).unwrap();
    let images = [
        TransferImage {
            source: group(20),
            target: group(21),
            image: &image,
        },
        TransferImage {
            source: group(20),
            target: group(21),
            image: &image,
        },
    ];
    assert_eq!(
        plan().next(&v.reads(), &images),
        Err(TransferOperationError::WrongImage)
    );
    let TransferAction::Import(import) = plan().next(&v.reads(), &images[..1]).unwrap() else {
        panic!("import")
    };
    assert_eq!(
        import,
        target_fixture::from_source(&source, 21, ConfigurationId::new(1).unwrap())
    );
}

#[test]
fn initial_progress_is_ordered_and_a_fence_never_substitutes_for_staging() {
    let mut v = Views::imported();
    v.intent = None;
    v.source = None;
    for target in &mut v.targets {
        target.staged_index = None;
        target.imported = None;
    }
    assert_eq!(v.next(), Ok(TransferAction::RecordIntent));
    v.intent = Views::imported().intent;
    assert_eq!(v.next(), Ok(TransferAction::Stage(group(21))));
    v.targets[0].staged_index = Some(1);
    assert_eq!(v.next(), Ok(TransferAction::Stage(group(22))));
    v.source = Views::imported().source;
    assert_eq!(v.next(), Err(TransferOperationError::Inconsistent));
    v.source = None;
    v.targets[1].staged_index = Some(1);
    assert_eq!(v.next(), Ok(TransferAction::Fence(group(20))));
}

#[test]
fn only_matching_recorded_activations_finish_the_original_operation() {
    let mut v = Views::imported();
    v.publish();
    for i in 0..2 {
        let g = 21 + i as u128;
        let mut target = target_fixture::fresh_for(g);
        let import = target_fixture::from_source(
            &target_fixture::frozen(),
            g,
            ConfigurationId::new(1).unwrap(),
        );
        target
            .apply_batch(&[
                source_fixture::entry(1, 200, target.bootstrap_command(65536).unwrap()),
                source_fixture::entry(2, 200, target.import_command(&import, 65536).unwrap()),
            ])
            .unwrap();
        let activation = TargetActivation {
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            decision: v.publication.clone().unwrap(),
        };
        target
            .apply_batch(&[source_fixture::entry(
                3,
                200,
                target.activation_command(&activation, 65536).unwrap(),
            )])
            .unwrap();
        v.targets[i] = target.status();
        assert_eq!(matches!(v.next(), Ok(TransferAction::Complete)), i == 1);
    }
    v.source_configuration = 8;
    assert_eq!(v.next(), Ok(TransferAction::Complete));
    let decoded = v
        .reads()
        .iter()
        .map(|r| {
            TransferObservation::decode_authenticated(
                &r.encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(plan().next(&decoded, &[]), Ok(TransferAction::Complete));
    let complete = v.clone();
    v.targets[0].activated.as_mut().unwrap().publication_index += 1;
    assert_eq!(v.next(), Err(TransferOperationError::Inconsistent));
    let mut v = complete;
    v.publication = None;
    assert_eq!(v.next(), Err(TransferOperationError::Inconsistent));
}
