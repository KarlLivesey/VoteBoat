// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn plan() -> TransferOperation {
    TransferOperation::new(
        source_fixture::intent(),
        source_fixture::op(200),
        source_fixture::op(201),
    )
    .unwrap_or_else(|e| panic!("operator plan: {:?}", e.0))
}

// Retain the real Node-completed barrier instead of manufacturing observations
// from local state or from a membership ID read after completion.
fn completed<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    query: A::Query,
) -> ReadOutcome<A::ReadResult>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    A::Query: Clone,
{
    for _ in 0..4 {
        campaign(nodes, clock, g);
        nodes[0]
            .read(group(g), query.clone())
            .unwrap_or_else(|_| panic!("read admission"));
        let mut result = None;
        drive(nodes, clock, |ns| {
            if let Some(reply) = ns[0].poll_read() {
                result = Some(
                    ns[0]
                        .complete_read(reply)
                        .unwrap_or_else(|_| panic!("read ownership")),
                );
            }
            result.is_some()
        });
        match result.unwrap() {
            outcome @ ReadOutcome::Read { result: Ok(_), .. } => return outcome,
            ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged)
            | ReadOutcome::NotRead(voteboat::raft::RaftError::NotLeader)
            | ReadOutcome::NotRead(voteboat::raft::RaftError::ReadNotReady) => {}
            _ => panic!("read unavailable"),
        }
    }
    panic!("operator read repeatedly lost leadership")
}

fn observations(rig: &mut Split) -> Vec<TransferObservation> {
    let mut values = vec![
        TransferObservation::intent_read(&completed(
            &mut rig.parent,
            &rig.clock,
            1,
            DirectoryQuery::Transfer(source_fixture::op(200)),
        ))
        .unwrap(),
        TransferObservation::publication_read(&completed(
            &mut rig.parent,
            &rig.clock,
            1,
            DirectoryQuery::Publication(source_fixture::op(200)),
        ))
        .unwrap(),
        TransferObservation::source_read(&completed(
            &mut rig.source,
            &rig.clock,
            20,
            SourceQuery::Freeze,
        ))
        .unwrap(),
    ];
    for i in 0..2 {
        values.push(
            TransferObservation::target_read(&completed(
                &mut rig.targets[i],
                &rig.clock,
                21 + i as u128,
                TargetQuery::Status,
            ))
            .unwrap(),
        );
    }
    values
}

fn check_reads(plan: &TransferOperation, reads: &[TransferObservation]) {
    assert_eq!(
        TransferObservation::intent_read(&ReadOutcome::NotRead(
            voteboat::raft::RaftError::NotLeader
        ))
        .unwrap_err(),
        TransferOperationError::NotRead
    );
    assert_eq!(
        TransferObservation::intent_read(&ReadOutcome::Unavailable(
            ReadUnavailable::LeadershipChanged
        ))
        .unwrap_err(),
        TransferOperationError::NotRead
    );
    assert!(matches!(
        plan.next(&reads[..reads.len() - 1], &[]),
        Err(TransferOperationError::MissingRead(_))
    ));
    let mut duplicate = reads.to_vec();
    duplicate[0] = duplicate[1].clone();
    assert_eq!(
        plan.next(&duplicate, &[]),
        Err(TransferOperationError::DuplicateRead)
    );
    let wrong = TransferOperation::new(
        source_fixture::intent(),
        source_fixture::op(202),
        source_fixture::op(203),
    )
    .unwrap();
    assert_eq!(
        wrong.next(reads, &[]),
        Err(TransferOperationError::Inconsistent)
    );
}

fn action(rig: &Split, plan: &TransferOperation, reads: &[TransferObservation]) -> TransferAction {
    let action = plan.next(reads, &[]).unwrap();
    assert_eq!(plan.next(reads, &[]).unwrap(), action);
    let TransferAction::Export(export) = action else {
        return action;
    };
    assert_eq!(export.source, group(20));
    let image = rig.source[0].local().applications[&export.source]
        .export_target(export.target, 65536)
        .unwrap();
    let wrong = voteboat::scope::ScopeImage::new(
        image.schema(),
        image.scheme(),
        image.scope(),
        image.source_applied(),
        vec![0],
    )
    .unwrap();
    assert_eq!(
        plan.next(
            reads,
            &[TransferImage {
                source: export.source,
                target: export.target,
                image: &wrong
            }]
        ),
        Err(TransferOperationError::WrongImage)
    );
    let images = [TransferImage {
        source: export.source,
        target: export.target,
        image: &image,
    }];
    let action = plan.next(reads, &images).unwrap();
    assert_eq!(plan.next(reads, &images).unwrap(), action);
    action
}

pub(super) fn resume_one(rig: &mut Split) -> Phase {
    let plan = plan(); // Reconstructed on every invocation with original IDs.
    let reads = observations(rig);
    check_reads(&plan, &reads);
    let action = action(rig, &plan, &reads);
    let (g, operation, bytes, phase) = match action {
        TransferAction::RecordIntent => {
            (1, 200, plan.intent().encode(32768).unwrap(), Phase::Intent)
        }
        TransferAction::Stage(g) => {
            let g = g.id.get();
            let bytes = rig.targets[(g - 21) as usize][0].local().applications[&group(g)]
                .bootstrap_command(65536)
                .unwrap();
            (g, 200, bytes, Phase::Stage(g))
        }
        TransferAction::Fence(g) => {
            assert_eq!(g, group(20));
            (
                20,
                200,
                source_fixture::Source::freeze_command(plan.intent(), 32776).unwrap(),
                Phase::Fence,
            )
        }
        TransferAction::Import(import) => {
            let g = import.target().id.get();
            let bytes = rig.targets[(g - 21) as usize][0].local().applications[&group(g)]
                .import_command(&import, 65536)
                .unwrap();
            (g, 200, bytes, Phase::Import(g))
        }
        TransferAction::Publish(publication) => {
            (1, 201, publication.encode(65536).unwrap(), Phase::Publish)
        }
        TransferAction::Activate { target, activation } => {
            let g = target.id.get();
            let bytes = rig.targets[(g - 21) as usize][0].local().applications[&target]
                .activation_command(&activation, 65536)
                .unwrap();
            (g, 200, bytes, Phase::Activate(g))
        }
        TransferAction::Complete => return Phase::Done,
        TransferAction::Export(_) => panic!("export did not produce import"),
    };
    // Deliberately discard every client receipt. Next invocation must observe
    // durable application history, including after close/reopen or compaction.
    match g {
        1 => {
            propose_recovering(&mut rig.parent, &rig.clock, g, operation, bytes);
        }
        20 => {
            propose_recovering(&mut rig.source, &rig.clock, g, operation, bytes);
        }
        21..=22 => {
            propose_recovering(
                &mut rig.targets[(g - 21) as usize],
                &rig.clock,
                g,
                operation,
                bytes,
            );
        }
        _ => unreachable!(),
    }
    phase
}
