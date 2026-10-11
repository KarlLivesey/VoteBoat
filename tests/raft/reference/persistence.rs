// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
impl Comparison {
    pub(super) fn persistence(&mut self, id: u64, operation: Persistence) -> Result<(), Mismatch> {
        match operation {
            Persistence::Hold => {
                self.native.hold(id);
                self.reference.hold(id);
                self.evidence.holds += 1;
            }
            Persistence::Write => {
                self.native.write_pending(id)?;
                self.reference.write_pending(id)?;
                self.evidence.storage_writes += 1;
            }
            Persistence::Sync => {
                self.native.sync(id)?;
                self.reference.sync(id)?;
                self.evidence.barriers += 1;
            }
            Persistence::Complete => {
                self.native.complete(id)?;
                self.reference.complete(id)?;
                self.evidence.completions += 1;
            }
            Persistence::ProcessCrash | Persistence::PowerLoss => {
                let power = operation == Persistence::PowerLoss;
                self.native.restart(id, power)?;
                self.reference.restart(id, power)?;
                self.evidence.restarts += 1;
            }
            Persistence::Stale => {
                self.native.stale(id)?;
                self.reference.stale(id)?;
                self.evidence.stale_checks += 1;
            }
            Persistence::LieReferenceSync => self.reference.lie_sync(id),
        }
        if self.native.phase(id) != self.reference.phase(id) {
            return Err(Mismatch::Action(id));
        }
        Ok(())
    }
}
fn stage(id: u64, operation: Persistence) -> Action {
    Action::Persistence(id, operation)
}
fn settle(actions: &mut Vec<Action>, leader: u64) {
    actions.extend([
        Action::Pump,
        Action::Heartbeat(leader),
        Action::Pump,
        Action::Check,
    ]);
}
fn base(id: u64, operation: u128) -> Vec<Action> {
    let mut actions = vec![Action::Campaign(id)];
    settle(&mut actions, id);
    actions.push(Action::Write(id, operation, 7));
    settle(&mut actions, id);
    actions
}
#[test]
fn accepted_written_and_synced_original_commands_match_after_process_and_power_cuts() {
    for id in 1..=3 {
        for phase in [Phase::Accepted, Phase::Written, Phase::Synced] {
            for power in [false, true] {
                let operation = u128::from(id) * 100;
                let mut actions = base(id, operation);
                actions.extend([
                    stage(id, Persistence::Hold),
                    Action::Write(id, operation + 1, 3),
                ]);
                if phase != Phase::Accepted {
                    actions.push(stage(id, Persistence::Write));
                }
                if phase == Phase::Synced {
                    actions.push(stage(id, Persistence::Sync));
                }
                actions.push(stage(
                    id,
                    if power {
                        Persistence::PowerLoss
                    } else {
                        Persistence::ProcessCrash
                    },
                ));
                if phase == Phase::Synced {
                    actions.push(stage(id, Persistence::Stale));
                }
                actions.push(Action::Campaign(id));
                settle(&mut actions, id);
                actions.push(Action::Write(id, operation + 1, 3));
                settle(&mut actions, id);
                actions.push(Action::Read(id, 71));
                let trace = Trace { seed: id, actions };
                let observation = replay(&trace).unwrap_or_else(|error| {
                    panic!("{error:?}\n{}", String::from_utf8(trace.encode()).unwrap())
                });
                assert_eq!(
                    replay(&Trace::decode(&trace.encode()).unwrap()),
                    Ok(observation.clone())
                );
                assert!(observation.values.values().all(|value| *value == 10));
                assert_eq!(observation.evidence.holds, 1);
                assert_eq!(
                    observation.evidence.storage_writes,
                    usize::from(phase != Phase::Accepted)
                );
                assert_eq!(
                    observation.evidence.barriers,
                    usize::from(phase == Phase::Synced)
                );
                assert_eq!(
                    observation.evidence.stale_checks,
                    usize::from(phase == Phase::Synced)
                );
                assert_eq!(observation.evidence.restarts, 1);
                assert_eq!(observation.evidence.reads, 1);
            }
        }
    }
}
#[test]
fn original_follower_acknowledgements_wait_for_explicit_durability_completion() {
    let mut run = Comparison::new();
    for action in [
        Action::Campaign(1),
        Action::Pump,
        Action::Heartbeat(1),
        Action::Pump,
        Action::Check,
        stage(2, Persistence::Hold),
        stage(3, Persistence::Hold),
        Action::Write(1, 501, 7),
        Action::Pump,
    ] {
        run.action(&action).unwrap();
    }
    for id in [2, 3] {
        assert_eq!(run.native.phase(id), Some(Phase::Accepted));
        assert_eq!(run.reference.phase(id), Some(Phase::Accepted));
        run.action(&stage(id, Persistence::Write)).unwrap();
        run.action(&stage(id, Persistence::Sync)).unwrap();
        run.action(&Action::Pump).unwrap();
    }
    assert!(run.observation().commands.values().all(Vec::is_empty));
    run.action(&stage(2, Persistence::Complete)).unwrap();
    run.action(&Action::Pump).unwrap();
    assert_eq!(run.commands(1), vec![(501, 7)]);
    assert_eq!(run.reference.commands(1), vec![(501, 7)]);
    run.action(&stage(3, Persistence::Complete)).unwrap();
    for action in [
        Action::Pump,
        Action::Heartbeat(1),
        Action::Pump,
        Action::Check,
        Action::Read(1, 1),
    ] {
        run.action(&action).unwrap();
    }
    assert!(run.observation().values.values().all(|value| *value == 7));
    assert_eq!(run.evidence.barriers, 2);
    assert_eq!(run.evidence.completions, 2);
}
#[test]
fn acknowledged_completed_original_ids_survive_restart_and_stale_callbacks() {
    for power in [false, true] {
        for id in 1..=3 {
            let mut actions = base(id, 600);
            actions.extend([
                stage(id, Persistence::Hold),
                Action::Write(id, 601, 3),
                stage(id, Persistence::Write),
                stage(id, Persistence::Sync),
                stage(id, Persistence::Complete),
            ]);
            settle(&mut actions, id);
            actions.push(stage(
                id,
                if power {
                    Persistence::PowerLoss
                } else {
                    Persistence::ProcessCrash
                },
            ));
            actions.extend([stage(id, Persistence::Stale), Action::Campaign(id)]);
            settle(&mut actions, id);
            actions.push(Action::Write(id, 601, 3));
            settle(&mut actions, id);
            actions.push(Action::Read(id, 72));
            let trace = Trace {
                seed: 60 + id,
                actions,
            };
            let result = replay(&trace).unwrap();
            assert_eq!(
                replay(&Trace::decode(&trace.encode()).unwrap()),
                Ok(result.clone())
            );
            assert!(result.values.values().all(|value| *value == 10));
            assert_eq!(result.evidence.completions, 1);
            assert_eq!(result.evidence.stale_checks, 1);
        }
    }
}
#[test]
fn falsely_reported_reference_sync_loses_original_ack_and_reduces_with_actual_power_cut() {
    let mut actions = base(1, 701);
    actions.extend([
        stage(1, Persistence::LieReferenceSync),
        stage(1, Persistence::Hold),
        Action::Write(1, 702, 3),
        stage(1, Persistence::Write),
        stage(1, Persistence::Sync),
        stage(1, Persistence::Complete),
    ]);
    settle(&mut actions, 1);
    actions.push(stage(1, Persistence::PowerLoss));
    let trace = Trace { seed: 70, actions };
    let target = Mismatch::ReferenceRecovery(1, 702);
    assert_eq!(replay(&trace), Err(target.clone()));
    let reduced = histories::reduce(trace.clone(), &target);
    assert!(reduced.actions.len() < trace.actions.len());
    assert!(reduced
        .actions
        .contains(&stage(1, Persistence::LieReferenceSync)));
    assert!(reduced.actions.contains(&stage(1, Persistence::PowerLoss)));
    assert!(reduced
        .actions
        .iter()
        .any(|action| matches!(action, Action::Write(1, 702, 3))));
    assert_eq!(
        replay(&Trace::decode(&reduced.encode()).unwrap()),
        Err(target)
    );
    let mut corrected = reduced.clone();
    corrected
        .actions
        .retain(|action| *action != stage(1, Persistence::LieReferenceSync));
    assert!(replay(&corrected).is_ok());
    println!(
        "reduced false reference sync:\n{}",
        String::from_utf8(reduced.encode()).unwrap()
    );
}
#[test]
fn persistence_opcodes_are_versioned_and_invalid_phase_order_is_refused() {
    assert!(Trace::decode(b"VBDIF1 1\nhold 1\n").is_err());
    assert!(Trace::decode(b"VBDIF2 1\nsync 4\n").is_err());
    let trace = Trace {
        seed: 1,
        actions: vec![stage(1, Persistence::Sync)],
    };
    assert_eq!(
        replay(&Trace::decode(&trace.encode()).unwrap()),
        Err(Mismatch::Action(1))
    );
}
