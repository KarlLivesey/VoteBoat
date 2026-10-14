// SPDX-License-Identifier: RPL-1.5
//! Private semantic controls; phase facts here are NOT native quorum provenance.
use super::*;
use crate::{application::*, identity::*, log::*};
mod admission;
mod atomic;
mod recovery;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn key(id: u64) -> KeyId {
    KeyId::new(id).unwrap()
}
fn profile() -> ProgramProfile {
    ProgramProfile::new(
        1,
        vec![group(1), group(2)],
        vec![group(3), group(4), group(5)],
    )
    .unwrap()
}
fn program(id: u128, version: u64) -> Program {
    let parts = [3, 4, 5]
        .into_iter()
        .map(|id| {
            ParticipantProgram::new(
                group(id),
                vec![
                    KeyAccess {
                        key: key(1),
                        expected_version: KeyVersion::new(version),
                        write: Some(11),
                    },
                    KeyAccess {
                        key: key(2),
                        expected_version: KeyVersion::default(),
                        write: None,
                    },
                ],
            )
            .unwrap()
        })
        .collect();
    Program::new(TransactionId::new(id).unwrap(), profile(), parts).unwrap()
}
fn participant(id: u128, quota: usize) -> ProgramApplication {
    ProgramApplication::participant(
        group(id),
        profile(),
        &[(key(1), 10), (key(2), 20)],
        ProgramLimits::new(quota, 2).unwrap(),
    )
    .unwrap()
}
fn coordinator(program: &Program, quota: usize) -> ProgramApplication {
    ProgramApplication::coordinator(
        program.coordinator(),
        profile(),
        ProgramLimits::new(quota, 2).unwrap(),
    )
    .unwrap()
}
fn command(
    p: &Program,
    step: ProgramStep,
    decision: Option<Decision>,
    facts: Vec<PhaseFact>,
) -> Command {
    Command {
        program: p.clone(),
        step,
        decision,
        facts: facts.into_boxed_slice(),
        refusal: None,
    }
}
fn apply(app: &mut ProgramApplication, c: Command) -> ProgramReceipt {
    let entry = entry(app.applied + 1, app.group, &c);
    app.apply_batch(&[entry]).unwrap().remove(0)
}
fn entry(index: u64, group: GroupIdentity, c: &Command) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: c.operation(group).unwrap(),
            bytes: codec::encode(c).unwrap().into(),
        },
    }
}
fn fact(app: &ProgramApplication, r: ProgramReceipt) -> PhaseFact {
    PhaseFact {
        group: app.group,
        operation: r.operation,
        index: r.first_index,
    }
}
fn prepare(app: &mut ProgramApplication, p: &Program) -> ProgramReceipt {
    apply(app, command(p, ProgramStep::Prepare, None, vec![]))
}
struct Fixture {
    c: ProgramApplication,
    p: [ProgramApplication; 3],
}
impl Fixture {
    fn new(p: &Program) -> Self {
        Self {
            c: coordinator(p, 4),
            p: [participant(3, 4), participant(4, 4), participant(5, 4)],
        }
    }
    fn start(&mut self, p: &Program) {
        assert_eq!(
            apply(&mut self.c, Command::start(p.clone())).outcome,
            ProgramOutcome::Started
        );
    }
    fn prepared(&mut self, p: &Program) -> Vec<PhaseFact> {
        self.p
            .iter_mut()
            .map(|app| {
                let r = prepare(app, p);
                assert_eq!(r.outcome, ProgramOutcome::Prepare(PrepareOutcome::Prepared));
                fact(app, r)
            })
            .collect()
    }
    fn decide(&mut self, p: &Program, d: Decision, facts: Vec<PhaseFact>) -> PhaseFact {
        let r = apply(&mut self.c, command(p, ProgramStep::Decide, Some(d), facts));
        assert_eq!(r.outcome, ProgramOutcome::Decided(d));
        fact(&self.c, r)
    }
    fn finish(&mut self, p: &Program, d: Decision, decision: PhaseFact) {
        let applied = self
            .p
            .iter_mut()
            .map(|app| {
                let r = apply(
                    app,
                    command(p, ProgramStep::ApplyDecision, Some(d), vec![decision]),
                );
                assert_eq!(r.outcome, ProgramOutcome::DecisionApplied(d));
                fact(app, r)
            })
            .collect();
        let r = apply(
            &mut self.c,
            command(p, ProgramStep::AppliedAll, Some(d), applied),
        );
        assert_eq!(r.outcome, ProgramOutcome::AppliedAll(d));
        let all = fact(&self.c, r);
        let released = self
            .p
            .iter_mut()
            .map(|app| {
                let r = apply(app, command(p, ProgramStep::Release, Some(d), vec![all]));
                assert_eq!(r.outcome, ProgramOutcome::Released(d));
                fact(app, r)
            })
            .collect();
        assert_eq!(
            apply(
                &mut self.c,
                command(p, ProgramStep::Finish, Some(d), released)
            )
            .outcome,
            ProgramOutcome::Finished(d)
        );
    }
}
