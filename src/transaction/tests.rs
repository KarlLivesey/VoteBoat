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
//! Application-unit assertions use original applied phase results; they do not
//! establish native Node/quorum provenance for the later protocol driver.
use super::*;
use crate::{application::*, identity::*, log::*};
mod admission;
mod recovery;
mod refusal;

fn gid(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn aid(id: u64) -> AccountId {
    AccountId::new(id).unwrap()
}
fn spec(id: u128) -> TransferSpec {
    let profile = TransferProfile::new(gid(10), [gid(20), gid(30)]).unwrap();
    let ids: Vec<_> = (id * 100..id * 100 + 10)
        .map(|id| OperationId::new(id).unwrap())
        .collect();
    TransferSpec::new(
        TransactionId::new(id).unwrap(),
        profile,
        AccountRef::new(gid(20), aid(1)),
        AccountRef::new(gid(30), aid(2)),
        5,
        PhaseIds::new(
            ids[..4].try_into().unwrap(),
            [
                ids[4..7].try_into().unwrap(),
                ids[7..10].try_into().unwrap(),
            ],
        )
        .unwrap(),
    )
    .unwrap()
}
fn command(
    spec: TransferSpec,
    step: TransferStep,
    decision: Option<TransferDecision>,
    facts: [Option<PhaseFact>; 2],
) -> Command {
    Command {
        spec,
        step,
        decision,
        facts,
        refusal: None,
    }
}
fn apply(app: &mut TransferApplication, command: Command) -> TransferReceipt {
    let operation = command.operation(app.group()).unwrap();
    let entry = LogEntry {
        index: app.applied_index() + 1,
        term: 1,
        payload: EntryPayload::Command {
            operation,
            bytes: codec::encode(command).unwrap(),
        },
    };
    app.apply_batch(&[entry]).unwrap().remove(0)
}
fn fact(app: &TransferApplication, r: TransferReceipt) -> PhaseFact {
    PhaseFact {
        group: app.group(),
        operation: r.operation,
        index: r.first_index,
    }
}
struct Fixture {
    c: TransferApplication,
    p: [TransferApplication; 2],
}
impl Fixture {
    fn new(n: usize) -> Self {
        let profile = spec(1).profile();
        let limits = TransferLimits::new(n, 2).unwrap();
        Self {
            c: TransferApplication::coordinator(gid(10), profile, limits).unwrap(),
            p: [
                TransferApplication::participant(gid(20), profile, &[(aid(1), 20)], limits)
                    .unwrap(),
                TransferApplication::participant(gid(30), profile, &[(aid(2), 0)], limits).unwrap(),
            ],
        }
    }
    fn start(&mut self, s: TransferSpec) {
        assert_eq!(
            apply(&mut self.c, Command::start(s)).outcome,
            TransferOutcome::Started
        );
    }
    fn prepared(&mut self, s: TransferSpec) -> [Option<PhaseFact>; 2] {
        std::array::from_fn(|p| {
            let r = apply(
                &mut self.p[p],
                command(s, TransferStep::Prepare, None, [None; 2]),
            );
            assert!(matches!(
                r.outcome,
                TransferOutcome::Prepare(PrepareOutcome::Prepared { .. })
            ));
            Some(fact(&self.p[p], r))
        })
    }
    fn commit(&mut self, s: TransferSpec) {
        let facts = self.prepared(s);
        let decision = apply(
            &mut self.c,
            command(
                s,
                TransferStep::Decide,
                Some(TransferDecision::Commit),
                facts,
            ),
        );
        self.finish(s, TransferDecision::Commit, fact(&self.c, decision));
    }
    fn finish(&mut self, s: TransferSpec, d: TransferDecision, decision: PhaseFact) {
        let applied = std::array::from_fn(|p| {
            let r = apply(
                &mut self.p[p],
                command(
                    s,
                    TransferStep::ApplyDecision,
                    Some(d),
                    [Some(decision), None],
                ),
            );
            assert_eq!(r.outcome, TransferOutcome::DecisionApplied(d));
            Some(fact(&self.p[p], r))
        });
        for (p, app) in self.p.iter().enumerate() {
            if matches!(
                app.status(s.id()).unwrap().prepare,
                Some(PrepareOutcome::Prepared { .. })
            ) {
                assert_eq!(
                    app.account(s.account(app.group()).unwrap().account()),
                    AccountRead::Locked(s.id()),
                    "participant {p}"
                );
            }
        }
        let all = apply(
            &mut self.c,
            command(s, TransferStep::AppliedAll, Some(d), applied),
        );
        let all = fact(&self.c, all);
        let released = std::array::from_fn(|p| {
            let r = apply(
                &mut self.p[p],
                command(s, TransferStep::Release, Some(d), [Some(all), None]),
            );
            assert_eq!(r.outcome, TransferOutcome::Released(d));
            Some(fact(&self.p[p], r))
        });
        assert_eq!(
            apply(
                &mut self.c,
                command(s, TransferStep::Finish, Some(d), released)
            )
            .outcome,
            TransferOutcome::Finished(d)
        );
    }
}
#[test]
fn complete_capacity_one_retains_every_original_outcome_and_first_position() {
    let mut f = Fixture::new(1);
    let s = spec(1);
    f.start(s);
    f.commit(s);
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Balance(15));
    assert_eq!(f.p[1].account(aid(2)), AccountRead::Balance(5));
    assert_eq!(f.c.remaining_transactions(), 0);
    for app in [&mut f.c, &mut f.p[0]] {
        let before = app.status(s.id()).unwrap();
        for phase in before.phases.into_iter().flatten() {
            let original = app.records[&s.id()]
                .phases
                .iter()
                .flatten()
                .find(|p| p.index == phase.index)
                .unwrap()
                .command;
            let r = apply(app, original);
            assert!(r.duplicate);
            assert_eq!(r.first_index, phase.index);
            assert!(r.index > r.first_index);
            assert_eq!(r.outcome, phase.outcome);
        }
        assert_eq!(app.status(s.id()), Some(before));
    }
}
#[test]
fn participant_status_binds_exact_destination_and_reverse_direction_is_supported() {
    let mut f = Fixture::new(2);
    let s = spec(1);
    f.start(s);
    f.commit(s);
    for p in 0..2 {
        let status = f.p[p].status(s.id()).unwrap();
        assert_eq!(
            status.phases[0].unwrap().operation,
            s.phases().participants()[p][0]
        );
    }
    let original = spec(2);
    let reverse = TransferSpec::new(
        original.id(),
        original.profile(),
        original.credit(),
        original.debit(),
        5,
        original.phases(),
    )
    .unwrap();
    f.start(reverse);
    f.commit(reverse);
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Balance(20));
    assert_eq!(f.p[1].account(aid(2)), AccountRead::Balance(0));
}
