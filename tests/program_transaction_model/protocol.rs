// SPDX-License-Identifier: RPL-1.5
use super::types::*;
pub fn actions(s: &State) -> Vec<Action> {
    let mut out = Vec::new();
    for t in 0..2 {
        let tx = &s.tx[t];
        match tx.pending {
            None if tx.stage != Step::Done => out.push(Action::Issue(t as u8)),
            Some(p) if !p.delivered => out.push(Action::Deliver(t as u8)),
            Some(_) => {
                out.push(Action::Observe(t as u8));
                out.push(Action::LoseThenResolve(t as u8));
            }
            _ => {}
        }
        if s.mode == Mode::TimeoutAbort
            && tx.decision == Some(Decision::Commit)
            && tx.participants.iter().any(|p| p.applied.is_none())
        {
            out.push(Action::Timeout(t as u8));
        }
    }
    out
}
pub fn apply(mut s: State, a: Action) -> State {
    let (Action::Issue(t)
    | Action::Deliver(t)
    | Action::Observe(t)
    | Action::LoseThenResolve(t)
    | Action::Timeout(t)) = a;
    let t = usize::from(t);
    match a {
        Action::Issue(_) => {
            s.tx[t].pending = Some(Pending {
                original: s.tx[t].stage,
                delivered: false,
                result: s.tx[t].stage,
            })
        }
        Action::Deliver(_) => {
            let original = s.tx[t].pending.unwrap().original;
            let result = deliver(&mut s, t, original);
            s.tx[t].retained[original.retained_slot()] = Some(result);
            s.tx[t].pending = Some(Pending {
                original,
                delivered: true,
                result,
            });
        }
        Action::Observe(_) => {
            s.tx[t].stage = s.tx[t].pending.unwrap().result;
            s.tx[t].pending = None;
        }
        Action::LoseThenResolve(_) => {
            // Abstract fresh quorum resolution of this exact committed phase.
            // The pending local reply is discarded, not used as the authority.
            // This action does not claim physical crash or own-byte recovery.
            let original = s.tx[t].pending.take().unwrap().original;
            s.tx[t].stage = s.tx[t].retained[original.retained_slot()].unwrap();
        }
        Action::Timeout(_) => {
            if let Some(p) = s.tx[t]
                .participants
                .iter_mut()
                .find(|p| p.prepare == Some(true) && p.applied.is_none())
            {
                p.applied = Some(Decision::Abort);
            }
        }
    }
    s
}
fn deliver(s: &mut State, t: usize, step: Step) -> Step {
    match step {
        Step::Start => Step::Prepare(0),
        Step::Prepare(p) => prepare(s, t, usize::from(p)),
        Step::Decide => {
            let commit = s.tx[t].participants.iter().all(|p| p.prepare == Some(true));
            s.tx[t].decision = Some(if commit {
                Decision::Commit
            } else {
                Decision::Abort
            });
            Step::Apply(0)
        }
        Step::Apply(p) => {
            decision(s, t, usize::from(p));
            if p == 2 {
                Step::All
            } else {
                Step::Apply(p + 1)
            }
        }
        Step::All => {
            s.tx[t].all = true;
            Step::Release(0)
        }
        Step::Release(p) => {
            for k in &mut s.keys[usize::from(p)] {
                if k.lock == Some(t as u8) {
                    k.lock = None;
                }
            }
            s.tx[t].participants[usize::from(p)].released = true;
            if p == 2 {
                Step::Finish
            } else {
                Step::Release(p + 1)
            }
        }
        Step::Finish => {
            s.tx[t].finished = true;
            Step::Done
        }
        Step::Done => Step::Done,
    }
}
fn prepare(s: &mut State, t: usize, p: usize) -> Step {
    let keys = &mut s.keys[p];
    let ready = keys
        .iter()
        .all(|k| k.lock.is_none() && (s.mode == Mode::SkipVersion || k.version == 0));
    if ready {
        for key in keys {
            key.lock = Some(t as u8);
        }
    } else if s.mode == Mode::PartialPrepare {
        if let Some(key) = keys.iter_mut().find(|k| k.lock.is_none()) {
            key.lock = Some(t as u8);
        }
    }
    s.tx[t].participants[p].prepare = Some(ready);
    if ready && p < 2 {
        Step::Prepare(p as u8 + 1)
    } else {
        Step::Decide
    }
}
fn decision(s: &mut State, t: usize, p: usize) {
    let decision = s.tx[t].decision.unwrap();
    if decision == Decision::Commit {
        let written = &mut s.keys[p][t];
        written.value = 0;
        written.version += 1;
    }
    s.tx[t].participants[p].applied = Some(decision);
    if s.mode == Mode::EarlyRelease {
        for key in &mut s.keys[p] {
            if key.lock == Some(t as u8) {
                key.lock = None;
            }
        }
    }
}
