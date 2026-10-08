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
//! Independent finite state-machine specification for local accepted membership
//! and ballot promises. No production helpers, network/quorum oracle, or disk
//! implementation is used; distributed leader/log safety is outside this model.
use std::collections::{HashMap, VecDeque};
const LAST_PHASE: u8 = 6;
const LAST_TERM: u8 = 2;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
struct Image {
    phase: u8,
    commit: u8,
    base: u8,
    term: u8,
    ballot: u8,
}
// Four candidate identities: node 1/store A, node 1/store B, node 2/store A,
// node 3/store A. Configurations move node 1 out, replace its learner store,
// then promote the replacement through a second joint transition.
fn voter(phase: u8, candidate: u8) -> bool {
    match phase {
        0 | 1 => matches!(candidate, 0 | 2),
        2 => matches!(candidate, 0 | 2 | 3),
        3 | 4 => matches!(candidate, 2 | 3),
        5 => matches!(candidate, 1..=3),
        6 => matches!(candidate, 1 | 2),
        _ => false,
    }
}
fn node(candidate: u8) -> u8 {
    match candidate {
        0 | 1 => 1,
        2 => 2,
        3 => 3,
        _ => unreachable!(),
    }
}
fn ballot(candidate: u8, phase: u8) -> u8 {
    1 + candidate * 7 + phase
}
fn identity(promise: u8) -> u8 {
    (promise - 1) / 7
}
fn origin(promise: u8) -> u8 {
    (promise - 1) % 7
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct Pending {
    next: Image,
    kind: Kind,
    written: bool,
    synced: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Kind {
    Vote,
    Configuration,
    Commit,
    Compact,
    Term,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct State {
    core: Image,
    disk: Image,
    pending: Option<Pending>,
    // Ghost history records promises that became recoverable, independently of
    // the current image or receipts. Zero means no promise at that term yet.
    history: [u8; 3],
    generation: u8,
    local: u8,
    receipt: Option<(u8, u8, u8)>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Variant {
    Correct,
    EraseRollback,
    ReplyAtWrite,
    IgnoreStore,
}
fn remember(state: &mut State) {
    let image = state.disk;
    if image.ballot != 0 && state.history[image.term as usize] == 0 {
        state.history[image.term as usize] = image.ballot;
    }
}
fn invariant(state: &State) -> Result<(), &'static str> {
    for image in [state.core, state.disk]
        .into_iter()
        .chain(state.pending.map(|p| p.next))
    {
        if image.base > image.commit
            || image.commit > image.phase
            || image.phase > LAST_PHASE
            || image.term > LAST_TERM
        {
            return Err("prefix/term bounds");
        }
        if image.ballot != 0
            && (image.term == 0 || !voter(origin(image.ballot), identity(image.ballot)))
        {
            return Err("promise origin eligibility");
        }
    }
    let promised = state.history[state.disk.term as usize];
    if promised != 0 && state.disk.ballot != promised {
        return Err("durable promise erased or changed in its term");
    }
    if let Some((term, promise, generation)) = state.receipt {
        if generation != state.generation || state.history[term as usize] != promise || promise == 0
        {
            return Err("reply lacks recoverable promise in this incarnation");
        }
    }
    if state.pending.is_none() && state.core != state.disk {
        return Err("quiescent recovery mismatch");
    }
    Ok(())
}
fn successors(s: State, variant: Variant) -> Vec<(String, State)> {
    let mut out = Vec::new();
    if let Some(p) = s.pending {
        if !p.written {
            let mut next = s;
            next.pending.as_mut().unwrap().written = true;
            if variant == Variant::ReplyAtWrite && p.kind == Kind::Vote {
                next.receipt = Some((p.next.term, p.next.ballot, s.generation));
            }
            out.push(("Write".into(), next));
        }
        if p.written && !p.synced {
            let mut next = s;
            next.disk = p.next;
            next.pending.as_mut().unwrap().synced = true;
            remember(&mut next);
            out.push(("Synchronize".into(), next));
        }
        if p.synced {
            let mut next = s;
            next.core = p.next;
            next.pending = None;
            if p.kind == Kind::Vote {
                next.receipt = Some((p.next.term, p.next.ballot, s.generation));
            }
            out.push(("CompleteExact".into(), next));
        }
        // A foreign/previous incarnation or duplicate receipt cannot consume
        // the pending transition, even after the disk has synchronized it.
        out.push(("CompleteStale (ignored)".into(), s));
    } else {
        let mut begin = |name: String, image: Image, kind: Kind| {
            let mut next = s;
            next.receipt = None;
            next.pending = Some(Pending {
                next: image,
                kind,
                written: false,
                synced: false,
            });
            out.push((name, next));
        };
        if s.core.phase < LAST_PHASE && s.core.commit == s.core.phase {
            let mut image = s.core;
            image.phase += 1;
            begin(
                format!("AcceptConfiguration {}", image.phase),
                image,
                Kind::Configuration,
            );
        }
        for head in s.core.commit..s.core.phase {
            if head < s.core.base {
                continue;
            }
            let mut image = s.core;
            image.phase = head;
            if variant == Variant::EraseRollback
                && image.ballot != 0
                && !voter(head, identity(image.ballot))
            {
                image.ballot = 0;
            }
            begin(format!("Rollback {head}"), image, Kind::Configuration);
        }
        if s.core.commit < s.core.phase {
            let mut image = s.core;
            image.commit = image.phase;
            begin("ObserveCommittedPrefix".into(), image, Kind::Commit);
        }
        if s.core.base < s.core.commit {
            let mut image = s.core;
            image.base = image.commit;
            begin("CompactVerifiedPrefix".into(), image, Kind::Compact);
        }
        if s.core.term < LAST_TERM {
            let mut image = s.core;
            image.term += 1;
            image.ballot = 0;
            begin("ObserveHigherTerm".into(), image, Kind::Term);
        }
        if voter(s.core.phase, s.local) {
            for term in s.core.term.max(1)..=LAST_TERM {
                for candidate in 0..4 {
                    if !voter(s.core.phase, candidate) {
                        continue;
                    }
                    let mut image = s.core;
                    image.term = term;
                    if term > s.core.term || s.core.ballot == 0 {
                        image.ballot = ballot(candidate, s.core.phase);
                    } else if identity(s.core.ballot) == candidate {
                        image.ballot = s.core.ballot;
                    } else if variant == Variant::IgnoreStore
                        && node(identity(s.core.ballot)) == node(candidate)
                    {
                        image.ballot = ballot(candidate, s.core.phase);
                    } else {
                        continue;
                    }
                    begin(
                        format!("AcceptVote term {term} identity {candidate}"),
                        image,
                        Kind::Vote,
                    );
                }
            }
        }
    }
    // At most one restart in this bounded instance. A written complete tail may
    // be recovered without an observed sync completion; neither branch emits a
    // vote before restart. Power-loss of unsynchronized bytes chooses old disk.
    if s.generation == 0 {
        let mut next = s;
        next.core = next.disk;
        next.pending = None;
        next.generation = 1;
        next.receipt = None;
        out.push(("CrashRecoverDurable".into(), next));
        if let Some(p) = s.pending.filter(|p| p.written && !p.synced) {
            let mut next = s;
            next.disk = p.next;
            remember(&mut next);
            next.core = next.disk;
            next.pending = None;
            next.generation = 1;
            next.receipt = None;
            out.push(("CrashRecoverCompleteTail".into(), next));
        }
    }
    out
}
#[derive(Debug)]
struct Exploration {
    states: usize,
    edges: usize,
    counterexample: Option<Vec<String>>,
    phases: u8,
    rollback_origin: bool,
    compacted_origin: bool,
    replacement_vote: bool,
}
fn explore(variant: Variant) -> Exploration {
    let mut states = Vec::new();
    let mut seen = HashMap::new();
    let mut queue = VecDeque::new();
    // Local replica 1/A is removed/replaced; 2/A remains eligible throughout.
    for local in [0, 2] {
        let initial = State {
            core: Image::default(),
            disk: Image::default(),
            pending: None,
            history: [0; 3],
            generation: 0,
            local,
            receipt: None,
        };
        seen.insert(initial, states.len());
        states.push((initial, None::<(usize, String)>));
        queue.push_back(states.len() - 1);
    }
    let mut report = Exploration {
        states: 0,
        edges: 0,
        counterexample: None,
        phases: 0,
        rollback_origin: false,
        compacted_origin: false,
        replacement_vote: false,
    };
    while let Some(index) = queue.pop_front() {
        let s = states[index].0;
        report.phases |= 1 << s.core.phase;
        report.rollback_origin |= s.disk.ballot != 0 && origin(s.disk.ballot) > s.disk.phase;
        report.compacted_origin |= s.disk.ballot != 0 && origin(s.disk.ballot) < s.disk.base;
        report.replacement_vote |= s.disk.ballot != 0 && identity(s.disk.ballot) == 1;
        if let Err(error) = invariant(&s) {
            let mut trace = vec![format!("VIOLATION: {error}")];
            let mut cursor = index;
            while let Some((parent, action)) = &states[cursor].1 {
                trace.push(action.clone());
                cursor = *parent;
            }
            trace.reverse();
            report.counterexample = Some(trace);
            break;
        }
        for (action, next) in successors(s, variant) {
            report.edges += 1;
            if let std::collections::hash_map::Entry::Vacant(entry) = seen.entry(next) {
                let next_index = states.len();
                entry.insert(next_index);
                states.push((next, Some((index, action))));
                queue.push_back(next_index);
            }
        }
        assert!(
            states.len() < 3_000_000,
            "model budget exceeded; report must not claim complete exploration"
        );
    }
    report.states = states.len();
    report
}
#[test]
fn bounded_local_membership_ballot_model_checks_all_reachable_states() {
    let report = explore(Variant::Correct);
    println!("correct: {report:?}");
    assert!(report.counterexample.is_none());
    assert_eq!(report.phases, 0x7f);
    assert!(report.rollback_origin && report.compacted_origin && report.replacement_vote);
}
#[test]
fn negative_controls_find_erased_promise_early_reply_and_store_reuse_counterexamples() {
    for variant in [
        Variant::EraseRollback,
        Variant::ReplyAtWrite,
        Variant::IgnoreStore,
    ] {
        let report = explore(variant);
        println!("{variant:?}: {report:?}");
        assert!(report.counterexample.is_some());
    }
}
