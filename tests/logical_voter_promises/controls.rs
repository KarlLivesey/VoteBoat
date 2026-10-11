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
use super::{oracle, program::Job, search, types::*};
fn control(graph: Graph, weak: Weak, expected: &str) {
    let _serial = super::MODEL.lock().unwrap_or_else(|e| e.into_inner());
    let counter = search::counterexample(graph, weak);
    println!(
        "{graph:?} {weak:?}: states={} edges={} length={} failure={} trace={:?}",
        counter.states,
        counter.edges,
        counter.trace.len(),
        counter.failure,
        counter.trace
    );
    assert_eq!(counter.failure, expected);
    let bad = search::replay(graph, &counter.trace, weak);
    assert_eq!(oracle::check(&bad), Err(counter.failure));
    let mut safe = State::new(graph);
    let mut unsafe_state = State::new(graph);
    for step in &counter.trace {
        let actual = if weak == Weak::WorkerOnly
            && *step == Step::Parent
            && matches!(safe.job(), Job::ReadAck(_, true))
        {
            Step::Admit
        } else {
            *step
        };
        let next = safe.advance(actual, Weak::None);
        oracle::transition(&safe, actual, &next).unwrap();
        let bad_next = unsafe_state.advance(*step, weak);
        let refused = weak == Weak::EraseVote
            && actual == Step::Admit
            && next.pending.is_none()
            && bad_next.pending.is_some();
        safe = next;
        unsafe_state = bad_next;
        if refused {
            break;
        }
    }
    assert_eq!(oracle::check(&safe), Ok(()));
    protected(&safe, weak);
    let continuation = search::complete(safe.clone());
    for step in continuation {
        let next = safe.advance(step, Weak::None);
        oracle::transition(&safe, step, &next).unwrap();
        safe = next;
    }
    assert!(safe.terminal);
    if weak == Weak::WorkerOnly {
        assert_eq!(safe.reads[1].completed, Some(Value::Y));
    }
}
fn protected(safe: &State, weak: Weak) {
    match weak {
        Weak::WorkerOnly => {
            assert_eq!(safe.writes.last(), Some(&Value::Y));
            assert_eq!(safe.reads[0].invoked_after, Some(Value::Y));
            assert!(safe.reads[0].completed.is_none());
            assert_ne!(safe.refused & 2, 0);
        }
        Weak::Written => {
            assert_ne!(safe.refused & 4, 0);
            let p = safe.pending.unwrap();
            assert!(p.wait_owned);
            assert!(!safe.cells[p.cell].records[p.position as usize - 1].applied);
        }
        Weak::CountPhysical => {
            assert_ne!(safe.refused & 8, 0);
            assert!(safe.elections.is_empty());
        }
        Weak::StaleCompletion => {
            assert!(safe.pending.unwrap().wait_owned);
            assert!(safe.held.is_none());
        }
        Weak::EraseVote => {
            assert_ne!(safe.refused & 1, 0);
            assert!(safe.cells[0].image.vote.is_some());
        }
        Weak::None => unreachable!(),
    }
}
#[test]
fn stale_cached_read_after_newer_completed_write() {
    control(
        Graph::Read,
        Weak::WorkerOnly,
        "fresh read returned older acknowledged value",
    );
}
#[test]
fn local_written_result_is_not_child_committed_applied_evidence() {
    control(
        Graph::Written,
        Weak::Written,
        "written child action escaped before exact commit and apply",
    );
}
#[test]
fn two_physical_representatives_are_one_logical_vote() {
    control(
        Graph::Repeated,
        Weak::CountPhysical,
        "physical representatives counted as extra logical voters",
    );
}
#[test]
fn old_original_output_cannot_free_a_fresh_epoch_effect() {
    control(
        Graph::StaleCompletion,
        Weak::StaleCompletion,
        "old action completion freed a fresh effect",
    );
}
#[test]
fn replacement_cannot_erase_parent_vote() {
    control(
        Graph::Promises,
        Weak::EraseVote,
        "logical voter durably granted both candidates in one term",
    );
}
