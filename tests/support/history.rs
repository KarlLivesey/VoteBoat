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
//! Bounded independent single-counter history checker, used only by tests.
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Write { operation: u128, delta: i64 },
    Read,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Reply {
    Value(i64),
    Conflict,
    Overflow,
    NotAdmitted,
    Unknown,
}
#[derive(Clone, Debug)]
pub struct Call {
    pub invoked: u64,
    pub completed: u64,
    pub action: Action,
    pub reply: Reply,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub calls: usize,
    pub states: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            calls: 32,
            states: 100_000,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckError {
    InvalidInput,
    NotLinearizable,
    SearchExhausted,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
struct Model {
    value: i64,
    applied: BTreeMap<u128, (i64, Reply)>,
}
impl Model {
    fn apply(&mut self, action: Action) -> Reply {
        let Action::Write { operation, delta } = action else {
            return Reply::Value(self.value);
        };
        if let Some((original, reply)) = self.applied.get(&operation) {
            return if *original == delta {
                *reply
            } else {
                Reply::Conflict
            };
        }
        let reply = if let Some(next) = self.value.checked_add(delta) {
            self.value = next;
            Reply::Value(next)
        } else {
            Reply::Overflow
        };
        self.applied.insert(operation, (delta, reply));
        reply
    }
}
struct Search<'a> {
    calls: &'a [Call],
    predecessors: Vec<u64>,
    required: u64,
    seen: BTreeSet<(u64, Model)>,
    remaining: usize,
}
impl Search<'_> {
    fn visit(
        &mut self,
        done: u64,
        model: Model,
        order: &mut Vec<usize>,
    ) -> Result<bool, CheckError> {
        if done & self.required == self.required {
            return Ok(true);
        }
        if self.seen.contains(&(done, model.clone())) {
            return Ok(false);
        }
        if self.remaining == 0 {
            return Err(CheckError::SearchExhausted);
        }
        self.remaining -= 1;
        self.seen.insert((done, model.clone()));
        for index in 0..self.calls.len() {
            let bit = 1u64 << index;
            if done & bit != 0 || self.predecessors[index] & !done != 0 {
                continue;
            }
            let call = &self.calls[index];
            let mut next = model.clone();
            let result = if call.reply == Reply::NotAdmitted {
                Reply::NotAdmitted
            } else {
                next.apply(call.action)
            };
            if call.reply != Reply::Unknown && result != call.reply {
                continue;
            }
            order.push(index);
            if self.visit(done | bit, next, order)? {
                return Ok(true);
            }
            order.pop();
        }
        Ok(false)
    }
}
/// Witness indices give a sequential order; omitted unknown calls have no effect.
/// Unknown completion is an observation of uncertainty, not the write's latest
/// possible linearization point. A bounded-out search is never reported valid.
pub fn check(calls: &[Call], limits: Limits) -> Result<Vec<usize>, CheckError> {
    if limits.calls > 63 || calls.len() > limits.calls || limits.states == 0 {
        return Err(CheckError::InvalidInput);
    }
    let mut events = BTreeSet::new();
    for call in calls {
        if call.invoked >= call.completed
            || !events.insert(call.invoked)
            || !events.insert(call.completed)
            || matches!(call.action, Action::Write { operation: 0, .. })
            || matches!(
                (call.action, call.reply),
                (Action::Read, Reply::Conflict | Reply::Overflow)
            )
        {
            return Err(CheckError::InvalidInput);
        }
    }
    let predecessors = calls
        .iter()
        .map(|call| {
            calls
                .iter()
                .enumerate()
                .filter(|(_, prior)| {
                    prior.reply != Reply::Unknown && prior.completed < call.invoked
                })
                .fold(0, |mask, (index, _)| mask | (1u64 << index))
        })
        .collect();
    let required = calls
        .iter()
        .enumerate()
        .filter(|(_, call)| call.reply != Reply::Unknown)
        .fold(0, |mask, (index, _)| mask | (1u64 << index));
    let mut search = Search {
        calls,
        predecessors,
        required,
        seen: BTreeSet::new(),
        remaining: limits.states,
    };
    let mut order = Vec::new();
    if search.visit(0, Model::default(), &mut order)? {
        Ok(order)
    } else {
        Err(CheckError::NotLinearizable)
    }
}
