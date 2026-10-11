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
use super::types::*;
#[derive(Clone, Copy, Debug)]
pub enum Select {
    Chosen,
    Opposite,
    A,
    B,
}
impl Select {
    pub fn get(self, state: &State) -> Candidate {
        match self {
            Self::Chosen => state.selected.unwrap(),
            Self::Opposite => state.selected.unwrap().other(),
            Self::A => Candidate::A,
            Self::B => Candidate::B,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum Job {
    Claim(usize, u8),
    Choose,
    Promise(usize, u8, Select),
    Vote(usize, u8, Select, bool),
    Election(u8, Select, bool),
    Dedup,
    Append(usize, u8, Value, u8),
    AppendAck(usize, u8),
    Commit(usize, u8),
    Ack(Value),
    Invoke(bool),
    ReadAck(usize, bool),
    Read(bool),
    Held(bool),
    End,
}
pub fn jobs(graph: Graph) -> Vec<Job> {
    use Job::*;
    use Select::*;
    let mut jobs = vec![Claim(0, 1), Claim(1, 1), Claim(2, 1)];
    match graph {
        Graph::Promises => jobs.extend([
            Choose,
            Promise(0, 1, Chosen),
            Vote(0, 1, Chosen, false),
            Claim(0, 2),
            Promise(0, 1, Opposite),
            Promise(1, 1, Chosen),
            Vote(1, 1, Chosen, false),
            Election(1, Chosen, false),
            Promise(2, 1, Chosen),
            Vote(2, 1, Chosen, false),
            Dedup,
        ]),
        Graph::Repeated => jobs.extend([
            Promise(0, 1, A),
            Vote(0, 1, A, false),
            Claim(0, 2),
            Vote(0, 1, A, false),
            Election(1, A, true),
            Dedup,
            Promise(1, 1, A),
            Vote(1, 1, A, false),
            Election(1, A, false),
        ]),
        Graph::Read => {
            initial_election(&mut jobs);
            jobs.extend([
                Append(0, 1, Value::X, 1),
                AppendAck(0, 1),
                Append(1, 1, Value::X, 1),
                AppendAck(1, 1),
                Commit(0, 1),
                Ack(Value::X),
                Claim(0, 2),
                Claim(1, 2),
                Promise(0, 2, B),
                Vote(0, 2, B, false),
                Promise(1, 2, B),
                Vote(1, 2, B, false),
                Election(2, B, false),
                Append(0, 2, Value::Y, 2),
                AppendAck(0, 2),
                Append(1, 2, Value::Y, 2),
                AppendAck(1, 2),
                Commit(0, 2),
                Ack(Value::Y),
                Invoke(true),
                ReadAck(0, true),
                ReadAck(1, true),
                Read(true),
                Invoke(false),
                ReadAck(0, false),
                ReadAck(1, false),
                Read(false),
            ]);
        }
        Graph::Written => {
            initial_election(&mut jobs);
            jobs.push(Append(0, 1, Value::X, 1));
        }
        Graph::StaleCompletion => {
            initial_election(&mut jobs);
            jobs.extend([Vote(0, 1, A, true), Claim(0, 2), Held(true), Held(false)]);
        }
    }
    jobs.push(End);
    jobs
}
fn initial_election(jobs: &mut Vec<Job>) {
    jobs.extend([
        Job::Promise(0, 1, Select::A),
        Job::Vote(0, 1, Select::A, false),
        Job::Promise(1, 1, Select::A),
        Job::Vote(1, 1, Select::A, false),
        Job::Election(1, Select::A, false),
    ]);
}
impl State {
    pub fn job(&self) -> Job {
        jobs(self.graph)[self.cursor]
    }
}
