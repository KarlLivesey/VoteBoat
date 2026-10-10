// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "multi_rows.rs"]
mod rows;
use rows::{Assignment, Progress, Row};

impl Runner {
    fn multi_status(&mut self) -> Result<Progress, Failure> {
        let text = self.source_observation(None)?;
        Progress::parse(&text, self.sequence, self.operation)
    }
    fn multi_begin(&mut self) -> Result<Progress, Failure> {
        match self.exchange(
            self.source,
            &format!("drain-node {} {}", self.sequence, self.operation),
        )? {
            Attempt::Reply(text) if text.starts_with("OK ") => {
                Progress::parse(&text, self.sequence, self.operation)
            }
            Attempt::Interrupted(_) => self.multi_status(),
            Attempt::Reply(text)
                if text.starts_with("UNKNOWN ") || text == "ERR group drain publication busy\n" =>
            {
                self.multi_status()
            }
            Attempt::Reply(text) => Err(text.into()),
            Attempt::Unavailable => Err("source unavailable before group drain request".into()),
        }
    }
    fn multi_row(&mut self, offset: usize, count: usize) -> Result<Row, Failure> {
        let text = self.source_observation(Some(offset))?;
        Row::parse(&text, self.sequence, self.operation, offset, count)
    }
    fn assignments(&mut self, count: usize) -> Result<Vec<Assignment>, Failure> {
        let mut assignments = Vec::with_capacity(count);
        for offset in 0..count {
            let row = self.multi_row(offset, count)?;
            if row.assignment.source.node.get() != self.endpoints[self.source].node {
                return Err("group row belongs to a different drain source".into());
            }
            if assignments
                .last()
                .is_some_and(|old: &Assignment| old.group >= row.assignment.group)
            {
                return Err("source group rows are not sorted and unique".into());
            }
            if let Some(voter) = row.assignment.voter {
                if voter.target == self.endpoints[self.source].node
                    || !self.endpoints.iter().any(|p| p.node == voter.target)
                {
                    return Err("handoff target absent from peers or matches source".into());
                }
            }
            assignments.push(row.assignment);
        }
        Ok(assignments)
    }
    pub(super) fn execute_multi(&mut self) -> Result<(), Failure> {
        let original = self.multi_begin()?;
        self.execute_multi_started(original)
    }
    pub(super) fn execute_planned_single(&mut self, text: &str) -> Result<(), Failure> {
        let original = Progress::parse(text, self.sequence, self.operation)?;
        if original.groups != 1 {
            return Err("drain-run requires exactly one group; use group-drain-run".into());
        }
        self.execute_multi_started(original)
    }
    fn execute_multi_started(&mut self, original: Progress) -> Result<(), Failure> {
        let assignments = self.assignments(original.groups)?;
        let resume = self.source(&format!(
            "resume-drain {} {}",
            self.sequence, self.operation
        ))?;
        let mut current = Progress::parse(&resume, self.sequence, self.operation)?;
        loop {
            original.verify(&current)?;
            if current.ready {
                break;
            }
            let started = Instant::now();
            let remaining_before = self.remaining;
            for (offset, assignment) in assignments.iter().enumerate() {
                let row = self.multi_row(offset, original.groups)?;
                if &row.assignment != assignment {
                    return Err("source assignment changed during group drain".into());
                }
                if !row.done {
                    self.advance_group(assignment)?;
                }
            }
            current = self.multi_status()?;
            if !current.ready {
                self.pause_round(started, remaining_before);
            }
        }
        let text = self.source(&format!("drain-stop {} {}", self.sequence, self.operation))?;
        identity(&text, self.sequence, self.operation)?;
        if field(&text, "stopping")? != "true" || field(&text, "multi")? != "true" {
            return Err("source did not accept group drain shutdown".into());
        }
        println!("OK sequence={} operation={} groups={} shutdown_requested=true evidence=source_ready_and_stop_accepted retained_replica=true multi=true", self.sequence, self.operation, original.groups);
        Ok(())
    }
}

#[path = "multi_actions.rs"]
mod actions;
