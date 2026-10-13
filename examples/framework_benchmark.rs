// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Volatile, in-process public-core attribution scaffold, not a durability benchmark.
use std::{env, error::Error};

#[path = "framework_benchmark/cluster.rs"]
mod cluster;
#[path = "framework_benchmark/store.rs"]
mod store;
#[path = "framework_benchmark/workload.rs"]
mod workload;

type Failure = Box<dyn Error>;

#[derive(Clone, Copy, Debug)]
struct Settings {
    clients: usize,
    operations: usize,
    warmup: usize,
    batch: usize,
    retained_entries: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            clients: 1,
            operations: 1024,
            warmup: 1024,
            batch: 1,
            retained_entries: 16384,
        }
    }
}
impl Settings {
    fn validate(self) -> Result<Self, Failure> {
        if self.batch != 1 {
            return Err(
                "batch writes unsupported: public Event::Propose admits one command".into(),
            );
        }
        if self.clients == 0 || self.clients > 4096 || self.operations < self.clients {
            return Err("clients must be 1..=4096 and operations must cover every client".into());
        }
        if !self.operations.is_multiple_of(self.clients) {
            return Err(
                "operations must be divisible by clients; every client runs the same count".into(),
            );
        }
        let history = self
            .operations
            .checked_add(self.warmup)
            .and_then(|n| n.checked_add(1));
        if history.is_none_or(|n| n > self.retained_entries) {
            return Err("operations plus warmup and leader no-op exceed retained history; no hidden reset or checkpoint".into());
        }
        Ok(self)
    }
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Failure> {
        let mut settings = Self::default();
        let mut args = args;
        while let Some(name) = args.next() {
            let value = args
                .next()
                .ok_or("every option requires a numeric value")?
                .parse()?;
            match name.as_str() {
                "--clients" => settings.clients = value,
                "--operations" => settings.operations = value,
                "--warmup" => settings.warmup = value,
                "--batch" => settings.batch = value,
                "--retained-entries" => settings.retained_entries = value,
                _ => return Err(format!("unknown option: {name}").into()),
            }
        }
        settings.validate()
    }
}

fn main() -> Result<(), Failure> {
    let settings = Settings::parse(env::args().skip(1))?;
    let report = workload::run(settings)?;
    println!("{}", report.json(settings));
    Ok(())
}

#[cfg(test)]
#[path = "framework_benchmark/tests.rs"]
mod tests;
