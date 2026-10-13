// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Volatile native-log staging diagnostic: no physical durability, network,
//! consensus or application throughput claim. Real codec and validation paths.
use std::{fs::OpenOptions, io::Write, path::PathBuf};

#[path = "log_staging_benchmark/journal.rs"]
mod journal;
#[cfg(test)]
#[path = "log_staging_benchmark/tests.rs"]
mod tests;
#[path = "log_staging_benchmark/workload.rs"]
mod workload;

type Failure = Box<dyn std::error::Error>;
const WARMUP: usize = 16;

fn checked<T, E: std::fmt::Debug>(result: Result<T, E>) -> Result<T, Failure> {
    result.map_err(|error| format!("{error:?}").into())
}

#[derive(Clone, Copy, Debug)]
struct Workload {
    unrelated: usize,
    history: usize,
    iterations: usize,
}

impl Workload {
    fn validate(self) -> Result<Self, Failure> {
        if self.unrelated > 32
            || !(2..=4096).contains(&self.history)
            || !(1..=2000).contains(&self.iterations)
        {
            return Err(
                "expected unrelated groups 0..32, history 2..4096, iterations 1..2000".into(),
            );
        }
        Ok(self)
    }
}

fn arguments(args: &[String]) -> Result<(PathBuf, Workload), Failure> {
    let [output, unrelated, history, iterations] = args else {
        return Err(concat!(
            "usage: log_staging_benchmark FRESH_CSV_PATH ",
            "UNRELATED_GROUPS(0..32) HISTORY(2..4096) ITERATIONS(1..2000)"
        )
        .into());
    };
    let config = Workload {
        unrelated: unrelated.parse()?,
        history: history.parse()?,
        iterations: iterations.parse()?,
    }
    .validate()?;
    Ok((output.into(), config))
}

fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let (path, config) = arguments(&args)?;
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    let samples = workload::run(config)?;
    writeln!(output, "scope,workload,unrelated_groups,total_groups,history_per_group,warmup_iterations,iteration,append_ns,barrier_ns,total_ns,encoded_wal_bytes,manifest_bytes,append_calls,simulated_sync_calls,simulated_publish_calls,state_verified,recovery_verified")?;
    for sample in samples {
        writeln!(
            output,
            "volatile_native_log_staging,suffix_replacement,{},{},{},{},{},{},{},{},{},{},{},{},{},true,true",
            config.unrelated,
            config.unrelated + 1,
            config.history,
            WARMUP,
            sample.iteration,
            sample.append_ns,
            sample.barrier_ns,
            sample.total_ns,
            sample.io.wal_bytes,
            sample.io.manifest_bytes,
            sample.io.appends,
            sample.io.syncs,
            sample.io.publications,
        )?;
    }
    output.sync_all()?;
    println!(
        "scope=VOLATILE native_codec=true state_and_restart_verified=true unrelated_groups={} history={} measured_iterations={} warmup_iterations={WARMUP}; NOT a durability or consensus benchmark",
        config.unrelated, config.history, config.iterations
    );
    Ok(())
}
