// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Static partitioned lanes, using the same public Node and native providers.
use super::*;
use std::{path::PathBuf, sync::mpsc, thread};

#[path = "lanes/report.rs"]
mod report;
#[cfg(test)]
#[path = "lanes/tests.rs"]
mod tests;
#[path = "lanes/worker.rs"]
mod worker;

#[derive(Clone, Copy, Debug)]
struct Config {
    lanes: usize,
    groups: usize,
    count: usize,
    window: usize,
}
impl Config {
    fn validate(self) -> Result<Self, Failure> {
        if !(1..=4).contains(&self.lanes)
            || !(self.lanes..=32).contains(&self.groups)
            || !(self.lanes..=50000).contains(&self.count)
            || !(self.lanes..=32).contains(&self.window)
        {
            return Err("invalid lanes/groups/operations/window bounds".into());
        }
        for lane in 0..self.lanes {
            let plan = self.plan(lane);
            if (plan.count + plan.warmup).div_ceil(plan.partition.groups) + 64
                > LogLimits::default().max_entries_per_group
            {
                return Err("lane workload exceeds default log capacity".into());
            }
        }
        Ok(self)
    }
    fn plan(self, lane: usize) -> Plan {
        let groups = share(self.groups, self.lanes, lane);
        let offset = (0..lane).map(|i| share(self.groups, self.lanes, i)).sum();
        Plan {
            partition: shared::Partition {
                offset,
                groups,
                lane: lane as u64 + 1,
            },
            count: share(self.count, self.lanes, lane),
            warmup: share(WARMUP, self.lanes, lane),
            window: share(self.window, self.lanes, lane),
        }
    }
}
fn share(total: usize, lanes: usize, lane: usize) -> usize {
    total / lanes + usize::from(lane < total % lanes)
}
#[derive(Clone, Copy)]
struct Plan {
    partition: shared::Partition,
    count: usize,
    warmup: usize,
    window: usize,
}
struct Report {
    plan: Plan,
    measured: Measurement,
    retries: usize,
}
enum Command {
    Start(Instant),
    Abort,
    Verify,
}
struct Worker {
    command: mpsc::SyncSender<Command>,
    ready: mpsc::Receiver<()>,
    join: thread::JoinHandle<Result<Report, String>>,
}
impl Worker {
    fn spawn(root: PathBuf, plan: Plan, protocol: NativePeerProtocol) -> Result<Self, Failure> {
        let (command, commands) = mpsc::sync_channel(1);
        let (prepared, ready) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name(format!("voteboat-benchmark-lane-{}", plan.partition.lane))
            .spawn(move || {
                worker::run(root, plan, protocol, commands, prepared)
                    .map_err(|error| error.to_string())
            })?;
        Ok(Self {
            command,
            ready,
            join,
        })
    }
}

pub(super) fn run(args: &[String]) -> Result<(), Failure> {
    let [root, protocol, count, window, groups, lanes] = args else {
        return Err("usage: native_benchmark --lanes FRESH_DIRECTORY tcp|quic OPERATIONS WINDOW GROUPS LANES(1..4)".into());
    };
    let protocol = match protocol.as_str() {
        "tcp" => NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        "quic" => NativePeerProtocol::Quic,
        _ => return Err("unsupported protocol".into()),
    };
    let config = Config {
        lanes: lanes.parse()?,
        groups: groups.parse()?,
        count: count.parse()?,
        window: window.parse()?,
    }
    .validate()?;
    let root = Path::new(root);
    std::fs::create_dir(root)?;
    execute(root, config, protocol)
}
fn execute(root: &Path, config: Config, protocol: NativePeerProtocol) -> Result<(), Failure> {
    report::plan(root, config)?;
    let mut workers = Vec::new();
    let started = (|| -> Result<(), Failure> {
        for lane in 0..config.lanes {
            workers.push(Worker::spawn(
                root.join(format!("lane{}", lane + 1)),
                config.plan(lane),
                protocol,
            )?);
        }
        for worker in &workers {
            worker
                .ready
                .recv_timeout(Duration::from_secs(180))
                .map_err(|error| format!("lane did not prepare: {error}"))?;
        }
        Ok(())
    })();
    let start = Instant::now();
    let mut errors = started
        .err()
        .map(|error| error.to_string())
        .into_iter()
        .collect::<Vec<_>>();
    let prepared = errors.is_empty();
    for worker in &workers {
        let command = if prepared {
            Command::Start(start)
        } else {
            Command::Abort
        };
        if worker.command.send(command).is_err() {
            errors.push("lane command receiver closed".into());
        }
    }
    if prepared {
        for worker in &workers {
            if !errors.is_empty() {
                break;
            }
            if let Err(error) = worker.ready.recv_timeout(Duration::from_secs(180)) {
                errors.push(format!("lane measurement failed: {error}"));
                break;
            }
        }
        for worker in &workers {
            let command = if errors.is_empty() {
                Command::Verify
            } else {
                Command::Abort
            };
            if worker.command.send(command).is_err() {
                errors.push("lane verification receiver closed".into());
            }
        }
    }
    let mut reports = Vec::new();
    for worker in workers {
        match worker.join.join() {
            Ok(Ok(report)) => reports.push(report),
            Ok(Err(error)) => errors.push(error),
            Err(_) => errors.push("lane host thread panicked".into()),
        }
    }
    if !errors.is_empty() {
        let mut output = exclusive(&root.join("failure.txt"))?;
        writeln!(
            output,
            "valid_measurement=false host_threads_joined=true\n{}",
            errors.join("\n")
        )?;
        output.sync_all()?;
        return Err(errors.join("; ").into());
    }
    report::write(root, config, protocol, &reports)
}
