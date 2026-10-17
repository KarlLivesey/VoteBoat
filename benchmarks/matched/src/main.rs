// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Standalone comparison-only runtime; no production Tokio/OpenRaft adapter.
mod application;
mod attribution;
mod envelopes;
mod guard;
mod openraft_engine;
mod snapshots;
mod store;
mod voteboat_engine;
mod workload;

use serde::Serialize;
use std::{env, error::Error, time::Duration};
use tokio::runtime::{Builder, Runtime};

type Failure = Box<dyn Error + Send + Sync>;
const UPSTREAM: &str = "e88bb40707f2e511d8ecc44d31edcb1d814a11a8";
const CHANNEL_CAPACITY: usize = 65_536;
const COMMON_ENTRY_BATCH_CAPACITY: usize = 64;
const API_REQUEST_BATCH_CAPACITY: usize = 4096;
const MAX_PROPOSAL_ENTRIES: usize = 16384;
const DEFAULT_SERVER_WORKERS: usize = 16;
const DEFAULT_CAMPAIGN_TIMEOUT_SECS: u64 = 3600;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Maintenance {
    DefaultRetention,
    RetainAll,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Envelopes {
    UpstreamDefault,
    Common64,
}
#[derive(Clone, Copy, Debug, Serialize)]
struct Settings {
    clients: usize,
    server_workers: usize,
    batch: usize,
    requested_operations: usize,
    operations: usize,
    warmup: usize,
    maintenance: Maintenance,
    envelopes: Envelopes,
    attribution: bool,
    campaign_timeout_secs: u64,
}
impl Settings {
    fn parse() -> Result<(String, Self), Failure> {
        Self::parse_from(env::args().skip(1))
    }
    fn parse_from(mut args: impl Iterator<Item = String>) -> Result<(String, Self), Failure> {
        let mut engine = String::from("voteboat");
        let mut settings = Self {
            clients: 4096,
            server_workers: DEFAULT_SERVER_WORKERS,
            batch: 1,
            requested_operations: 20_000_000,
            operations: 0,
            warmup: 1024,
            maintenance: Maintenance::DefaultRetention,
            envelopes: Envelopes::UpstreamDefault,
            attribution: false,
            campaign_timeout_secs: DEFAULT_CAMPAIGN_TIMEOUT_SECS,
        };
        while let Some(name) = args.next() {
            let value = args.next().ok_or("every option requires a value")?;
            match name.as_str() {
                "--engine" => engine = value,
                "--campaign-timeout-secs" => settings.campaign_timeout_secs = value.parse()?,
                "--attribution" => settings.attribution = value.parse()?,
                "--server-workers" => settings.server_workers = value.parse()?,
                "--clients" => settings.clients = value.parse()?,
                "--batch" => settings.batch = value.parse()?,
                "--operations" => settings.requested_operations = value.parse()?,
                "--warmup" => settings.warmup = value.parse()?,
                "--envelopes" => {
                    settings.envelopes = match value.as_str() {
                        "upstream-default" => Envelopes::UpstreamDefault,
                        "common64" => Envelopes::Common64,
                        _ => return Err("unknown envelope profile".into()),
                    }
                }
                "--maintenance" => {
                    settings.maintenance = match value.as_str() {
                        "default-retention" => Maintenance::DefaultRetention,
                        "retain-all" => Maintenance::RetainAll,
                        _ => return Err("unknown maintenance profile".into()),
                    }
                }
                _ => return Err(format!("unknown option: {name}").into()),
            }
        }
        if !matches!(engine.as_str(), "voteboat" | "openraft") {
            return Err("engine must be voteboat or openraft".into());
        }
        if engine == "openraft" && settings.attribution {
            return Err("elapsed phase attribution is available only for voteboat".into());
        }
        Ok((engine, settings.validate()?))
    }
    fn validate(mut self) -> Result<Self, Failure> {
        if !(1..=4096).contains(&self.clients) || !matches!(self.batch, 1 | 4) {
            return Err("clients must be1..4096 and batch1 or4".into());
        }
        if !(1..=32_768).contains(&self.server_workers) {
            return Err("server workers must be1..32768".into());
        }
        if self.requested_operations > 20_000_000 || self.warmup > 65_536 {
            return Err("run exceeds declared operation/warmup budget".into());
        }
        if !(1..=7200).contains(&self.campaign_timeout_secs) {
            return Err("campaign timeout must be1..7200 seconds".into());
        }
        let round = self
            .clients
            .checked_mul(self.batch)
            .ok_or("round overflow")?;
        self.operations = self.requested_operations / round * round;
        if self.operations == 0 {
            return Err("operations must cover every client batch".into());
        }
        Ok(self)
    }
    fn retained_budget(self) -> usize {
        match self.maintenance {
            Maintenance::DefaultRetention => CHANNEL_CAPACITY,
            Maintenance::RetainAll => self.operations + self.warmup + 2,
        }
    }
}

fn server_runtime(settings: Settings) -> Result<Runtime, Failure> {
    Ok(Builder::new_multi_thread()
        .worker_threads(settings.server_workers)
        .thread_name("matched-server")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()?)
}

fn main() -> Result<(), Failure> {
    let (engine, settings) = Settings::parse()?;
    let server = server_runtime(settings)?;
    // One client executor in either topology; drive it on the caller to avoid
    // the extra worker handoff while keeping the server runtime independent.
    let clients = Builder::new_current_thread()
        .thread_name("matched-client")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()?;
    let mut cluster = server.block_on(workload::Cluster::new(&engine, settings))?;
    let result = clients.block_on(async {
        guard::bounded(
            "campaign",
            Duration::from_secs(settings.campaign_timeout_secs),
            workload::run(&mut cluster, settings),
        )
        .await
    });
    // Cleanup runs for successful/failed/timeout campaigns alike.
    let cleanup = server.block_on(guard::bounded(
        "cleanup",
        Duration::from_secs(guard::CLEANUP_TIMEOUT_SECS),
        cluster.shutdown(),
    ));
    let mut report = guard::finish(result, cleanup)?;
    report.attach_attribution(&cluster);
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rounding_matches_equal_upstream_client_batches() {
        let settings = Settings {
            clients: 4096,
            server_workers: DEFAULT_SERVER_WORKERS,
            batch: 4,
            requested_operations: 20_000_000,
            operations: 0,
            warmup: 1024,
            maintenance: Maintenance::DefaultRetention,
            envelopes: Envelopes::UpstreamDefault,
            attribution: false,
            campaign_timeout_secs: DEFAULT_CAMPAIGN_TIMEOUT_SECS,
        }
        .validate()
        .unwrap();
        assert_eq!(settings.operations, 19_988_480);
        assert_eq!(settings.operations % (settings.clients * settings.batch), 0);
        assert!(Settings {
            requested_operations: 100,
            ..settings
        }
        .validate()
        .is_err());
        assert_eq!(settings.retained_budget(), 65_536);
        let retained = Settings {
            maintenance: Maintenance::RetainAll,
            ..settings
        };
        assert_eq!(
            retained.retained_budget(),
            retained.operations + retained.warmup + 2
        );
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    fn parse(arguments: &[&str]) -> Result<(String, Settings), Failure> {
        Settings::parse_from(arguments.iter().map(|s| String::from(*s)))
    }
    #[test]
    fn server_workers_cli_defaults_and_invalid_values() {
        assert_eq!(parse(&[]).unwrap().1.server_workers, 16);
        for value in ["1", "2", "16", "32768"] {
            assert_eq!(
                parse(&["--server-workers", value])
                    .unwrap()
                    .1
                    .server_workers,
                value.parse::<usize>().unwrap()
            );
        }
        for value in ["0", "32769", "-1", "invalid", "18446744073709551616"] {
            assert!(parse(&["--server-workers", value]).is_err());
        }
        assert!(parse(&["--server-workers"]).is_err());
    }
    #[test]
    fn campaign_cli_default_and_finite_bounds_are_validated() {
        let (_, defaults) = parse(&[]).unwrap();
        assert_eq!(defaults.campaign_timeout_secs, 3600);
        for seconds in ["1", "7200"] {
            let (_, settings) = parse(&["--campaign-timeout-secs", seconds]).unwrap();
            assert_eq!(
                settings.campaign_timeout_secs,
                seconds.parse::<u64>().unwrap()
            );
        }
        for seconds in ["0", "7201", "18446744073709551615", "-1", "invalid"] {
            assert!(parse(&["--campaign-timeout-secs", seconds]).is_err());
        }
        assert!(parse(&["--campaign-timeout-secs"]).is_err());
    }
}
