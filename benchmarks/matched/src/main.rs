// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Standalone comparison-only runtime; no production Tokio/OpenRaft adapter.
mod application;
mod attribution;
mod openraft_engine;
mod snapshots;
mod store;
mod voteboat_engine;
mod workload;

use serde::Serialize;
use std::{env, error::Error, time::Duration};
use tokio::runtime::Builder;

type Failure = Box<dyn Error + Send + Sync>;
const UPSTREAM: &str = "e88bb40707f2e511d8ecc44d31edcb1d814a11a8";
const CHANNEL_CAPACITY: usize = 65_536;
const COMMON_ENTRY_BATCH_CAPACITY: usize = 64;
const MAX_PROPOSAL_ENTRIES: usize = 4096;
const TIMEOUT: Duration = Duration::from_secs(400);

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
impl Envelopes {
    fn voteboat_limits(self) -> voteboat::raft::BatchLimits {
        let (max_proposal_entries, max_replication_entries) = match self {
            Self::UpstreamDefault => (MAX_PROPOSAL_ENTRIES, 1024),
            Self::Common64 => (COMMON_ENTRY_BATCH_CAPACITY, COMMON_ENTRY_BATCH_CAPACITY),
        };
        voteboat::raft::BatchLimits {
            max_proposal_entries,
            max_replication_entries,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
struct Settings {
    clients: usize,
    batch: usize,
    requested_operations: usize,
    operations: usize,
    warmup: usize,
    maintenance: Maintenance,
    envelopes: Envelopes,
    attribution: bool,
}
impl Settings {
    fn parse() -> Result<(String, Self), Failure> {
        let mut engine = String::from("voteboat");
        let mut settings = Self {
            clients: 4096,
            batch: 1,
            requested_operations: 20_000_000,
            operations: 0,
            warmup: 1024,
            maintenance: Maintenance::DefaultRetention,
            envelopes: Envelopes::UpstreamDefault,
            attribution: false,
        };
        let mut args = env::args().skip(1);
        while let Some(name) = args.next() {
            let value = args.next().ok_or("every option requires a value")?;
            match name.as_str() {
                "--engine" => engine = value,
                "--attribution" => settings.attribution = value.parse()?,
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
        if self.requested_operations > 20_000_000 || self.warmup > 65_536 {
            return Err("run exceeds declared operation/warmup budget".into());
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

fn main() -> Result<(), Failure> {
    let (engine, settings) = Settings::parse()?;
    let server = Builder::new_multi_thread()
        .worker_threads(16)
        .thread_name("matched-server")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()?;
    let clients = Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("matched-client")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()?;
    let mut cluster = server.block_on(workload::Cluster::new(&engine, settings))?;
    let result = clients.block_on(async {
        tokio::time::timeout(TIMEOUT, workload::run(&mut cluster, settings)).await?
    });
    // Cleanup runs for successful/failed/timeout campaigns alike.
    let cleanup = server.block_on(cluster.shutdown());
    let mut report = result?;
    cleanup?;
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
            batch: 4,
            requested_operations: 20_000_000,
            operations: 0,
            warmup: 1024,
            maintenance: Maintenance::DefaultRetention,
            envelopes: Envelopes::UpstreamDefault,
            attribution: false,
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
