// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{error, NativeStartupError, TimerConfig};

/// Construction-time liveness settings, independent of quorum and durability.
/// Throughput/edge follow the design pack proposals; they are not measured guarantees.
/// Hosts may instead supply their own validated `TimerConfig` to startup.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NativeTimingProfile {
    /// The original runtime settings: 50ms heartbeat and 150–300ms elections.
    Legacy,
    /// Durable-node service default: 100ms heartbeat and 1000–2000ms elections.
    #[default]
    Throughput,
    /// Edge-node proposal: 250ms heartbeat and 1500–3000ms elections.
    Edge,
}
impl NativeTimingProfile {
    pub fn timers(self) -> TimerConfig {
        let (heartbeat_ms, election_min_ms, election_spread_ms) = match self {
            Self::Legacy => (50, 150, 150),
            Self::Throughput => (100, 1000, 1000),
            Self::Edge => (250, 1500, 1500),
        };
        TimerConfig {
            heartbeat_ms,
            election_min_ms,
            election_spread_ms,
            expirations_per_poll: 32,
        }
    }
}
impl std::str::FromStr for NativeTimingProfile {
    type Err = NativeStartupError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "legacy" => Ok(Self::Legacy),
            "throughput" => Ok(Self::Throughput),
            "edge" => Ok(Self::Edge),
            _ => Err(error(
                "timing profile",
                "expected throughput, edge or legacy",
            )),
        }
    }
}
