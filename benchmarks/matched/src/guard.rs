// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Finite measurement guards preserve owned cleanup and the first failure.
use crate::Failure;
use std::{error::Error, fmt, future::Future, time::Duration};

pub(super) const CLEANUP_TIMEOUT_SECS: u64 = 30;

pub(super) async fn bounded<T>(
    phase: &'static str,
    budget: Duration,
    future: impl Future<Output = Result<T, Failure>>,
) -> Result<T, Failure> {
    tokio::time::timeout(budget, future)
        .await
        .map_err(|source| -> Failure {
            Box::new(Expired {
                phase,
                budget,
                source,
            })
        })?
}
#[derive(Debug)]
struct Expired {
    phase: &'static str,
    budget: Duration,
    source: tokio::time::error::Elapsed,
}
impl fmt::Display for Expired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} timed out after {:?}; incomplete measurement",
            self.phase, self.budget
        )
    }
}
impl Error for Expired {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
#[derive(Debug)]
struct BothFailed {
    campaign: Failure,
    cleanup: Failure,
}
impl fmt::Display for BothFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "campaign failed: {}; cleanup also failed: {}",
            self.campaign, self.cleanup
        )
    }
}
impl Error for BothFailed {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.campaign.as_ref())
    }
}
/// Preserve the original error unchanged unless a second cleanup failure needs retention.
pub(super) fn finish<T>(
    campaign: Result<T, Failure>,
    cleanup: Result<(), Failure>,
) -> Result<T, Failure> {
    match (campaign, cleanup) {
        (Err(campaign), Err(cleanup)) => Err(Box::new(BothFailed { campaign, cleanup })),
        (Err(campaign), Ok(())) => Err(campaign),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Ok(value), Ok(())) => Ok(value),
    }
}
#[cfg(test)]
mod tests;
