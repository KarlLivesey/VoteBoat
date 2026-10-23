// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Finite maintenance control through the unchanged production actor and helpers.
use super::*;

async fn writes(
    cluster: &Cluster,
    width: usize,
    first: usize,
    count: usize,
) -> Result<(), Failure> {
    let client = cluster.client();
    for round in first..first + count {
        let ids = (round * width + 1..=(round + 1) * width)
            .map(|n| (1u128 << 100) | n as u128)
            .collect::<Vec<_>>();
        if client.write(&ids).await? != width {
            return Err("not every original completed".into());
        }
    }
    Ok(())
}

async fn maintenance(cluster: &Cluster, width: usize) -> Result<(), Failure> {
    let healthy = 12_000usize.div_ceil(width);
    writes(cluster, width, 0, healthy).await?;
    cluster.verify(healthy * width, 0).await?;
    let diagnostics = serde_json::to_value(cluster.diagnostics())?;
    if diagnostics
        .as_array()
        .ok_or("diagnostics absent")?
        .iter()
        .any(|stats| {
            stats["checkpoint_count"]
                .as_u64()
                .is_none_or(|count| count < 2)
                || stats["retained_entries"]
                    .as_u64()
                    .is_none_or(|count| count >= 5000)
        })
    {
        return Err("all three actual applications must cross two default checkpoints".into());
    }
    cluster.pause_follower(2, true);
    let lagged = 6_000usize.div_ceil(width);
    writes(cluster, width, healthy, lagged).await?;
    cluster.pause_follower(2, false);
    cluster.verify((healthy + lagged) * width, 0).await?;
    if cluster.stats[2].snapshot_installs.load(Ordering::Acquire) == 0 {
        return Err("lagging follower did not install a snapshot".into());
    }
    writes(cluster, width, healthy + lagged, 1).await?;
    cluster.verify((healthy + lagged + 1) * width, 0).await?;
    let stats = serde_json::to_value(cluster.diagnostics())?;
    let widths = stats[0]["proposal_widths"]
        .as_array()
        .ok_or("width diagnostics absent")?;
    if !widths.iter().any(|entry| {
        entry["original_entries"] == width
            && entry["batches"]
                .as_u64()
                .is_some_and(|count| count >= healthy as u64)
    }) {
        return Err("actual leader did not execute the requested proposal width".into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_plans_all_three_applications_cross_default_checkpoints_catch_up_and_resume() {
    for width in [1usize, 231, 515] {
        let settings = Settings {
            clients: width.div_ceil(4),
            server_workers: crate::DEFAULT_SERVER_WORKERS,
            batch: 4,
            requested_operations: 24_000,
            operations: 24_000,
            warmup: 0,
            maintenance: crate::Maintenance::DefaultRetention,
            envelopes: crate::Envelopes::UpstreamDefault,
            attribution: false,
            campaign_timeout_secs: crate::DEFAULT_CAMPAIGN_TIMEOUT_SECS,
        }
        .validate()
        .unwrap();
        let mut cluster = Cluster::new(settings).await.unwrap();
        let result = maintenance(&cluster, width).await;
        let cleanup = cluster.shutdown().await;
        assert!(
            result.is_ok(),
            "width {width}: {result:?}; actor cleanup={cleanup:?}"
        );
        cleanup.unwrap();
        assert!(cluster.actors.is_empty());
    }
}
