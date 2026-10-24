// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual Actor tasks, public completion helpers and selected snapshot provider.
use super::*;
use voteboat::application::{CheckpointStateMachine, StateMachine};

const ID_BASE: u128 = 1 << 100;

async fn round(cluster: &Cluster, width: usize, first: usize) -> Result<(), Failure> {
    if width > 1 {
        cluster.paused_proposals.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !cluster.proposals_pause_observed[0].load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
    }
    let chunk = if width == 1 { 1 } else { 4 };
    let mut waits = Vec::with_capacity(width);
    for offset in (0..width).step_by(chunk) {
        let mut originals = Vec::new();
        for n in offset..width.min(offset + chunk) {
            let operation = OperationId::new(ID_BASE + (first + n) as u128).unwrap();
            let (reply, wait) = oneshot::channel();
            originals.push(Original {
                operation,
                bytes: Vec::new(),
                reply,
            });
            waits.push((operation, wait));
        }
        cluster.senders[0]
            .try_send(Request { originals })
            .map_err(|_| "fixture admission overflow")?;
    }
    cluster.paused_proposals.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(10), async {
        for (operation, wait) in waits {
            if wait.await? != operation {
                return Err("full original identity differs".into());
            }
        }
        Ok::<_, Failure>(())
    })
    .await??;
    if width > 1 {
        tokio::time::timeout(Duration::from_secs(10), async {
            while cluster.proposals_pause_observed[0].load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
    }
    Ok(())
}

async fn history(cluster: &Cluster, width: usize) -> Result<usize, Failure> {
    let rounds = 12_000usize.div_ceil(width);
    cluster.pause_follower(2, true);
    let mut completed = 0;
    for _ in 0..rounds {
        round(cluster, width, completed + 1).await?;
        completed += width;
    }
    if cluster.frontiers[2].load(Ordering::Acquire) >= 4001 {
        return Err("paused follower advanced through the compacted prefix".into());
    }
    cluster.pause_follower(2, false);
    cluster.verify(completed, 0).await?;
    if cluster.stats[2].snapshot_installs.load(Ordering::Relaxed) == 0 {
        return Err("catch-up did not install an actual provider image".into());
    }
    for _ in 0..rounds {
        round(cluster, width, completed + 1).await?;
        completed += width;
    }
    cluster.verify(completed, 0).await?;
    Ok(completed)
}

fn recovered_end(end: &stats::ShutdownState, originals: usize) {
    let expected = originals as u64 + 1;
    assert!(!end.fenced);
    assert_eq!(
        (
            end.state.commit_index,
            end.state.last_index(),
            end.applied,
            end.reported
        ),
        (expected, expected, expected, expected)
    );
    assert_eq!(end.checkpoint, expected.to_le_bytes());
    // The original policy keeps 1000 at each 5000-entry checkpoint. A stop
    // between checkpoints may therefore retain up to 5999, not just 4999.
    assert_eq!(
        end.state.entries.len() as u64,
        expected - end.state.base_index()
    );
    assert!(end.state.entries.len() < 6000);
    for entry in &end.state.entries {
        let EntryPayload::Command { operation, bytes } = &entry.payload else {
            panic!("retained original command")
        };
        assert_eq!(operation.get(), ID_BASE + u128::from(entry.index - 1));
        assert!(bytes.is_empty());
    }
    let image = end
        .snapshot
        .as_ref()
        .expect("actual log-anchored provider image");
    assert!(end.state.snapshot.unwrap().matches(image));
    let log = VolatileLog::new(end.binding, end.limits).unwrap();
    let core = Raft::recover_volatile(end.node, &log, end.state.clone()).unwrap();
    assert_eq!(core.state(), &end.state);
    let mut application = Noop {
        group: end.state.bootstrap.group,
        applied: 0,
    };
    application
        .restore_checkpoint(
            image.metadata.application_schema,
            image.metadata.index,
            &image.application,
        )
        .unwrap();
    application.apply_entries(core.replay_committed()).unwrap();
    assert_eq!(application.applied, expected);
    assert_eq!(application.checkpoint(8).unwrap(), end.checkpoint);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_fronts_keep_all_three_applications_through_two_checkpoints_catchup_resume_and_shutdown(
) {
    for width in [1usize, 231, 515] {
        let chunk = if width == 1 { 1 } else { 4 };
        let settings = Settings {
            clients: width.div_ceil(chunk),
            server_workers: 4,
            batch: chunk,
            requested_operations: 32_768,
            operations: 0,
            warmup: 0,
            maintenance: crate::Maintenance::DefaultRetention,
            envelopes: crate::Envelopes::UpstreamDefault,
            attribution: false,
            campaign_timeout_secs: crate::DEFAULT_CAMPAIGN_TIMEOUT_SECS,
        }
        .validate()
        .unwrap();
        let mut cluster = Cluster::new(settings).await.unwrap();
        let checked = history(&cluster, width).await;
        // Always perform the real owned shutdown before checking the history.
        let shutdown = cluster.shutdown().await;
        assert!(cluster.actors.is_empty());
        let completed = checked.unwrap();
        shutdown.unwrap();
        for stats in cluster.stats.iter() {
            assert!(stats.checkpoint_count.load(Ordering::Relaxed) >= 2);
            let captured = stats.shutdown.lock().unwrap();
            let end = captured.as_ref().expect("joined actor's final state");
            println!("width={width} node={} originals={completed} commit={} applied={} reported={} base={} retained={} checkpoints={} installs={}", end.node.get(), end.state.commit_index, end.applied, end.reported, end.state.base_index(), end.state.entries.len(), stats.checkpoint_count.load(Ordering::Relaxed), stats.snapshot_installs.load(Ordering::Relaxed));
            recovered_end(
                captured.as_ref().expect("joined actor's final state"),
                completed,
            );
        }
        let diagnostics = serde_json::to_value(&cluster.diagnostics()[0]).unwrap();
        assert_eq!(diagnostics["maximum_proposal_width"], width);
        assert_eq!(diagnostics["proposed_originals"], completed);
    }
}
