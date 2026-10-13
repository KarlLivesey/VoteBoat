// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

struct Dropped(Arc<AtomicBool>, Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        if let Some(done) = self.1.take() {
            let _ = done.send(());
        }
    }
}
#[tokio::test]
async fn zero_budget_ready_result_succeeds_without_waiting() {
    assert_eq!(
        bounded("campaign", Duration::ZERO, async { Ok(17) })
            .await
            .unwrap(),
        17
    );
}
#[tokio::test]
async fn timed_out_owned_clients_are_canceled_and_cleanup_still_runs() {
    let canceled = Arc::new(AtomicBool::new(false));
    let (started, observed) = tokio::sync::oneshot::channel();
    let (released, release_observed) = tokio::sync::oneshot::channel();
    let mut tasks = tokio::task::JoinSet::new();
    let owned = canceled.clone();
    tasks.spawn(async move {
        let _owned = Dropped(owned, Some(released));
        started.send(()).unwrap();
        std::future::pending::<Result<(), Failure>>().await
    });
    observed.await.unwrap();
    let campaign = bounded("campaign", Duration::ZERO, async move {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    })
    .await;
    assert!(campaign
        .as_ref()
        .unwrap_err()
        .to_string()
        .contains("campaign timed out after 0ns"));
    bounded("cancel observation", Duration::from_secs(1), async {
        release_observed.await?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(canceled.load(Ordering::Acquire));
    let cleanup_called = AtomicBool::new(false);
    let cleanup = bounded("cleanup", Duration::ZERO, async {
        cleanup_called.store(true, Ordering::Release);
        Err("controlled cleanup failure".into())
    })
    .await;
    let failure = finish(campaign, cleanup).unwrap_err();
    assert!(cleanup_called.load(Ordering::Acquire));
    assert!(failure.to_string().contains("controlled cleanup failure"));
    assert!(failure
        .source()
        .unwrap()
        .to_string()
        .contains("campaign timed out"));
}
#[tokio::test]
async fn cleanup_expiry_is_finite_and_does_not_substitute_a_success_report() {
    let cleanup = bounded(
        "cleanup",
        Duration::ZERO,
        std::future::pending::<Result<(), Failure>>(),
    )
    .await;
    let failure = finish(Ok(17), cleanup).unwrap_err();
    assert!(failure.to_string().contains("cleanup timed out"));
}
#[test]
fn original_failure_type_and_cleanup_only_failure_are_preserved() {
    let original: Failure = Box::new(std::io::Error::other("original application error"));
    let error = finish::<()>(Err(original), Ok(())).unwrap_err();
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    assert_eq!(error.to_string(), "original application error");
    assert_eq!(
        finish::<()>(Ok(()), Err("cleanup only".into()))
            .unwrap_err()
            .to_string(),
        "cleanup only"
    );
}
