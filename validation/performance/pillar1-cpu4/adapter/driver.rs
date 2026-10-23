// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Source-only R3 overlay. Not executed by the builder timing path.
use super::*;

pub(crate) fn r3_drive<const TRACE: bool>(
    mut parts: Vec<Parts<TRACE>>,
    client: &mpsc::Sender<Request>,
    spec: &str,
) -> Result<(), Failure> {
    let v: Vec<usize> = spec.split(',').map(|x| x.parse().unwrap()).collect();
    let (rounds, width, per) = (v[0], v[1], v.get(2).copied().unwrap_or(4).max(1));
    let publish = std::env::var_os("CPU1_OMIT_PUBLICATION").is_none();
    let original_budget = parts[0]
        .settings
        .clients
        .checked_mul(4)
        .and_then(|n| n.checked_add(1))
        .ok_or("r3 original budget overflow")?;
    let (domain, factory, maximum) =
        r3_ports::registration(parts[0].domain.binding(), original_budget, width, per)?;
    parts[0].domain = domain;
    let mut pool = r3_ports::Pool::new(&factory, client, width, per, maximum)?;
    let mut actors: Vec<Actor<TRACE>> = parts.into_iter().map(Actor::new).collect();
    let warm = 1024;
    let expected = ((warm + rounds) * width + 1) as u64;
    let mut next: u128 = 1;
    let mut replied = 0usize;
    let mut wrong_reply = false;
    let execution = (|| -> Result<Duration, Failure> {
        actors[0].step(Event::Campaign)?;
        r3_pump(&mut actors, publish)?;
        if actors[0].parts.core.role() != Role::Leader {
            return Err("r3: no leader".into());
        }
        let mut started = Instant::now();
        for round in 0..warm + rounds {
            if round == warm {
                started = Instant::now();
            }
            let mut left = width;
            while left > 0 {
                let n = left.min(per);
                left -= n;
                pool.enqueue(next, n)?;
                next += n as u128;
            }
            let first = actors[0]
                .parts
                .receiver
                .try_recv()
                .map_err(|_| "r3 empty")?;
            actors[0].requests(first)?;
            while let Ok(more) = actors[0].parts.receiver.try_recv() {
                actors[0].requests(more)?;
            }
            if let Some(more) = actors[0].deferred.take() {
                actors[0].requests(more)?;
            }
            r3_service(&mut actors[0], publish)?;
            r3_pump(&mut actors, publish)?;
            loop {
                match pool.try_take_front() {
                    Ok(Some(_)) => replied += 1,
                    Ok(None) => break,
                    Err(error) => {
                        wrong_reply = true;
                        return Err(error);
                    }
                }
            }
        }
        Ok(started.elapsed())
    })();
    let commit_frontiers: Vec<_> = actors
        .iter()
        .map(|a| a.parts.core.state().commit_index)
        .collect();
    let applied_frontiers: Vec<_> = actors.iter().map(|a| a.parts.application.applied).collect();
    let reported_frontiers: Vec<_> = actors
        .iter()
        .map(|a| a.parts.frontiers[a.parts.slot].load(Ordering::Acquire))
        .collect();
    let fenced: Vec<_> = actors.iter().map(|a| a.parts.core.is_fenced()).collect();
    let checks = [
        ("replicas", actors.len() == 3),
        (
            "expected_count",
            next - 1 == ((warm + rounds) * width) as u128,
        ),
        ("reply_count", replied == (warm + rounds) * width),
        ("reply_ids", !wrong_reply),
        ("unreplied", pool.unreplied() == 0),
        ("commit_frontiers", commit_frontiers == vec![expected; 3]),
        ("applied_frontiers", applied_frontiers == vec![expected; 3]),
        (
            "reported_frontiers",
            reported_frontiers == vec![expected; 3],
        ),
        ("nonfenced", fenced.iter().all(|x| !x)),
        ("propagated_error", execution.is_ok()),
    ];
    let failed: Vec<_> = checks
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(name, _)| *name)
        .collect();
    let diagnostics: Vec<_> = actors
        .iter()
        .map(|a| a.parts.stats[a.parts.slot].read())
        .collect();
    let elapsed = execution.as_ref().map(|d| d.as_nanos()).unwrap_or(0);
    let doc = serde_json::json!({
        "diagnostic": "fixed_schedule_r3", "rounds": rounds, "width": width, "per_request": per,
        "completed_originals": replied.saturating_sub(warm * width),
        "warmup_rounds": warm, "warmup_originals": warm * width,
        "submitted_including_warmup": next - 1, "replied_including_warmup": replied,
        "expected_frontier": expected, "unreplied": pool.unreplied(), "wrong_reply": wrong_reply,
        "commit_frontiers": commit_frontiers, "applied_frontiers": applied_frontiers,
        "reported_frontiers": reported_frontiers, "fenced": fenced,
        "checks": checks.iter().copied().collect::<std::collections::BTreeMap<_, _>>(),
        "failed_conjuncts": failed, "propagated_error": execution.as_ref().err().map(|e| e.to_string()),
        "publication_enabled": publish,
        "default_retention": actors.iter().all(|a| a.parts.settings.maintenance == Maintenance::DefaultRetention),
        "settings": actors.iter().map(|a| a.parts.settings).collect::<Vec<_>>(),
        "internal_elapsed_ns": elapsed, "internal_ns_per_original": elapsed as f64 / (rounds * width) as f64,
        "diagnostics": diagnostics,
        "shutdown_scope": "diagnostic process::exit before owned shutdown; opaque early scope, not lifecycle/canonical gate"
    });
    if !failed.is_empty() {
        eprintln!("{}", serde_json::to_string(&doc)?);
        return Err(execution
            .err()
            .unwrap_or_else(|| "cpu1 diagnostic committed/applied/failure guard".into()));
    }
    println!("{}", serde_json::to_string(&doc)?);
    Ok(())
}
fn r3_service<const TRACE: bool>(actor: &mut Actor<TRACE>, publish: bool) -> Result<(), Failure> {
    actor.maintain()?;
    actor.parts.stats[actor.parts.slot]
        .retained
        .store(actor.parts.core.state().entries.len(), Ordering::Relaxed);
    if publish {
        actor.parts.frontiers[actor.parts.slot]
            .store(actor.parts.application.applied, Ordering::Release);
    }
    Ok(())
}
fn r3_pump<const TRACE: bool>(actors: &mut [Actor<TRACE>], publish: bool) -> Result<(), Failure> {
    loop {
        let mut any = false;
        for actor in actors.iter_mut() {
            while let Ok(message) = actor.parts.peer_receiver.try_recv() {
                actor.step(Event::Receive(message))?;
                r3_service(actor, publish)?;
                any = true;
            }
        }
        if !any {
            return Ok(());
        }
    }
}
