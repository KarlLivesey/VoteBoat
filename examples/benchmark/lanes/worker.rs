// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
type Replicas = Vec<Replica<shared::SharedLog>>;

pub(super) fn run(
    root: PathBuf,
    plan: Plan,
    protocol: NativePeerProtocol,
    commands: mpsc::Receiver<Command>,
    prepared: mpsc::SyncSender<()>,
) -> Result<Report, Failure> {
    std::fs::create_dir(&root)?;
    let clock = Instant::now();
    let (mut replicas, traces) = open(&root, plan, protocol, &clock, NativeOpenMode::Create)?;
    let mut storage = Vec::new();
    let result = (|| {
        campaign_partition(&mut replicas, &clock, plan)?;
        let warmup = write(&mut replicas, &clock, &root, plan, false, Instant::now())?;
        verify_partition(&mut replicas, &clock, plan, plan.warmup, &warmup.samples)?;
        prepared
            .send(())
            .map_err(|_| "preparation receiver closed")?;
        let start = await_command(&mut replicas, &clock, &commands, true)?
            .ok_or("missing measurement start")?;
        observe::capture(&mut storage, "before_measurement", &traces);
        let measured = write(&mut replicas, &clock, &root, plan, true, start)?;
        observe::capture(&mut storage, "after_measurement", &traces);
        prepared
            .send(())
            .map_err(|_| "measurement receiver closed")?;
        await_command(&mut replicas, &clock, &commands, false)?;
        failure::write_samples(
            &mut exclusive(&root.join("samples.csv"))?,
            &measured.samples,
        )?;
        verify_partition(
            &mut replicas,
            &clock,
            plan,
            plan.warmup + plan.count,
            &warmup
                .samples
                .iter()
                .chain(&measured.samples)
                .map(copy_sample)
                .collect::<Vec<_>>(),
        )?;
        Ok((warmup, measured))
    })();
    let (warmup, measured) = match result {
        Ok(result) => {
            close(replicas, &clock)?;
            result
        }
        Err(error) => {
            return failure::cleanup(replicas, &clock, &root, error);
        }
    };
    observe::capture(&mut storage, "after_create_join", &traces);
    let (mut replicas, traces) = open(&root, plan, protocol, &clock, NativeOpenMode::Recover)?;
    let recovery = recover(&mut replicas, &clock, plan, &warmup, &measured);
    let retries = match recovery {
        Ok(retries) => {
            close(replicas, &clock)?;
            retries
        }
        Err(error) => {
            return failure::cleanup(replicas, &clock, &root, error).and_then(|()| unreachable!());
        }
    };
    observe::capture(&mut storage, "after_recover_join", &traces);
    observe::write_csv(&mut exclusive(&root.join("storage.csv"))?, &storage)?;
    Ok(Report {
        plan,
        measured,
        retries,
    })
}
fn copy_sample(sample: &Sample) -> Sample {
    Sample {
        group: sample.group,
        operation: sample.operation,
        submitted_ns: sample.submitted_ns,
        completed_ns: sample.completed_ns,
        index: sample.index,
        value: sample.value,
    }
}
fn open(
    root: &Path,
    plan: Plan,
    protocol: NativePeerProtocol,
    clock: &Instant,
    mode: NativeOpenMode,
) -> Result<(Replicas, Vec<observe::Trace>), Failure> {
    shared::open_partition(
        root,
        mode,
        protocol,
        plan.warmup + plan.count,
        clock,
        plan.partition,
    )
}
fn write(
    replicas: &mut [Replica<shared::SharedLog>],
    clock: &Instant,
    root: &Path,
    plan: Plan,
    measured: bool,
    start: Instant,
) -> Result<Measurement, Failure> {
    workload_in_partition(
        replicas,
        clock,
        Workload {
            first: if measured { plan.warmup + 1 } else { 1 },
            count: if measured { plan.count } else { plan.warmup },
            window: plan.window,
            groups: plan.partition.groups,
            diagnostic: Some((root, if measured { "measurement" } else { "warmup" })),
        },
        plan.partition.offset,
        start,
    )
}
fn groups(plan: Plan) -> impl Iterator<Item = GroupIdentity> {
    (plan.partition.offset + 1..=plan.partition.offset + plan.partition.groups).map(group_id)
}
fn campaign_partition(
    replicas: &mut [Replica<shared::SharedLog>],
    clock: &Instant,
    plan: Plan,
) -> Result<(), Failure> {
    for group in groups(plan) {
        if leader(replicas, group).is_err() {
            checked(replicas[0].control(group, NodeControl::Campaign))?;
        }
    }
    until(replicas, clock, |ns| {
        Ok(groups(plan).all(|group| leader(ns, group).is_ok()))
    })?;
    Ok(())
}
fn verify_partition(
    replicas: &mut [Replica<shared::SharedLog>],
    clock: &Instant,
    plan: Plan,
    total: usize,
    samples: &[Sample],
) -> Result<(), Failure> {
    let expected = groups(plan)
        .enumerate()
        .map(|(g, group)| (group, ((total - g - 1) / plan.partition.groups + 1) as i64))
        .collect();
    verify_expected(
        replicas,
        clock,
        &expected,
        &boundaries(samples.iter().map(|s| (s.group, s.index))),
    )
}
fn recover(
    replicas: &mut [Replica<shared::SharedLog>],
    clock: &Instant,
    plan: Plan,
    warmup: &Measurement,
    measured: &Measurement,
) -> Result<usize, Failure> {
    campaign_partition(replicas, clock, plan)?;
    let all = warmup
        .samples
        .iter()
        .chain(&measured.samples)
        .map(copy_sample)
        .collect::<Vec<_>>();
    verify_partition(replicas, clock, plan, plan.warmup + plan.count, &all)?;
    let mut retries = 0;
    for sample in [
        &warmup.samples[0],
        measured.samples.last().ok_or("missing measured receipt")?,
    ] {
        let mut complete = false;
        for attempt in 0..4 {
            campaign_partition(replicas, clock, plan)?;
            if retry_group_once(
                replicas,
                clock,
                sample.operation as usize,
                sample.value,
                sample.group,
            )? {
                retries += attempt;
                complete = true;
                break;
            }
        }
        if !complete {
            return Err("lane recovery retry repeatedly lost leadership".into());
        }
    }
    let last = boundaries(replicas.iter().flat_map(|n| {
        n.local()
            .applications
            .iter()
            .map(|(g, a)| (*g, a.applied_index()))
    }));
    let expected = groups(plan)
        .enumerate()
        .map(|(g, group)| {
            (
                group,
                ((plan.warmup + plan.count - g - 1) / plan.partition.groups + 1) as i64,
            )
        })
        .collect();
    verify_expected(replicas, clock, &expected, &last)?;
    Ok(retries)
}

fn await_command(
    replicas: &mut [Replica<shared::SharedLog>],
    clock: &Instant,
    commands: &mpsc::Receiver<Command>,
    starting: bool,
) -> Result<Option<Instant>, Failure> {
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut totals = PollTotals::default();
    loop {
        match commands.try_recv() {
            Ok(Command::Start(start)) if starting => return Ok(Some(start)),
            Ok(Command::Verify) if !starting => return Ok(None),
            Ok(Command::Abort) | Err(mpsc::TryRecvError::Disconnected) => {
                return Err("lane aborted before measurement".into())
            }
            Ok(_) => return Err("unexpected lane command".into()),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if Instant::now() > deadline {
            return Err("lane start timed out".into());
        }
        poll(replicas, clock, &mut totals)?;
        thread::park_timeout(Duration::from_micros(100));
    }
}
