// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Benchmark-only transparent provider observers. Totals are not durability evidence.
use super::*;
use std::{io, sync::Mutex};
use voteboat::{contracts::StorageError, native::log_store::JournalIo};

#[derive(Clone, Default)]
pub(super) struct Totals {
    append_calls: u64,
    append_units: u64,
    append_commands: u64,
    append_ns: u128,
    append_max_ns: u128,
    barrier_calls: u64,
    barrier_tickets: u64,
    barrier_ns: u128,
    barrier_max_ns: u128,
    errors: u64,
    encoded_bytes: u64,
    io_append_calls: u64,
    io_append_ns: u128,
    sync_calls: u64,
    sync_ns: u128,
    publish_calls: u64,
    publish_ns: u128,
    histogram: BTreeMap<usize, u64>,
    groups: BTreeMap<u128, u64>,
    publication: Option<voteboat::native::log_store::JournalTimingSnapshot>,
}
#[derive(Clone, Default)]
pub(super) struct Trace(
    Arc<Mutex<Totals>>,
    Option<voteboat::native::log_store::JournalTimings>,
);
impl Trace {
    pub(super) fn journal(timing: voteboat::native::log_store::JournalTimings) -> Self {
        Self(Arc::default(), Some(timing))
    }
    fn update(&self, f: impl FnOnce(&mut Totals)) {
        f(&mut self.0.lock().expect("benchmark trace poisoned"));
    }
    pub(super) fn snapshot(&self) -> Totals {
        let mut totals = self.0.lock().expect("benchmark trace poisoned").clone();
        if let Some(timing) = &self.1 {
            let snapshot = timing.snapshot();
            totals.io_append_calls = snapshot.append.calls;
            totals.io_append_ns = snapshot.append.elapsed_ns.into();
            totals.sync_calls = snapshot.log_sync.calls;
            totals.sync_ns = snapshot.log_sync.elapsed_ns.into();
            totals.publish_calls = snapshot.manifest.calls;
            totals.publish_ns = snapshot.manifest.elapsed_ns.into();
            totals.errors = snapshot
                .append
                .errors
                .saturating_add(snapshot.log_sync.errors)
                .saturating_add(snapshot.manifest.errors);
            totals.publication = Some(snapshot);
        }
        totals
    }
}
pub(super) struct ObservedIo<I> {
    pub(super) inner: I,
    pub(super) trace: Trace,
}
impl<I: JournalIo> JournalIo for ObservedIo<I> {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.inner.read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        self.inner.read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let start = Instant::now();
        let result = self.inner.append(bytes);
        let ns = start.elapsed().as_nanos();
        self.trace.update(|t| {
            t.io_append_calls += 1;
            t.io_append_ns += ns;
            t.encoded_bytes += bytes.len() as u64;
        });
        result
    }
    fn sync_log(&mut self) -> io::Result<()> {
        let start = Instant::now();
        let result = self.inner.sync_log();
        let ns = start.elapsed().as_nanos();
        self.trace.update(|t| {
            t.sync_calls += 1;
            t.sync_ns += ns;
        });
        result
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.inner.truncate_log(length)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let start = Instant::now();
        let result = self.inner.publish_manifest(bytes);
        let ns = start.elapsed().as_nanos();
        self.trace.update(|t| {
            t.publish_calls += 1;
            t.publish_ns += ns;
        });
        result
    }
    fn supports_replacement(&self) -> bool {
        self.inner.supports_replacement()
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        self.inner.replace_log(bytes, manifest)
    }
}
pub(super) struct ObservedLog<L> {
    pub(super) inner: L,
    pub(super) trace: Trace,
}
impl<L: LogStore> LogStore for ObservedLog<L> {
    fn binding(&self) -> StoreBinding {
        self.inner.binding()
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.inner.state(group)
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        let units = mutations.len();
        let mut commands = 0;
        let mut groups = Vec::with_capacity(units);
        for mutation in &mutations {
            groups.push(match mutation {
                LogMutation::Create(b) => b.group.id.get(),
                LogMutation::Update(u) => {
                    commands += u.suffix.as_ref().map_or(0, |s| {
                        s.entries
                            .iter()
                            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                            .count()
                    });
                    u.group.id.get()
                }
            });
        }
        let start = Instant::now();
        let result = self.inner.append_batch(mutations);
        let ns = start.elapsed().as_nanos();
        self.trace.update(|t| {
            t.append_calls += 1;
            t.append_units += units as u64;
            t.append_commands += commands as u64;
            t.append_ns += ns;
            t.append_max_ns = t.append_max_ns.max(ns);
            t.errors += u64::from(result.is_err());
            *t.histogram.entry(units).or_default() += 1;
            for g in groups {
                *t.groups.entry(g).or_default() += 1;
            }
        });
        result
    }
    fn barrier(&mut self, tickets: &[LogTicket]) -> Result<DurableLog, StorageError> {
        let start = Instant::now();
        let result = self.inner.barrier(tickets);
        let ns = start.elapsed().as_nanos();
        self.trace.update(|t| {
            t.barrier_calls += 1;
            t.barrier_tickets += tickets.len() as u64;
            t.barrier_ns += ns;
            t.barrier_max_ns = t.barrier_max_ns.max(ns);
            t.errors += u64::from(result.is_err());
        });
        result
    }
    fn supports_reclaim(&self) -> bool {
        self.inner.supports_reclaim()
    }
    fn reclaim(&mut self, max_bytes: usize) -> Result<LogReclaimed, StorageError> {
        self.inner.reclaim(max_bytes)
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.inner
            .fetch_range(group, generation, from, max_entries, max_bytes)
    }
}
pub(super) fn write_csv(
    file: &mut File,
    snapshots: &[(String, usize, Totals)],
) -> Result<(), Failure> {
    writeln!(file, "stage,replica,append_calls,append_units,append_commands,append_ns,append_max_ns,barrier_calls,barrier_tickets,barrier_ns,barrier_max_ns,errors,encoded_bytes,io_append_calls,io_append_ns,sync_calls,sync_ns,publish_calls,publish_ns,batch_histogram,group_units")?;
    for (stage, replica, t) in snapshots {
        let histogram = t
            .histogram
            .iter()
            .map(|(n, c)| format!("{n}:{c}"))
            .collect::<Vec<_>>()
            .join("|");
        let groups = t
            .groups
            .iter()
            .map(|(g, c)| format!("{g}:{c}"))
            .collect::<Vec<_>>()
            .join("|");
        writeln!(file, "{stage},{replica},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{histogram},{groups}",
            t.append_calls,t.append_units,t.append_commands,t.append_ns,t.append_max_ns,t.barrier_calls,t.barrier_tickets,t.barrier_ns,t.barrier_max_ns,t.errors,t.encoded_bytes,t.io_append_calls,t.io_append_ns,t.sync_calls,t.sync_ns,t.publish_calls,t.publish_ns)?;
    }
    file.sync_all()?;
    Ok(())
}
pub(super) fn retain_journal(
    root: &Path,
    snapshots: &mut Vec<(String, usize, Totals)>,
    stage: &str,
    traces: &[Trace],
) {
    if traces.is_empty() {
        return;
    }
    capture(snapshots, stage, traces);
    let result = write_journal(root, snapshots);
    if let Err(error) = result {
        eprintln!("journal diagnostic retention failed: {error}");
    }
}
pub(super) fn write_journal(
    root: &Path,
    snapshots: &[(String, usize, Totals)],
) -> Result<(), Failure> {
    write_csv(&mut exclusive(&root.join("journal.csv"))?, snapshots)?;
    write_publication(root, snapshots)
}
fn write_publication(root: &Path, snapshots: &[(String, usize, Totals)]) -> Result<(), Failure> {
    let mut file = exclusive(&root.join("publication.csv"))?;
    writeln!(file, "stage,replica,step,calls,errors,elapsed_ns,max_ns,manifest_calls,manifest_errors,manifest_ns")?;
    for (stage, replica, totals) in snapshots {
        let Some(snapshot) = totals.publication else {
            return Err("publication diagnostics require native journal timings".into());
        };
        let p = snapshot.publication;
        for (step, timing) in [
            ("open", p.open),
            ("write", p.write),
            ("file_sync", p.file_sync),
            ("rename", p.rename),
            ("directory_sync", p.directory_sync),
        ] {
            writeln!(
                file,
                "{stage},{replica},{step},{},{},{},{},{},{},{}",
                timing.calls,
                timing.errors,
                timing.elapsed_ns,
                timing.max_ns,
                snapshot.manifest.calls,
                snapshot.manifest.errors,
                snapshot.manifest.elapsed_ns
            )?;
        }
    }
    file.sync_all()?;
    Ok(())
}
pub(super) fn capture(snapshots: &mut Vec<(String, usize, Totals)>, stage: &str, traces: &[Trace]) {
    for (i, trace) in traces.iter().enumerate() {
        snapshots.push((stage.into(), i + 1, trace.snapshot()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voteboat::native::log_store::{FileLogIo, NativeLogStore};

    #[test]
    fn observer_preserves_real_store_rejection_and_durable_ticket_scope() {
        let root = std::env::temp_dir().join(format!("voteboat-observer-{}", std::process::id()));
        let trace = Trace::default();
        let inner = NativeLogStore::create(
            ObservedIo {
                inner: FileLogIo::create(&root).unwrap(),
                trace: trace.clone(),
            },
            store(1),
            LogLimits::default(),
        )
        .unwrap();
        let binding = inner.binding();
        let maintenance = inner.supports_reclaim();
        let mut observed = ObservedLog {
            inner,
            trace: trace.clone(),
        };
        assert_eq!(observed.binding(), binding);
        assert_eq!(observed.supports_reclaim(), maintenance);
        assert_eq!(
            observed.barrier(&[]),
            Err(StorageError::Rejected("barrier dependency budget"))
        );
        let bootstrap = Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node(1)), Limits::default()).unwrap(),
            voter_stores: [(node(1), store(1))].into(),
        };
        let tickets = observed
            .append_batch(vec![LogMutation::Create(bootstrap.clone())])
            .unwrap();
        assert!(
            observed.state(group()).is_err(),
            "append alone cannot become durable evidence"
        );
        let completion = observed.barrier(&tickets).unwrap();
        assert_eq!(completion.tickets, tickets);
        assert_eq!(observed.state(group()).unwrap().bootstrap, bootstrap);
        assert_eq!(observed.barrier(&tickets), Err(StorageError::StaleTicket));
        assert!(observed.reclaim(0).is_err());
        let totals = trace.snapshot();
        assert_eq!(totals.append_units, 1);
        assert_eq!(totals.barrier_calls, 3);
        assert_eq!(totals.errors, 2);
        assert_eq!(
            totals.sync_calls, 2,
            "create plus one real barrier; rejected barriers must not synchronize"
        );
        assert_eq!(totals.publish_calls, 2);
        drop(observed);
        let recovered = NativeLogStore::recover(
            FileLogIo::open(&root).unwrap(),
            store(1),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(recovered.state(group()).unwrap().bootstrap, bootstrap);
        drop(recovered);
        std::fs::remove_dir_all(root).unwrap();
    }
}
