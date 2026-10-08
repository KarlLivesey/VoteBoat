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
//! Native local WAL stage timing, not a consensus or application throughput test.
use std::{
    cell::RefCell,
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    rc::Rc,
    time::Instant,
};
use voteboat::{
    application::*, contracts::HardState, identity::*, log::*, native::log_store::*, quorum::*,
};
type Failure = Box<dyn std::error::Error>;
fn checked<T, E: std::fmt::Debug>(r: Result<T, E>) -> Result<T, Failure> {
    r.map_err(|e| format!("{e:?}").into())
}
const WARMUP: usize = 8;
#[derive(Clone, Copy, Default)]
struct Timings {
    append_ns: u128,
    sync_ns: u128,
    publish_ns: u128,
    bytes: usize,
    appends: usize,
    syncs: usize,
    publications: usize,
}
struct ObservedIo {
    inner: FileLogIo,
    timings: Rc<RefCell<Timings>>,
}
impl JournalIo for ObservedIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.inner.read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        self.inner.read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let start = Instant::now();
        let r = self.inner.append(bytes);
        let elapsed = start.elapsed().as_nanos();
        let mut t = self.timings.borrow_mut();
        t.append_ns += elapsed;
        t.appends += 1;
        t.bytes += bytes.len();
        r
    }
    fn sync_log(&mut self) -> io::Result<()> {
        let start = Instant::now();
        let r = self.inner.sync_log();
        let elapsed = start.elapsed().as_nanos();
        let mut t = self.timings.borrow_mut();
        t.sync_ns += elapsed;
        t.syncs += 1;
        r
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.inner.truncate_log(length)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let start = Instant::now();
        let r = self.inner.publish_manifest(bytes);
        let elapsed = start.elapsed().as_nanos();
        let mut t = self.timings.borrow_mut();
        t.publish_ns += elapsed;
        t.publications += 1;
        r
    }
    fn supports_replacement(&self) -> bool {
        self.inner.supports_replacement()
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        self.inner.replace_log(bytes, manifest)
    }
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn identity() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
struct Sample {
    batch: usize,
    entries: usize,
    append_ns: u128,
    barrier_ns: u128,
    total_ns: u128,
    io: Timings,
}
fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [root, batches, entries] = args.as_slice() else {
        return Err(
            "usage: wal_benchmark FRESH_DIRECTORY BATCHES(1..512) ENTRIES_PER_BATCH(1..32)".into(),
        );
    };
    let batches: usize = batches.parse()?;
    let entries: usize = entries.parse()?;
    if !(1..=512).contains(&batches) || !(1..=32).contains(&entries) {
        return Err("count out of bounds".into());
    }
    if (batches + WARMUP) * entries > LogLimits::default().max_entries_per_group {
        return Err("warm-up plus workload exceeds native retained-entry capacity".into());
    }
    let root = Path::new(root);
    std::fs::create_dir(root)?;
    let timings = Rc::new(RefCell::new(Timings::default()));
    let mut store = checked(NativeLogStore::create(
        ObservedIo {
            inner: FileLogIo::create(root.join("store"))?,
            timings: timings.clone(),
        },
        identity(),
        LogLimits::default(),
    ))?;
    let n = NodeId::new(1).unwrap();
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: checked(Policy::new(Tree::Voter(n), Limits::default()))?,
        voter_stores: [(n, identity())].into(),
    };
    let tickets = checked(store.append_batch(vec![LogMutation::Create(bootstrap)]))?;
    checked(store.barrier(&tickets))?;
    let mut samples = Vec::with_capacity(batches);
    let mut measured_start = None;
    for batch in 0..batches + WARMUP {
        let old = checked(store.state(group()))?;
        let first = old.last_index() + 1;
        let last = first + entries as u64 - 1;
        let mutation = LogMutation::Update(LogUpdate {
            snapshot_membership: None,
            group: group(),
            expected_revision: old.revision,
            hard_state: HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: last,
            snapshot: None,
            suffix: Some(Suffix {
                from: first,
                entries: (first..=last)
                    .map(|index| LogEntry {
                        index,
                        term: 1,
                        payload: EntryPayload::Command {
                            operation: OperationId::new(index as u128).unwrap(),
                            bytes: 1i64.to_le_bytes().to_vec(),
                        },
                    })
                    .collect(),
            }),
        });
        *timings.borrow_mut() = Timings::default();
        if batch == WARMUP {
            measured_start = Some(Instant::now());
        }
        let start = Instant::now();
        let tickets = checked(store.append_batch(vec![mutation]))?;
        let append_ns = start.elapsed().as_nanos();
        let barrier_start = Instant::now();
        let durable = checked(store.barrier(&tickets))?;
        let barrier_ns = barrier_start.elapsed().as_nanos();
        let total_ns = start.elapsed().as_nanos();
        if durable.tickets != tickets {
            return Err("barrier scope mismatch".into());
        }
        if batch >= WARMUP {
            samples.push(Sample {
                batch: batch - WARMUP,
                entries,
                append_ns,
                barrier_ns,
                total_ns,
                io: *timings.borrow(),
            });
        }
    }
    let elapsed = measured_start.unwrap().elapsed();
    let expected = checked(store.state(group()))?;
    drop(store);
    let recovered = checked(NativeLogStore::recover(
        FileLogIo::open(root.join("store"))?,
        identity(),
        LogLimits::default(),
    ))?;
    let actual = checked(recovered.state(group()))?;
    if actual != expected {
        return Err("recovered WAL differs from acknowledged state".into());
    }
    let count = (batches + WARMUP) * entries;
    let mut app = checked(Counter::new(count))?;
    let receipts = checked(app.apply_batch(&actual.entries))?;
    if checked(app.read_applied(actual.commit_index))? != count as i64 || receipts.len() != count {
        return Err("replay mismatch".into());
    }
    for index in [1, count] {
        let duplicate = LogEntry {
            index: app.applied_index() + 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(index as u128).unwrap(),
                bytes: 1i64.to_le_bytes().to_vec(),
            },
        };
        let r = checked(app.apply_batch(&[duplicate]))?.remove(0);
        if !r.duplicate || r.outcome != CounterOutcome::Value(index as i64) {
            return Err("historical retry mismatch".into());
        }
    }
    if checked(app.read_applied(app.applied_index()))? != count as i64 {
        return Err("retry changed value".into());
    }
    drop(recovered);
    let mut csv = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("samples.csv"))?;
    writeln!(csv,"batch,entries,append_ns,barrier_ns,total_ns,file_append_ns,wal_sync_ns,manifest_publish_ns,wal_bytes")?;
    for s in &samples {
        if s.io.appends != 1 || s.io.syncs != 1 || s.io.publications != 1 {
            return Err("primitive count mismatch".into());
        }
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{}",
            s.batch,
            s.entries,
            s.append_ns,
            s.barrier_ns,
            s.total_ns,
            s.io.append_ns,
            s.io.sync_ns,
            s.io.publish_ns,
            s.io.bytes
        )?;
    }
    csv.sync_all()?;
    let sum = |f: fn(&Sample) -> u128| samples.iter().map(f).sum::<u128>() as f64 / 1e6;
    let mut barriers = samples.iter().map(|s| s.barrier_ns).collect::<Vec<_>>();
    barriers.sort_unstable();
    let p99 = barriers[(batches * 99).div_ceil(100) - 1] as f64 / 1e6;
    let bytes = samples.iter().map(|s| s.io.bytes).sum::<usize>();
    let summary=format!("scope=local_wal groups=1 warmup_batches={WARMUP} batches={batches} entries_per_batch={entries} measured_records={} elapsed_s={:.6} durable_records_s={:.3} barrier_p99_ms={p99:.3} total_append_ms={:.3} file_append_ms={:.3} wal_sync_ms={:.3} manifest_publish_ms={:.3} total_barrier_ms={:.3} wal_bytes={bytes} recovered_records={count} replay_retry_verified=true\n",batches*entries,elapsed.as_secs_f64(),(batches*entries) as f64/elapsed.as_secs_f64(),sum(|s|s.append_ns),sum(|s|s.io.append_ns),sum(|s|s.io.sync_ns),sum(|s|s.io.publish_ns),sum(|s|s.barrier_ns));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("summary.txt"))?;
    file.write_all(summary.as_bytes())?;
    file.sync_all()?;
    print!("{summary}");
    Ok(())
}
