// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Offline attribution of sealed native single-group benchmark journals.
//! This issues no durability evidence and measures no operation latency.
use std::{collections::BTreeMap, io::Write, path::Path};
use voteboat::{identity::*, log::*, native::log_store::*};
#[path = "journal_attribution/copy.rs"]
mod copy;
#[cfg(test)]
#[path = "journal_attribution/tests.rs"]
mod tests;

type Failure = Box<dyn std::error::Error>;
const MAX_FRAMES: usize = 8192;
const MAX_EVENTS: usize = 16384;

fn checked<T, E: std::fmt::Debug>(result: Result<T, E>) -> Result<T, Failure> {
    result.map_err(|e| format!("{e:?}").into())
}
fn limits() -> LogLimits {
    LogLimits {
        max_groups: 1,
        max_entries_per_group: 2048,
        max_batch_units: 1,
        max_wal_bytes: 8 * 1024 * 1024,
        ..LogLimits::default()
    }
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Event {
    kind: &'static str,
    operation: u128,
    index: u64,
    term: u64,
    delta: i64,
}
struct Batch {
    sequence: u64,
    offset: usize,
    bytes: usize,
    class: &'static str,
    before: u64,
    after: u64,
    revision: u64,
    generation: u64,
    appended: usize,
    appended_commands: usize,
    committed_commands: usize,
    hard_state_changed: bool,
    events: Vec<Event>,
}
struct Analysis {
    identity: StoreIdentity,
    batches: Vec<Batch>,
    state: GroupLog,
}

fn inspect(root: &Path, identity: StoreIdentity) -> Result<Analysis, Failure> {
    // FileLogIo normally creates LOCK if absent. Refuse that mutation here.
    if !root.join("LOCK").is_file() || root.join("CURRENT").exists() {
        return Err("requires an existing uncompacted native benchmark journal and LOCK".into());
    }
    let mut source = FileLogIo::open(root)?;
    let manifest = source.read_manifest()?;
    let bytes = source.read_log(limits().max_wal_bytes)?;
    // The source lock remains owned until every copied validation/replay finishes.
    analyse(&manifest, &bytes, identity)
}

fn analyse(manifest: &[u8], bytes: &[u8], identity: StoreIdentity) -> Result<Analysis, Failure> {
    let recovered = copy::validate(manifest, bytes, identity, limits())?;
    if recovered.snapshot.is_some() {
        return Err("snapshot/compacted benchmark journals are unsupported".into());
    }
    let mut state = BTreeMap::new();
    let mut batches = Vec::new();
    let mut offset = 0;
    let mut events = 0usize;
    while offset < bytes.len() {
        if batches.len() == MAX_FRAMES {
            return Err("physical frame limit exceeded".into());
        }
        let remaining = &bytes[offset..];
        let size = checked(NativeLogCodec.frame_length(remaining, limits()))?;
        let frame = remaining.get(..size).ok_or("incomplete native frame")?;
        let (sequence, mutations) = checked(NativeLogCodec.decode_batch(frame, limits()))?;
        let before = state.get(&group()).cloned();
        checked(apply_batch(&mut state, &mutations, limits()))?;
        let after = state
            .get(&group())
            .ok_or("not the benchmark group1/incarnation1")?;
        let batch = attribute(sequence, offset, size, &mutations, before.as_ref(), after)?;
        events = events
            .checked_add(batch.events.len())
            .ok_or("event count overflow")?;
        if events > MAX_EVENTS {
            return Err("command event limit exceeded".into());
        }
        batches.push(batch);
        offset += size;
    }
    if state.len() != 1 || state.get(&group()) != Some(&recovered) {
        return Err("physical replay differs from native recovery".into());
    }
    Ok(Analysis {
        identity,
        batches,
        state: recovered,
    })
}

fn event(entry: &LogEntry, kind: &'static str) -> Result<Option<Event>, Failure> {
    match &entry.payload {
        EntryPayload::Command { operation, bytes } => {
            let data: [u8; 8] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| "requires native benchmark8-byte counter commands")?;
            Ok(Some(Event {
                kind,
                operation: operation.get(),
                index: entry.index,
                term: entry.term,
                delta: i64::from_le_bytes(data),
            }))
        }
        EntryPayload::Noop | EntryPayload::Configuration(_) => Ok(None),
    }
}

fn attribute(
    sequence: u64,
    offset: usize,
    bytes: usize,
    mutations: &[LogMutation],
    before: Option<&GroupLog>,
    after: &GroupLog,
) -> Result<Batch, Failure> {
    if after.snapshot.is_some() {
        return Err("snapshot transition unsupported".into());
    }
    let mut events = Vec::new();
    let mut appended = 0;
    for mutation in mutations {
        if let LogMutation::Update(update) = mutation {
            if let Some(suffix) = &update.suffix {
                appended += suffix.entries.len();
                for entry in &suffix.entries {
                    if let Some(event) = event(entry, "append")? {
                        events.push(event);
                    }
                }
            }
        }
    }
    let appended_commands = events.len();
    let old_commit = before.map_or(0, |s| s.commit_index);
    for index in old_commit + 1..=after.commit_index {
        let entry = after
            .entry_at(index)
            .ok_or("committed entry missing from physical replay")?;
        if let Some(event) = event(entry, "commit")? {
            events.push(event);
        }
    }
    let committed_commands = events.len() - appended_commands;
    let class = match (
        before.is_none(),
        appended_commands > 0,
        committed_commands > 0,
    ) {
        (true, _, _) => "bootstrap",
        (false, true, true) => "combined",
        (false, true, false) => "append_only",
        (false, false, true) => "commit_only",
        _ => "control",
    };
    Ok(Batch {
        sequence,
        offset,
        bytes,
        class,
        before: old_commit,
        after: after.commit_index,
        revision: after.revision.get(),
        generation: after.generation.get(),
        appended,
        appended_commands,
        committed_commands,
        hard_state_changed: before.is_some_and(|s| s.hard_state != after.hard_state),
        events,
    })
}

fn csv(analysis: &Analysis, writer: &mut impl Write) -> Result<(), Failure> {
    writeln!(writer,"store,store_incarnation,group,group_incarnation,batch,offset,frame_bytes,class,event,operation,index,term,delta,commit_before,commit_after,revision,generation,appended_entries,appended_commands,committed_commands,hard_state_changed")?;
    for batch in &analysis.batches {
        for event in std::iter::once(None).chain(batch.events.iter().map(Some)) {
            let (kind, operation, index, term, delta) = event.map_or_else(
                || {
                    (
                        "batch",
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                    )
                },
                |e| {
                    (
                        e.kind,
                        e.operation.to_string(),
                        e.index.to_string(),
                        e.term.to_string(),
                        e.delta.to_string(),
                    )
                },
            );
            writeln!(
                writer,
                "{},{},1,1,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                analysis.identity.id.get(),
                analysis.identity.incarnation.get(),
                batch.sequence,
                batch.offset,
                batch.bytes,
                batch.class,
                kind,
                operation,
                index,
                term,
                delta,
                batch.before,
                batch.after,
                batch.revision,
                batch.generation,
                batch.appended,
                batch.appended_commands,
                batch.committed_commands,
                batch.hard_state_changed
            )?;
        }
    }
    Ok(())
}

fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [root, id, incarnation] = args.as_slice() else {
        return Err(
            "usage: journal_attribution EXISTING_REPLICA_DIRECTORY STORE_ID STORE_INCARNATION"
                .into(),
        );
    };
    let identity = StoreIdentity {
        id: StoreId::new(id.parse()?).ok_or("store ID must be nonzero")?,
        incarnation: StoreIncarnation::new(incarnation.parse()?)
            .ok_or("store incarnation must be nonzero")?,
    };
    let analysis = inspect(Path::new(root), identity)?;
    eprintln!("scope=offline_native_single_group_sealed frames={} events={} committed={} source_unchanged=true latency_measured=false",
        analysis.batches.len(), analysis.batches.iter().map(|b| b.events.len()).sum::<usize>(), analysis.state.commit_index);
    let mut writer = std::io::BufWriter::new(std::io::stdout().lock());
    csv(&analysis, &mut writer)?;
    writer.flush()?;
    Ok(())
}
