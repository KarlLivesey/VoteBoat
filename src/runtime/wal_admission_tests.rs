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
//! ST2 round 5 (R5-1): the shard's WAL account bounds exactly the new suffix
//! a follower would accept. Real `Shard`, `Raft`, `apply_batch` planner and
//! native checkpoint codec: the live image `L` of a rewrite is the encoded
//! checkpoint of every registered group's state, and must stay at most
//! `H + R` (high-water mark plus control reserve) after any admitted step.
use super::*;
use crate::{
    contracts::HardState,
    log::*,
    log_format::{LogCodec, NativeLogCodec},
    membership::Configuration,
    quorum::{Limits, Policy, Tree},
};

struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        2
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&group) {
            self.0.push_back(group);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, group: GroupIdentity) {
        self.0.retain(|id| *id != group);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(u128::from(n)).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn binding(n: u64) -> StoreBinding {
    StoreBinding {
        identity: store(n),
        session: StoreSession::new(1).unwrap(),
    }
}
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
/// Sol's limits: 4.5 MiB WAL, 64 KiB reserve, 1 MiB batches, 400,000 entries.
fn limits() -> LogLimits {
    LogLimits {
        max_entries_per_group: 400_000,
        max_wal_bytes: 4608 * 1024,
        control_reserve_bytes: 64 * 1024,
        ..LogLimits::default()
    }
}
/// `H` and `R` as the node publishes them for a native WAL of `limits()`.
fn mark_and_reserve() -> (usize, usize) {
    let l = limits();
    let usable = l.max_wal_bytes - l.control_reserve_bytes;
    (
        WalMaintenanceStatus::default().high_water_bytes(usable),
        l.control_reserve_bytes,
    )
}
fn command(index: u64, term: u64, bytes: usize) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::from(index)).unwrap(),
            bytes: vec![7; bytes].into(),
        },
    }
}
fn noop(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Noop,
    }
}
/// Group `id` of voters 1–3 as recovered on node 1: hard-state term 1,
/// nothing committed, holding `entries`.
fn state(id: u128, entries: Vec<LogEntry>) -> GroupLog {
    let configuration = Configuration::new(
        ConfigurationId::new(1).unwrap(),
        Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        (1..=3).map(|n| (node(n), store(n))).collect(),
        BTreeMap::new(),
    )
    .unwrap();
    let bootstrap = Bootstrap {
        group: group(id),
        configuration: configuration.id(),
        policy: configuration.policy().clone(),
        voter_stores: configuration.voter_stores().clone(),
    };
    let mut states = BTreeMap::new();
    apply_batch(&mut states, &[LogMutation::Create(bootstrap)], limits()).unwrap();
    let update = LogUpdate {
        group: group(id),
        expected_revision: states[&group(id)].revision,
        hard_state: HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: 0,
        suffix: Some(Suffix { from: 1, entries }),
        snapshot: None,
        snapshot_membership: None,
    };
    apply_batch(&mut states, &[LogMutation::Update(update)], limits()).unwrap();
    states.remove(&group(id)).unwrap()
}
/// The live image a rewrite of `states` writes: the native checkpoint.
fn live_image(states: &BTreeMap<GroupIdentity, GroupLog>) -> usize {
    NativeLogCodec
        .encode_checkpoint(1, states, limits(), limits().max_wal_bytes)
        .unwrap()
        .len()
}
/// Group 1 holds a noop and command 2; group 2 holds `noops` uncommitted
/// term-1 noops and one command of `pad` bytes.
fn states(noops: u64, pad: usize) -> BTreeMap<GroupIdentity, GroupLog> {
    let mut other: Vec<_> = (1..=noops).map(|i| noop(i, 1)).collect();
    other.push(command(noops + 1, 1, pad));
    BTreeMap::from([
        (group(1), state(1, vec![noop(1, 1), command(2, 1, 8)])),
        (group(2), state(2, other)),
    ])
}
/// States whose live image is exactly `target` bytes.
fn states_with_image(target: usize) -> BTreeMap<GroupIdentity, GroupLog> {
    let base = live_image(&states(0, 0));
    let per_noop = live_image(&states(1, 0)) - base;
    let noops = ((target - base) / per_noop) as u64;
    let pad = target - live_image(&states(noops, 0));
    let states = states(noops, pad);
    assert_eq!(live_image(&states), target, "fixture image");
    states
}
fn shard(states: &BTreeMap<GroupIdentity, GroupLog>) -> Shard<Ready> {
    let owner = RuntimeOwner {
        store: binding(1),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let shard_limits = ShardLimits {
        max_groups: 2,
        ..ShardLimits::default()
    };
    let mut shard = Shard::new(owner, shard_limits, Ready(VecDeque::new())).unwrap();
    for state in states.values() {
        shard
            .register(Raft::recover(node(1), binding(1), state.clone(), limits()).unwrap())
            .unwrap();
    }
    shard
}
/// Node 2, leader of term 65, appends `entries` to group 1 after the held
/// term-1 entry at `previous_index`.
fn append(previous_index: u64, entries: Vec<LogEntry>) -> Event {
    Event::Receive(Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: binding(2),
        to: node(1),
        term: 65,
        context: RequestContext {
            origin: binding(2),
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index,
            previous_term: 1,
            entries,
            leader_commit: 0,
        },
    })
}
/// The matching command 2, then new noops at indexes 3–66 in terms 2–65.
fn matched_prefix_then_noops() -> Vec<LogEntry> {
    let mut entries = vec![command(2, 1, 8)];
    entries.extend((3..=66).map(|i| noop(i, i - 1)));
    entries
}
/// Step `event` on group 1 with the account published at `published` and
/// apply every staged update to `states` with the real planner. Returns the
/// staged bound and the live image of the resulting state.
fn step(
    shard: &mut Shard<Ready>,
    states: &mut BTreeMap<GroupIdentity, GroupLog>,
    published: usize,
    event: Event,
) -> (usize, usize) {
    let (limit, reserve) = mark_and_reserve();
    shard.set_wal_budget(Some(WalBudget {
        limit,
        published,
        reserve,
    }));
    shard.admit(group(1), event).unwrap();
    let ticket = shard.poll(MonoTime(0)).unwrap().unwrap();
    let stepped = shard.step_next(ticket, MonoTime(0)).unwrap().unwrap();
    for effect in stepped.result.unwrap() {
        if let Effect::Persist(update) = effect {
            apply_batch(states, &[LogMutation::Update(update)], limits()).unwrap();
        }
    }
    (shard.wal_account().pending, live_image(states))
}

/// Sol's round-4 reproduction (P2-1). Another group's uncommitted noops
/// leave 616 bytes of noop room in the live image: one leader noop (576
/// bytes) fits, 64 do not. The append's received range starts with command
/// 2, which group 1 already holds; its new suffix is 64 noops. The account
/// must bound that new suffix against the noop room, so the follower
/// declines it (typed and counted) and the live image stays at most `H + R`.
/// On cd90fb7a the whole message was classified as data, only "one noop
/// fits" reached the core, and the 64 noops were taken: `L = H + R + 472`.
#[test]
fn matched_data_prefix_cannot_bypass_the_shared_noop_bound() {
    let (limit, reserve) = mark_and_reserve();
    assert_eq!((limit, reserve), (3_489_792, 65_536));
    let ceiling = limit + reserve;
    let before = ceiling - 616;
    let mut states = states_with_image(before);
    let mut shard = shard(&states);
    let (staged, after) = step(
        &mut shard,
        &mut states,
        before,
        append(1, matched_prefix_then_noops()),
    );
    assert!(
        after <= ceiling,
        "live image {after} exceeds H + R = {ceiling} by {} (staged bound {staged})",
        after - ceiling
    );
    assert_eq!(after, before, "a term record is one fixed record per group");
    assert_eq!(states[&group(1)].last_index(), 2, "no noop taken");
    assert_eq!(states[&group(1)].hard_state.term, 65, "term record kept");
    let core = shard.core(group(1)).unwrap();
    assert_eq!(core.declined_appends(), 1);
    assert_eq!(core.wal_declined_appends(), (1, 1));
    let account = shard.wal_account();
    assert_eq!((account.declined, account.noop_refused), (1, 1));
    assert_eq!(staged, UPDATE_WAL_OVERHEAD, "only the term record staged");
}

/// R5-1: a new suffix that mixes noops and new data is data, checked
/// against the mark alone, and declined above it as before, whether or not
/// its received range starts with a matching entry.
#[test]
fn a_new_suffix_with_data_is_still_declined_above_the_mark() {
    let (limit, reserve) = mark_and_reserve();
    let before = limit + reserve - 8 * 1024;
    for matched in [true, false] {
        let mut states = states_with_image(before);
        let mut shard = shard(&states);
        // New suffix: noops 3–5, new command 6, noops 7–10 (term 2).
        let new = (3..=10).map(|i| if i == 6 { command(i, 2, 8) } else { noop(i, 2) });
        let event = if matched {
            append(1, std::iter::once(command(2, 1, 8)).chain(new).collect())
        } else {
            append(2, new.collect())
        };
        let (_, after) = step(&mut shard, &mut states, before, event);
        assert!(after <= limit + reserve);
        assert_eq!(states[&group(1)].last_index(), 2, "matched {matched}");
        let account = shard.wal_account();
        assert_eq!(
            (account.declined, account.noop_refused),
            (1, 0),
            "matched {matched}"
        );
    }
}

/// R5-1: with room under `H + R`, the same matched-prefix append is taken,
/// and the account stages exactly the new suffix's bound (the matching
/// command is not persisted again, so it is not counted).
#[test]
fn a_matched_prefix_append_with_room_stages_only_the_new_suffix_bound() {
    let (limit, reserve) = mark_and_reserve();
    let before = limit + reserve - 8 * 1024;
    let mut states = states_with_image(before);
    let mut shard = shard(&states);
    let (staged, after) = step(
        &mut shard,
        &mut states,
        before,
        append(1, matched_prefix_then_noops()),
    );
    assert_eq!(states[&group(1)].last_index(), 66);
    assert_eq!(staged, UPDATE_WAL_OVERHEAD + 64 * entry_wal_bound(0));
    assert!(after <= limit + reserve);
    let account = shard.wal_account();
    assert_eq!((account.declined, account.noop_refused), (0, 0));
}
