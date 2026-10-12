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
use super::*;
pub(super) struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        100
    }
    fn enqueue(&mut self, g: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&g) {
            self.0.push_back(g);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, g: GroupIdentity) {
        self.0.retain(|x| *x != g);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
pub(super) struct Timers {
    owner: RuntimeOwner,
    sequence: u64,
    entries: BTreeMap<u64, TimerToken>,
}
impl TimerService for Timers {
    fn owner(&self) -> RuntimeOwner {
        self.owner
    }
    fn capacity(&self) -> usize {
        100
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        self.sequence += 1;
        let token = TimerToken {
            owner: self.owner,
            group,
            kind,
            deadline,
            sequence: self.sequence,
        };
        self.entries.insert(token.sequence, token);
        Ok(token)
    }
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError> {
        self.entries.remove(&token.sequence);
        Ok(())
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        let mut due = self
            .entries
            .values()
            .filter(|t| t.deadline <= now)
            .copied()
            .collect::<Vec<_>>();
        due.sort_by_key(|t| (t.deadline, t.sequence));
        due.truncate(limit);
        Ok(due
            .into_iter()
            .map(|token| {
                self.entries.remove(&token.sequence);
                Expiration {
                    token,
                    late_ms: now.0 - token.deadline.0,
                }
            })
            .collect())
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
}
pub(super) struct Entropy(u64);
impl ElectionEntropy for Entropy {
    fn sample(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}
pub(super) type Owner = EffectOwner<Ready, Timers, Entropy>;
pub(super) fn owner(log: &HostLogStore, core: Raft) -> Owner {
    owner_many(log, vec![core])
}
pub(super) fn owner_many(log: &HostLogStore, cores: Vec<Raft>) -> Owner {
    let id = RuntimeOwner {
        store: log.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        id,
        ShardLimits {
            max_groups: 16,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    for core in cores {
        shard.register(core).unwrap();
    }
    let timed = TimedShard::new(
        shard,
        Timers {
            owner: id,
            sequence: 0,
            entries: BTreeMap::new(),
        },
        Entropy(7),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    EffectOwner::new(
        timed,
        WorkerBinding {
            store: log.binding(),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap()
}
