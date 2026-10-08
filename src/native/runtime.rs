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
//! Bounded caller-polled scheduling and deadlines. No worker threads or hidden
//! runtime. Cloned clocks share one epoch; providers do not close host resources.
use crate::{identity::GroupIdentity, runtime::*};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};

pub struct FairScheduler {
    capacity: usize,
    ready: VecDeque<GroupIdentity>,
    present: BTreeSet<GroupIdentity>,
}
impl FairScheduler {
    pub fn new(capacity: usize) -> Result<Self, RuntimeError> {
        if capacity == 0 || capacity > 65536 {
            return Err(RuntimeError::InvalidLimits);
        }
        Ok(Self {
            capacity,
            ready: VecDeque::new(),
            present: BTreeSet::new(),
        })
    }
}
impl ReadyScheduler for FairScheduler {
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        if self.present.contains(&group) {
            return Ok(());
        }
        if self.present.len() == self.capacity {
            return Err(RuntimeError::Overloaded);
        }
        self.present.insert(group);
        self.ready.push_back(group);
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        let group = self.ready.pop_front()?;
        self.present.remove(&group);
        Some(group)
    }
    fn cancel(&mut self, group: GroupIdentity) {
        if self.present.remove(&group) {
            self.ready.retain(|g| *g != group);
        }
    }
    fn len(&self) -> usize {
        self.present.len()
    }
}

/// Ordered deadlines, O(log registered timers) updates with no lazy tombstones.
/// This initial queue is not advertised as a constant-time timer wheel.
pub struct DeadlineQueue {
    owner: RuntimeOwner,
    capacity: usize,
    sequence: u64,
    now: MonoTime,
    by_deadline: BTreeMap<(MonoTime, u64), TimerToken>,
    by_group: BTreeMap<(GroupIdentity, TimerKind), TimerToken>,
}
impl DeadlineQueue {
    pub fn new(owner: RuntimeOwner, capacity: usize) -> Result<Self, RuntimeError> {
        if capacity == 0 || capacity > 131072 {
            return Err(RuntimeError::InvalidLimits);
        }
        Ok(Self {
            owner,
            capacity,
            sequence: 0,
            now: MonoTime(0),
            by_deadline: BTreeMap::new(),
            by_group: BTreeMap::new(),
        })
    }
}
impl TimerService for DeadlineQueue {
    fn owner(&self) -> RuntimeOwner {
        self.owner
    }
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        let key = (group, kind);
        if !self.by_group.contains_key(&key) && self.by_group.len() == self.capacity {
            return Err(RuntimeError::Overloaded);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RuntimeError::Exhausted)?;
        if let Some(old) = self.by_group.remove(&key) {
            self.by_deadline.remove(&(old.deadline, old.sequence));
        }
        let token = TimerToken {
            owner: self.owner,
            group,
            kind,
            sequence,
            deadline,
        };
        self.sequence = sequence;
        self.by_group.insert(key, token);
        self.by_deadline.insert((deadline, sequence), token);
        Ok(token)
    }
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError> {
        if token.owner != self.owner
            || self.by_group.get(&(token.group, token.kind)) != Some(&token)
        {
            return Err(RuntimeError::StaleTicket);
        }
        self.by_group.remove(&(token.group, token.kind));
        self.by_deadline.remove(&(token.deadline, token.sequence));
        Ok(())
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        if now < self.now {
            return Err(RuntimeError::ClockRegressed);
        }
        self.now = now;
        let mut expired = Vec::new();
        for _ in 0..limit.min(self.capacity) {
            if self
                .by_deadline
                .first_key_value()
                .is_none_or(|(key, _)| key.0 > now)
            {
                break;
            }
            let (_, token) = self.by_deadline.pop_first().unwrap();
            self.by_group.remove(&(token.group, token.kind));
            expired.push(Expiration {
                token,
                late_ms: now.0 - token.deadline.0,
            });
        }
        Ok(expired)
    }
    fn len(&self) -> usize {
        self.by_group.len()
    }
}

#[derive(Clone)]
pub struct MonotonicClock {
    epoch: Instant,
}
impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}
impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}
impl Clock for MonotonicClock {
    fn now(&self) -> MonoTime {
        MonoTime(self.epoch.elapsed().as_millis().min(u64::MAX as u128) as u64)
    }
}

/// Explicitly seeded, reproducible SplitMix64 election jitter. Hosts should
/// choose independent per-node seeds. No cryptographic/security use is supported.
pub struct JitterEntropy {
    state: u64,
}
impl JitterEntropy {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}
impl ElectionEntropy for JitterEntropy {
    fn sample(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut x = self.state;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }
}
