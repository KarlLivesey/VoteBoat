// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) struct Registry<R> {
    pub binding: WriteBinding,
    pub closed: AtomicBool,
    pub limits: WriteDomainLimits,
    pub state: Mutex<Registration<R>>,
}
pub(super) struct Registration<R> {
    pub slots: Vec<Option<Weak<Shared<R>>>>,
    pub usage: WriteUsage,
}
pub(super) struct Shared<R> {
    pub registry: Arc<Registry<R>>,
    pub slot: usize,
    pub limits: WritePortLimits,
    pub state: Mutex<State<R>>,
    #[cfg(test)]
    pub before_observe: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
pub(super) struct State<R> {
    pub epoch: Arc<()>,
    pub generation: u64,
    pub generation_max: u64,
    pub owners: usize,
    pub len: usize,
    pub closed: Option<WriteUnknown>,
    pub waiter: Option<(usize, Waker)>,
    pub cells: Vec<Cell<R>>,
}
pub(super) struct Cell<R> {
    pub operation: Option<OperationId>,
    pub bytes: Vec<u8>,
    pub producer: Producer,
    pub observation: Observation,
    pub output: Option<OriginalOutcome<R>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Producer {
    Unextracted,
    Extracted,
    Bound(ProposalPosition),
    Terminal,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Observation {
    Open,
    Consumed,
    Closed,
}
pub(super) struct Key<R> {
    pub shared: Arc<Shared<R>>,
    pub epoch: Arc<()>,
    pub generation: u64,
}
/// One genuinely owned affine key; plain Key copies are internal identity
/// borrows only. Retire under the publication lock or on Drop, never Clone.
pub(super) struct Lease<R> {
    key: Option<Key<R>>,
}
impl<R> Lease<R> {
    pub fn new(key: Key<R>) -> Self {
        Self { key: Some(key) }
    }
    pub fn is_live(&self) -> bool {
        self.key.is_some()
    }
    pub fn retire_locked(&mut self, state: &mut State<R>, keepalive: &Arc<Shared<R>>) {
        if let Some(key) = self.key.take() {
            debug_assert!(Arc::ptr_eq(keepalive, &key.shared));
            if key.check(state).is_ok() {
                state.owners -= 1;
            }
            // keepalive owns the Shared allocation; only trivial epoch/shared
            // refcounts can be destroyed while this mutex is held.
            drop(key);
        }
    }
    pub fn resolve(
        &mut self,
        ordinal: usize,
        outcome: OriginalOutcome<R>,
    ) -> Result<(), WriteError> {
        let shared = self.shared.clone();
        let mut outcome = Some(outcome);
        let (result, wake) = {
            let (mut state, poisoned) = match shared.state.lock() {
                Ok(state) => (state, false),
                Err(p) => (p.into_inner(), true),
            };
            let poison_wake = if poisoned {
                state.fail(WriteUnknown::OwnerFailed)
            } else {
                None
            };
            let result = if poisoned {
                Err(WriteError::Poisoned)
            } else {
                (|| {
                    let cell = self.original(&mut state, ordinal)?;
                    if cell.producer == Producer::Terminal {
                        return Err(WriteError::AlreadyTerminal);
                    }
                    if cell.output.is_some() {
                        return Err(WriteError::ProviderViolation);
                    }
                    cell.producer = Producer::Terminal;
                    if cell.observation != Observation::Open {
                        return Err(WriteError::ConsumerClosed);
                    }
                    cell.output = outcome.take();
                    Ok(())
                })()
            };
            let wake = poison_wake.or_else(|| {
                if result.is_ok() && state.waiter.as_ref().is_some_and(|(i, _)| *i == ordinal) {
                    state.waiter.take().map(|(_, w)| w)
                } else {
                    None
                }
            });
            self.retire_locked(&mut state, &shared);
            if poisoned {
                shared.state.clear_poison();
            }
            (result, wake)
        };
        drop(shared);
        drop(outcome);
        if let Some(wake) = wake {
            wake.wake();
        }
        result
    }
}
impl<R> std::ops::Deref for Lease<R> {
    type Target = Key<R>;
    fn deref(&self) -> &Key<R> {
        self.key.as_ref().expect("live affine write capability")
    }
}
impl<R> Drop for Lease<R> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            key.retire();
        }
    }
}
impl<R> State<R> {
    pub fn fail(&mut self, reason: WriteUnknown) -> Option<Waker> {
        self.closed.get_or_insert(reason);
        for cell in self.cells.iter_mut().take(self.len) {
            if cell.producer != Producer::Terminal {
                cell.producer = Producer::Terminal;
                if cell.observation == Observation::Open && cell.output.is_none() {
                    cell.output = Some(OriginalOutcome::Unknown(reason));
                }
            }
        }
        self.waiter.take().map(|(_, w)| w)
    }
}
impl<R> Key<R> {
    pub fn copy(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            epoch: self.epoch.clone(),
            generation: self.generation,
        }
    }
    pub fn check(&self, state: &State<R>) -> Result<(), WriteError> {
        if self.generation != state.generation || !Arc::ptr_eq(&self.epoch, &state.epoch) {
            return Err(WriteError::StaleInvocation);
        }
        Ok(())
    }
    pub fn original<'s>(
        &self,
        state: &'s mut State<R>,
        ordinal: usize,
    ) -> Result<&'s mut Cell<R>, WriteError> {
        self.check(state)?;
        if ordinal >= state.len {
            return Err(WriteError::UnknownOriginal);
        }
        Ok(&mut state.cells[ordinal])
    }
    pub fn retire(self) {
        let shared = self.shared.clone();
        let mut state = shared.state.lock().unwrap_or_else(|p| p.into_inner());
        if self.check(&state).is_ok() {
            state.owners -= 1;
        }
        // This independent Shared Arc prevents its R/State destructor here;
        // the only other reference destroyed is Arc<()> with no user Drop.
        drop(self);
        drop(state);
        drop(shared);
    }
}
impl<R> Cell<R> {
    pub fn empty(command_bytes: usize) -> Result<Self, WriteError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(command_bytes)
            .map_err(|_| WriteError::Allocation)?;
        Ok(Self {
            operation: None,
            bytes,
            producer: Producer::Terminal,
            observation: Observation::Consumed,
            output: None,
        })
    }
    pub fn matches(&self, entry: &LogEntry) -> bool {
        matches!(&entry.payload, EntryPayload::Command { operation, bytes }
            if self.operation == Some(*operation) && self.bytes.as_slice() == bytes.as_slice())
    }
}
impl<R> Shared<R> {
    pub fn binding(&self) -> WriteBinding {
        self.registry.binding
    }
    pub fn lock(&self) -> Result<MutexGuard<'_, State<R>>, WriteError> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poison) => {
                // No user value or Waker is destroyed with this guard held.
                drop(poison.into_inner());
                self.close(WriteUnknown::OwnerFailed);
                self.state.clear_poison();
                Err(WriteError::Poisoned)
            }
        }
    }
    pub fn close(&self, reason: WriteUnknown) {
        let wake = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.fail(reason)
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    pub fn cancel(&self, key: &Key<R>, ordinal: usize) -> Result<(), WriteError> {
        let (output, waker) = {
            let mut state = self.lock()?;
            let cell = key.original(&mut state, ordinal)?;
            cell.observation = Observation::Closed;
            let output = cell.output.take();
            let waker = if state.waiter.as_ref().is_some_and(|(i, _)| *i == ordinal) {
                state.waiter.take().map(|(_, w)| w)
            } else {
                None
            };
            (output, waker)
        };
        drop(output);
        drop(waker);
        Ok(())
    }
}
impl<R> Drop for Shared<R> {
    fn drop(&mut self) {
        // Destroy arbitrary R/Wakers and retained buffers before unregistering;
        // a reentrant destructor must still see the old reservation charged.
        let state = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        let cells = std::mem::take(&mut state.cells);
        let waiter = state.waiter.take();
        drop(cells);
        drop(waiter);
        let mut registration = self
            .registry
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        registration.slots[self.slot] = None;
        registration.usage.ports -= 1;
        registration.usage.retained_bytes -= self.limits.retained_bytes;
        // Shared's State/R destruction follows this unlocked Drop body.
    }
}
