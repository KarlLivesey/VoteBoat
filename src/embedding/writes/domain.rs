// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

impl<R> WriteDomain<R> {
    /// Create one unique host authority and one movable registration capability.
    pub fn new(
        binding: WriteBinding,
        limits: WriteDomainLimits,
    ) -> Result<(Self, WriteFactory<R>), WriteError> {
        if limits.ports == 0 || limits.ports > 65536 {
            return Err(WriteError::InvalidLimits);
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(limits.ports)
            .map_err(|_| WriteError::Allocation)?;
        let bytes = slots
            .capacity()
            .checked_mul(size_of::<Option<Weak<Shared<R>>>>())
            .and_then(|n| {
                n.checked_add(
                    size_of::<Registry<R>>()
                        + size_of::<Self>()
                        + size_of::<WriteFactory<R>>()
                        + 2 * size_of::<usize>()
                        + 64,
                )
            })
            .ok_or(WriteError::InvalidLimits)?;
        if bytes >= limits.retained_bytes {
            return Err(WriteError::InvalidLimits);
        }
        slots.resize_with(limits.ports, || None);
        let registry = Arc::new(Registry {
            binding,
            closed: AtomicBool::new(false),
            limits,
            state: Mutex::new(Registration {
                slots,
                usage: WriteUsage {
                    ports: 0,
                    retained_bytes: bytes,
                },
            }),
        });
        Ok((
            Self {
                registry: registry.clone(),
            },
            WriteFactory { registry },
        ))
    }
    pub fn binding(&self) -> WriteBinding {
        self.registry.binding
    }
    /// Validate the exact live request domain before this host extracts/proposes
    /// anything. Numeric group/store equality cannot identify an opaque owner.
    pub fn validate_submission(&self, submission: &WriteSubmission<R>) -> Result<(), WriteError> {
        if !Arc::ptr_eq(&self.registry, &submission.key.shared.registry)
            || self.binding() != submission.binding()
        {
            return Err(WriteError::WrongBinding);
        }
        if self.registry.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        let state = submission.key.shared.lock()?;
        submission.key.check(&state)
    }
    /// Preserve Applied results, resolve remaining unknowns and wake outside all
    /// locks. Does not stop any caller-owned executor or another domain.
    pub fn close(&mut self, reason: WriteUnknown) {
        self.registry.closed.store(true, Ordering::Release);
        for slot in 0..self.registry.limits.ports {
            let port = {
                let registration = self
                    .registry
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                registration.slots[slot].as_ref().and_then(Weak::upgrade)
            };
            if let Some(port) = port {
                port.close(reason);
            }
        }
    }
}
impl<R> Drop for WriteDomain<R> {
    fn drop(&mut self) {
        self.close(WriteUnknown::OwnerFailed);
    }
}
impl<R> WriteFactory<R> {
    pub fn binding(&self) -> WriteBinding {
        self.registry.binding
    }
    pub fn usage(&self) -> Result<WriteUsage, WriteError> {
        self.registry
            .state
            .lock()
            .map(|state| state.usage)
            .map_err(|_| WriteError::Poisoned)
    }
    /// Allocates only on cold registration. The advertised reservation remains
    /// charged until ALL port/submission/producer/observation owners disappear.
    pub fn open_port(&self, limits: WritePortLimits) -> Result<WritePort<R>, WriteError> {
        validate::<R>(limits)?;
        let mut registration = self
            .registry
            .state
            .lock()
            .map_err(|_| WriteError::Poisoned)?;
        if self.registry.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        let slot = registration
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or(WriteError::Overloaded)?;
        let charged = registration
            .usage
            .retained_bytes
            .checked_add(limits.retained_bytes)
            .filter(|n| *n <= self.registry.limits.retained_bytes)
            .ok_or(WriteError::Overloaded)?;
        let cells = make_cells::<R>(limits)?;
        let shared = Arc::new(Shared {
            registry: self.registry.clone(),
            slot,
            limits,
            state: Mutex::new(State {
                epoch: Arc::new(()),
                generation: 0,
                generation_max: u64::MAX,
                owners: 0,
                len: 0,
                closed: None,
                waiter: None,
                cells,
            }),
            #[cfg(test)]
            before_observe: Mutex::new(None),
        });
        registration.slots[slot] = Some(Arc::downgrade(&shared));
        registration.usage.ports += 1;
        registration.usage.retained_bytes = charged;
        Ok(WritePort { shared })
    }
}
fn validate<R>(limits: WritePortLimits) -> Result<(), WriteError> {
    if limits.originals == 0
        || limits.originals > 16384
        || limits.command_bytes == 0
        || limits.command_bytes > 16 * 1024 * 1024
        || limits.receipt_bytes < size_of::<R>()
        || limits.receipt_bytes > 16 * 1024 * 1024
        || limits.waker_bytes == 0
        || limits
            .originals
            .checked_mul(limits.command_bytes)
            .is_none_or(|n| n > limits.command_capacity_bytes)
        || limits.required_bytes::<R>()? > limits.retained_bytes
    {
        return Err(WriteError::InvalidLimits);
    }
    Ok(())
}
fn make_cells<R>(limits: WritePortLimits) -> Result<Vec<Cell<R>>, WriteError> {
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(limits.originals)
        .map_err(|_| WriteError::Allocation)?;
    let mut commands = 0usize;
    for _ in 0..limits.originals {
        let cell = Cell::empty(limits.command_bytes)?;
        commands = commands
            .checked_add(cell.bytes.capacity())
            .ok_or(WriteError::InvalidLimits)?;
        cells.push(cell);
    }
    if commands > limits.command_capacity_bytes
        || limits.required_for::<R>(cells.capacity(), commands)? > limits.retained_bytes
    {
        return Err(WriteError::InvalidLimits);
    }
    Ok(cells)
}
impl<R> WritePort<R> {
    pub fn binding(&self) -> WriteBinding {
        self.shared.binding()
    }
    pub fn limits(&self) -> WritePortLimits {
        self.shared.limits
    }
    /// Reclaim a fully drained invocation while retaining the bounded buffers.
    pub fn reap(&mut self) -> Result<(), WriteError> {
        let len = {
            let state = self.shared.lock()?;
            if state.owners != 0 {
                return Err(WriteError::BusyDrain);
            }
            state.len
        };
        for ordinal in 0..len {
            let output = {
                let mut state = self.shared.lock()?;
                let cell = &mut state.cells[ordinal];
                if cell.producer != Producer::Terminal {
                    return Err(WriteError::BusyDrain);
                }
                cell.bytes.clear();
                cell.operation = None;
                cell.observation = Observation::Consumed;
                cell.output.take()
            };
            drop(output);
        }
        let (closed, waiter) = {
            let mut state = self.shared.lock()?;
            state.len = 0;
            (
                state.closed.is_some() || self.shared.registry.closed.load(Ordering::Acquire),
                state.waiter.take(),
            )
        };
        drop(waiter);
        if closed {
            Err(WriteError::Closed)
        } else {
            Ok(())
        }
    }
    /// Copy immutable inputs into preowned capacity before any ownership transfer.
    pub fn begin(
        &mut self,
        originals: &[WriteInput<'_>],
    ) -> Result<(WriteSubmission<R>, WriteInvocation<R>), WriteError> {
        if originals.is_empty()
            || originals.len() > self.shared.limits.originals
            || originals
                .iter()
                .any(|input| input.bytes.len() > self.shared.limits.command_bytes)
        {
            return Err(WriteError::InvalidShape);
        }
        self.reap()?;
        let renew = {
            let state = self.shared.lock()?;
            state.generation == state.generation_max
        };
        // Cold, fully drained epoch renewal. Never allocate an epoch per begin.
        let epoch = renew.then(|| Arc::new(()));
        let mut state = self.shared.lock()?;
        if self.shared.registry.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        if let Some(epoch) = epoch {
            state.epoch = epoch;
            state.generation = 0;
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(WriteError::BusyDrain)?;
        for (cell, input) in state.cells.iter_mut().zip(originals) {
            cell.operation = Some(input.operation);
            cell.bytes.extend_from_slice(input.bytes);
            cell.producer = Producer::Unextracted;
            cell.observation = Observation::Open;
        }
        state.len = originals.len();
        state.owners = 2;
        let key = Key {
            shared: self.shared.clone(),
            epoch: state.epoch.clone(),
            generation: state.generation,
        };
        Ok((
            WriteSubmission {
                key: Lease::new(key.copy()),
                next: 0,
                len: originals.len(),
            },
            WriteInvocation {
                key: Lease::new(key),
                len: originals.len(),
            },
        ))
    }
}
impl<R> Drop for WritePort<R> {
    fn drop(&mut self) {
        let len = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len;
        for ordinal in 0..len {
            let key = {
                let state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
                Key {
                    shared: self.shared.clone(),
                    epoch: state.epoch.clone(),
                    generation: state.generation,
                }
            };
            let _ = self.shared.cancel(&key, ordinal);
        }
    }
}
