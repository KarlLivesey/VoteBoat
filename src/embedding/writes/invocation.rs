// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

impl<R> WriteInvocation<R> {
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Check the full opaque invocation and original identity, not just its ID.
    pub fn matches(&self, envelope: &OriginalEnvelope<R>, ordinal: usize) -> bool {
        let key = &envelope.lease.0;
        Arc::ptr_eq(&self.key.shared, &key.shared)
            && Arc::ptr_eq(&self.key.epoch, &key.epoch)
            && self.key.generation == key.generation
            && envelope.ordinal == ordinal
            && ordinal < self.len
    }
    pub fn try_take(&mut self, ordinal: usize) -> Result<Option<OriginalEnvelope<R>>, WriteError> {
        observe(&self.key, ordinal, None)
    }
    pub fn waiter(&mut self, ordinal: usize) -> Result<OriginalWait<'_, R>, WriteError> {
        {
            let mut state = self.key.shared.lock()?;
            self.key.original(&mut state, ordinal)?;
        }
        Ok(OriginalWait {
            invocation: self,
            ordinal,
            completed: false,
        })
    }
    pub fn cancel_wait(&mut self, ordinal: usize) -> Result<(), WriteError> {
        self.key.shared.cancel(&self.key, ordinal)
    }
    /// Consumes the invocation; unresolved observations close on any error.
    pub fn finish(self) -> Result<(), WriteError> {
        let state = self.key.shared.lock()?;
        self.key.check(&state)?;
        if state
            .cells
            .iter()
            .take(self.len)
            .any(|cell| cell.observation == Observation::Open)
        {
            return Err(WriteError::BusyDrain);
        }
        Ok(())
    }
}
fn observe<R>(
    key: &Key<R>,
    ordinal: usize,
    mut candidate: Option<Waker>,
) -> Result<Option<OriginalEnvelope<R>>, WriteError> {
    let lease = key.copy();
    #[cfg(test)]
    {
        let hook = key
            .shared
            .before_observe
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }
    let (result, old) = {
        let mut state = key.shared.lock()?;
        let cell = key.original(&mut state, ordinal)?;
        if cell.observation != Observation::Open {
            return Err(if cell.observation == Observation::Closed {
                WriteError::ConsumerClosed
            } else {
                WriteError::AlreadyTerminal
            });
        }
        let operation = cell.operation.ok_or(WriteError::ProviderViolation)?;
        if let Some(outcome) = cell.output.take() {
            cell.observation = Observation::Consumed;
            state.owners += 1;
            let old = state.waiter.take().map(|(_, w)| w);
            (
                Some(OriginalEnvelope {
                    lease: EnvelopeLease(Lease::new(lease)),
                    ordinal,
                    operation,
                    outcome,
                }),
                old,
            )
        } else {
            let old = candidate
                .take()
                .and_then(|waker| state.waiter.replace((ordinal, waker)))
                .map(|(_, w)| w);
            (None, old)
        }
    };
    drop(old);
    drop(candidate);
    Ok(result)
}
impl<R> Future for OriginalWait<'_, R> {
    type Output = Result<OriginalEnvelope<R>, WriteError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // User-defined Waker vtables run before the port mutex is acquired.
        let candidate = cx.waker().clone();
        match observe(&this.invocation.key, this.ordinal, Some(candidate)) {
            Ok(None) => Poll::Pending,
            result => {
                this.completed = true;
                Poll::Ready(result.and_then(|value| value.ok_or(WriteError::ProviderViolation)))
            }
        }
    }
}
impl<R> Drop for OriginalWait<'_, R> {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.invocation.cancel_wait(self.ordinal);
        }
    }
}
impl<R> Drop for WriteInvocation<R> {
    fn drop(&mut self) {
        for ordinal in 0..self.len {
            let _ = self.key.shared.cancel(&self.key, ordinal);
        }
    }
}
