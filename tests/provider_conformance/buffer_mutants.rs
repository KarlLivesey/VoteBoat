// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::super::support::buffer::HostBuffer;
use super::*;

#[derive(Clone)]
struct BrokenPool {
    inner: HostPool,
    early_release: bool,
}
struct BrokenBuffer {
    inner: HostBuffer,
}
impl AsRef<[u8]> for BrokenBuffer {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
impl AsMut<[u8]> for BrokenBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.inner.as_mut()
    }
}
impl FrameBuffer for BrokenBuffer {
    fn reservation(&self) -> usize {
        self.inner.reservation()
    }
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    fn resize(&mut self, len: usize) -> Result<(), BufferError> {
        let old = self.inner.as_ref().len();
        self.inner.resize(len)?;
        if len > old {
            self.inner.as_mut()[old..].fill(255);
        }
        Ok(())
    }
}
impl BufferPool for BrokenPool {
    type Buffer = BrokenBuffer;
    fn limits(&self) -> BufferLimits {
        self.inner.limits()
    }
    fn usage(&self) -> BufferUsage {
        if self.early_release {
            BufferUsage::default()
        } else {
            self.inner.usage()
        }
    }
    fn owner_limits(&self) -> Option<BufferOwnerLimits> {
        self.inner.owner_limits()
    }
    fn control_reserve(&self) -> Option<BufferLimits> {
        self.inner.control_reserve()
    }
    fn bind_owner(&mut self, owner: BufferOwner) -> Result<(), BufferError> {
        self.inner.bind_owner(owner)
    }
    fn acquire(&self, r: usize, initial: usize) -> Result<Self::Buffer, BufferError> {
        self.inner
            .acquire(r, initial)
            .map(|inner| BrokenBuffer { inner })
    }
    fn acquire_class(
        &self,
        class: BufferClass,
        r: usize,
        initial: usize,
    ) -> Result<Self::Buffer, BufferError> {
        self.inner
            .acquire_class(class, r, initial)
            .map(|inner| BrokenBuffer { inner })
    }
    fn close(&mut self) {
        self.inner.close();
    }
}
fn failure(early_release: bool) -> String {
    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        history(
            BrokenPool {
                inner: host(),
                early_release,
            },
            1,
        );
    }))
    .unwrap_err();
    if let Some(text) = error.downcast_ref::<String>() {
        text.clone()
    } else {
        error.downcast_ref::<&str>().unwrap().to_string()
    }
}
#[test]
fn shared_buffer_checker_detects_early_credit_release() {
    assert!(failure(true).contains("live reservations after action"));
}
#[test]
fn shared_buffer_checker_detects_nonzero_growth() {
    assert!(failure(false).contains("preserved prefix and zeroed growth"));
}
