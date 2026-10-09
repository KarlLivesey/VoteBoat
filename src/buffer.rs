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
//! Owned, bounded byte storage for asynchronous transport frames.
/// Rust contract version; independent of wire and persistent formats.
pub const BUFFER_POOL_CONTRACT_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BufferError {
    InvalidLimits,
    Overloaded,
    Closed,
    TooLarge,
    AllocationFailed,
    ProviderViolation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferLimits {
    pub reserved_bytes: usize,
    pub leases: usize,
}
impl BufferLimits {
    pub fn validate(self) -> Result<Self, BufferError> {
        if self.reserved_bytes == 0 || self.leases == 0 {
            return Err(BufferError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BufferUsage {
    pub reserved_bytes: usize,
    pub leases: usize,
}
/// An owned reservation. Drop releases storage before returning its credits.
/// Resize preserves the existing prefix, zeroes new bytes, and never exceeds
/// reservation or changes credits. Failure leaves the existing bytes intact.
/// No borrowed bytes may outlive the lease. Providers must bound allocation,
/// including capacity rather than only visible length. AsRef and AsMut expose
/// the same stable length and storage between resize calls.
pub trait FrameBuffer: AsRef<[u8]> + AsMut<[u8]> {
    fn reservation(&self) -> usize;
    fn capacity(&self) -> usize;
    fn resize(&mut self, len: usize) -> Result<(), BufferError>;
}
/// Explicit host-selected resource view. Acquire is nonblocking and bounded;
/// refusal transfers no ownership/credits. A lease reserves its maximum size,
/// but initially allocates only initial_len. Accepted leases remain valid after
/// closing/dropping this view. Closing a view cannot close another host view.
/// No required cache, worker, singleton or on-disk state. Usage is diagnostic:
/// concurrent samples need not be an atomic snapshot of both fields.
pub trait BufferPool {
    type Buffer: FrameBuffer;
    fn limits(&self) -> BufferLimits;
    fn usage(&self) -> BufferUsage;
    fn acquire(&self, reservation: usize, initial_len: usize) -> Result<Self::Buffer, BufferError>;
    fn close(&mut self);
}
