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
pub const BUFFER_POOL_CONTRACT_VERSION: u32 = 3;
/// Accounting identity; it grants no authorization. Connection generations are
/// excluded so reconnects share outstanding reservations for the same stores.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BufferOwner {
    pub local_node: crate::identity::NodeId,
    pub local_store: crate::identity::StoreId,
    pub local_incarnation: crate::identity::StoreIncarnation,
    pub peer_node: crate::identity::NodeId,
    pub peer_store: crate::identity::StoreId,
    pub peer_incarnation: crate::identity::StoreIncarnation,
}
/// Non-overbooked bulk quotas. Native binding metadata is bounded to 1024 owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferOwnerLimits {
    pub owners: usize,
    pub bulk: BufferLimits,
}
impl BufferOwnerLimits {
    pub fn validate(self, total: BufferLimits, reserve: BufferLimits) -> Result<Self, BufferError> {
        total.validate_control_reserve(reserve)?;
        self.bulk.validate()?;
        if self.owners == 0
            || self.owners > 1024
            || self
                .bulk
                .reserved_bytes
                .checked_mul(self.owners)
                .is_none_or(|n| n > total.reserved_bytes - reserve.reserved_bytes)
            || self
                .bulk
                .leases
                .checked_mul(self.owners)
                .is_none_or(|n| n > total.leases - reserve.leases)
        {
            return Err(BufferError::InvalidLimits);
        }
        Ok(self)
    }
}
/// Unclassified input and application/snapshot sends are Bulk. Only validated
/// protocol-control sends may use protected capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BufferClass {
    Bulk,
    Control,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BufferError {
    InvalidLimits,
    InvalidOwner,
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
    /// Leave positive bulk capacity in both dimensions.
    pub fn validate_control_reserve(self, reserve: Self) -> Result<Self, BufferError> {
        self.validate()?;
        reserve.validate()?;
        if reserve.reserved_bytes >= self.reserved_bytes || reserve.leases >= self.leases {
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
    /// Immutable optional quota declaration, shared across views. Selected
    /// views must bind before acquisition and cannot be rebound to another owner.
    fn owner_limits(&self) -> Option<BufferOwnerLimits> {
        None
    }
    /// Bind before accepted work. Same-identity reconnects retain held credits.
    /// Unsupported declarations refuse rather than silently ignoring quotas.
    fn bind_owner(&mut self, _owner: BufferOwner) -> Result<(), BufferError> {
        if self.owner_limits().is_some() {
            Err(BufferError::ProviderViolation)
        } else {
            Ok(())
        }
    }
    fn acquire(&self, reservation: usize, initial_len: usize) -> Result<Self::Buffer, BufferError>;
    /// Declared capacity protected from Bulk reservations, including acquire.
    /// None preserves the original undivided-budget contract. Control may use
    /// any free total capacity; it cannot exceed limits. No deadline guarantee.
    fn control_reserve(&self) -> Option<BufferLimits> {
        None
    }
    /// Same ownership/failure contract as acquire. Providers declaring reserve
    /// must override this method; the compatibility default cannot honor it.
    fn acquire_class(
        &self,
        class: BufferClass,
        reservation: usize,
        initial_len: usize,
    ) -> Result<Self::Buffer, BufferError> {
        let _ = class;
        if self.control_reserve().is_some() {
            return Err(BufferError::ProviderViolation);
        }
        self.acquire(reservation, initial_len)
    }
    fn close(&mut self);
}
