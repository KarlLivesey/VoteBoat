// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Immutable command content with conservative per-owner retained accounting.
use std::{
    alloc::Layout,
    fmt,
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
};

/// Immutable replicated command bytes. Empty values own no allocation.
///
/// Clones share nonempty backing, without exposing its Arc or mutable contents.
/// Replace a value to edit a command; ownership and persistence gates still
/// validate the full operation, headers and content. Retained accounting charges
/// the complete content and shared header independently for every retained clone.
#[derive(Clone, Default)]
pub struct CommandBytes(Option<Arc<[u8]>>);

/// Most a command's retained charge exceeds its length: the shared header and
/// the padding to its alignment (23 bytes on 64-bit targets).
pub(crate) const RETAINED_EXTRA_MAX: usize = {
    let align = std::mem::align_of::<AtomicUsize>();
    std::mem::size_of::<[AtomicUsize; 2]>() + if align > 2 { align } else { 2 } - 1
};

impl CommandBytes {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_deref().unwrap_or(&[])
    }
    /// Make an independent mutable copy; it cannot change this value or clones.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
    /// Conservative retained charge, excluding the inline command value.
    pub fn retained_bytes(&self) -> usize {
        Self::retained_bytes_for_len(self.len()).expect("allocated command charge fits usize")
    }
    pub(crate) fn retained_bytes_for_len(len: usize) -> Option<usize> {
        if len == 0 {
            Some(0)
        } else {
            let header = Layout::new::<[AtomicUsize; 2]>().align_to(2).ok()?;
            let content = Layout::array::<u8>(len).ok()?;
            Some(header.extend(content).ok()?.0.pad_to_align().size())
        }
    }
}
impl From<Vec<u8>> for CommandBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(bytes.as_slice())
    }
}
impl From<&[u8]> for CommandBytes {
    fn from(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            Self(None)
        } else {
            Self(Some(Arc::from(bytes)))
        }
    }
}
impl AsRef<[u8]> for CommandBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl Deref for CommandBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl fmt::Debug for CommandBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(formatter)
    }
}
impl PartialEq for CommandBytes {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right) || left == right,
            (None, Some(_)) | (Some(_), None) => false,
        }
    }
}
impl Eq for CommandBytes {}

#[cfg(test)]
mod tests;
