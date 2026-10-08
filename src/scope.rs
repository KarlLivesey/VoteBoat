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
//! Bounded application data movement. Images do not certify ownership or durability.
use crate::{application::*, routing::*};

pub const SCOPE_APPLICATION_CONTRACT_VERSION: u32 = 1;
pub const MAX_SCOPE_IMAGE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SCOPE_IMPORTS: usize = 256;

/// Immutable provider image, not a source-fence, target-ready or activation receipt.
/// The lifecycle protocol must bind its bytes to the exact source, operation,
/// epoch, committed fence, target and published decision before serving.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopeImage {
    schema: u64,
    scheme: PartitionScheme,
    scope: BucketRange,
    source_applied: u64,
    bytes: Vec<u8>,
}
impl ScopeImage {
    pub fn new(
        schema: u64,
        scheme: PartitionScheme,
        scope: BucketRange,
        source_applied: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, (ApplicationError, Vec<u8>)> {
        if schema == 0
            || scheme.version == 0
            || bytes.is_empty()
            || bytes.capacity() > MAX_SCOPE_IMAGE_BYTES
        {
            return Err((ApplicationError::InvalidCheckpoint, bytes));
        }
        Ok(Self {
            schema,
            scheme,
            scope,
            source_applied,
            bytes,
        })
    }
    pub fn schema(&self) -> u64 {
        self.schema
    }
    pub fn scheme(&self) -> PartitionScheme {
        self.scheme
    }
    pub fn scope(&self) -> BucketRange {
        self.scope
    }
    /// Lineage boundary in the source log, never the target applied index.
    pub fn source_applied(&self) -> u64 {
        self.source_applied
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Optional deterministic in-memory scope adapter over ordinary application
/// checkpoint/replay. Calls perform no I/O and change no authority. An export
/// includes data, original request identity/results and pending outbox items.
/// The caller must export the exact committed/applied fence, verify imported
/// data through the target's persistence path, and keep it non-serving until
/// committed activation. A successful import alone grants no serving rights.
///
/// Import replaces only an empty staging application, at its next *target*
/// index. Images must provide disjoint complete coverage of its configured
/// scope. Failure is atomic. Imported retry history is not constrained by the
/// target's shorter log length. Host providers report and enforce all bounds.
pub trait ScopeStateMachine: CheckpointStateMachine {
    fn scope(&self) -> BucketRange;
    fn scheme(&self) -> PartitionScheme;
    /// Exact application key, for checking a routed envelope against its payload.
    fn command_key<'a>(&self, command: &'a [u8]) -> Result<&'a [u8], ApplicationError>;
    fn export_scope(
        &self,
        scope: BucketRange,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError>;
    fn import_scopes(
        &mut self,
        images: &[ScopeImage],
        target_index: u64,
    ) -> Result<(), ApplicationError>;
}
