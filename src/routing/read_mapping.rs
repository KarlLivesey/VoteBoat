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
//! Pure application read-shape adapters; these never establish quorum authority.
use super::ResponsibilityManifest;
use crate::{
    application::ApplicationError, identity::ResponsibilityIdentity, metadata_transfer::*,
    transfer::*,
};
/// Stateless conversion of a manifest query and its original application result.
/// Implementations perform no I/O, mutate no state and allocate no retained work.
/// Only a current authoritative manifest branch may succeed; historical,
/// inactive or fenced results must fail. Identity/epoch checks remain with the
/// discovery consumer, and the original Node read supplies the quorum barrier.
pub trait ManifestReadMapping {
    type Query;
    type ReadResult;
    fn query(responsibility: ResponsibilityIdentity) -> Self::Query;
    fn manifest(
        result: Self::ReadResult,
    ) -> Result<Option<ResponsibilityManifest>, ApplicationError>;
}
#[derive(Clone, Copy, Debug, Default)]
pub struct LifecycleManifestRead;
impl ManifestReadMapping for LifecycleManifestRead {
    type Query = DirectoryQuery;
    type ReadResult = DirectoryRead;
    fn query(r: ResponsibilityIdentity) -> Self::Query {
        DirectoryQuery::Manifest(r)
    }
    fn manifest(r: Self::ReadResult) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
        directory(r)
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct MetadataSourceManifestRead;
impl ManifestReadMapping for MetadataSourceManifestRead {
    type Query = MetadataSourceQuery;
    type ReadResult = MetadataSourceRead;
    fn query(r: ResponsibilityIdentity) -> Self::Query {
        MetadataSourceQuery::Directory(DirectoryQuery::Manifest(r))
    }
    fn manifest(r: Self::ReadResult) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
        source(r)
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct MetadataPublishingManifestRead;
impl ManifestReadMapping for MetadataPublishingManifestRead {
    type Query = MetadataPublishingQuery;
    type ReadResult = MetadataPublishingRead;
    fn query(r: ResponsibilityIdentity) -> Self::Query {
        MetadataPublishingQuery::Source(MetadataSourceManifestRead::query(r))
    }
    fn manifest(r: Self::ReadResult) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
        match r {
            MetadataPublishingRead::Source(r) => source(r),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct MetadataServingManifestRead;
impl ManifestReadMapping for MetadataServingManifestRead {
    type Query = MetadataServingQuery;
    type ReadResult = MetadataServingRead;
    fn query(r: ResponsibilityIdentity) -> Self::Query {
        MetadataServingQuery::Directory(DirectoryQuery::Manifest(r))
    }
    fn manifest(r: Self::ReadResult) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
        serving(r)
    }
}
fn directory(r: DirectoryRead) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
    match r {
        DirectoryRead::Manifest(m) => Ok(m),
        _ => Err(ApplicationError::InvalidCommand),
    }
}
fn source(r: MetadataSourceRead) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
    match r {
        MetadataSourceRead::Directory(r) => directory(r),
        MetadataSourceRead::Serving(r) => serving(r),
        MetadataSourceRead::Fenced => Err(ApplicationError::NotApplied),
        _ => Err(ApplicationError::InvalidCommand),
    }
}
fn serving(r: MetadataServingRead) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
    match r {
        MetadataServingRead::Directory(r) => directory(r),
        MetadataServingRead::NotActive => Err(ApplicationError::NotApplied),
        _ => Err(ApplicationError::InvalidCommand),
    }
}
