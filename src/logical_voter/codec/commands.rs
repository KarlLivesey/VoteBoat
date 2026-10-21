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
use super::*;
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn encode_body(
        &self,
        e: &mut Encoder,
        b: &CommandBody,
    ) -> Result<(), ApplicationError> {
        match b {
            CommandBody::EnrollmentClaim(import) => {
                if !self.candidate() {
                    return Err(ApplicationError::InvalidCommand);
                }
                e.u8(5)?;
                self.encode_import(e, import)
            }
            CommandBody::Claim { expected_epoch } => {
                e.u8(0)?;
                e.u64(expected_epoch.map_or(0, RepresentativeEpoch::get))?;
                // A claim that can create the parent image binds it: the
                // exact parent bootstrap of the encoding replica.
                if expected_epoch.is_none() {
                    e.blob(self.parent_binding())?;
                }
                Ok(())
            }
            CommandBody::Mutate {
                expected_generation,
                update,
                origin,
            } => {
                e.u8(1)?;
                e.u64(expected_generation.get())?;
                e.persist(*origin)?;
                let bytes = self
                    .log_codec
                    .encode_batch(
                        1,
                        &[LogMutation::Update(update.clone())],
                        self.limits.parent_log,
                    )
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                e.blob(&bytes)
            }
            CommandBody::Publish {
                expected_revision,
                durable,
                snapshot,
                origin,
            } => {
                e.u8(2)?;
                e.u64(expected_revision.get())?;
                e.optional_reference(*durable)?;
                e.snapshot_origin(*origin)?;
                e.blob(&self.snapshot_bytes(snapshot)?)
            }
            CommandBody::Reconcile {
                expected_revision,
                reference,
                origin,
            } => {
                e.u8(3)?;
                e.u64(expected_revision.get())?;
                e.reference(*reference)?;
                e.snapshot_origin(*origin)
            }
            CommandBody::Authorize(c) => {
                e.u8(4)?;
                e.u64(c.expected_revision.get())?;
                e.u64(c.expected_generation.get())?;
                e.u64(c.local_configuration.get())?;
                e.capsule_origin(c.origin)?;
                self.encode_capsule(e, &c.body)
            }
        }
    }
    pub(super) fn decode_body(
        &self,
        d: &mut Decoder<'_>,
        rep: RepresentativeBinding,
    ) -> Result<CommandBody, ApplicationError> {
        Ok(match d.u8()? {
            0 => {
                let expected_epoch = match d.u64()? {
                    0 => None,
                    n => RepresentativeEpoch::new(n),
                };
                // Another parent bootstrap is a deployment fault of this
                // replica: refused at admission, `Err` at apply.
                if expected_epoch.is_none()
                    && d.blob(self.parent_binding().len())? != self.parent_binding()
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                CommandBody::Claim { expected_epoch }
            }
            1 => {
                let expected_generation = d.id(LogGeneration::new)?;
                let origin = d.persist()?;
                let bytes = d.blob(self.limits.parent_log.max_batch_bytes)?;
                let (sequence, mut mutations) = self
                    .log_codec
                    .decode_batch(bytes, self.limits.parent_log)
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                if sequence != 1 || mutations.len() != 1 {
                    return Err(ApplicationError::InvalidCommand);
                }
                let LogMutation::Update(update) = mutations.remove(0) else {
                    return Err(ApplicationError::InvalidCommand);
                };
                CommandBody::Mutate {
                    expected_generation,
                    update,
                    origin,
                }
            }
            2 => {
                let expected_revision = d.id(LogRevision::new)?;
                let durable = d.optional_reference()?;
                let origin = d.snapshot_origin()?;
                let snapshot = self
                    .snapshot_codec
                    .decode(
                        d.blob(self.limits.max_command_bytes)?,
                        self.limits.parent_snapshots,
                    )
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                CommandBody::Publish {
                    expected_revision,
                    durable,
                    snapshot,
                    origin,
                }
            }
            3 => CommandBody::Reconcile {
                expected_revision: d.id(LogRevision::new)?,
                reference: d.reference()?,
                origin: d.snapshot_origin()?,
            },
            4 => CommandBody::Authorize(LogicalCapsule {
                expected_revision: d.id(LogRevision::new)?,
                expected_generation: d.id(LogGeneration::new)?,
                local_configuration: d.id(ConfigurationId::new)?,
                origin: d.capsule_origin()?,
                body: self.decode_capsule(d, rep)?,
            }),
            5 if self.candidate() => {
                let import = self.decode_import(d)?;
                // The import's snapshot carries the source's parent
                // bootstrap; another one is a deployment fault of this
                // candidate, as for a first claim.
                if import.snapshot.metadata.bootstrap != self.parent_bootstrap {
                    return Err(ApplicationError::InvalidCommand);
                }
                CommandBody::EnrollmentClaim(Box::new(import))
            }
            _ => return Err(ApplicationError::InvalidCommand),
        })
    }
    fn encode_capsule(
        &self,
        e: &mut Encoder,
        b: &LogicalCapsuleBody,
    ) -> Result<(), ApplicationError> {
        match b {
            LogicalCapsuleBody::Protocol(m) => {
                e.u8(0)?;
                e.u64(m.to.get())?;
                let bytes = self
                    .wire_codec
                    .encode_batch(
                        crate::wire::WireScope {
                            from: m.from,
                            sender: m.sender,
                            to: m.to,
                        },
                        std::slice::from_ref(m),
                    )
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                if bytes.len() > self.limits.max_capsule_bytes {
                    return Err(ApplicationError::InvalidCommand);
                }
                e.blob(&bytes)
            }
            LogicalCapsuleBody::ClientApplied {
                command,
                schema,
                receipt,
            } => {
                self.capsule_size(command, receipt)?;
                e.u8(1)?;
                e.u64(*schema)?;
                e.blob(command)?;
                e.blob(receipt)
            }
            LogicalCapsuleBody::ReadResult {
                schema,
                query,
                result,
            } => {
                self.capsule_size(query, result)?;
                e.u8(2)?;
                e.u64(*schema)?;
                e.blob(query)?;
                e.blob(result)
            }
        }
    }
    fn capsule_size(&self, a: &[u8], b: &[u8]) -> Result<(), ApplicationError> {
        if a.len()
            .checked_add(b.len())
            .is_none_or(|n| n > self.limits.max_capsule_bytes)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn decode_capsule(
        &self,
        d: &mut Decoder<'_>,
        rep: RepresentativeBinding,
    ) -> Result<LogicalCapsuleBody, ApplicationError> {
        Ok(match d.u8()? {
            0 => {
                let to = d.id(NodeId::new)?;
                let frame = d.blob(self.limits.max_capsule_bytes)?;
                let mut messages = self
                    .wire_codec
                    .decode_batch(
                        crate::wire::WireScope {
                            from: self.mapping.voter,
                            sender: StoreBinding {
                                identity: self.mapping.store,
                                session: rep.logical_session,
                            },
                            to,
                        },
                        frame,
                    )
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                if messages.len() != 1 {
                    return Err(ApplicationError::InvalidCommand);
                }
                LogicalCapsuleBody::Protocol(messages.remove(0))
            }
            1 => LogicalCapsuleBody::ClientApplied {
                schema: d.sequence()?,
                command: d.blob(self.limits.max_capsule_bytes)?.to_vec(),
                receipt: d.blob(self.limits.max_capsule_bytes)?.to_vec(),
            },
            2 => LogicalCapsuleBody::ReadResult {
                schema: d.sequence()?,
                query: d.blob(self.limits.max_capsule_bytes)?.to_vec(),
                result: d.blob(self.limits.max_capsule_bytes)?.to_vec(),
            },
            _ => return Err(ApplicationError::InvalidCommand),
        })
    }
}
