// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    profile::{self, Binding, Profile},
    setup::{checked, Failure},
    wire,
};
use voteboat::{
    application::*, bucket_counter::*, directory::*, identity::*,
    native::routing::NativeBytePartition, routed::*, routing::*, runtime::ReadOutcome, transfer::*,
    transfer_publication::*, transfer_source::*, transfer_target::*,
};
pub type Source = TransferSource<BucketCounter<NativeBytePartition>, NativeBytePartition>;
pub type Target = TransferTarget<BucketCounter<NativeBytePartition>, NativeBytePartition>;
pub trait App:
    ProposalAdmission<Receipt: ApplicationReceipt + std::fmt::Debug>
    + BoundedReadableStateMachine
    + CheckpointStateMachine
{
    fn command(
        &self,
        p: &Profile,
        b: Binding,
        words: &[&str],
    ) -> Result<(OperationId, Vec<u8>), Failure>;
    fn query(&self, p: &Profile, b: Binding, words: &[&str]) -> Result<Self::Query, Failure>;
    fn answer(
        &self,
        p: &Profile,
        b: Binding,
        words: &[String],
        value: &ReadOutcome<Self::ReadResult>,
    ) -> Result<String, Failure>;
}
fn limits() -> BucketCounterLimits {
    BucketCounterLimits {
        operations: 32,
        semantic_bytes: 8192,
    }
}
pub fn metadata(p: &Profile) -> Result<LifecycleDirectory, Failure> {
    let plan = checked(DirectoryPlan::new(
        p.operation.intent().before().input().authority,
        vec![p.operation.intent().before().clone()],
    ))?;
    Ok(LifecycleDirectory::new(checked(Directory::new(
        plan,
        DirectoryLimits {
            operations: 16,
            history_bytes: 128 * 1024,
        },
    ))?))
}
pub fn source(p: &Profile, b: Binding) -> Result<Source, Failure> {
    let inner = checked(BucketCounter::new(
        source_scope(p, b.group)?,
        NativeBytePartition,
        limits(),
    ))?;
    let routed = RoutedApplication::new(
        b.group,
        p.operation.intent().before().clone(),
        inner,
        NativeBytePartition,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 16384,
            payload_bytes: 1024,
            inner_checkpoint_bytes: checked(limits().checkpoint_bound())?,
        },
    )
    .map_err(|e| format!("routed source: {:?}", e.error))?;
    TransferSource::new(routed, wire::APP_BYTES)
        .map_err(|e| format!("source construction: {:?}", e.0).into())
}
pub fn target(p: &Profile, b: Binding) -> Result<Target, Failure> {
    let inner = checked(BucketCounter::new(
        target_scope(p, b.group)?,
        NativeBytePartition,
        limits(),
    ))?;
    TransferTarget::new(
        b.group,
        p.operation.operation(),
        p.operation.intent().clone(),
        inner,
        NativeBytePartition,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: checked(limits().checkpoint_bound())?,
        },
    )
    .map_err(|e| format!("target construction: {:?}", e.0).into())
}
fn source_scope(p: &Profile, group: GroupIdentity) -> Result<BucketRange, Failure> {
    p.operation
        .intent()
        .sources()
        .into_iter()
        .find(|r| r.target == RouteTarget::Group(group))
        .map(|r| r.scope)
        .ok_or_else(|| "missing source scope".into())
}
fn target_scope(p: &Profile, group: GroupIdentity) -> Result<BucketRange, Failure> {
    p.operation
        .intent()
        .targets()
        .into_iter()
        .find(|r| r.target == RouteTarget::Group(group))
        .map(|r| r.scope)
        .ok_or_else(|| "missing target scope".into())
}
fn observation(value: TransferObservation) -> Result<String, Failure> {
    Ok(format!(
        "OK observation {}",
        wire::hex(&checked(value.encode(MAX_TRANSFER_OBSERVATION_BYTES))?)
    ))
}
fn hint(p: &Profile, b: Binding, key: u8) -> Result<RouteHint, Failure> {
    let manifest = if b.role == profile::Role::Source {
        p.operation.intent().before().clone()
    } else {
        p.operation
            .intent()
            .target_manifest(b.group)
            .ok_or("missing target manifest")?
            .clone()
    };
    let m = manifest.input();
    Ok(RouteHint {
        responsibility: m.responsibility,
        group: b.group,
        application: m.application,
        scheme: m.scheme,
        scope: if b.role == profile::Role::Source {
            source_scope(p, b.group)?
        } else {
            target_scope(p, b.group)?
        },
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    })
}
fn data(p: &Profile, b: Binding, words: &[&str]) -> Result<(OperationId, Vec<u8>), Failure> {
    let ["add", operation, key, delta] = words else {
        return Err("expected add OPERATION KEY DELTA".into());
    };
    let key = key.parse::<u8>()?;
    let inner = checked(encode_add(&[key], delta.parse()?, b"effect", 1024))?;
    Ok((
        profile::op(operation)?,
        checked(encode_routed(hint(p, b, key)?, &[key], &inner, 4096))?,
    ))
}
fn query_data(p: &Profile, b: Binding, words: &[&str]) -> Result<RoutedQuery<Vec<u8>>, Failure> {
    let ["read", key] = words else {
        return Err("expected read KEY".into());
    };
    let key = key.parse::<u8>()?;
    Ok(RoutedQuery {
        hint: hint(p, b, key)?,
        key: vec![key],
        query: vec![key],
    })
}
impl App for LifecycleDirectory {
    fn command(
        &self,
        p: &Profile,
        b: Binding,
        w: &[&str],
    ) -> Result<(OperationId, Vec<u8>), Failure> {
        match w {
            ["initialize"] => Ok((
                b.bootstrap,
                checked(self.directory().bootstrap_command(wire::APP_BYTES))?,
            )),
            ["grant"] => Ok((
                b.grant.ok_or("missing grant operation")?,
                checked(
                    DirectoryCommand {
                        expected: None,
                        manifest: p.operation.intent().before().clone(),
                    }
                    .encode(wire::APP_BYTES),
                )?,
            )),
            ["transfer-step", "intent"] => Ok((
                p.operation.operation(),
                checked(p.operation.intent().encode(wire::APP_BYTES))?,
            )),
            ["transfer-step", "publish", hex] => {
                let publication = checked(TransferPublication::decode(&wire::unhex(
                    hex,
                    wire::APP_BYTES,
                )?))?;
                if publication.operation() != p.operation.operation()
                    || publication.intent() != p.operation.intent()
                {
                    return Err("wrong publication".into());
                }
                Ok((
                    p.operation.publication_operation(),
                    checked(publication.encode(wire::APP_BYTES))?,
                ))
            }
            _ => Err("unsupported metadata command".into()),
        }
    }
    fn query(&self, p: &Profile, _: Binding, w: &[&str]) -> Result<DirectoryQuery, Failure> {
        match w {
            ["transfer-read", "intent"] => Ok(DirectoryQuery::Transfer(p.operation.operation())),
            ["transfer-read", "publication"] => {
                Ok(DirectoryQuery::Publication(p.operation.operation()))
            }
            _ => Err("unsupported metadata query".into()),
        }
    }
    fn answer(
        &self,
        _: &Profile,
        _: Binding,
        w: &[String],
        v: &ReadOutcome<DirectoryRead>,
    ) -> Result<String, Failure> {
        match w.get(1).map(String::as_str) {
            Some("intent") => observation(checked(TransferObservation::intent_read(v))?),
            Some("publication") => observation(checked(TransferObservation::publication_read(v))?),
            _ => Err("unsupported observation".into()),
        }
    }
}
impl App for Source {
    fn command(
        &self,
        p: &Profile,
        b: Binding,
        w: &[&str],
    ) -> Result<(OperationId, Vec<u8>), Failure> {
        match w {
            ["initialize"] => Ok((
                b.bootstrap,
                checked(self.bootstrap_command(wire::APP_BYTES))?,
            )),
            ["transfer-step", "fence"] => Ok((
                p.operation.operation(),
                checked(Source::freeze_command(
                    p.operation.intent(),
                    wire::APP_BYTES,
                ))?,
            )),
            _ => data(p, b, w),
        }
    }
    fn query(&self, p: &Profile, b: Binding, w: &[&str]) -> Result<SourceQuery<Vec<u8>>, Failure> {
        match w {
            ["transfer-read", "source"] | ["transfer-export", _] => Ok(SourceQuery::Freeze),
            _ => Ok(SourceQuery::Data(query_data(p, b, w)?)),
        }
    }
    fn answer(
        &self,
        p: &Profile,
        _: Binding,
        w: &[String],
        v: &ReadOutcome<SourceRead<i64>>,
    ) -> Result<String, Failure> {
        if w[0] == "transfer-read" {
            return observation(checked(TransferObservation::source_read(v))?);
        }
        if w[0] == "transfer-export" {
            let ReadOutcome::Read {
                result: Ok(SourceRead::Freeze(Some(status))),
                ..
            } = v
            else {
                return Err("source not fenced".into());
            };
            if status.intent != *p.operation.intent()
                || status.fence.operation != p.operation.operation()
            {
                return Err("wrong frozen intent".into());
            }
            let target = p.binding(&w[1])?;
            return Ok(format!(
                "OK image {}",
                wire::image(&checked(
                    self.export_target(target.group, wire::APP_BYTES - 40)
                )?)
            ));
        }
        match v {
            ReadOutcome::Read {
                result: Ok(SourceRead::Data(RoutedRead::Served(value))),
                ..
            } => Ok(format!("OK value={value}")),
            ReadOutcome::Read {
                result: Ok(other), ..
            } => Err(format!("source read refused: {other:?}").into()),
            _ => Err("source read unavailable".into()),
        }
    }
}
impl App for Target {
    fn command(
        &self,
        p: &Profile,
        b: Binding,
        w: &[&str],
    ) -> Result<(OperationId, Vec<u8>), Failure> {
        let bytes = match w {
            ["transfer-step", "stage"] => checked(self.bootstrap_command(wire::APP_BYTES))?,
            ["transfer-step", "import", hex] => {
                let import = checked(TargetImport::decode(&wire::unhex(hex, wire::APP_BYTES)?))?;
                if import.operation() != p.operation.operation()
                    || import.intent() != p.operation.intent()
                    || import.target() != b.group
                {
                    return Err("wrong import".into());
                }
                checked(self.import_command(&import, wire::APP_BYTES))?
            }
            ["transfer-step", "activate", configuration, hex] => {
                let decision = checked(TransferPublicationStatus::decode(&wire::unhex(
                    hex,
                    wire::APP_BYTES,
                )?))?;
                if decision.publication_operation != p.operation.publication_operation()
                    || decision.publication.intent() != p.operation.intent()
                {
                    return Err("wrong activation".into());
                }
                checked(
                    self.activation_command(
                        &TargetActivation {
                            metadata_configuration: ConfigurationId::new(configuration.parse()?)
                                .ok_or("invalid configuration")?,
                            decision,
                        },
                        wire::APP_BYTES,
                    ),
                )?
            }
            _ => return data(p, b, w),
        };
        Ok((p.operation.operation(), bytes))
    }
    fn query(&self, p: &Profile, b: Binding, w: &[&str]) -> Result<TargetQuery<Vec<u8>>, Failure> {
        match w {
            ["transfer-read", "target"] => Ok(TargetQuery::Status),
            _ => Ok(TargetQuery::Data(query_data(p, b, w)?)),
        }
    }
    fn answer(
        &self,
        _: &Profile,
        _: Binding,
        w: &[String],
        v: &ReadOutcome<TargetRead<i64>>,
    ) -> Result<String, Failure> {
        if w[0] == "transfer-read" {
            observation(checked(TransferObservation::target_read(v))?)
        } else {
            match v {
                ReadOutcome::Read {
                    result: Ok(TargetRead::Data(value)),
                    ..
                } => Ok(format!("OK value={value}")),
                ReadOutcome::Read {
                    result: Ok(other), ..
                } => Err(format!("target read refused: {other:?}").into()),
                _ => Err("target read unavailable".into()),
            }
        }
    }
}
