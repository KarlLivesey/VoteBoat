// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    app::{App, Source},
    profile::{self, Binding, Profile},
    setup::{checked, Failure},
    wire,
};
use voteboat::{identity::*, retirement::*, runtime::ReadOutcome, transfer::*, transfer_source::*};

pub type RetirableSource = RetirementGuard<Source>;
impl App for RetirableSource {
    fn command(
        &self,
        p: &Profile,
        b: Binding,
        w: &[&str],
    ) -> Result<(OperationId, Vec<u8>), Failure> {
        if let ["retire-group", hex] = w {
            let proof = checked(RetirementProof::decode(&wire::unhex(
                hex,
                MAX_RETIREMENT_PROOF_BYTES,
            )?))?;
            if proof.release.source != b.group
                || proof.release.operation != p.operation.operation()
                || proof.decision.publication_operation != p.operation.publication_operation()
                || proof.decision.publication.intent() != p.operation.intent()
            {
                return Err("wrong retirement profile".into());
            }
            return Ok((
                proof.release.operation,
                checked(self.retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES))?,
            ));
        }
        self.owner().ok_or("source retired")?.command(p, b, w)
    }
    fn query(&self, p: &Profile, b: Binding, w: &[&str]) -> Result<Self::Query, Failure> {
        match w {
            ["retirement-status"] => Ok(RetirementQuery::Status),
            ["transfer-read", "source"] => Ok(RetirementQuery::Freeze),
            _ => Ok(RetirementQuery::Owner(
                self.owner().ok_or("source retired")?.query(p, b, w)?,
            )),
        }
    }
    fn answer(
        &self,
        p: &Profile,
        b: Binding,
        w: &[String],
        v: &ReadOutcome<Self::ReadResult>,
    ) -> Result<String, Failure> {
        let ReadOutcome::Read {
            barrier,
            result: Ok(value),
        } = v
        else {
            return Err("retirement read unavailable".into());
        };
        match value {
            RetirementRead::Status(status) if w[0] == "retirement-status" => Ok(match status {
                Some(s) => format!("OK retirement=retired source={} incarnation={} operation={} fence={} release={} index={} read_index={} evidence=quorum",s.source.id.get(),s.source.incarnation.get(),s.operation.get(),s.fence_index,s.retention_release.get(),s.index,barrier.index()),
                None => format!("OK retirement=live source={} incarnation={} operation={} read_index={} evidence=quorum",b.group.id.get(),b.group.incarnation.get(),p.operation.operation().get(),barrier.index()),
            }),
            RetirementRead::Freeze(status) if w[0] == "transfer-read" => {
                let inner = ReadOutcome::Read { barrier: *barrier, result: Ok(SourceRead::<i64>::Freeze(status.clone())) };
                Ok(format!("OK observation {}", wire::hex(&checked(checked(TransferObservation::source_read(&inner))?.encode(MAX_TRANSFER_OBSERVATION_BYTES))?)))
            }
            RetirementRead::Owner(value) => self.owner().ok_or("source retired")?.answer(p,b,w,&ReadOutcome::Read { barrier: *barrier, result: Ok(value.clone()) }),
            _ => Err("source retired or wrong retirement query".into()),
        }
    }
}

pub fn source(p: &Profile, b: Binding) -> Result<RetirableSource, Failure> {
    RetirementGuard::new(super::app::source(p, b)?)
        .map_err(|e| format!("retirement source: {:?}", e.0).into())
}

pub fn release(
    p: &Profile,
    group: &str,
    id: &str,
) -> Result<(GroupIdentity, OperationId), Failure> {
    let binding = p.binding(group)?;
    if !p.retirement || binding.role != profile::Role::Source {
        return Err("retirement requires the original v2 source".into());
    }
    Ok((binding.group, profile::op(id)?))
}
