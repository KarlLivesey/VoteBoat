// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::identity::*;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Progress {
    pub groups: usize,
    pub ready: bool,
    digest: String,
}
impl Progress {
    pub fn parse(text: &str, sequence: u64, operation: u128) -> Result<Self, Failure> {
        identity(text, sequence, operation)?;
        if field(text, "phase")? != "Active"
            || field(text, "multi")? != "true"
            || field(text, "membership_change")? != "true"
        {
            return Err("runner requires an active original multi-group drain".into());
        }
        let groups = field(text, "groups")?.parse::<usize>()?;
        let digest = field(text, "plan_digest")?;
        if !(1..=256).contains(&groups)
            || digest.len() != 64
            || !digest.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("invalid group drain count or fingerprint".into());
        }
        Ok(Self {
            groups,
            ready: field(text, "ready")?.parse()?,
            digest: digest.into(),
        })
    }
    pub fn verify(&self, current: &Self) -> Result<(), Failure> {
        if self.groups != current.groups || self.digest != current.digest {
            return Err("source group drain plan changed".into());
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Voter {
    pub target: u64,
    pub store: StoreIdentity,
    pub operation: OperationId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Assignment {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub voter: Option<Voter>,
}
impl Assignment {
    pub fn command(&self, suffix: &str) -> String {
        format!(
            "group {} {} {suffix}",
            self.group.id.get(),
            self.group.incarnation.get()
        )
    }
}
#[derive(Debug, Eq, PartialEq)]
pub(super) struct Row {
    pub assignment: Assignment,
    pub done: bool,
}
impl Row {
    pub fn parse(
        text: &str,
        sequence: u64,
        operation: u128,
        offset: usize,
        count: usize,
    ) -> Result<Self, Failure> {
        identity(text, sequence, operation)?;
        if field(text, "offset")?.parse::<usize>()? != offset
            || field(text, "groups")?.parse::<usize>()? != count
        {
            return Err("source group row offset or count differs".into());
        }
        let group = GroupIdentity {
            id: GroupId::new(field(text, "group")?.parse()?).ok_or("invalid row group")?,
            incarnation: GroupIncarnation::new(field(text, "incarnation")?.parse()?)
                .ok_or("invalid row incarnation")?,
        };
        let configuration = ConfigurationId::new(field(text, "configuration")?.parse()?)
            .ok_or("invalid row configuration")?;
        let voter = match field(text, "kind")? {
            "retained" => None,
            "voter" => Some(Voter {
                target: NodeId::new(field(text, "target")?.parse()?)
                    .ok_or("invalid row target")?
                    .get(),
                store: StoreIdentity {
                    id: StoreId::new(field(text, "store")?.parse()?).ok_or("invalid row store")?,
                    incarnation: StoreIncarnation::new(field(text, "store_incarnation")?.parse()?)
                        .ok_or("invalid row store incarnation")?,
                },
                operation: OperationId::new(field(text, "configuration_operation")?.parse()?)
                    .ok_or("invalid row operation")?,
            }),
            _ => return Err("invalid group drain role".into()),
        };
        Ok(Self {
            assignment: Assignment {
                group,
                configuration,
                voter,
            },
            done: field(text, "done")?.parse()?,
        })
    }
}

#[cfg(test)]
#[path = "multi_rows_tests.rs"]
mod tests;
